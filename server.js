// =============================================================================
// RADIANCE API SERVER v2.2.0 — FILTER CONTRACT COMPLIANT
// =============================================================================
// Sprints: 1-Capture, 2-Switching, 3-Leakage, 4-Basket, 5-Loyalty, 6-Panel, 7-Deck
// DOD-67 Compliant: ≥8 charts, disclaimers, UNKNOWN handling, projections labeled
// Filter Contract v1: Unified parsing, dual paths (product/commerce), k-anon
// =============================================================================

const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// =============================================================================
// DATABASE CONNECTION
// =============================================================================
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000
});

pool.on('error', (err) => console.error('Pool error:', err));

// =============================================================================
// CONSTANTS
// =============================================================================
const VERSION = '2.2.0';
const SPRINTS = ['1-capture', '2-switching', '3-leakage', '4-basket', '5-loyalty', '6-panel', '7-deck'];

// Category column mappings by domain
const CAT_COLS = {
  product: { l1: 'category_l1', l2: 'category_l2', l3: 'category_l3', l4: 'category_l4' },
  commerce: { l1: 'commerce_l1', l2: 'commerce_l2', l3: 'commerce_l3', l4: 'commerce_l4' }
};
const VALID_CAT_COLS = CAT_COLS.product; // Legacy alias
const VALID_LEVELS = ['l1', 'l2', 'l3', 'l4'];
const VALID_DOMAINS = ['product', 'commerce'];
const VALID_PEER_SCOPES = ['all', 'peers', 'extended'];

// Suppression constants
const SUPPRESSED = {
  UNKNOWN: 'UNKNOWN',
  OTHER: 'OTHER_SUPPRESSED'
};

// Default filter values
const FILTER_DEFAULTS = {
  k_threshold: 5,
  peer_scope: 'all',
  category_level: 'l1',
  category_domain: 'product',
  reconcile_ok: null
};

const CAPTURE_DISCLAIMERS = [
  'Panel observado; no representa universo total sin calibración oficial.',
  'SoW = spend_in_x / spend_market para usuarios del panel.',
  'leakage_usd = spend_market - spend_in_x.',
  'Ventana end-exclusive. reconcile_ok aplicado a nivel transacción.'
];

const SWITCHING_DISCLAIMERS = [
  'Destinos calculados sobre cohorte que compró en X durante la ventana.',
  'Excluye compras en el mismo issuer_ruc de origen.',
  'K-anonymity aplicado; destinos con N < k → OTHER_SUPPRESSED.'
];

const LEAKAGE_DISCLAIMERS = [
  'Waterfall 6-bucket: RETAINED, CATEGORY_GONE, REDUCED_BASKET, REDUCED_FREQ, DELAYED_ONLY, FULL_CHURN.',
  'Transiciones mes-a-mes dentro de la ventana.',
  'Sin inferencia causal. Solo observación de comportamiento.'
];

const BASKET_DISCLAIMERS = [
  'Breadth = número de categorías distintas compradas por usuario/mes.',
  'Market = todas las compras del usuario; In-X = solo en issuer_ruc.',
  'Panel observado; no representa universo total.'
];

const LOYALTY_DISCLAIMERS = [
  'user_brand_share_pct = 100 * brand_spend / category_spend.',
  'Tiers: EXCLUSIVE ≥95%, LOYAL ≥80%, PREFER ≥50%, LIGHT <50%.',
  'penetration_pct = 100 * brand_buyers / category_buyers.',
  'UNKNOWN incluido pero marcado SUPPRESSED.'
];

const PANEL_DISCLAIMERS = [
  'Panel observado; no representa universo sin calibración oficial.',
  'Proyección REFERENCIAL basada en factor de expansión configurable.',
  'Sin inferencia causal. Solo observaciones del panel LÜM.',
  'Expansion factor es estimado; no sustituye proyección demográfica certificada.'
];

const DECK_DISCLAIMERS = [
  'Panel observado LÜM. No representa universo sin calibración.',
  'Proyección REFERENCIAL / ILUSTRATIVA. Sin inferencia causal.',
  'Ventana end-exclusive. reconcile_ok aplicado a nivel txn.',
  'K-anonymity: items con N < k → OTHER_SUPPRESSED.',
  'UNKNOWN siempre incluido, marcado SUPPRESSED.',
  'Auto-selección determinística: top category por spend, top brand por buyers.'
];

// =============================================================================
// HELPERS - BASIC PARSERS
// =============================================================================
const parseDate = (val) => {
  if (!val) return null;
  const d = new Date(val);
  return isNaN(d.getTime()) ? null : d.toISOString().split('T')[0];
};

const parseString = (val) => (typeof val === 'string' && val.trim()) ? val.trim() : null;

const parseBool = (val, defaultVal = null) => {
  if (val === undefined || val === null || val === '') return defaultVal;
  if (val === 'true' || val === '1' || val === true) return true;
  if (val === 'false' || val === '0' || val === false) return false;
  return defaultVal;
};

const parseKThreshold = (val) => {
  const k = parseInt(val);
  return isNaN(k) || k < 1 ? FILTER_DEFAULTS.k_threshold : k;
};

const asyncHandler = (fn) => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);

// =============================================================================
// HELPERS - FILTER CONTRACT v1
// =============================================================================

/**
 * Normalize dimension values: NULL, empty, whitespace → UNKNOWN
 */
function normDim(val) {
  if (val === null || val === undefined) return SUPPRESSED.UNKNOWN;
  const trimmed = String(val).trim();
  return trimmed === '' || trimmed.toLowerCase() === 'null' ? SUPPRESSED.UNKNOWN : trimmed;
}

/**
 * Resolve category column based on domain and level
 */
function resolveCatCol(domain, level) {
  const d = CAT_COLS[domain] || CAT_COLS.product;
  return d[level] || d.l1;
}

/**
 * Central filter parser - single source of truth
 */
function parseFilters(req, options = {}) {
  const {
    requireIssuer = true,
    requireDates = true,
    requireCategoryValue = false,
    supportsPeerScope = false,
    supportsCategoryPath = true
  } = options;

  const invalid = [];
  const ignored = [];

  // Core filters
  const startDate = parseDate(req.query.start);
  const endDate = parseDate(req.query.end);
  const issuerRuc = parseString(req.query.issuer_ruc);
  const storeId = parseString(req.query.store_id);
  const reconcileOk = parseBool(req.query.reconcile_ok, FILTER_DEFAULTS.reconcile_ok);
  const kThreshold = parseKThreshold(req.query.k_threshold);

  // Category configuration
  const categoryLevel = VALID_LEVELS.includes(req.query.category_level?.toLowerCase()) 
    ? req.query.category_level.toLowerCase() 
    : FILTER_DEFAULTS.category_level;
  const categoryDomain = VALID_DOMAINS.includes(req.query.category_domain?.toLowerCase())
    ? req.query.category_domain.toLowerCase()
    : FILTER_DEFAULTS.category_domain;
  const categoryValue = parseString(req.query.category_value);

  // Peer scope
  let peerScope = FILTER_DEFAULTS.peer_scope;
  if (supportsPeerScope) {
    peerScope = VALID_PEER_SCOPES.includes(req.query.peer_scope?.toLowerCase())
      ? req.query.peer_scope.toLowerCase()
      : FILTER_DEFAULTS.peer_scope;
  } else if (req.query.peer_scope) {
    ignored.push('peer_scope');
  }

  // Product category path (hierarchical filter)
  let productPath = { l1: null, l2: null, l3: null, l4: null };
  if (supportsCategoryPath) {
    productPath = {
      l1: parseString(req.query.category_l1),
      l2: parseString(req.query.category_l2),
      l3: parseString(req.query.category_l3),
      l4: parseString(req.query.category_l4)
    };
  }

  // Validation
  if (requireDates) {
    if (!startDate) invalid.push({ field: 'start', reason: 'Invalid or missing start date' });
    if (!endDate) invalid.push({ field: 'end', reason: 'Invalid or missing end date' });
  }
  if (requireIssuer && !issuerRuc) {
    invalid.push({ field: 'issuer_ruc', reason: 'issuer_ruc is required' });
  }
  if (requireCategoryValue && !categoryValue) {
    invalid.push({ field: 'category_value', reason: 'category_value is required' });
  }

  // Resolve column based on domain
  const catCol = resolveCatCol(categoryDomain, categoryLevel);

  // Determine drill-down level
  let groupByLevel = categoryLevel;
  if (productPath.l1 && !productPath.l2) groupByLevel = 'l2';
  else if (productPath.l2 && !productPath.l3) groupByLevel = 'l3';
  else if (productPath.l3 && !productPath.l4) groupByLevel = 'l4';
  else if (productPath.l4) groupByLevel = 'l4';
  
  const groupByCol = resolveCatCol(categoryDomain, groupByLevel);

  return {
    applied: {
      start: startDate,
      end: endDate,
      issuer_ruc: issuerRuc,
      store_id: storeId,
      reconcile_ok: reconcileOk,
      k_threshold: kThreshold,
      category_level: categoryLevel,
      category_domain: categoryDomain,
      category_value: categoryValue,
      peer_scope: peerScope,
      product_path: productPath,
	  category_path: productPath  
    },
    derived: {
      cat_col: catCol,
      group_by_level: groupByLevel,
      group_by_col: groupByCol
    },
    defaults: FILTER_DEFAULTS,
    ignored,
    invalid,
    isValid: invalid.length === 0
  };
}

/**
 * Build SQL filter for category path
 */
 function buildCategoryPathSQL(startParamIndex, domain = 'product', prefix = 'b.') {
  const cols = CAT_COLS[domain] || CAT_COLS.product;
  return `
    AND ($${startParamIndex}::text IS NULL OR ${prefix}${cols.l1} = $${startParamIndex})
    AND ($${startParamIndex + 1}::text IS NULL OR ${prefix}${cols.l2} = $${startParamIndex + 1})
    AND ($${startParamIndex + 2}::text IS NULL OR ${prefix}${cols.l3} = $${startParamIndex + 2})
    AND ($${startParamIndex + 3}::text IS NULL OR ${prefix}${cols.l4} = $${startParamIndex + 3})
  `;
}


/**
 * Build filter response object
 */
function buildFilterResponse(filters, module) {
  return {
    meta: { version: VERSION, module },
    filters: {
      applied: {
        start: filters.applied.start,
        end: filters.applied.end,
        issuer_ruc: filters.applied.issuer_ruc,
        reconcile_ok: filters.applied.reconcile_ok,
        k_threshold: filters.applied.k_threshold,
        category_level: filters.applied.category_level,
        peer_scope: filters.applied.peer_scope,
        category_path: filters.applied.product_path,
        category_value: filters.applied.category_value
      },
      grouping: {
        level: filters.derived.group_by_level,
        column: filters.derived.group_by_col
      },
      ignored: filters.ignored,
      defaults: filters.defaults
    }
  };
}

// =============================================================================
// EXPANSION FACTOR CONFIG
// =============================================================================
function getExpansionConfig() {
  const defaultFactor = parseFloat(process.env.PANEL_EXPANSION_FACTOR_DEFAULT) || 100;
  let overrides = {};
  try {
    const json = process.env.PANEL_EXPANSION_OVERRIDES;
    if (json) overrides = JSON.parse(json);
  } catch (e) { console.warn('PANEL_EXPANSION_OVERRIDES parse error:', e.message); }
  return { defaultFactor, overrides };
}

function getExpansionFactor(issuerRuc, categoryL1) {
  const config = getExpansionConfig();
  if (issuerRuc && config.overrides[`issuer_ruc:${issuerRuc}`]) {
    return { factor: config.overrides[`issuer_ruc:${issuerRuc}`], source: 'override:issuer_ruc' };
  }
  if (categoryL1 && config.overrides[`category_l1:${categoryL1}`]) {
    return { factor: config.overrides[`category_l1:${categoryL1}`], source: 'override:category_l1' };
  }
  return { factor: config.defaultFactor, source: 'default' };
}

// =============================================================================
// ROUTES: LANDING & HEALTH
// =============================================================================
app.get('/', (req, res) => {
  res.json({ 
    name: 'LÜM Radiance API', 
    version: VERSION, 
    status: 'online',
    filter_contract: 'v1',
    endpoints: [
      '/api/health',
      '/api/retailers',
      '/api/categories',
      '/api/categories/children',
      '/api/kpis/summary',
      '/api/sow_leakage/by_category',
      '/api/switching/destinations',
      '/api/leakage/tree',
      '/api/basket/breadth',
      '/api/loyalty/brands',
      '/api/panel/summary'
    ]
  });
});

app.get('/api/health', asyncHandler(async (req, res) => {
  const start = Date.now();
  const result = await pool.query('SELECT NOW() as db_time, current_database() as db_name');
  res.json({
    status: 'ok',
    version: VERSION,
    filter_contract: 'v1',
    database: { connected: true, name: result.rows[0].db_name, time: result.rows[0].db_time },
    latency_ms: Date.now() - start
  });
}));

// =============================================================================
// RETAILERS LIST
// =============================================================================
app.get('/api/retailers', asyncHandler(async (req, res) => {
  const query = `
    SELECT DISTINCT 
      b.issuer_ruc,
      COALESCE(b.issuer_name, b.issuer_ruc) AS issuer_name,
      COUNT(DISTINCT b.user_id) AS users,
      COUNT(DISTINCT b.cufe) AS invoices
    FROM analytics.radiance_base_v1 b
    WHERE b.issuer_ruc IS NOT NULL
    GROUP BY b.issuer_ruc, b.issuer_name
    HAVING COUNT(DISTINCT b.user_id) >= 1
    ORDER BY invoices DESC
    LIMIT 50
  `;
  const { rows } = await pool.query(query);
  res.json({
    meta: { version: VERSION, module: 'retailers' },
    data: rows.map(r => ({
      issuer_ruc: r.issuer_ruc,
      issuer_name: r.issuer_name,
      users: Number(r.users),
      invoices: Number(r.invoices)
    }))
  });
}));

// =============================================================================
// CATEGORIES LIST
// =============================================================================
app.get('/api/categories', asyncHandler(async (req, res) => {
  const categoryLevel = (req.query.category_level || 'l1').toLowerCase();
  const type = req.query.type || 'all';
  
  const productCol = VALID_CAT_COLS[categoryLevel] || 'category_l1';
  const commerceCol = `commerce_${categoryLevel}`;
  
  let query;
  
  if (type === 'product') {
    query = `
      SELECT DISTINCT COALESCE(b.${productCol}, 'UNKNOWN') AS category_value,
        'product' AS category_type,
        COUNT(DISTINCT b.user_id) AS users,
        SUM(COALESCE(b.line_total, 0)) AS spend
      FROM analytics.radiance_base_v1 b
      WHERE b.${productCol} IS NOT NULL AND b.${productCol} != 'UNKNOWN'
      GROUP BY 1 HAVING COUNT(DISTINCT b.user_id) >= 1
      ORDER BY spend DESC LIMIT 100
    `;
  } else if (type === 'commerce') {
    query = `
      SELECT DISTINCT COALESCE(b.${commerceCol}, 'UNKNOWN') AS category_value,
        'commerce' AS category_type,
        COUNT(DISTINCT b.user_id) AS users,
        SUM(COALESCE(b.line_total, 0)) AS spend
      FROM analytics.radiance_base_v1 b
      WHERE b.${commerceCol} IS NOT NULL AND b.${commerceCol} != 'UNKNOWN'
      GROUP BY 1 HAVING COUNT(DISTINCT b.user_id) >= 1
      ORDER BY spend DESC LIMIT 100
    `;
  } else {
    query = `
      (SELECT DISTINCT COALESCE(b.${productCol}, 'UNKNOWN') AS category_value,
        'product' AS category_type, COUNT(DISTINCT b.user_id) AS users,
        SUM(COALESCE(b.line_total, 0)) AS spend
      FROM analytics.radiance_base_v1 b
      WHERE b.${productCol} IS NOT NULL AND b.${productCol} != 'UNKNOWN'
      GROUP BY 1 HAVING COUNT(DISTINCT b.user_id) >= 1
      ORDER BY spend DESC LIMIT 50)
      UNION ALL
      (SELECT DISTINCT COALESCE(b.${commerceCol}, 'UNKNOWN') AS category_value,
        'commerce' AS category_type, COUNT(DISTINCT b.user_id) AS users,
        SUM(COALESCE(b.line_total, 0)) AS spend
      FROM analytics.radiance_base_v1 b
      WHERE b.${commerceCol} IS NOT NULL AND b.${commerceCol} != 'UNKNOWN'
      GROUP BY 1 HAVING COUNT(DISTINCT b.user_id) >= 1
      ORDER BY spend DESC LIMIT 50)
    `;
  }
  
  const { rows } = await pool.query(query);
  res.json({
    meta: { version: VERSION, module: 'categories' },
    filters: { category_level: categoryLevel, type },
    data: rows.map(r => ({
      category_value: r.category_value,
      category_type: r.category_type,
      users: Number(r.users),
      spend: Number(r.spend)
    }))
  });
}));

// =============================================================================
// CATEGORIES CHILDREN
// =============================================================================
app.get('/api/categories/children', asyncHandler(async (req, res) => {
  const filters = parseFilters(req, { requireIssuer: false, requireDates: true, supportsCategoryPath: true });
  
  if (!filters.isValid) {
    return res.status(400).json({ error: filters.invalid.map(i => i.reason).join('; ') });
  }

  const level = (req.query.level || 'l2').toLowerCase();
  const domain = (req.query.domain || 'product').toLowerCase();
  
  if (!VALID_LEVELS.includes(level)) {
    return res.status(400).json({ error: 'Invalid level' });
  }

  const targetCol = resolveCatCol(domain, level);
  const { start, end, reconcile_ok, issuer_ruc, product_path } = filters.applied;
  
  let parentFilters = '';
  const params = [start, end, reconcile_ok];
  let paramIndex = 4;

const pathCols = CAT_COLS[domain] || CAT_COLS.product;
if (product_path.l1) {
  parentFilters += ` AND b.${pathCols.l1} = $${paramIndex}`;
    params.push(product_path.l1);
    paramIndex++;
  }
if (product_path.l2) {
  parentFilters += ` AND b.${pathCols.l2} = $${paramIndex}`;
    params.push(product_path.l2);
    paramIndex++;
  }
if (product_path.l3) {
  parentFilters += ` AND b.${pathCols.l3} = $${paramIndex}`;
    params.push(product_path.l3);
    paramIndex++;
  }

  let issuerFilter = '';
  if (issuer_ruc) {
    issuerFilter = ` AND b.issuer_ruc = $${paramIndex}`;
    params.push(issuer_ruc);
  }

  const query = `
    SELECT 
      COALESCE(b.${targetCol}, 'UNKNOWN') AS category_value,
      COUNT(DISTINCT b.user_id) AS users,
      SUM(COALESCE(b.line_total, 0)) AS spend
    FROM analytics.radiance_base_v1 b
    LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
    WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
      AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
      AND b.${targetCol} IS NOT NULL AND b.${targetCol} != 'UNKNOWN'
      ${parentFilters}
      ${issuerFilter}
    GROUP BY COALESCE(b.${targetCol}, 'UNKNOWN')
    HAVING COUNT(DISTINCT b.user_id) >= 1
    ORDER BY spend DESC LIMIT 100
  `;

  const { rows } = await pool.query(query, params);
  
  res.json({
    meta: { version: VERSION, module: 'categories/children' },
    filters: { start, end, level, domain, parent_path: product_path },
    data: rows.map(r => ({
      category_value: r.category_value,
      users: Number(r.users),
      spend: Number(r.spend),
      is_unknown: r.category_value === SUPPRESSED.UNKNOWN
    }))
  });
}));

// =============================================================================
// SPRINT 0: KPIs SUMMARY
// =============================================================================
app.get('/api/kpis/summary', asyncHandler(async (req, res) => {
  const filters = parseFilters(req, { requireIssuer: true, requireDates: true, supportsCategoryPath: true, supportsPeerScope: true });
  
  if (!filters.isValid) {
    return res.status(400).json({ error: filters.invalid.map(i => i.reason).join('; ') });
  }

  const { start, end, issuer_ruc, reconcile_ok, product_path } = filters.applied;
  const { cat_col, group_by_col } = filters.derived;
  const timerStart = Date.now();

  const startD = new Date(start);
  const endD = new Date(end);
  const daysDiff = Math.round((endD - startD) / (1000 * 60 * 60 * 24));
  const prevEnd = start;
  const prevStart = new Date(startD);
  prevStart.setDate(prevStart.getDate() - daysDiff);
  const prevStartStr = prevStart.toISOString().split('T')[0];

  const categoryPathFilter = buildCategoryPathSQL(5, filters.applied.category_domain);
  const categoryParams = [start, end, reconcile_ok, issuer_ruc, product_path.l1, product_path.l2, product_path.l3, product_path.l4];

  const kpisQuery = `
    WITH base AS (
      SELECT b.user_id, b.cufe, b.invoice_date, COALESCE(b.${cat_col}, 'UNKNOWN') AS category,
        SUM(COALESCE(b.line_total, 0)) AS line_total
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.issuer_ruc = $4 AND b.user_id IS NOT NULL
        ${categoryPathFilter}
      GROUP BY b.user_id, b.cufe, b.invoice_date, COALESCE(b.${cat_col}, 'UNKNOWN')
    ),
    kpis AS (
      SELECT SUM(line_total) AS ventas, COUNT(DISTINCT cufe) AS transacciones,
        COUNT(DISTINCT user_id) AS clientes, COUNT(DISTINCT category) AS categorias
      FROM base
    ),
    market AS (
      SELECT SUM(COALESCE(b.line_total, 0)) AS spend_market
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.user_id IN (SELECT DISTINCT user_id FROM base)
        ${categoryPathFilter}
    )
    SELECT k.*, CASE WHEN k.transacciones > 0 THEN k.ventas / k.transacciones ELSE 0 END AS ticket_promedio,
      CASE WHEN k.clientes > 0 THEN k.transacciones::float / k.clientes ELSE 0 END AS frecuencia,
      CASE WHEN m.spend_market > 0 THEN 100.0 * k.ventas / m.spend_market ELSE 0 END AS sow_pct
    FROM kpis k CROSS JOIN market m
  `;

  const prevKpisQuery = `
    WITH base AS (
      SELECT b.user_id, b.cufe, SUM(COALESCE(b.line_total, 0)) AS line_total
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.issuer_ruc = $4 AND b.user_id IS NOT NULL
        ${categoryPathFilter}
      GROUP BY b.user_id, b.cufe
    )
    SELECT SUM(line_total) AS ventas, COUNT(DISTINCT cufe) AS transacciones, COUNT(DISTINCT user_id) AS clientes,
      CASE WHEN COUNT(DISTINCT cufe) > 0 THEN SUM(line_total) / COUNT(DISTINCT cufe) ELSE 0 END AS ticket_promedio
    FROM base
  `;

  const trendsQuery = `
    SELECT DATE_TRUNC('month', b.invoice_date)::date AS month,
      SUM(COALESCE(b.line_total, 0)) AS ventas, COUNT(DISTINCT b.user_id) AS clientes, COUNT(DISTINCT b.cufe) AS transacciones
    FROM analytics.radiance_base_v1 b
    LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
    WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
      AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
      AND b.issuer_ruc = $4 AND b.user_id IS NOT NULL
      ${categoryPathFilter}
    GROUP BY DATE_TRUNC('month', b.invoice_date) ORDER BY month
  `;

  const weekdayQuery = `
    WITH base AS (
      SELECT EXTRACT(DOW FROM b.invoice_date) AS dow, COUNT(DISTINCT b.cufe) AS txn
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.issuer_ruc = $4 AND b.user_id IS NOT NULL
        ${categoryPathFilter}
      GROUP BY dow
    ),
    total AS (SELECT SUM(txn) AS total FROM base)
    SELECT dow, CASE dow WHEN 0 THEN 'Dom' WHEN 1 THEN 'Lun' WHEN 2 THEN 'Mar' 
      WHEN 3 THEN 'Mié' WHEN 4 THEN 'Jue' WHEN 5 THEN 'Vie' WHEN 6 THEN 'Sáb' END AS day_name,
      txn, ROUND(100.0 * txn / NULLIF(total, 0), 1) AS pct
    FROM base CROSS JOIN total ORDER BY dow
  `;

  const topCatQuery = `
    SELECT COALESCE(b.${group_by_col}, 'UNKNOWN') AS category,
      SUM(COALESCE(b.line_total, 0)) AS ventas, COUNT(DISTINCT b.user_id) AS clientes
    FROM analytics.radiance_base_v1 b
    LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
    WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
      AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
      AND b.issuer_ruc = $4 AND b.user_id IS NOT NULL
      ${categoryPathFilter}
    GROUP BY COALESCE(b.${group_by_col}, 'UNKNOWN') ORDER BY ventas DESC LIMIT 10
  `;

  const [kpisResult, prevResult, trendsResult, weekdayResult, topCatResult] = await Promise.all([
    pool.query(kpisQuery, categoryParams),
    pool.query(prevKpisQuery, [prevStartStr, prevEnd, reconcile_ok, issuer_ruc, product_path.l1, product_path.l2, product_path.l3, product_path.l4]),
    pool.query(trendsQuery, categoryParams),
    pool.query(weekdayQuery, categoryParams),
    pool.query(topCatQuery, categoryParams)
  ]);

  const current = kpisResult.rows[0] || {};
  const previous = prevResult.rows[0] || null;

  res.set('X-Query-Time-Ms', String(Date.now() - timerStart));
  res.json({
    ...buildFilterResponse(filters, 'kpis/summary'),
    current: {
      ventas: Number(current.ventas || 0), transacciones: Number(current.transacciones || 0),
      clientes: Number(current.clientes || 0), ticket_promedio: Number(current.ticket_promedio || 0),
      frecuencia: Number(current.frecuencia || 0), categorias: Number(current.categorias || 0),
      sow_pct: Number(current.sow_pct || 0)
    },
    previous: previous ? {
      ventas: Number(previous.ventas || 0), transacciones: Number(previous.transacciones || 0),
      clientes: Number(previous.clientes || 0), ticket_promedio: Number(previous.ticket_promedio || 0)
    } : null,
    trends: trendsResult.rows.map(r => ({
      month: r.month.toISOString().split('T')[0].substring(0, 7),
      ventas: Number(r.ventas || 0), clientes: Number(r.clientes || 0), transacciones: Number(r.transacciones || 0)
    })),
    by_weekday: weekdayResult.rows.map(r => ({ dow: Number(r.dow), day_name: r.day_name, txn: Number(r.txn || 0), pct: Number(r.pct || 0) })),
    top_categories: topCatResult.rows.map(r => ({ category: r.category, ventas: Number(r.ventas || 0), clientes: Number(r.clientes || 0), is_unknown: r.category === SUPPRESSED.UNKNOWN }))
  });
}));

// =============================================================================
// SPRINT 1: CAPTURE (SoW & Leakage by Category)
// =============================================================================
app.get('/api/sow_leakage/by_category', asyncHandler(async (req, res) => {
  const filters = parseFilters(req, { requireIssuer: true, requireDates: true, supportsCategoryPath: true, supportsPeerScope: true });
  
  if (!filters.isValid) {
    return res.status(400).json({ error: filters.invalid.map(i => i.reason).join('; ') });
  }

  const { start, end, issuer_ruc, reconcile_ok, k_threshold, peer_scope, product_path } = filters.applied;
  const { group_by_col } = filters.derived;
  const timerStart = Date.now();

  let peerScopeJoin = '', peerScopeWhere = '';
  if (peer_scope === 'peers') {
    peerScopeJoin = `LEFT JOIN public.dim_issuer di_x ON di_x.issuer_ruc = $4 LEFT JOIN public.dim_issuer di_b ON di_b.issuer_ruc = b.issuer_ruc`;
    peerScopeWhere = `AND (b.issuer_ruc = $4 OR di_b.issuer_l1 = di_x.issuer_l1)`;
  }

  const categoryPathFilter = buildCategoryPathSQL(6, filters.applied.category_domain);

  const query = `
 WITH cohort AS (
  SELECT DISTINCT b.user_id FROM analytics.radiance_base_v1 b
  LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
  WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
    AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
    AND b.issuer_ruc = $4 AND b.user_id IS NOT NULL ${categoryPathFilter}
),
peer_spend AS (
  SELECT b.user_id, COALESCE(b.${group_by_col}, 'UNKNOWN') AS category_value, b.issuer_ruc, SUM(COALESCE(b.line_total, 0)) AS spend
  FROM analytics.radiance_base_v1 b INNER JOIN cohort c ON b.user_id = c.user_id
  LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe ${peerScopeJoin}
  WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
    AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3) ${peerScopeWhere} ${categoryPathFilter}
  GROUP BY b.user_id, COALESCE(b.${group_by_col}, 'UNKNOWN'), b.issuer_ruc
),
grouped AS (
  SELECT category_value, 
    COUNT(DISTINCT user_id) AS users,
    SUM(CASE WHEN issuer_ruc = $4 THEN spend ELSE 0 END) AS spend_in_x_usd, 
    SUM(spend) AS spend_market_usd,
    SUM(spend) - SUM(CASE WHEN issuer_ruc = $4 THEN spend ELSE 0 END) AS leakage_usd,
    ROUND(100.0 * SUM(CASE WHEN issuer_ruc = $4 THEN spend ELSE 0 END) / NULLIF(SUM(spend), 0), 2) AS sow_pct
  FROM peer_spend GROUP BY category_value
)
SELECT 
  CASE WHEN users < $5 THEN 'OTHER_SUPPRESSED' ELSE category_value END AS category_value,
  SUM(users) AS users, 
  SUM(spend_in_x_usd) AS spend_in_x_usd,
  SUM(spend_market_usd) AS spend_market_usd,
  SUM(leakage_usd) AS leakage_usd,
  ROUND(100.0 * SUM(spend_in_x_usd) / NULLIF(SUM(spend_market_usd), 0), 2) AS sow_pct
FROM grouped
GROUP BY CASE WHEN users < $5 THEN 'OTHER_SUPPRESSED' ELSE category_value END
ORDER BY SUM(spend_in_x_usd) DESC
`;

  const { rows } = await pool.query(query, [start, end, reconcile_ok, issuer_ruc, k_threshold, product_path.l1, product_path.l2, product_path.l3, product_path.l4]);

  res.set('X-Query-Time-Ms', String(Date.now() - timerStart));
  res.json({
    ...buildFilterResponse(filters, 'sow_leakage/by_category'),
    data: rows.map(r => ({
      category_value: r.category_value, users: Number(r.users),
      spend_in_x_usd: Math.round(Number(r.spend_in_x_usd) * 100) / 100,
      spend_market_usd: Math.round(Number(r.spend_market_usd) * 100) / 100,
      leakage_usd: Math.round(Number(r.leakage_usd) * 100) / 100,
      sow_pct: Number(r.sow_pct), is_unknown: r.category_value === SUPPRESSED.UNKNOWN,
      trust_level: r.category_value === SUPPRESSED.UNKNOWN ? 'SUPPRESSED' : null
    })),
    disclaimers: CAPTURE_DISCLAIMERS
  });
}));

// =============================================================================
// SPRINT 2: SWITCHING DESTINATIONS
// =============================================================================
app.get('/api/switching/destinations', asyncHandler(async (req, res) => {
  const filters = parseFilters(req, { requireIssuer: true, requireDates: true, supportsCategoryPath: true });
  
  if (!filters.isValid) {
    return res.status(400).json({ error: filters.invalid.map(i => i.reason).join('; ') });
  }

  const { start, end, issuer_ruc, reconcile_ok, k_threshold, product_path } = filters.applied;
  const timerStart = Date.now();
  const categoryPathFilter = buildCategoryPathSQL(6, filters.applied.category_domain);

  const query = `
    WITH cohort AS (
      SELECT DISTINCT user_id FROM analytics.radiance_base_v1
      WHERE invoice_date >= $1::date AND invoice_date < $2::date AND issuer_ruc = $4 AND user_id IS NOT NULL ${categoryPathFilter}
    ),
    elsewhere AS (
      SELECT b.user_id, COALESCE(b.issuer_name, b.issuer_ruc) AS destination
      FROM analytics.radiance_base_v1 b INNER JOIN cohort c ON b.user_id = c.user_id
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3) AND b.issuer_ruc != $4 ${categoryPathFilter}
    )
    SELECT destination, COUNT(DISTINCT user_id) AS users,
      ROUND(100.0 * COUNT(DISTINCT user_id) / NULLIF((SELECT COUNT(*) FROM cohort), 0), 2) AS pct
    , grouped AS (
  SELECT destination, COUNT(DISTINCT user_id) AS users, ...
  FROM elsewhere GROUP BY destination
)
SELECT 
  CASE WHEN users < $5 THEN 'OTHER_SUPPRESSED' ELSE destination END AS destination,
  SUM(users) AS users, ...
FROM grouped
GROUP BY CASE WHEN users < $5 THEN 'OTHER_SUPPRESSED' ELSE destination END
ORDER BY users DESC LIMIT 25
  `;

  const { rows } = await pool.query(query, [start, end, reconcile_ok, issuer_ruc, k_threshold, product_path.l1, product_path.l2, product_path.l3, product_path.l4]);

  res.set('X-Query-Time-Ms', String(Date.now() - timerStart));
  res.json({
    ...buildFilterResponse(filters, 'switching/destinations'),
    data: rows.map(r => ({ destination: r.destination, users: Number(r.users), pct: Number(r.pct) })),
    disclaimers: SWITCHING_DISCLAIMERS
  });
}));

// =============================================================================
// SPRINT 3: LEAKAGE TREE
// =============================================================================
app.get('/api/leakage/tree', asyncHandler(async (req, res) => {
  const filters = parseFilters(req, { requireIssuer: true, requireDates: true, requireCategoryValue: true, supportsCategoryPath: false });
  
  if (!filters.isValid) {
    return res.status(400).json({ error: filters.invalid.map(i => i.reason).join('; ') });
  }

  const { start, end, issuer_ruc, reconcile_ok, category_value } = filters.applied;
  const { cat_col } = filters.derived;
  const timerStart = Date.now();

  const query = `
    WITH base_txn AS (
      SELECT b.user_id, DATE_TRUNC('month', b.invoice_date)::date AS txn_month, b.issuer_ruc,
        SUM(COALESCE(b.line_total, 0)) AS line_total, COUNT(DISTINCT b.cufe) AS visits
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.user_id IS NOT NULL AND COALESCE(b.${cat_col}, 'UNKNOWN') = $5
      GROUP BY b.user_id, DATE_TRUNC('month', b.invoice_date)::date, b.issuer_ruc
    ),
    months AS (SELECT DISTINCT txn_month FROM base_txn ORDER BY txn_month),
    month_pairs AS (SELECT txn_month AS origin, LEAD(txn_month) OVER (ORDER BY txn_month) AS next FROM months),
    user_month AS (
      SELECT user_id, txn_month, SUM(CASE WHEN issuer_ruc = $4 THEN visits ELSE 0 END) AS visits_x,
        SUM(CASE WHEN issuer_ruc = $4 THEN line_total ELSE 0 END) AS spend_x, SUM(visits) AS visits_total
      FROM base_txn GROUP BY user_id, txn_month
    ),
    cohort AS (SELECT user_id, txn_month AS origin FROM user_month WHERE visits_x > 0),
    transitions AS (
      SELECT c.user_id, c.origin, mp.next, COALESCE(um_o.visits_x, 0) AS vx_o, COALESCE(um_o.spend_x, 0) AS sx_o,
        COALESCE(um_n.visits_x, 0) AS vx_n, COALESCE(um_n.spend_x, 0) AS sx_n, COALESCE(um_n.visits_total, 0) AS vt_n
      FROM cohort c INNER JOIN month_pairs mp ON c.origin = mp.origin
      LEFT JOIN user_month um_o ON c.user_id = um_o.user_id AND c.origin = um_o.txn_month
      LEFT JOIN user_month um_n ON c.user_id = um_n.user_id AND mp.next = um_n.txn_month WHERE mp.next IS NOT NULL
    ),
    buckets AS (
      SELECT CASE
        WHEN vx_n > 0 AND sx_n >= sx_o * 0.9 THEN 'RETAINED'
        WHEN vt_n > 0 AND vx_n = 0 THEN 'CATEGORY_GONE'
        WHEN vx_n > 0 AND sx_n < sx_o * 0.5 THEN 'REDUCED_BASKET'
        WHEN vx_n > 0 AND vx_n < vx_o THEN 'REDUCED_FREQ'
        WHEN vt_n = 0 THEN 'FULL_CHURN' ELSE 'DELAYED_ONLY' END AS bucket
      FROM transitions
    )
    SELECT bucket, COUNT(*) AS users, ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER(), 0), 2) AS pct FROM buckets GROUP BY bucket
  `;

  const { rows } = await pool.query(query, [start, end, reconcile_ok, issuer_ruc, category_value]);
  const bucketOrder = ['RETAINED', 'CATEGORY_GONE', 'REDUCED_BASKET', 'REDUCED_FREQ', 'DELAYED_ONLY', 'FULL_CHURN'];
  const bucketMap = {};
  rows.forEach(r => { bucketMap[r.bucket] = { users: Number(r.users), pct: Number(r.pct) }; });

  res.set('X-Query-Time-Ms', String(Date.now() - timerStart));
  res.json({
    ...buildFilterResponse(filters, 'leakage/tree'),
    waterfall: bucketOrder.map(b => ({ bucket: b, users: bucketMap[b]?.users || 0, pct: bucketMap[b]?.pct || 0 })),
    disclaimers: LEAKAGE_DISCLAIMERS
  });
}));

// =============================================================================
// SPRINT 4: BASKET BREADTH
// =============================================================================
app.get('/api/basket/breadth', asyncHandler(async (req, res) => {
  const filters = parseFilters(req, { requireIssuer: true, requireDates: true, supportsCategoryPath: true });
  
  if (!filters.isValid) {
    return res.status(400).json({ error: filters.invalid.map(i => i.reason).join('; ') });
  }

  const { start, end, issuer_ruc, reconcile_ok, k_threshold, product_path } = filters.applied;
  const { cat_col } = filters.derived;
  const timerStart = Date.now();
  const categoryPathFilter = buildCategoryPathSQL(6, filters.applied.category_domain);

  const query = `
  WITH base_txn AS (
  SELECT b.user_id, DATE_TRUNC('month', b.invoice_date)::date AS txn_month, b.issuer_ruc, COALESCE(b.${cat_col}, 'UNKNOWN') AS cat_val
  FROM analytics.radiance_base_v1 b LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
  WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
    AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3) AND b.user_id IS NOT NULL ${categoryPathFilter}
),
cohort AS (SELECT DISTINCT user_id, txn_month FROM base_txn WHERE issuer_ruc = $4),
user_breadth AS (
  SELECT c.user_id, c.txn_month, COUNT(DISTINCT bt.cat_val) AS breadth_total,
    COUNT(DISTINCT CASE WHEN bt.issuer_ruc = $4 THEN bt.cat_val END) AS breadth_in_x
  FROM cohort c INNER JOIN base_txn bt ON c.user_id = bt.user_id AND c.txn_month = bt.txn_month GROUP BY c.user_id, c.txn_month
),
monthly AS (
  SELECT txn_month AS origin_month, 
    COUNT(DISTINCT user_id) AS users,
    ROUND(AVG(breadth_total), 2) AS avg_breadth_market, 
    ROUND(AVG(breadth_in_x), 2) AS avg_breadth_in_x
  FROM user_breadth GROUP BY txn_month
)
SELECT origin_month, users, avg_breadth_market, avg_breadth_in_x,
  CASE WHEN users < $5 THEN true ELSE false END AS is_suppressed
FROM monthly
ORDER BY origin_month 
`;

  const { rows } = await pool.query(query, [start, end, reconcile_ok, issuer_ruc, k_threshold, product_path.l1, product_path.l2, product_path.l3, product_path.l4]);

  res.set('X-Query-Time-Ms', String(Date.now() - timerStart));
  res.json({
    ...buildFilterResponse(filters, 'basket/breadth'),
    data: rows.map(r => ({
      origin_month: r.origin_month.toISOString().split('T')[0].substring(0, 7),
      users: Number(r.users), avg_breadth_market: Number(r.avg_breadth_market), avg_breadth_in_x: Number(r.avg_breadth_in_x)
    })),
    disclaimers: BASKET_DISCLAIMERS
  });
}));

// =============================================================================
// SPRINT 5: BRAND LOYALTY
// =============================================================================
app.get('/api/loyalty/brands', asyncHandler(async (req, res) => {
  const filters = parseFilters(req, { requireIssuer: true, requireDates: true, requireCategoryValue: true, supportsCategoryPath: false });
  
  if (!filters.isValid) {
    return res.status(400).json({ error: filters.invalid.map(i => i.reason).join('; ') });
  }

  const { start, end, issuer_ruc, reconcile_ok, k_threshold, category_value } = filters.applied;
  const { cat_col } = filters.derived;
  const timerStart = Date.now();

  const query = `
    WITH base AS (
  SELECT b.user_id, COALESCE(NULLIF(TRIM(b.product_brand), ''), 'UNKNOWN') AS brand, SUM(COALESCE(b.line_total, 0)) AS brand_spend
  FROM analytics.radiance_base_v1 b LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
  WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
    AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
    AND b.issuer_ruc = $4 AND b.user_id IS NOT NULL AND COALESCE(b.${cat_col}, 'UNKNOWN') = $5
  GROUP BY b.user_id, COALESCE(NULLIF(TRIM(b.product_brand), ''), 'UNKNOWN')
),
user_cat_spend AS (SELECT user_id, SUM(brand_spend) AS cat_spend FROM base GROUP BY user_id),
shares AS (
  SELECT b.user_id, b.brand, ROUND(100.0 * b.brand_spend / NULLIF(u.cat_spend, 0), 2) AS share_pct
  FROM base b JOIN user_cat_spend u ON b.user_id = u.user_id WHERE b.brand_spend > 0
),
brand_agg AS (
  SELECT brand, COUNT(DISTINCT user_id) AS brand_buyers,
    ROUND(100.0 * COUNT(DISTINCT user_id) / NULLIF((SELECT COUNT(*) FROM user_cat_spend), 0), 2) AS penetration_pct,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY share_pct)::numeric, 2) AS p75,
    COUNT(DISTINCT user_id) FILTER (WHERE share_pct >= 80) AS loyal_users
  FROM shares GROUP BY brand
),
tiers AS (
  SELECT COUNT(*) FILTER (WHERE share_pct >= 95) AS exclusive, 
    COUNT(*) FILTER (WHERE share_pct >= 80 AND share_pct < 95) AS loyal,
    COUNT(*) FILTER (WHERE share_pct >= 50 AND share_pct < 80) AS prefer, 
    COUNT(*) FILTER (WHERE share_pct < 50) AS light
  FROM shares
),
dist AS (
  SELECT ROUND(PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY share_pct)::numeric, 2) AS p10,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY share_pct)::numeric, 2) AS p25,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY share_pct)::numeric, 2) AS p50,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY share_pct)::numeric, 2) AS p75,
    ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY share_pct)::numeric, 2) AS p90
  FROM shares
),
brand_final AS (
  SELECT 
    CASE WHEN brand_buyers < $6 AND brand != 'UNKNOWN' THEN 'OTHER_SUPPRESSED' ELSE brand END AS brand,
    SUM(brand_buyers) AS brand_buyers,
    ROUND(AVG(penetration_pct), 2) AS penetration_pct,
    ROUND(AVG(p75), 2) AS p75,
    SUM(loyal_users) AS loyal_users
  FROM brand_agg
  GROUP BY CASE WHEN brand_buyers < $6 AND brand != 'UNKNOWN' THEN 'OTHER_SUPPRESSED' ELSE brand END
)
SELECT brand, brand_buyers, penetration_pct, p75, loyal_users,
  ROUND(100.0 * loyal_users / NULLIF(brand_buyers, 0), 2) AS loyalty_rate_pct,
  (SELECT row_to_json(tiers.*) FROM tiers) AS tiers_json, 
  (SELECT row_to_json(dist.*) FROM dist) AS dist_json
FROM brand_final
ORDER BY CASE brand WHEN 'UNKNOWN' THEN 2 WHEN 'OTHER_SUPPRESSED' THEN 3 ELSE 1 END, brand_buyers DESC
`;

  const { rows } = await pool.query(query, [start, end, reconcile_ok, issuer_ruc, category_value, k_threshold]);
  const first = rows[0] || {};

  res.set('X-Query-Time-Ms', String(Date.now() - timerStart));
  res.json({
    ...buildFilterResponse(filters, 'loyalty/brands'),
    tiers: first.tiers_json || { exclusive: 0, loyal: 0, prefer: 0, light: 0 },
    distribution: first.dist_json || { p10: 0, p25: 0, p50: 0, p75: 0, p90: 0 },
    data: rows.map(r => ({
      brand: r.brand, brand_buyers: Number(r.brand_buyers), penetration_pct: Number(r.penetration_pct),
      p75: Number(r.p75), loyalty_rate_pct: Number(r.loyalty_rate_pct),
      is_unknown: r.brand === SUPPRESSED.UNKNOWN, trust_level: r.brand === SUPPRESSED.UNKNOWN ? 'SUPPRESSED' : null
    })),
    disclaimers: LOYALTY_DISCLAIMERS
  });
}));

// =============================================================================
// SPRINT 6: PANEL SUMMARY
// =============================================================================
app.get('/api/panel/summary', asyncHandler(async (req, res) => {
  const filters = parseFilters(req, { requireIssuer: true, requireDates: true, supportsCategoryPath: true });
  
  if (!filters.isValid) {
    return res.status(400).json({ error: filters.invalid.map(i => i.reason).join('; ') });
  }

  const { start, end, issuer_ruc, reconcile_ok, product_path } = filters.applied;
  const timerStart = Date.now();
  const categoryPathFilter = buildCategoryPathSQL(5, filters.applied.category_domain);

  const query = `
    WITH base_txn AS (
      SELECT b.user_id, b.cufe, b.invoice_date, b.issuer_ruc, SUM(COALESCE(b.line_total, 0)) AS line_total
      FROM analytics.radiance_base_v1 b LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3) AND b.user_id IS NOT NULL ${categoryPathFilter}
      GROUP BY b.user_id, b.cufe, b.invoice_date, b.issuer_ruc
    ),
    panel_x AS (
      SELECT COUNT(DISTINCT user_id) AS customers_n, SUM(line_total) AS spend_in_x_usd,
        COUNT(DISTINCT cufe) AS invoices_n, COUNT(DISTINCT DATE_TRUNC('month', invoice_date)) AS active_months
      FROM base_txn WHERE issuer_ruc = $4
    ),
    panel_market AS (SELECT SUM(line_total) AS spend_market_usd FROM base_txn WHERE user_id IN (SELECT DISTINCT user_id FROM base_txn WHERE issuer_ruc = $4))
    SELECT px.*, pm.spend_market_usd, ROUND(100.0 * px.spend_in_x_usd / NULLIF(pm.spend_market_usd, 0), 2) AS sow_pct,
      CASE WHEN px.customers_n > 0 THEN ROUND(px.spend_in_x_usd / px.customers_n, 2) ELSE 0 END AS avg_spend
    FROM panel_x px CROSS JOIN panel_market pm
  `;

  const { rows } = await pool.query(query, [start, end, reconcile_ok, issuer_ruc, product_path.l1, product_path.l2, product_path.l3, product_path.l4]);
  const r = rows[0] || {};
  const customersN = Number(r.customers_n || 0);
  const expansion = getExpansionFactor(issuer_ruc, null);

  res.set('X-Query-Time-Ms', String(Date.now() - timerStart));
  res.json({
    ...buildFilterResponse(filters, 'panel/summary'),
    panel: {
      customers_n: customersN, spend_in_x_usd: Math.round(Number(r.spend_in_x_usd || 0) * 100) / 100,
      spend_market_usd: Math.round(Number(r.spend_market_usd || 0) * 100) / 100, sow_pct: Number(r.sow_pct || 0),
      invoices_n: Number(r.invoices_n || 0), active_months: Number(r.active_months || 0)
    },
    projection: {
      method: 'expansion_factor', expansion_factor: expansion.factor, expansion_source: expansion.source,
      projected_households: Math.round(customersN * expansion.factor), notes: ['REFERENTIAL', 'ILLUSTRATIVE']
    },
    disclaimers: PANEL_DISCLAIMERS
  });
}));

// =============================================================================
// ERROR HANDLING
// =============================================================================
app.use((err, req, res, next) => {
  console.error('Error:', err);
  res.status(500).json({ error: 'Internal server error', message: err.message });
});

// =============================================================================
// START SERVER
// =============================================================================
const PORT = process.env.PORT || 3000;

async function startServer() {
  try {
    const client = await pool.connect();
    console.log('✅ Database connected');
    client.release();
    app.listen(PORT, () => {
      console.log(`✅ Radiance API v${VERSION} running on port ${PORT}`);
      console.log(`   Filter Contract: v1`);
      console.log(`   Sprints: ${SPRINTS.join(', ')}`);
    });
  } catch (err) {
    console.error('❌ Database connection failed:', err.message);
    process.exit(1);
  }
}

startServer();
