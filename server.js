// =============================================================================
// RADIANCE API v2.0 - COMPLETE SERVER
// All Sprints (1-7) Consolidated
// 
// Sprints:
//   1. Commerce Capture (SoW/Leakage by category)
//   2. Switching Engine (destination retailers)
//   3. Leakage Diagnosis Tree (6-bucket waterfall)
//   4. Basket Missions (breadth, attachment, mission_split)
//   5. Brand Loyalty v1.1 (penetration, share distribution, tiers)
//   6. Panel Summary + Household Projection
//   7. Deck HTML Builder
//
// Deploy: Replit / Render
// Database: PostgreSQL (Neon/Supabase)
// =============================================================================

const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json());

// =============================================================================
// DATABASE CONNECTION
// =============================================================================
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// Test connection on startup
pool.query('SELECT NOW()')
  .then(() => console.log('✅ Database connected'))
  .catch(err => console.error('❌ Database connection error:', err.message));

// =============================================================================
// SHARED HELPERS
// =============================================================================

function parseDate(val) {
  if (!val) return null;
  const d = new Date(val);
  return isNaN(d.getTime()) ? null : val;
}

function parseString(val) {
  if (val === undefined || val === null || val === '') return null;
  return String(val).trim();
}

function parseBool(val, allowNull = false) {
  if (val === undefined || val === null || val === '' || val === 'all') {
    return allowNull ? null : null;
  }
  if (val === 'true' || val === true || val === '1') return true;
  if (val === 'false' || val === false || val === '0') return false;
  return allowNull ? null : null;
}

function parseKThreshold(val) {
  const k = parseInt(val);
  return (!isNaN(k) && k >= 1) ? k : 5;
}

// Async handler wrapper
const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

// Simple in-memory cache
const cache = new Map();
const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

function getCacheKey(endpoint, params) {
  return `${endpoint}:${JSON.stringify(params)}`;
}

function getFromCache(key) {
  const entry = cache.get(key);
  if (!entry) return null;
  if (Date.now() - entry.timestamp > CACHE_TTL_MS) {
    cache.delete(key);
    return null;
  }
  return entry.data;
}

function setCache(key, data) {
  cache.set(key, { data, timestamp: Date.now() });
}

function logRequest(endpoint, params, durationMs, rowCount, fromCache) {
  console.log(`[${new Date().toISOString()}] ${endpoint} | ${durationMs}ms | rows=${rowCount} | cache=${fromCache}`);
}

// =============================================================================
// SHARED CONSTANTS
// =============================================================================

const VALID_CAT_COLS = { 
  'l1': 'category_l1', 
  'l2': 'category_l2', 
  'l3': 'category_l3', 
  'l4': 'category_l4' 
};

function getCatCol(level) { 
  return VALID_CAT_COLS[level] || null; 
}



// =============================================================================
// SPRINT 1: COMMERCE CAPTURE (SoW / Leakage by Category)
// =============================================================================

const CAPTURE_DISCLAIMERS = [
  'Panel observado (no proyectado). Sin claims causales.',
  'Ventana: [start,end) end-exclusive.',
  'reconcile_ok aplicado a nivel transacción.',
  'K-anonymity: categorías con users < k se suprimen.',
  'UNKNOWN incluido; nunca filtrado.',
  'SoW = spend_in_x / spend_market × 100.'
];

app.get('/api/sow_leakage/by_category', asyncHandler(async (req, res) => {
  const startDate = parseDate(req.query.start);
  const endDate = parseDate(req.query.end);
  const issuerRuc = parseString(req.query.issuer_ruc);
  const storeId = parseString(req.query.store_id);
  const reconcileOk = parseBool(req.query.reconcile_ok, true);
  const categoryLevel = (req.query.category_level || 'l1').toLowerCase();
  const kThreshold = parseKThreshold(req.query.k_threshold);
  const limit = Math.min(Math.max(parseInt(req.query.limit) || 50, 1), 100);

  const errors = [];
  if (!startDate) errors.push('Invalid or missing start date');
  if (!endDate) errors.push('Invalid or missing end date');
  if (!issuerRuc) errors.push('issuer_ruc is required');
  const catCol = getCatCol(categoryLevel);
  if (!catCol) errors.push('category_level must be one of: l1, l2, l3, l4');
  if (errors.length > 0) return res.status(400).json({ error: errors.join('; ') });

  const start = Date.now();

  const query = `
    WITH cohort AS (
      SELECT DISTINCT b.user_id
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.issuer_ruc = $4
        AND ($5::text IS NULL OR b.store_id::text = $5)
        AND b.user_id IS NOT NULL
    ),
    user_cat_spend AS (
      SELECT b.user_id,
        COALESCE(b.${catCol}, 'UNKNOWN') AS category_value,
        b.issuer_ruc,
        SUM(COALESCE(b.line_total, 0)) AS spend
      FROM analytics.radiance_base_v1 b
      INNER JOIN cohort c ON b.user_id = c.user_id
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
      GROUP BY b.user_id, COALESCE(b.${catCol}, 'UNKNOWN'), b.issuer_ruc
    ),
    category_agg AS (
      SELECT category_value,
        COUNT(DISTINCT user_id) AS users,
        SUM(CASE WHEN issuer_ruc = $4 THEN spend ELSE 0 END) AS spend_in_x_usd,
        SUM(spend) AS spend_market_usd
      FROM user_cat_spend
      GROUP BY category_value
      HAVING COUNT(DISTINCT user_id) >= $6
    )
    SELECT category_value, users,
      ROUND(spend_in_x_usd::numeric, 2) AS spend_in_x_usd,
      ROUND(spend_market_usd::numeric, 2) AS spend_market_usd,
      ROUND(100.0 * spend_in_x_usd / NULLIF(spend_market_usd, 0), 2) AS sow_pct,
      ROUND(spend_market_usd - spend_in_x_usd, 2) AS leakage_usd
    FROM category_agg
    ORDER BY spend_market_usd DESC
    LIMIT $7
  `;

  const { rows } = await pool.query(query, [
    startDate, endDate, reconcileOk, issuerRuc, storeId, kThreshold, limit
  ]);
  const duration = Date.now() - start;

  const result = {
    filters: {
      start: startDate, end: endDate,
      issuer_ruc: issuerRuc, store_id: storeId,
      category_level: categoryLevel,
      reconcile_ok: reconcileOk,
      k_threshold: kThreshold, limit
    },
    data: rows.map(r => ({
      category_value: r.category_value,
      users: Number(r.users),
      spend_in_x_usd: Number(r.spend_in_x_usd),
      spend_market_usd: Number(r.spend_market_usd),
      sow_pct: Number(r.sow_pct),
      leakage_usd: Number(r.leakage_usd)
    })),
    disclaimers: CAPTURE_DISCLAIMERS
  };

  res.set('X-Query-Time-Ms', String(duration));
  logRequest('/api/sow_leakage/by_category', {}, duration, rows.length, false);
  res.json(result);
}));

// =============================================================================
// SPRINT 2: SWITCHING ENGINE
// =============================================================================

const SWITCHING_DISCLAIMERS = [
  'Panel observado (no proyectado). Sin claims causales.',
  'Ventana: [start,end) end-exclusive.',
  'reconcile_ok aplicado a nivel transacción.',
  'Switching = gasto del cohort X en retailers distintos a X.',
  'K-anonymity aplicado.'
];

app.get('/api/switching/destinations', asyncHandler(async (req, res) => {
  const startDate = parseDate(req.query.start);
  const endDate = parseDate(req.query.end);
  const issuerRuc = parseString(req.query.issuer_ruc);
  const storeId = parseString(req.query.store_id);
  const reconcileOk = parseBool(req.query.reconcile_ok, true);
  const categoryLevel = (req.query.category_level || 'l1').toLowerCase();
  const categoryValue = parseString(req.query.category_value);
  const kThreshold = parseKThreshold(req.query.k_threshold);
  const limit = Math.min(Math.max(parseInt(req.query.limit) || 20, 1), 50);

  const errors = [];
  if (!startDate) errors.push('Invalid or missing start date');
  if (!endDate) errors.push('Invalid or missing end date');
  if (!issuerRuc) errors.push('issuer_ruc is required');
  const catCol = getCatCol(categoryLevel);
  if (!catCol) errors.push('category_level must be one of: l1, l2, l3, l4');
  if (errors.length > 0) return res.status(400).json({ error: errors.join('; ') });

  const start = Date.now();

  const categoryFilter = categoryValue 
    ? `AND COALESCE(b.${catCol}, 'UNKNOWN') = $6` 
    : '';
  const params = categoryValue 
    ? [startDate, endDate, reconcileOk, issuerRuc, storeId, categoryValue, kThreshold, limit]
    : [startDate, endDate, reconcileOk, issuerRuc, storeId, kThreshold, limit];
  const kIdx = categoryValue ? 7 : 6;
  const limIdx = categoryValue ? 8 : 7;

  const query = `
    WITH cohort AS (
      SELECT DISTINCT b.user_id
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.issuer_ruc = $4
        AND ($5::text IS NULL OR b.store_id::text = $5)
        AND b.user_id IS NOT NULL
    ),
    elsewhere AS (
      SELECT b.user_id, b.issuer_ruc AS dest_ruc, 
        COALESCE(b.issuer_name, b.issuer_ruc) AS dest_name,
        SUM(COALESCE(b.line_total, 0)) AS spend
      FROM analytics.radiance_base_v1 b
      INNER JOIN cohort c ON b.user_id = c.user_id
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.issuer_ruc != $4
        ${categoryFilter}
      GROUP BY b.user_id, b.issuer_ruc, b.issuer_name
    ),
    dest_agg AS (
      SELECT dest_ruc, dest_name,
        COUNT(DISTINCT user_id) AS users,
        SUM(spend) AS total_spend
      FROM elsewhere
      GROUP BY dest_ruc, dest_name
      HAVING COUNT(DISTINCT user_id) >= $${kIdx}
    )
    SELECT dest_ruc, dest_name, users,
      ROUND(total_spend::numeric, 2) AS total_spend,
      ROUND(100.0 * users / NULLIF((SELECT COUNT(*) FROM cohort), 0), 2) AS pct_of_cohort
    FROM dest_agg
    ORDER BY users DESC
    LIMIT $${limIdx}
  `;

  const { rows } = await pool.query(query, params);
  const duration = Date.now() - start;

  res.set('X-Query-Time-Ms', String(duration));
  res.json({
    filters: {
      start: startDate, end: endDate,
      issuer_ruc: issuerRuc, store_id: storeId,
      category_level: categoryLevel, category_value: categoryValue,
      reconcile_ok: reconcileOk, k_threshold: kThreshold, limit
    },
    data: rows.map(r => ({
      dest_ruc: r.dest_ruc,
      dest_name: r.dest_name,
      users: Number(r.users),
      total_spend: Number(r.total_spend),
      pct_of_cohort: Number(r.pct_of_cohort)
    })),
    disclaimers: SWITCHING_DISCLAIMERS
  });
}));

// =============================================================================
// SPRINT 3: LEAKAGE DIAGNOSIS TREE
// =============================================================================

const LEAKAGE_DISCLAIMERS = [
  'Panel observado (no proyectado). Sin claims causales.',
  'Ventana: [start,end) end-exclusive.',
  'reconcile_ok aplicado a nivel transacción.',
  '6-bucket waterfall: RETAINED, CATEGORY_GONE, REDUCED_BASKET, REDUCED_FREQ, DELAYED_ONLY, FULL_CHURN.',
  'K-anonymity: meses con cohort < k se suprimen.',
  'UNKNOWN incluido en categorías.',
  'Trust gating: suppressed_months[] indica meses con datos insuficientes.'
];

app.get('/api/leakage/tree', asyncHandler(async (req, res) => {
  const startDate = parseDate(req.query.start);
  const endDate = parseDate(req.query.end);
  const issuerRuc = parseString(req.query.issuer_ruc);
  const storeId = parseString(req.query.store_id);
  const reconcileOk = parseBool(req.query.reconcile_ok, true);
  const categoryLevel = (req.query.category_level || 'l1').toLowerCase();
  const categoryValue = parseString(req.query.category_value);
  const kThreshold = parseKThreshold(req.query.k_threshold);
  const minN = Math.max(parseInt(req.query.min_n) || 10, 1);

  const errors = [];
  if (!startDate) errors.push('Invalid or missing start date');
  if (!endDate) errors.push('Invalid or missing end date');
  if (!issuerRuc) errors.push('issuer_ruc is required');
  const catCol = getCatCol(categoryLevel);
  if (!catCol) errors.push('category_level must be one of: l1, l2, l3, l4');
  if (errors.length > 0) return res.status(400).json({ error: errors.join('; ') });

  const start = Date.now();

  const categoryFilter = categoryValue 
    ? `AND COALESCE(b.${catCol}, 'UNKNOWN') = $6` 
    : '';
  const params = categoryValue 
    ? [startDate, endDate, reconcileOk, issuerRuc, storeId, categoryValue, kThreshold]
    : [startDate, endDate, reconcileOk, issuerRuc, storeId, kThreshold];
  const kIdx = categoryValue ? 7 : 6;

  const query = `
    WITH base_txn AS (
      SELECT b.user_id, b.cufe, 
        DATE_TRUNC('month', b.invoice_date)::date AS txn_month,
        b.issuer_ruc, b.store_id,
        COALESCE(b.${catCol}, 'UNKNOWN') AS cat_val,
        COALESCE(b.line_total, 0) AS line_total
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.user_id IS NOT NULL
        ${categoryFilter}
    ),
    months AS (
      SELECT DISTINCT txn_month FROM base_txn ORDER BY txn_month
    ),
    month_pairs AS (
      SELECT txn_month AS origin_month,
        LEAD(txn_month) OVER (ORDER BY txn_month) AS next_month
      FROM months
    ),
    user_month_stats AS (
      SELECT user_id, txn_month,
        COUNT(DISTINCT CASE WHEN issuer_ruc = $4 AND ($5::text IS NULL OR store_id::text = $5) THEN cufe END) AS visits_x,
        COUNT(DISTINCT CASE WHEN issuer_ruc = $4 AND ($5::text IS NULL OR store_id::text = $5) THEN cat_val END) AS cats_x,
        SUM(CASE WHEN issuer_ruc = $4 AND ($5::text IS NULL OR store_id::text = $5) THEN line_total ELSE 0 END) AS spend_x,
        COUNT(DISTINCT cufe) AS visits_total,
        COUNT(DISTINCT cat_val) AS cats_total,
        SUM(line_total) AS spend_total
      FROM base_txn
      GROUP BY user_id, txn_month
    ),
    cohort_origin AS (
      SELECT user_id, txn_month AS origin_month
      FROM user_month_stats
      WHERE visits_x > 0
    ),
    user_transitions AS (
      SELECT co.user_id, co.origin_month, mp.next_month,
        COALESCE(um_o.visits_x, 0) AS visits_x_origin,
        COALESCE(um_o.cats_x, 0) AS cats_x_origin,
        COALESCE(um_o.spend_x, 0) AS spend_x_origin,
        COALESCE(um_n.visits_x, 0) AS visits_x_next,
        COALESCE(um_n.cats_x, 0) AS cats_x_next,
        COALESCE(um_n.spend_x, 0) AS spend_x_next,
        COALESCE(um_n.visits_total, 0) AS visits_total_next,
        COALESCE(um_n.cats_total, 0) AS cats_total_next
      FROM cohort_origin co
      INNER JOIN month_pairs mp ON co.origin_month = mp.origin_month
      LEFT JOIN user_month_stats um_o ON co.user_id = um_o.user_id AND co.origin_month = um_o.txn_month
      LEFT JOIN user_month_stats um_n ON co.user_id = um_n.user_id AND mp.next_month = um_n.txn_month
      WHERE mp.next_month IS NOT NULL
    ),
    user_buckets AS (
      SELECT user_id, origin_month,
        CASE
          WHEN visits_x_next > 0 AND spend_x_next >= spend_x_origin * 0.9 THEN 'RETAINED'
          WHEN visits_total_next > 0 AND cats_total_next > 0 AND cats_x_next = 0 THEN 'CATEGORY_GONE'
          WHEN visits_x_next > 0 AND cats_x_next < cats_x_origin THEN 'REDUCED_BASKET'
          WHEN visits_x_next > 0 AND visits_x_next < visits_x_origin THEN 'REDUCED_FREQ'
          WHEN visits_total_next > 0 AND visits_x_next = 0 THEN 'DELAYED_ONLY'
          ELSE 'FULL_CHURN'
        END AS bucket
      FROM user_transitions
    ),
    month_cohort_size AS (
      SELECT origin_month, COUNT(DISTINCT user_id) AS cohort_size
      FROM user_buckets
      GROUP BY origin_month
    ),
    bucket_counts AS (
      SELECT ub.origin_month, ub.bucket, COUNT(DISTINCT ub.user_id) AS users
      FROM user_buckets ub
      INNER JOIN month_cohort_size mcs ON ub.origin_month = mcs.origin_month
      WHERE mcs.cohort_size >= $${kIdx}
      GROUP BY ub.origin_month, ub.bucket
    )
    SELECT bc.origin_month, bc.bucket, bc.users,
      mcs.cohort_size,
      ROUND(100.0 * bc.users / NULLIF(mcs.cohort_size, 0), 2) AS pct
    FROM bucket_counts bc
    INNER JOIN month_cohort_size mcs ON bc.origin_month = mcs.origin_month
    ORDER BY bc.origin_month, 
      CASE bc.bucket 
        WHEN 'RETAINED' THEN 1 
        WHEN 'CATEGORY_GONE' THEN 2 
        WHEN 'REDUCED_BASKET' THEN 3 
        WHEN 'REDUCED_FREQ' THEN 4 
        WHEN 'DELAYED_ONLY' THEN 5 
        ELSE 6 
      END
  `;

  const { rows } = await pool.query(query, params);
  const duration = Date.now() - start;

  // Group by month
  const byMonth = {};
  const suppressedMonths = [];
  
  for (const r of rows) {
    const m = r.origin_month.toISOString().split('T')[0].substring(0, 7);
    if (!byMonth[m]) {
      byMonth[m] = { origin_month: m, cohort_size: Number(r.cohort_size), waterfall: [] };
    }
    byMonth[m].waterfall.push({
      bucket: r.bucket,
      users: Number(r.users),
      pct: Number(r.pct)
    });
  }

  // Check for suppressed months
  Object.values(byMonth).forEach(m => {
    if (m.cohort_size < minN) {
      suppressedMonths.push(m.origin_month);
    }
  });

  const data = Object.values(byMonth).filter(m => m.cohort_size >= minN);

  res.set('X-Query-Time-Ms', String(duration));
  res.json({
    filters: {
      start: startDate, end: endDate,
      issuer_ruc: issuerRuc, store_id: storeId,
      category_level: categoryLevel, category_value: categoryValue,
      reconcile_ok: reconcileOk, k_threshold: kThreshold, min_n: minN
    },
    data,
    suppressed_months: suppressedMonths,
    disclaimers: LEAKAGE_DISCLAIMERS
  });
}));

// =============================================================================
// SPRINT 4: BASKET MISSIONS
// =============================================================================

const BASKET_DISCLAIMERS = [
  'Panel observado (no proyectado). Sin claims causales.',
  'Ventana: [start,end) end-exclusive.',
  'reconcile_ok aplicado a nivel transacción.',
  'Breadth = # categorías distintas compradas.',
  'Mission share = breadth_in_x / breadth_total × 100.',
  'K-anonymity aplicado por mes.',
  'UNKNOWN incluido en categorías.'
];

app.get('/api/basket/breadth', asyncHandler(async (req, res) => {
  const startDate = parseDate(req.query.start);
  const endDate = parseDate(req.query.end);
  const issuerRuc = parseString(req.query.issuer_ruc);
  const storeId = parseString(req.query.store_id);
  const reconcileOk = parseBool(req.query.reconcile_ok, true);
  const categoryLevel = (req.query.category_level || 'l1').toLowerCase();
  const kThreshold = parseKThreshold(req.query.k_threshold);

  const errors = [];
  if (!startDate) errors.push('Invalid or missing start date');
  if (!endDate) errors.push('Invalid or missing end date');
  if (!issuerRuc) errors.push('issuer_ruc is required');
  const catCol = getCatCol(categoryLevel);
  if (!catCol) errors.push('category_level must be one of: l1, l2, l3, l4');
  if (errors.length > 0) return res.status(400).json({ error: errors.join('; ') });

  const start = Date.now();

  const query = `
    WITH base_txn AS (
      SELECT b.user_id, 
        DATE_TRUNC('month', b.invoice_date)::date AS txn_month,
        b.issuer_ruc, b.store_id,
        COALESCE(b.${catCol}, 'UNKNOWN') AS cat_val
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.user_id IS NOT NULL
    ),
    cohort AS (
      SELECT DISTINCT user_id, txn_month
      FROM base_txn
      WHERE issuer_ruc = $4 AND ($5::text IS NULL OR store_id::text = $5)
    ),
    user_month_breadth AS (
      SELECT c.user_id, c.txn_month,
        COUNT(DISTINCT bt.cat_val) AS breadth_total,
        COUNT(DISTINCT CASE WHEN bt.issuer_ruc = $4 AND ($5::text IS NULL OR bt.store_id::text = $5) THEN bt.cat_val END) AS breadth_in_x
      FROM cohort c
      INNER JOIN base_txn bt ON c.user_id = bt.user_id AND c.txn_month = bt.txn_month
      GROUP BY c.user_id, c.txn_month
    ),
    month_agg AS (
      SELECT txn_month AS origin_month,
        COUNT(DISTINCT user_id) AS users,
        ROUND(AVG(breadth_total), 2) AS avg_breadth_market,
        ROUND(AVG(breadth_in_x), 2) AS avg_breadth_in_x,
        ROUND(AVG(breadth_total - breadth_in_x), 2) AS gap_breadth_avg,
        ROUND(AVG(100.0 * breadth_in_x / NULLIF(breadth_total, 0)), 2) AS avg_mission_share_in_x_pct
      FROM user_month_breadth
      GROUP BY txn_month
      HAVING COUNT(DISTINCT user_id) >= $6
    )
    SELECT * FROM month_agg ORDER BY origin_month
  `;

  const { rows } = await pool.query(query, [
    startDate, endDate, reconcileOk, issuerRuc, storeId, kThreshold
  ]);
  const duration = Date.now() - start;

  res.set('X-Query-Time-Ms', String(duration));
  res.json({
    filters: {
      start: startDate, end: endDate,
      issuer_ruc: issuerRuc, store_id: storeId,
      category_level: categoryLevel,
      reconcile_ok: reconcileOk, k_threshold: kThreshold
    },
    data: rows.map(r => ({
      origin_month: r.origin_month.toISOString().split('T')[0].substring(0, 7),
      users: Number(r.users),
      avg_breadth_market: Number(r.avg_breadth_market),
      avg_breadth_in_x: Number(r.avg_breadth_in_x),
      gap_breadth_avg: Number(r.gap_breadth_avg),
      avg_mission_share_in_x_pct: Number(r.avg_mission_share_in_x_pct)
    })),
    disclaimers: BASKET_DISCLAIMERS
  });
}));

// =============================================================================
// SPRINT 5: BRAND LOYALTY v1.1
// =============================================================================

const LOYALTY_DISCLAIMERS = [
  'Panel observado (no proyectado). Sin claims causales.',
  'Ventana: [start,end) end-exclusive.',
  'reconcile_ok aplicado a nivel transacción.',
  'Eligibility: usuarios con >=min_receipts compras o >=min_months meses activos en la categoría.',
  'K-anonymity: marcas con brand_buyers < k se consolidan a OTHER_SUPPRESSED.',
  'UNKNOWN incluido; trust_level=SUPPRESSED siempre para UNKNOWN.',
  'user_brand_share_pct = 100 * brand_spend / category_spend por usuario elegible.',
  'Loyalty tiers: EXCLUSIVE (>=95%), LOYAL (>=80%), PREFER (>=50%), LIGHT (<50%).',
  'Scope: si issuer_ruc se especifica, métricas calculadas sobre transacciones en X.'
];

function trustLevel(eligibleUsers, minN, coveragePct, coverageThreshold) {
  if (eligibleUsers < minN || coveragePct < coverageThreshold) return 'SUPPRESSED';
  if (eligibleUsers < 30) return 'LOW';
  if (eligibleUsers < 100) return 'MEDIUM';
  return 'HIGH';
}

function suppressedReasons(eligibleUsers, minN, coveragePct, coverageThreshold) {
  const reasons = [];
  if (eligibleUsers < minN) reasons.push(`eligible_users (${eligibleUsers}) < min_n (${minN})`);
  if (coveragePct < coverageThreshold) reasons.push(`known_brand_coverage (${coveragePct}%) < threshold (${coverageThreshold}%)`);
  return reasons;
}

function validateLoyaltyParams(req, requireBrand = false, requireIssuer = false) {
  const startDate = parseDate(req.query.start);
  const endDate = parseDate(req.query.end);
  const categoryLevel = (req.query.category_level || 'l1').toLowerCase();
  const categoryValue = parseString(req.query.category_value);
  const brand = parseString(req.query.brand);
  const issuerRuc = parseString(req.query.issuer_ruc);
  const storeId = parseString(req.query.store_id);
  const reconcileOk = parseBool(req.query.reconcile_ok, true);

  const eligibilityMode = (req.query.eligibility_mode || 'receipts').toLowerCase();
  const minReceipts = Math.max(parseInt(req.query.min_receipts) || 2, 1);
  const minMonths = Math.max(parseInt(req.query.min_months) || 2, 1);
  const loyaltyThresholdPct = Math.min(Math.max(parseInt(req.query.loyalty_threshold_pct) || 80, 1), 100);
  const kThreshold = parseKThreshold(req.query.k_threshold);
  const minN = Math.max(parseInt(req.query.min_n) || 10, 1);
  const coverageThresholdPct = Math.max(parseInt(req.query.coverage_threshold_pct) || 60, 0);
  const limit = Math.min(Math.max(parseInt(req.query.limit) || 25, 1), 100);

  const errors = [];
  if (!startDate) errors.push('Invalid or missing start date');
  if (!endDate) errors.push('Invalid or missing end date');
  if (!categoryValue) errors.push('category_value is required');
  const catCol = getCatCol(categoryLevel);
  if (!catCol) errors.push('category_level must be one of: l1, l2, l3, l4');
  if (!['receipts', 'months'].includes(eligibilityMode)) errors.push('eligibility_mode must be receipts or months');
  if (requireBrand && !brand) errors.push('brand is required');
  if (requireIssuer && !issuerRuc) errors.push('issuer_ruc is required for retailer_compare');

  return {
    startDate, endDate, categoryLevel, categoryValue, brand,
    issuerRuc, storeId, reconcileOk,
    eligibilityMode, minReceipts, minMonths, loyaltyThresholdPct,
    kThreshold, minN, coverageThresholdPct, limit, catCol, errors
  };
}

function buildLoyaltyFilters(p, extras = {}) {
  return {
    start: p.startDate, end: p.endDate,
    category_level: p.categoryLevel, category_value: p.categoryValue,
    brand: p.brand, issuer_ruc: p.issuerRuc, store_id: p.storeId,
    reconcile_ok: p.reconcileOk, reconcile_scope: 'txn_level',
    eligibility_mode: p.eligibilityMode,
    min_receipts: p.minReceipts, min_months: p.minMonths,
    loyalty_threshold_pct: p.loyaltyThresholdPct,
    k_threshold: p.kThreshold, min_n: p.minN,
    coverage_threshold_pct: p.coverageThresholdPct,
    limit: p.limit,
    scope: p.issuerRuc ? 'retailer' : 'market',
    ...extras
  };
}

const AT_X_CONDITION = `(bt.issuer_ruc = $5 AND ($6::text IS NULL OR bt.store_id::text = $6))`;

function buildLoyaltyBaseCTEs(catCol, eligibilityMode, minReceipts, minMonths) {
  const eligibilityCondition = eligibilityMode === 'months'
    ? `COUNT(DISTINCT DATE_TRUNC('month', bt.invoice_date)) >= ${minMonths}`
    : `COUNT(DISTINCT bt.cufe) >= ${minReceipts}`;

  return `
    base_txn_all AS (
      SELECT b.user_id, b.cufe, b.invoice_date,
        DATE_TRUNC('month', b.invoice_date)::date AS month,
        b.issuer_ruc, b.store_id,
        COALESCE(b.${catCol}, 'UNKNOWN') AS cat_val,
        COALESCE(b.product_brand, 'UNKNOWN') AS brand,
        COALESCE(b.line_total, 0) AS line_total
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($4::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $4)
        AND b.user_id IS NOT NULL
        AND COALESCE(b.${catCol}, 'UNKNOWN') = $3
    ),
    base_txn AS (
      SELECT * FROM base_txn_all bt
      WHERE $5::text IS NULL OR ${AT_X_CONDITION}
    ),
    cohort AS (
      SELECT DISTINCT user_id FROM base_txn
    ),
    eligibility AS (
      SELECT bt.user_id
      FROM base_txn bt
      GROUP BY bt.user_id
      HAVING ${eligibilityCondition}
    ),
    coverage AS (
      SELECT
        (SELECT COUNT(DISTINCT user_id) FROM cohort) AS cohort_users,
        SUM(CASE WHEN brand != 'UNKNOWN' THEN line_total ELSE 0 END) AS known_spend,
        SUM(line_total) AS total_spend
      FROM base_txn
    ),
    user_brand_shares AS (
      SELECT
        bt.user_id, bt.brand,
        SUM(bt.line_total) AS brand_spend,
        cat_spend.category_spend,
        CASE WHEN cat_spend.category_spend > 0
          THEN ROUND(100.0 * SUM(bt.line_total) / cat_spend.category_spend, 2)
          ELSE 0 END AS share_pct
      FROM base_txn bt
      INNER JOIN eligibility e ON bt.user_id = e.user_id
      INNER JOIN (
        SELECT user_id, SUM(line_total) AS category_spend
        FROM base_txn bt2
        INNER JOIN eligibility e2 ON bt2.user_id = e2.user_id
        GROUP BY user_id
      ) cat_spend ON bt.user_id = cat_spend.user_id
      GROUP BY bt.user_id, bt.brand, cat_spend.category_spend
    )`;
}

function buildBrandsQuery(catCol, eligibilityMode, minReceipts, minMonths) {
  return `
    WITH ${buildLoyaltyBaseCTEs(catCol, eligibilityMode, minReceipts, minMonths)},
    brand_agg_raw AS (
      SELECT
        brand,
        COUNT(DISTINCT user_id) AS brand_buyers,
        ROUND(AVG(share_pct), 2) AS avg_share_pct,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY share_pct) AS p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY share_pct) AS p75,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY share_pct) AS p90,
        COUNT(DISTINCT user_id) FILTER (WHERE share_pct >= $8) AS loyal_users,
        COUNT(DISTINCT user_id) FILTER (WHERE share_pct >= 95) AS tier_exclusive,
        COUNT(DISTINCT user_id) FILTER (WHERE share_pct >= 80 AND share_pct < 95) AS tier_loyal,
        COUNT(DISTINCT user_id) FILTER (WHERE share_pct >= 50 AND share_pct < 80) AS tier_prefer,
        COUNT(DISTINCT user_id) FILTER (WHERE share_pct < 50) AS tier_light
      FROM user_brand_shares
      WHERE brand_spend > 0
      GROUP BY brand
    ),
    brand_kanon AS (
      SELECT
        CASE
          WHEN brand = 'UNKNOWN' THEN 'UNKNOWN'
          WHEN brand_buyers < $7 THEN 'OTHER_SUPPRESSED'
          ELSE brand
        END AS brand,
        brand_buyers, avg_share_pct, p50, p75, p90, loyal_users,
        tier_exclusive, tier_loyal, tier_prefer, tier_light,
        CASE WHEN brand = 'UNKNOWN' THEN TRUE
             WHEN brand_buyers < $7 THEN TRUE
             ELSE FALSE END AS suppressed
      FROM brand_agg_raw
    ),
    brand_agg AS (
      SELECT brand,
        SUM(brand_buyers) AS brand_buyers,
        ROUND(AVG(avg_share_pct), 2) AS avg_share_pct,
        ROUND(AVG(p50)::numeric, 2) AS p50,
        ROUND(AVG(p75)::numeric, 2) AS p75,
        ROUND(AVG(p90)::numeric, 2) AS p90,
        SUM(loyal_users) AS loyal_users,
        SUM(tier_exclusive) AS tier_exclusive,
        SUM(tier_loyal) AS tier_loyal,
        SUM(tier_prefer) AS tier_prefer,
        SUM(tier_light) AS tier_light,
        BOOL_OR(suppressed) AS suppressed
      FROM brand_kanon
      GROUP BY brand
    ),
    eligible_count AS (
      SELECT COUNT(*) AS eligible_users FROM eligibility
    )
    SELECT
      'brands' AS section,
      ba.brand,
      ba.brand_buyers,
      ec.eligible_users,
      ROUND(100.0 * ba.brand_buyers / NULLIF(ec.eligible_users, 0), 2) AS penetration_pct,
      ba.avg_share_pct,
      ROUND(ba.p50::numeric, 2) AS p50,
      ROUND(ba.p75::numeric, 2) AS p75,
      ROUND(ba.p90::numeric, 2) AS p90,
      ba.loyal_users,
      ROUND(100.0 * ba.loyal_users / NULLIF(ba.brand_buyers, 0), 2) AS loyalty_rate_pct,
      ba.tier_exclusive, ba.tier_loyal, ba.tier_prefer, ba.tier_light,
      ba.suppressed,
      c.cohort_users,
      ROUND(100.0 * ec.eligible_users / NULLIF(c.cohort_users, 0), 2) AS eligibility_pct,
      ROUND(100.0 * c.known_spend / NULLIF(c.total_spend, 0), 2) AS known_brand_coverage_pct
    FROM brand_agg ba
    CROSS JOIN eligible_count ec
    CROSS JOIN coverage c
    ORDER BY
      CASE ba.brand WHEN 'OTHER_SUPPRESSED' THEN 2 WHEN 'UNKNOWN' THEN 3 ELSE 1 END,
      ba.brand_buyers DESC
    LIMIT $9
  `;
}

app.get('/api/loyalty/brands', asyncHandler(async (req, res) => {
  const p = validateLoyaltyParams(req);
  if (p.errors.length > 0) return res.status(400).json({ error: p.errors.join('; ') });

  const start = Date.now();
  const query = buildBrandsQuery(p.catCol, p.eligibilityMode, p.minReceipts, p.minMonths);
  
  const { rows } = await pool.query(query, [
    p.startDate, p.endDate, p.categoryValue, p.reconcileOk,
    p.issuerRuc, p.storeId, p.kThreshold, p.loyaltyThresholdPct, p.limit
  ]);
  const duration = Date.now() - start;

  const brands = [];
  let eligibleUsers = 0, cohortUsers = 0, eligibilityPct = 0, coveragePct = 0;
  let otherSuppressedUsers = 0, suppressedBrandsCount = 0;

  for (const r of rows) {
    eligibleUsers = Number(r.eligible_users);
    cohortUsers = Number(r.cohort_users);
    eligibilityPct = Number(r.eligibility_pct);
    coveragePct = Number(r.known_brand_coverage_pct);

    if (r.brand === 'OTHER_SUPPRESSED') {
      otherSuppressedUsers = Number(r.brand_buyers);
      suppressedBrandsCount++;
      continue;
    }

    const brandTrust = r.brand === 'UNKNOWN' ? 'SUPPRESSED' : (r.suppressed ? 'SUPPRESSED' : null);

    brands.push({
      brand: r.brand,
      brand_buyers: Number(r.brand_buyers),
      penetration_pct: Number(r.penetration_pct),
      avg_share_pct: Number(r.avg_share_pct),
      p50: Number(r.p50), p75: Number(r.p75), p90: Number(r.p90),
      loyal_users: Number(r.loyal_users),
      loyalty_rate_pct: Number(r.loyalty_rate_pct),
      tiers: [
        { tier: 'EXCLUSIVE', users: Number(r.tier_exclusive), pct: Math.round(1000 * r.tier_exclusive / Math.max(r.brand_buyers, 1)) / 10 },
        { tier: 'LOYAL', users: Number(r.tier_loyal), pct: Math.round(1000 * r.tier_loyal / Math.max(r.brand_buyers, 1)) / 10 },
        { tier: 'PREFER', users: Number(r.tier_prefer), pct: Math.round(1000 * r.tier_prefer / Math.max(r.brand_buyers, 1)) / 10 },
        { tier: 'LIGHT', users: Number(r.tier_light), pct: Math.round(1000 * r.tier_light / Math.max(r.brand_buyers, 1)) / 10 }
      ],
      trust_level: brandTrust
    });
  }

  const windowTrust = trustLevel(eligibleUsers, p.minN, coveragePct, p.coverageThresholdPct);
  const suppReasons = suppressedReasons(eligibleUsers, p.minN, coveragePct, p.coverageThresholdPct);

  res.set('X-Query-Time-Ms', String(duration));
  res.json({
    filters: buildLoyaltyFilters(p),
    window_trust: {
      eligible_users: eligibleUsers,
      cohort_users: cohortUsers,
      eligibility_pct: eligibilityPct,
      known_brand_coverage_sales_pct: coveragePct,
      trust_level: windowTrust,
      suppressed_reasons: suppReasons
    },
    data: windowTrust === 'SUPPRESSED' ? [] : brands,
    suppressed: {
      other_suppressed_users: otherSuppressedUsers,
      suppressed_brands_count: suppressedBrandsCount
    },
    disclaimers: LOYALTY_DISCLAIMERS
  });
}));

// =============================================================================
// SPRINT 6: PANEL & PROJECTION
// =============================================================================

const PANEL_DISCLAIMERS = [
  'Panel observado; no representa universo sin calibración oficial.',
  'Proyección referencial basada en factor de expansión configurable.',
  'Sin inferencia causal. Solo observaciones del panel LÜM.',
  'Expansion factor es estimado; no sustituye proyección demográfica certificada.'
];

function getExpansionConfig() {
  const defaultFactor = parseFloat(process.env.PANEL_EXPANSION_FACTOR_DEFAULT) || 100;
  let overrides = {};
  try {
    const overridesJson = process.env.PANEL_EXPANSION_OVERRIDES;
    if (overridesJson) overrides = JSON.parse(overridesJson);
  } catch (e) {
    console.warn('Failed to parse PANEL_EXPANSION_OVERRIDES:', e.message);
  }
  return { defaultFactor, overrides };
}

function getExpansionFactor(issuerRuc, categoryL1) {
  const config = getExpansionConfig();
  if (issuerRuc && config.overrides[`issuer_ruc:${issuerRuc}`]) {
    return { factor: config.overrides[`issuer_ruc:${issuerRuc}`], source: `override:issuer_ruc:${issuerRuc}` };
  }
  if (categoryL1 && config.overrides[`category_l1:${categoryL1}`]) {
    return { factor: config.overrides[`category_l1:${categoryL1}`], source: `override:category_l1:${categoryL1}` };
  }
  return { factor: config.defaultFactor, source: 'default' };
}

app.get('/api/panel/summary', asyncHandler(async (req, res) => {
  const startDate = parseDate(req.query.start);
  const endDate = parseDate(req.query.end);
  const issuerRuc = parseString(req.query.issuer_ruc);
  const storeId = parseString(req.query.store_id);
  const reconcileOk = parseBool(req.query.reconcile_ok, true);
  const categoryL1 = parseString(req.query.category_l1);

  const errors = [];
  if (!startDate) errors.push('Invalid or missing start date');
  if (!endDate) errors.push('Invalid or missing end date');
  if (!issuerRuc) errors.push('issuer_ruc is required');
  if (errors.length > 0) return res.status(400).json({ error: errors.join('; ') });

  const start = Date.now();

  const query = `
    WITH base_txn AS (
      SELECT b.user_id, b.cufe, b.invoice_date, b.issuer_ruc, b.store_id,
        COALESCE(b.line_total, 0) AS line_total
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.user_id IS NOT NULL
    ),
    panel_x AS (
      SELECT
        COUNT(DISTINCT user_id) AS customers_n,
        SUM(line_total) AS spend_in_x_usd,
        MIN(invoice_date) AS first_txn,
        MAX(invoice_date) AS last_txn,
        COUNT(DISTINCT cufe) AS invoices_n,
        COUNT(DISTINCT DATE_TRUNC('month', invoice_date)) AS active_months
      FROM base_txn
      WHERE issuer_ruc = $4 AND ($5::text IS NULL OR store_id::text = $5)
    ),
    panel_market AS (
      SELECT SUM(line_total) AS spend_market_usd
      FROM base_txn
      WHERE user_id IN (
        SELECT DISTINCT user_id FROM base_txn
        WHERE issuer_ruc = $4 AND ($5::text IS NULL OR store_id::text = $5)
      )
    )
    SELECT px.*, pm.spend_market_usd,
      ROUND(100.0 * px.spend_in_x_usd / NULLIF(pm.spend_market_usd, 0), 2) AS sow_pct,
      CASE WHEN px.customers_n > 0 
        THEN ROUND(px.spend_in_x_usd / px.customers_n, 2) 
        ELSE 0 END AS avg_spend_per_customer
    FROM panel_x px CROSS JOIN panel_market pm
  `;

  const { rows } = await pool.query(query, [startDate, endDate, reconcileOk, issuerRuc, storeId]);
  const duration = Date.now() - start;

  const r = rows[0] || {};
  const customersN = Number(r.customers_n || 0);
  const expansion = getExpansionFactor(issuerRuc, categoryL1);

  res.set('X-Query-Time-Ms', String(duration));
  res.json({
    filters: {
      start: startDate, end: endDate,
      issuer_ruc: issuerRuc, store_id: storeId,
      reconcile_ok: reconcileOk
    },
    panel: {
      customers_n: customersN,
      spend_in_x_usd: Math.round(Number(r.spend_in_x_usd || 0) * 100) / 100,
      spend_market_usd: Math.round(Number(r.spend_market_usd || 0) * 100) / 100,
      sow_pct: Number(r.sow_pct || 0),
      invoices_n: Number(r.invoices_n || 0),
      active_months: Number(r.active_months || 0),
      avg_spend_per_customer: Number(r.avg_spend_per_customer || 0),
      first_txn: r.first_txn ? r.first_txn.toISOString().split('T')[0] : null,
      last_txn: r.last_txn ? r.last_txn.toISOString().split('T')[0] : null
    },
    projection: {
      method: 'expansion_factor',
      expansion_factor: expansion.factor,
      expansion_source: expansion.source,
      projected_households: Math.round(customersN * expansion.factor),
      notes: ['referential_projection', 'not_demographically_calibrated']
    },
    disclaimers: PANEL_DISCLAIMERS
  });
}));

app.get('/api/panel/config', (req, res) => {
  const config = getExpansionConfig();
  res.json({
    method: 'expansion_factor',
    default_factor: config.defaultFactor,
    overrides_count: Object.keys(config.overrides).length,
    override_keys: Object.keys(config.overrides),
    disclaimers: PANEL_DISCLAIMERS
  });
});

// =============================================================================
// SPRINT 7: DECK HTML BUILDER
// =============================================================================

const DECK_DISCLAIMERS = [
  'Panel observado LÜM. No representa universo sin calibración.',
  'Proyección referencial. Sin inferencia causal.',
  'Ventana end-exclusive. reconcile_ok aplicado a nivel txn.',
  'K-anonymity: items con N < k consolidados a OTHER_SUPPRESSED.',
  'UNKNOWN incluido pero marcado SUPPRESSED.',
  'Auto-selección determinística: top category por spend, top brand por buyers.'
];

function generateDeckHTML(data) {
  const { panel, capture, switching, leakage, basket, loyalty, filters, generatedAt } = data;
  
  const trustBadge = (level) => {
    const styles = {
      HIGH: 'background: linear-gradient(135deg, #10b981, #059669); box-shadow: 0 0 12px rgba(16, 185, 129, 0.5);',
      MEDIUM: 'background: linear-gradient(135deg, #f59e0b, #d97706); box-shadow: 0 0 12px rgba(245, 158, 11, 0.5);',
      LOW: 'background: linear-gradient(135deg, #f97316, #ea580c); box-shadow: 0 0 12px rgba(249, 115, 22, 0.5);',
      SUPPRESSED: 'background: linear-gradient(135deg, #ef4444, #dc2626); box-shadow: 0 0 12px rgba(239, 68, 68, 0.5);'
    };
    return `<span class="trust-badge" style="${styles[level] || 'background: #475569;'}">${level || 'N/A'}</span>`;
  };

  const formatNum = (n) => n != null ? n.toLocaleString('en-US') : '—';
  const formatPct = (n) => n != null ? `${Number(n).toFixed(1)}%` : '—';
  const formatUSD = (n) => n != null ? `$${Number(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : '—';

  const startD = new Date(filters.start);
  const endD = new Date(filters.end);
  const windowDays = Math.round((endD - startD) / (1000 * 60 * 60 * 24));

  return `<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>LÜM Radiance | Commerce Insights</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg-primary: #0a0e1a;
      --bg-card: linear-gradient(145deg, #1a1f35 0%, #141827 100%);
      --border-card: rgba(99, 102, 241, 0.2);
      --text-primary: #f1f5f9;
      --text-secondary: #94a3b8;
      --text-muted: #64748b;
      --accent-cyan: #06b6d4;
      --accent-purple: #8b5cf6;
      --accent-gradient: linear-gradient(135deg, #06b6d4, #8b5cf6, #ec4899);
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Inter', sans-serif;
      background: var(--bg-primary);
      color: var(--text-primary);
      line-height: 1.6;
      min-height: 100vh;
    }
    body::before {
      content: '';
      position: fixed;
      top: 0; left: 0; right: 0; bottom: 0;
      background-image: linear-gradient(rgba(99, 102, 241, 0.03) 1px, transparent 1px),
        linear-gradient(90deg, rgba(99, 102, 241, 0.03) 1px, transparent 1px);
      background-size: 50px 50px;
      pointer-events: none;
      z-index: -1;
    }
    .container { max-width: 1200px; margin: 0 auto; padding: 32px 24px; }
    .header { text-align: center; margin-bottom: 48px; }
    .logo {
      font-size: 14px; font-weight: 700; letter-spacing: 4px; text-transform: uppercase;
      background: var(--accent-gradient);
      -webkit-background-clip: text; -webkit-text-fill-color: transparent;
      margin-bottom: 8px;
    }
    .header h1 {
      font-size: 36px; font-weight: 800; margin-bottom: 12px;
      background: linear-gradient(135deg, #fff 0%, #94a3b8 100%);
      -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    .header .subtitle { color: var(--text-secondary); font-size: 16px; margin-bottom: 24px; }
    .meta-chips { display: flex; justify-content: center; gap: 12px; flex-wrap: wrap; }
    .chip {
      background: rgba(99, 102, 241, 0.1);
      border: 1px solid rgba(99, 102, 241, 0.3);
      padding: 8px 16px; border-radius: 24px; font-size: 13px;
      font-family: 'JetBrains Mono', monospace; color: var(--text-secondary);
      display: flex; align-items: center; gap: 6px;
    }
    .chip .label { color: var(--text-muted); }
    .chip .value { color: var(--accent-cyan); font-weight: 500; }
    .card {
      background: var(--bg-card);
      border: 1px solid var(--border-card);
      border-radius: 16px; padding: 28px; margin-bottom: 24px;
      position: relative; overflow: hidden;
    }
    .card::before {
      content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px;
      background: var(--accent-gradient); opacity: 0.6;
    }
    .card-header { display: flex; align-items: flex-start; justify-content: space-between; margin-bottom: 20px; }
    .card-title { display: flex; align-items: center; gap: 12px; }
    .card-icon {
      width: 40px; height: 40px; border-radius: 12px;
      display: flex; align-items: center; justify-content: center; font-size: 20px;
      background: rgba(99, 102, 241, 0.15); border: 1px solid rgba(99, 102, 241, 0.3);
    }
    .card h2 { font-size: 18px; font-weight: 700; margin-bottom: 4px; }
    .card .question { font-size: 13px; color: var(--text-muted); font-style: italic; }
    .trust-badge {
      padding: 6px 14px; border-radius: 20px; font-size: 11px; font-weight: 600;
      text-transform: uppercase; letter-spacing: 0.5px; color: white;
    }
    .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 20px; margin-bottom: 24px; }
    .stat {
      background: rgba(0, 0, 0, 0.3); border: 1px solid rgba(255, 255, 255, 0.05);
      border-radius: 12px; padding: 20px; text-align: center;
    }
    .stat .value {
      font-size: 32px; font-weight: 800;
      background: linear-gradient(135deg, var(--accent-cyan), var(--accent-purple));
      -webkit-background-clip: text; -webkit-text-fill-color: transparent;
      line-height: 1.2; margin-bottom: 4px;
    }
    .stat .label { font-size: 11px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 1px; }
    .projection-box {
      background: linear-gradient(135deg, rgba(6, 182, 212, 0.15), rgba(139, 92, 246, 0.15));
      border: 1px solid rgba(139, 92, 246, 0.3);
      border-radius: 16px; padding: 24px; margin-top: 20px; text-align: center;
      position: relative; overflow: hidden;
    }
    .projection-box .label-top { font-size: 12px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 2px; margin-bottom: 8px; }
    .projection-box .big {
      font-size: 48px; font-weight: 800;
      background: var(--accent-gradient);
      -webkit-background-clip: text; -webkit-text-fill-color: transparent;
      line-height: 1.1;
    }
    .projection-box .unit { font-size: 18px; color: var(--text-secondary); margin-top: 4px; }
    .projection-box .note { font-size: 11px; color: var(--text-muted); margin-top: 12px; font-family: 'JetBrains Mono', monospace; }
    .table-wrapper { overflow-x: auto; border-radius: 12px; border: 1px solid rgba(255, 255, 255, 0.05); }
    table { width: 100%; border-collapse: collapse; font-size: 14px; }
    th { text-align: left; padding: 14px 16px; background: rgba(0, 0, 0, 0.4); color: var(--text-muted); font-weight: 600; font-size: 11px; text-transform: uppercase; }
    td { padding: 14px 16px; border-bottom: 1px solid rgba(255, 255, 255, 0.03); color: var(--text-secondary); }
    tr:hover td { background: rgba(139, 92, 246, 0.05); color: var(--text-primary); }
    .text-right { text-align: right; }
    .highlight { color: var(--accent-cyan); font-weight: 600; }
    .rank { display: inline-flex; align-items: center; justify-content: center; width: 24px; height: 24px; background: rgba(139, 92, 246, 0.2); border-radius: 6px; font-size: 12px; font-weight: 600; color: var(--accent-purple); }
    .section-empty { color: var(--text-muted); font-style: italic; padding: 40px; text-align: center; background: rgba(0, 0, 0, 0.2); border-radius: 12px; border: 1px dashed rgba(255, 255, 255, 0.1); }
    .disclaimers { background: rgba(234, 179, 8, 0.1); border: 1px solid rgba(234, 179, 8, 0.3); border-radius: 12px; padding: 24px; margin-top: 32px; }
    .disclaimers h3 { font-size: 14px; color: #fbbf24; margin-bottom: 12px; }
    .disclaimers ul { margin-left: 20px; color: var(--text-muted); font-size: 12px; }
    .disclaimers li { margin-bottom: 6px; }
    .footer { text-align: center; margin-top: 48px; padding-top: 24px; border-top: 1px solid rgba(255, 255, 255, 0.05); }
    .footer .brand { font-size: 12px; font-weight: 700; letter-spacing: 3px; background: var(--accent-gradient); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
    .footer .timestamp { font-size: 11px; color: var(--text-muted); font-family: 'JetBrains Mono', monospace; margin-top: 8px; }
  </style>
</head>
<body>
<div class="container">
  <header class="header">
    <div class="logo">LÜM RADIANCE</div>
    <h1>Commerce Insights Deck</h1>
    <p class="subtitle">Panel analytics para <strong>${filters.issuer_ruc}</strong> · ${windowDays} días de observación</p>
    <div class="meta-chips">
      <div class="chip"><span class="label">Ventana</span><span class="value">${filters.start} → ${filters.end}</span></div>
      <div class="chip"><span class="label">reconcile_ok</span><span class="value">${filters.reconcile_ok ?? 'all'}</span></div>
      <div class="chip"><span class="label">k_threshold</span><span class="value">${filters.k_threshold || 5}</span></div>
    </div>
  </header>

  <div class="card">
    <div class="card-header">
      <div class="card-title">
        <div class="card-icon">👥</div>
        <div><h2>Panel Observado</h2><p class="question">¿Cuántos clientes del panel sostienen estos insights?</p></div>
      </div>
    </div>
    <div class="stats-grid">
      <div class="stat"><div class="value">${formatNum(panel?.customers_n)}</div><div class="label">Clientes Panel</div></div>
      <div class="stat"><div class="value">${formatUSD(panel?.spend_in_x_usd)}</div><div class="label">Gasto en X</div></div>
      <div class="stat"><div class="value">${formatPct(panel?.sow_pct)}</div><div class="label">Share of Wallet</div></div>
      <div class="stat"><div class="value">${formatNum(panel?.invoices_n)}</div><div class="label">Facturas</div></div>
    </div>
    ${panel?.projection ? `
    <div class="projection-box">
      <div class="label-top">Proyección Referencial</div>
      <div class="big">≈ ${formatNum(panel.projection.projected_households)}</div>
      <div class="unit">hogares estimados</div>
      <div class="note">Factor: ${panel.projection.expansion_factor}x · ${panel.projection.notes?.join(' · ') || 'referential'}</div>
    </div>` : ''}
  </div>

  <div class="card">
    <div class="card-header">
      <div class="card-title">
        <div class="card-icon">🎯</div>
        <div><h2>Commerce Capture</h2><p class="question">¿Cuánto del gasto de categoría captura X vs mercado?</p></div>
      </div>
    </div>
    ${capture?.data?.length > 0 ? `
    <div class="table-wrapper">
      <table>
        <thead><tr><th>#</th><th>Categoría</th><th class="text-right">Users</th><th class="text-right">Gasto X</th><th class="text-right">SoW %</th></tr></thead>
        <tbody>
          ${capture.data.slice(0, 10).map((r, i) => `
          <tr>
            <td><span class="rank">${i + 1}</span></td>
            <td class="highlight">${r.category_value || '—'}</td>
            <td class="text-right">${formatNum(r.users)}</td>
            <td class="text-right">${formatUSD(r.spend_in_x_usd)}</td>
            <td class="text-right highlight">${formatPct(r.sow_pct)}</td>
          </tr>`).join('')}
        </tbody>
      </table>
    </div>` : '<div class="section-empty">Sin datos de capture disponibles</div>'}
  </div>

  <div class="card">
    <div class="card-header">
      <div class="card-title">
        <div class="card-icon">🔄</div>
        <div><h2>Switching Destinations</h2><p class="question">¿A dónde se van los clientes cuando no compran en X?</p></div>
      </div>
    </div>
    ${switching?.data?.length > 0 ? `
    <div class="table-wrapper">
      <table>
        <thead><tr><th>#</th><th>Destino</th><th class="text-right">Users</th><th class="text-right">% Cohort</th></tr></thead>
        <tbody>
          ${switching.data.slice(0, 10).map((r, i) => `
          <tr>
            <td><span class="rank">${i + 1}</span></td>
            <td class="highlight">${r.destination || r.dest_name || '—'}</td>
            <td class="text-right">${formatNum(r.users)}</td>
            <td class="text-right highlight">${formatPct(r.pct || r.pct_of_cohort)}</td>
          </tr>`).join('')}
        </tbody>
      </table>
    </div>` : '<div class="section-empty">Sin datos de switching disponibles</div>'}
  </div>

  <div class="card">
    <div class="card-header">
      <div class="card-title">
        <div class="card-icon">💎</div>
        <div><h2>Brand Loyalty</h2><p class="question">¿A qué marcas son leales los compradores?${loyalty?.category ? ` (${loyalty.category})` : ''}</p></div>
      </div>
      ${loyalty?.window_trust ? trustBadge(loyalty.window_trust.trust_level) : ''}
    </div>
    ${loyalty?.data?.length > 0 ? `
    <div class="table-wrapper">
      <table>
        <thead><tr><th>#</th><th>Marca</th><th class="text-right">Buyers</th><th class="text-right">Penetración</th><th class="text-right">Loyalty Rate</th></tr></thead>
        <tbody>
          ${loyalty.data.filter(r => r.brand !== 'OTHER_SUPPRESSED').slice(0, 10).map((r, i) => `
          <tr>
            <td><span class="rank">${i + 1}</span></td>
            <td class="highlight">${r.brand}${r.trust_level === 'SUPPRESSED' ? ' ⚠️' : ''}</td>
            <td class="text-right">${formatNum(r.brand_buyers)}</td>
            <td class="text-right">${formatPct(r.penetration_pct)}</td>
            <td class="text-right highlight">${formatPct(r.loyalty_rate_pct)}</td>
          </tr>`).join('')}
        </tbody>
      </table>
    </div>` : '<div class="section-empty">Sin datos de loyalty · Requiere category_value</div>'}
  </div>

  <div class="disclaimers">
    <h3>⚠️ Disclaimers Obligatorios</h3>
    <ul>${DECK_DISCLAIMERS.map(d => `<li>${d}</li>`).join('')}</ul>
  </div>

  <footer class="footer">
    <div class="brand">LÜM RADIANCE</div>
    <div class="timestamp">Generated: ${generatedAt} · Panel Analytics Platform</div>
  </footer>
</div>
</body>
</html>`;
}

app.get('/api/deck/commerce', asyncHandler(async (req, res) => {
  const startDate = parseDate(req.query.start);
  const endDate = parseDate(req.query.end);
  const issuerRuc = parseString(req.query.issuer_ruc);
  const storeId = parseString(req.query.store_id);
  const reconcileOk = parseBool(req.query.reconcile_ok, true);
  const kThreshold = parseKThreshold(req.query.k_threshold);
  const minN = parseInt(req.query.min_n) || 10;
  const categoryLevel = (req.query.category_level || 'l1').toLowerCase();
  const format = req.query.format || 'html';

  const errors = [];
  if (!startDate) errors.push('Invalid or missing start date');
  if (!endDate) errors.push('Invalid or missing end date');
  if (!issuerRuc) errors.push('issuer_ruc is required');
  if (errors.length > 0) return res.status(400).json({ error: errors.join('; ') });

  const filters = { start: startDate, end: endDate, issuer_ruc: issuerRuc, store_id: storeId, reconcile_ok: reconcileOk, k_threshold: kThreshold, min_n: minN, category_level: categoryLevel };
  const catCol = VALID_CAT_COLS[categoryLevel] || 'category_l1';
  const start = Date.now();

  // Panel
  const panelQuery = `
    WITH base_txn AS (
      SELECT b.user_id, b.cufe, b.invoice_date, b.issuer_ruc, b.store_id, COALESCE(b.line_total, 0) AS line_total
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3) AND b.user_id IS NOT NULL
    ),
    panel_x AS (
      SELECT COUNT(DISTINCT user_id) AS customers_n, SUM(line_total) AS spend_in_x_usd, COUNT(DISTINCT cufe) AS invoices_n
      FROM base_txn WHERE issuer_ruc = $4 AND ($5::text IS NULL OR store_id::text = $5)
    ),
    panel_market AS (
      SELECT SUM(line_total) AS spend_market_usd FROM base_txn
      WHERE user_id IN (SELECT DISTINCT user_id FROM base_txn WHERE issuer_ruc = $4)
    )
    SELECT px.*, pm.spend_market_usd, ROUND(100.0 * px.spend_in_x_usd / NULLIF(pm.spend_market_usd, 0), 2) AS sow_pct
    FROM panel_x px CROSS JOIN panel_market pm
  `;
  const panelResult = await pool.query(panelQuery, [startDate, endDate, reconcileOk, issuerRuc, storeId]);
  const panelRow = panelResult.rows[0] || {};
  const expansion = getExpansionFactor(issuerRuc, null);
  
  const panel = {
    customers_n: Number(panelRow.customers_n || 0),
    spend_in_x_usd: Number(panelRow.spend_in_x_usd || 0),
    sow_pct: Number(panelRow.sow_pct || 0),
    invoices_n: Number(panelRow.invoices_n || 0),
    projection: {
      expansion_factor: expansion.factor,
      projected_households: Math.round(Number(panelRow.customers_n || 0) * expansion.factor),
      notes: ['referential_projection']
    }
  };

  // Capture
  const captureQuery = `
    WITH cohort AS (
      SELECT DISTINCT b.user_id FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.issuer_ruc = $4 AND b.user_id IS NOT NULL
    ),
    user_cat AS (
      SELECT b.user_id, COALESCE(b.${catCol}, 'UNKNOWN') AS category_value, b.issuer_ruc, SUM(COALESCE(b.line_total, 0)) AS spend
      FROM analytics.radiance_base_v1 b
      INNER JOIN cohort c ON b.user_id = c.user_id
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
      GROUP BY b.user_id, category_value, b.issuer_ruc
    )
    SELECT category_value, COUNT(DISTINCT user_id) AS users,
      SUM(CASE WHEN issuer_ruc = $4 THEN spend ELSE 0 END) AS spend_in_x_usd,
      SUM(spend) AS spend_market_usd,
      ROUND(100.0 * SUM(CASE WHEN issuer_ruc = $4 THEN spend ELSE 0 END) / NULLIF(SUM(spend), 0), 2) AS sow_pct
    FROM user_cat GROUP BY category_value HAVING COUNT(DISTINCT user_id) >= $5
    ORDER BY spend_in_x_usd DESC LIMIT 15
  `;
  const captureResult = await pool.query(captureQuery, [startDate, endDate, reconcileOk, issuerRuc, kThreshold]);

  // Switching
  const switchingQuery = `
    WITH cohort AS (
      SELECT DISTINCT user_id FROM analytics.radiance_base_v1
      WHERE invoice_date >= $1::date AND invoice_date < $2::date AND issuer_ruc = $4
    ),
    elsewhere AS (
      SELECT b.user_id, COALESCE(b.issuer_name, b.issuer_ruc) AS destination
      FROM analytics.radiance_base_v1 b
      INNER JOIN cohort c ON b.user_id = c.user_id
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.issuer_ruc != $4
    )
    SELECT destination, COUNT(DISTINCT user_id) AS users,
      ROUND(100.0 * COUNT(DISTINCT user_id) / NULLIF((SELECT COUNT(*) FROM cohort), 0), 2) AS pct
    FROM elsewhere GROUP BY destination HAVING COUNT(DISTINCT user_id) >= $5
    ORDER BY users DESC LIMIT 10
  `;
  const switchingResult = await pool.query(switchingQuery, [startDate, endDate, reconcileOk, issuerRuc, kThreshold]);

  // Auto-select top category for loyalty
  const topCatQuery = `
    SELECT COALESCE(b.${catCol}, 'UNKNOWN') AS cat_val, SUM(COALESCE(b.line_total, 0)) AS spend
    FROM analytics.radiance_base_v1 b
    LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
    WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
      AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
      AND b.issuer_ruc = $4 AND b.user_id IS NOT NULL
    GROUP BY cat_val HAVING cat_val NOT IN ('UNKNOWN', 'OTHER_SUPPRESSED')
    ORDER BY spend DESC LIMIT 1
  `;
  const topCatResult = await pool.query(topCatQuery, [startDate, endDate, reconcileOk, issuerRuc]);
  const topCategory = topCatResult.rows[0]?.cat_val || null;

  // Loyalty for top category
  let loyaltyData = [];
  let loyaltyTrust = null;
  if (topCategory) {
    const loyaltyQuery = `
      WITH base AS (
        SELECT b.user_id, COALESCE(b.product_brand, 'UNKNOWN') AS brand, SUM(COALESCE(b.line_total, 0)) AS brand_spend
        FROM analytics.radiance_base_v1 b
        LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
        WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
          AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
          AND b.issuer_ruc = $4 AND b.user_id IS NOT NULL
          AND COALESCE(b.${catCol}, 'UNKNOWN') = $5
        GROUP BY b.user_id, brand
      ),
      user_cat_spend AS (SELECT user_id, SUM(brand_spend) AS cat_spend FROM base GROUP BY user_id),
      shares AS (
        SELECT b.user_id, b.brand, b.brand_spend, u.cat_spend,
          ROUND(100.0 * b.brand_spend / NULLIF(u.cat_spend, 0), 2) AS share_pct
        FROM base b JOIN user_cat_spend u ON b.user_id = u.user_id WHERE b.brand_spend > 0
      ),
      brand_agg AS (
        SELECT brand, COUNT(DISTINCT user_id) AS brand_buyers,
          ROUND(100.0 * COUNT(DISTINCT user_id) / NULLIF((SELECT COUNT(*) FROM user_cat_spend), 0), 2) AS penetration_pct,
          COUNT(DISTINCT user_id) FILTER (WHERE share_pct >= 80) AS loyal_users
        FROM shares GROUP BY brand
      )
      SELECT brand, brand_buyers, penetration_pct, loyal_users,
        ROUND(100.0 * loyal_users / NULLIF(brand_buyers, 0), 2) AS loyalty_rate_pct,
        (SELECT COUNT(*) FROM user_cat_spend) AS eligible_users
      FROM brand_agg WHERE brand_buyers >= $6 OR brand = 'UNKNOWN'
      ORDER BY CASE brand WHEN 'UNKNOWN' THEN 2 ELSE 1 END, brand_buyers DESC LIMIT 10
    `;
    const loyaltyResult = await pool.query(loyaltyQuery, [startDate, endDate, reconcileOk, issuerRuc, topCategory, kThreshold]);
    if (loyaltyResult.rows.length > 0) {
      const eligibleUsers = Number(loyaltyResult.rows[0]?.eligible_users || 0);
      loyaltyTrust = {
        eligible_users: eligibleUsers,
        trust_level: eligibleUsers < minN ? 'SUPPRESSED' : eligibleUsers < 30 ? 'LOW' : eligibleUsers < 100 ? 'MEDIUM' : 'HIGH'
      };
      loyaltyData = loyaltyResult.rows.map(r => ({
        brand: r.brand,
        brand_buyers: Number(r.brand_buyers),
        penetration_pct: Number(r.penetration_pct),
        loyalty_rate_pct: Number(r.loyalty_rate_pct),
        trust_level: r.brand === 'UNKNOWN' ? 'SUPPRESSED' : null
      }));
    }
  }

  const duration = Date.now() - start;
  const deckData = {
    filters: { ...filters, auto_selected_category: topCategory },
    generatedAt: new Date().toISOString(),
    panel,
    capture: { data: captureResult.rows.map(r => ({ ...r, users: Number(r.users), spend_in_x_usd: Number(r.spend_in_x_usd), sow_pct: Number(r.sow_pct) })) },
    switching: { data: switchingResult.rows.map(r => ({ ...r, users: Number(r.users), pct: Number(r.pct) })) },
    leakage: { data: [] },
    basket: { breadth: [] },
    loyalty: { data: loyaltyData, window_trust: loyaltyTrust, category: topCategory }
  };

  res.set('X-Query-Time-Ms', String(duration));
  if (format === 'json') {
    res.json(deckData);
  } else {
    res.set('Content-Type', 'text/html; charset=utf-8');
    res.send(generateDeckHTML(deckData));
  }
}));

// =============================================================================
// ERROR HANDLING
// =============================================================================

app.use((err, req, res, next) => {
  console.error(`[ERROR] ${req.method} ${req.path}:`, err.message);
  res.status(500).json({ error: 'Internal server error', message: err.message });
});

// =============================================================================
// START SERVER
// =============================================================================

const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Radiance API v2.0 running on port ${PORT}`);
  console.log(`   Endpoints: /, /api/health, /api/sow_leakage/*, /api/switching/*, /api/leakage/*, /api/basket/*, /api/loyalty/*, /api/panel/*, /api/deck/*`);
});
