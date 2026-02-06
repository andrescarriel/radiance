// =============================================================================
// RADIANCE SPRINT 1 - CAPTURE EXPLORER BACKEND
// Version: 1.0.0
// =============================================================================

require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 3000;

// =============================================================================
// DATABASE CONNECTION
// =============================================================================
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000
});

pool.query('SELECT NOW()')
  .then(() => console.log('✅ Database connected'))
  .catch(err => {
    console.error('❌ Database connection failed:', err.message);
    process.exit(1);
  });

app.use(cors());
app.use(express.json());

// =============================================================================
// IN-MEMORY CACHE (TTL 60s)
// =============================================================================
const cache = new Map();
const CACHE_TTL_MS = 60 * 1000;

function getCacheKey(endpoint, params) {
  const paramsStr = JSON.stringify(params, Object.keys(params).sort());
  return crypto.createHash('md5').update(`${endpoint}:${paramsStr}`).digest('hex');
}

function getFromCache(key) {
  const entry = cache.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expires) {
    cache.delete(key);
    return null;
  }
  return entry.data;
}

function setCache(key, data) {
  cache.set(key, { data, expires: Date.now() + CACHE_TTL_MS });
}

// =============================================================================
// REQUEST LOGGING
// =============================================================================
function logRequest(endpoint, params, durationMs, rows, cacheHit) {
  const paramsHash = crypto.createHash('md5').update(JSON.stringify(params)).digest('hex').slice(0, 8);
  console.log(JSON.stringify({
    ts: new Date().toISOString(),
    endpoint,
    params_hash: paramsHash,
    duration_ms: durationMs,
    rows: rows,
    cache_hit: cacheHit
  }));
}

// =============================================================================
// PARAMETER HELPERS
// =============================================================================
function parseDate(val) {
  if (!val) return null;
  const d = new Date(val);
  if (isNaN(d.getTime())) return null;
  return val;
}

function parseString(val) {
  if (val === undefined || val === null || val === '' || val === 'null' || val === 'undefined') {
    return null;
  }
  return String(val).trim();
}

function parseBool(val, defaultVal = true) {
  if (val === undefined || val === null || val === '' || val === 'all') return null;
  if (val === 'true' || val === '1' || val === true) return true;
  if (val === 'false' || val === '0' || val === false) return false;
  return defaultVal;
}

function parseCategoryLevel(val) {
  const valid = ['l1', 'l2', 'l3', 'l4'];
  if (valid.includes(val)) return val;
  return 'l1';
}

// =============================================================================
// ASYNC HANDLER
// =============================================================================
const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

// =============================================================================
// DISCLAIMER (Sprint 1 - Non-negotiable)
// =============================================================================
const DISCLAIMER = {
  panel_type: 'observed',
  panel_definition: 'N customers with >=1 purchase in selected retailer within window',
  notes: 'SoW and leakage are observational (not projected). No causal claims.',
  trust_explainer: 'Trust uses N + coverage + reconcile_ok filter'
};

// =============================================================================
// ENDPOINT 1: GET /api/health
// =============================================================================
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok' });
});

// =============================================================================
// ENDPOINT 2: GET /api/meta/dq_latest
// =============================================================================
app.get('/api/meta/dq_latest', asyncHandler(async (req, res) => {
  const start = Date.now();
  const bypassCache = req.query.cache === '0';
  const cacheKey = getCacheKey('/api/meta/dq_latest', {});
  
  if (!bypassCache) {
    const cached = getFromCache(cacheKey);
    if (cached) {
      res.set('X-Query-Time-Ms', '0');
      logRequest('/api/meta/dq_latest', {}, 0, 1, true);
      return res.json(cached);
    }
  }

  const { rows } = await pool.query('SELECT * FROM analytics.radiance_dq_v1 LIMIT 1');
  const duration = Date.now() - start;
  
  const result = rows[0] || {};
  setCache(cacheKey, result);
  
  res.set('X-Query-Time-Ms', String(duration));
  logRequest('/api/meta/dq_latest', {}, duration, rows.length, false);
  res.json(result);
}));

// =============================================================================
// ENDPOINT 3: GET /api/meta/issuers
// =============================================================================
app.get('/api/meta/issuers', asyncHandler(async (req, res) => {
  const start = Date.now();
  const bypassCache = req.query.cache === '0';
  const cacheKey = getCacheKey('/api/meta/issuers', {});
  
  if (!bypassCache) {
    const cached = getFromCache(cacheKey);
    if (cached) {
      res.set('X-Query-Time-Ms', '0');
      logRequest('/api/meta/issuers', {}, 0, cached.length, true);
      return res.json(cached);
    }
  }

  const query = `
    SELECT 
      issuer_ruc,
      MAX(retailer_name) AS retailer_name,
      MAX(issuer_category_l1) AS issuer_category_l1,
      COUNT(DISTINCT COALESCE(store_id, '1')) AS store_count,
      COUNT(DISTINCT cufe) AS receipt_count
    FROM analytics.radiance_base_v1
    WHERE issuer_ruc IS NOT NULL AND issuer_ruc != ''
    GROUP BY issuer_ruc
    ORDER BY receipt_count DESC
  `;
  
  const { rows } = await pool.query(query);
  const duration = Date.now() - start;
  
  setCache(cacheKey, rows);
  
  res.set('X-Query-Time-Ms', String(duration));
  logRequest('/api/meta/issuers', {}, duration, rows.length, false);
  res.json(rows);
}));

// =============================================================================
// ENDPOINT 4: GET /api/meta/stores?issuer_ruc=...
// =============================================================================
app.get('/api/meta/stores', asyncHandler(async (req, res) => {
  const issuerRuc = parseString(req.query.issuer_ruc);
  
  if (!issuerRuc) {
    return res.status(400).json({ error: 'issuer_ruc is required' });
  }

  const start = Date.now();
  const bypassCache = req.query.cache === '0';
  const cacheKey = getCacheKey('/api/meta/stores', { issuer_ruc: issuerRuc });
  
  if (!bypassCache) {
    const cached = getFromCache(cacheKey);
    if (cached) {
      res.set('X-Query-Time-Ms', '0');
      logRequest('/api/meta/stores', { issuer_ruc: issuerRuc }, 0, cached.length, true);
      return res.json(cached);
    }
  }

  const query = `
    SELECT 
      COALESCE(store_id, 'DEFAULT') AS store_id,
      MAX(COALESCE(store_name, retailer_name)) AS store_name,
      COUNT(DISTINCT cufe) AS receipt_count
    FROM analytics.radiance_base_v1
    WHERE issuer_ruc = $1
    GROUP BY store_id
    ORDER BY receipt_count DESC
  `;
  
  const { rows } = await pool.query(query, [issuerRuc]);
  const duration = Date.now() - start;
  
  setCache(cacheKey, rows);
  
  res.set('X-Query-Time-Ms', String(duration));
  logRequest('/api/meta/stores', { issuer_ruc: issuerRuc }, duration, rows.length, false);
  res.json(rows);
}));

// =============================================================================
// ENDPOINT 5: GET /api/capture/categories
// =============================================================================
app.get('/api/capture/categories', asyncHandler(async (req, res) => {
  // Validate required params
  const startDate = parseDate(req.query.start);
  const endDate = parseDate(req.query.end);
  const issuerRuc = parseString(req.query.issuer_ruc);
  
  if (!startDate) return res.status(400).json({ error: 'Invalid or missing start date' });
  if (!endDate) return res.status(400).json({ error: 'Invalid or missing end date' });
  if (!issuerRuc) return res.status(400).json({ error: 'issuer_ruc is required' });

  const storeId = parseString(req.query.store_id);
  const reconcileOk = parseBool(req.query.reconcile_ok, true);
  const categoryLevel = parseCategoryLevel(req.query.category_level);

  const start = Date.now();
  const bypassCache = req.query.cache === '0';
  const params = { start: startDate, end: endDate, issuer_ruc: issuerRuc, store_id: storeId, reconcile_ok: reconcileOk, category_level: categoryLevel };
  const cacheKey = getCacheKey('/api/capture/categories', params);
  
  if (!bypassCache) {
    const cached = getFromCache(cacheKey);
    if (cached) {
      res.set('X-Query-Time-Ms', '0');
      logRequest('/api/capture/categories', params, 0, cached.data.length, true);
      return res.json(cached);
    }
  }

  const query = `
    WITH base_data AS (
      SELECT 
        b.user_id, b.cufe, b.issuer_ruc, b.retailer_name, b.store_id, b.line_total,
        CASE $6
          WHEN 'l1' THEN b.category_l1
          WHEN 'l2' THEN b.category_l2
          WHEN 'l3' THEN b.category_l3
          WHEN 'l4' THEN b.category_l4
          ELSE b.category_l1
        END AS category_value
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date BETWEEN $1 AND $2
        AND b.line_total IS NOT NULL AND b.line_total > 0
        AND ($5::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $5)
    ),
    customers_of_x AS (
      SELECT DISTINCT user_id FROM base_data
      WHERE issuer_ruc = $3 AND ($4::text IS NULL OR store_id = $4)
    ),
    total_customers AS (SELECT COUNT(*) AS total_customers_x FROM customers_of_x),
    spend_in_x AS (
      SELECT category_value, SUM(line_total) AS spend_x, COUNT(DISTINCT user_id) AS users_in_x_cat
      FROM base_data bd
      INNER JOIN customers_of_x cx ON bd.user_id = cx.user_id
      WHERE bd.issuer_ruc = $3 AND ($4::text IS NULL OR bd.store_id = $4) AND bd.category_value IS NOT NULL
      GROUP BY category_value
    ),
    spend_market AS (
      SELECT category_value, SUM(line_total) AS spend_market, COUNT(DISTINCT user_id) AS unique_users_cat
      FROM base_data bd
      INNER JOIN customers_of_x cx ON bd.user_id = cx.user_id
      WHERE bd.category_value IS NOT NULL
      GROUP BY category_value
    ),
    category_coverage AS (
      SELECT category_value,
        SUM(CASE WHEN category_value IS NOT NULL AND category_value != 'UNKNOWN' THEN line_total ELSE 0 END) AS covered_sales,
        SUM(line_total) AS total_sales
      FROM base_data bd
      INNER JOIN customers_of_x cx ON bd.user_id = cx.user_id
      GROUP BY category_value
    )
    SELECT 
      COALESCE(sm.category_value, sx.category_value) AS category,
      $6::text AS category_level,
      ROUND(COALESCE(sx.spend_x, 0)::numeric, 2) AS spend_in_x_usd,
      ROUND(COALESCE(sm.spend_market, 0)::numeric, 2) AS spend_market_usd,
      ROUND(100.0 * COALESCE(sx.spend_x, 0) / NULLIF(sm.spend_market, 0), 2) AS customer_sow_cat_pct,
      ROUND((COALESCE(sm.spend_market, 0) - COALESCE(sx.spend_x, 0))::numeric, 2) AS customer_leakage_cat_usd,
      ROUND(100.0 * (COALESCE(sm.spend_market, 0) - COALESCE(sx.spend_x, 0)) / NULLIF(sm.spend_market, 0), 2) AS leakage_pct,
      COALESCE(sm.unique_users_cat, 0) AS unique_users_cat,
      ROUND(100.0 * cc.covered_sales / NULLIF(cc.total_sales, 0), 2) AS category_coverage_sales_pct,
      CASE 
        WHEN (100.0 * cc.covered_sales / NULLIF(cc.total_sales, 0)) < 30 THEN 'SUPPRESSED'
        WHEN COALESCE(sm.unique_users_cat, 0) >= 10 AND (100.0 * cc.covered_sales / NULLIF(cc.total_sales, 0)) >= 80 THEN 'HIGH'
        WHEN COALESCE(sm.unique_users_cat, 0) >= 5 AND (100.0 * cc.covered_sales / NULLIF(cc.total_sales, 0)) >= 60 THEN 'MEDIUM'
        WHEN COALESCE(sm.unique_users_cat, 0) >= 5 AND (100.0 * cc.covered_sales / NULLIF(cc.total_sales, 0)) >= 30 THEN 'LOW'
        ELSE 'SUPPRESSED'
      END AS trust_level,
      (SELECT total_customers_x FROM total_customers) AS total_customers_x
    FROM spend_market sm
    FULL OUTER JOIN spend_in_x sx ON sm.category_value = sx.category_value
    LEFT JOIN category_coverage cc ON COALESCE(sm.category_value, sx.category_value) = cc.category_value
    WHERE COALESCE(sm.category_value, sx.category_value) IS NOT NULL
      AND COALESCE(sm.category_value, sx.category_value) != 'UNKNOWN'
    ORDER BY customer_leakage_cat_usd DESC
  `;

  const { rows } = await pool.query(query, [startDate, endDate, issuerRuc, storeId, reconcileOk, categoryLevel]);
  const duration = Date.now() - start;

  // Calculate totals
  const totals = {
    total_spend_in_x: rows.reduce((sum, r) => sum + Number(r.spend_in_x_usd || 0), 0),
    total_spend_market: rows.reduce((sum, r) => sum + Number(r.spend_market_usd || 0), 0),
    total_leakage: rows.reduce((sum, r) => sum + Number(r.customer_leakage_cat_usd || 0), 0),
    total_customers_x: rows[0]?.total_customers_x || 0
  };
  totals.overall_sow_pct = totals.total_spend_market > 0 
    ? Math.round(10000 * totals.total_spend_in_x / totals.total_spend_market) / 100 
    : 0;

  const result = {
    filters: { start: startDate, end: endDate, issuer_ruc: issuerRuc, store_id: storeId, reconcile_ok: reconcileOk, category_level: categoryLevel },
    data: rows,
    totals
  };

  setCache(cacheKey, result);
  
  res.set('X-Query-Time-Ms', String(duration));
  logRequest('/api/capture/categories', params, duration, rows.length, false);
  res.json(result);
}));

// =============================================================================
// ENDPOINT 6: GET /api/capture/destinations
// =============================================================================
app.get('/api/capture/destinations', asyncHandler(async (req, res) => {
  const startDate = parseDate(req.query.start);
  const endDate = parseDate(req.query.end);
  const issuerRuc = parseString(req.query.issuer_ruc);
  const categoryValue = parseString(req.query.category_value);
  
  if (!startDate) return res.status(400).json({ error: 'Invalid or missing start date' });
  if (!endDate) return res.status(400).json({ error: 'Invalid or missing end date' });
  if (!issuerRuc) return res.status(400).json({ error: 'issuer_ruc is required' });
  if (!categoryValue) return res.status(400).json({ error: 'category_value is required' });

  const storeId = parseString(req.query.store_id);
  const reconcileOk = parseBool(req.query.reconcile_ok, true);
  const categoryLevel = parseCategoryLevel(req.query.category_level);
  const kThreshold = parseInt(req.query.k_threshold) || 5;

  const start = Date.now();
  const bypassCache = req.query.cache === '0';
  const params = { start: startDate, end: endDate, issuer_ruc: issuerRuc, category_value: categoryValue, store_id: storeId, reconcile_ok: reconcileOk, category_level: categoryLevel };
  const cacheKey = getCacheKey('/api/capture/destinations', params);
  
  if (!bypassCache) {
    const cached = getFromCache(cacheKey);
    if (cached) {
      res.set('X-Query-Time-Ms', '0');
      logRequest('/api/capture/destinations', params, 0, cached.data.length, true);
      return res.json(cached);
    }
  }

  const query = `
    WITH base_data AS (
      SELECT 
        b.user_id, b.cufe, b.issuer_ruc, b.retailer_name, b.store_id, b.line_total,
        b.issuer_category_l1, b.issuer_category_l2,
        CASE $6
          WHEN 'l1' THEN b.category_l1
          WHEN 'l2' THEN b.category_l2
          WHEN 'l3' THEN b.category_l3
          WHEN 'l4' THEN b.category_l4
          ELSE b.category_l1
        END AS category_value
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date BETWEEN $1 AND $2
        AND b.line_total IS NOT NULL AND b.line_total > 0
        AND ($5::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $5)
    ),
    customers_of_x AS (
      SELECT DISTINCT user_id FROM base_data
      WHERE issuer_ruc = $3 AND ($4::text IS NULL OR store_id = $4)
    ),
    leakage_raw AS (
      SELECT 
        bd.issuer_ruc AS dest_ruc,
        bd.retailer_name AS dest_name,
        bd.issuer_category_l1 AS dest_type,
        bd.issuer_category_l2 AS dest_subtype,
        SUM(bd.line_total) AS leakage_usd,
        COUNT(DISTINCT bd.user_id) AS users_leaked,
        COUNT(DISTINCT bd.cufe) AS transactions
      FROM base_data bd
      INNER JOIN customers_of_x cx ON bd.user_id = cx.user_id
      WHERE bd.issuer_ruc != $3 AND bd.category_value = $7
      GROUP BY bd.issuer_ruc, bd.retailer_name, bd.issuer_category_l1, bd.issuer_category_l2
    ),
    leakage_kanon AS (
      SELECT 
        CASE WHEN users_leaked >= $8 THEN dest_ruc ELSE 'OTHER_SUPPRESSED' END AS destination_issuer_ruc,
        CASE WHEN users_leaked >= $8 THEN dest_name ELSE 'Other (suppressed)' END AS destination_retailer_name,
        CASE WHEN users_leaked >= $8 THEN dest_type ELSE NULL END AS destination_type,
        CASE WHEN users_leaked >= $8 THEN dest_subtype ELSE NULL END AS destination_subtype,
        leakage_usd, users_leaked, transactions
      FROM leakage_raw
    ),
    leakage_agg AS (
      SELECT 
        destination_issuer_ruc, destination_retailer_name, destination_type, destination_subtype,
        SUM(leakage_usd) AS leakage_to_dest_usd,
        SUM(users_leaked) AS users_leaked_to_dest,
        SUM(transactions) AS transactions_to_dest
      FROM leakage_kanon
      GROUP BY destination_issuer_ruc, destination_retailer_name, destination_type, destination_subtype
    ),
    total_leak AS (SELECT SUM(leakage_to_dest_usd) AS total FROM leakage_agg)
    SELECT 
      $7::text AS category,
      $6::text AS category_level,
      la.destination_issuer_ruc,
      la.destination_retailer_name,
      la.destination_type,
      la.destination_subtype,
      ROUND(la.leakage_to_dest_usd::numeric, 2) AS leakage_to_dest_usd,
      ROUND(100.0 * la.leakage_to_dest_usd / NULLIF(tl.total, 0), 2) AS destination_share_of_leakage_pct,
      la.users_leaked_to_dest,
      la.transactions_to_dest
    FROM leakage_agg la
    CROSS JOIN total_leak tl
    ORDER BY la.leakage_to_dest_usd DESC
    LIMIT 20
  `;

  const { rows } = await pool.query(query, [startDate, endDate, issuerRuc, storeId, reconcileOk, categoryLevel, categoryValue, kThreshold]);
  const duration = Date.now() - start;

  const totalLeakage = rows.reduce((sum, r) => sum + Number(r.leakage_to_dest_usd || 0), 0);

  const result = {
    category: categoryValue,
    category_level: categoryLevel,
    total_leakage_usd: Math.round(totalLeakage * 100) / 100,
    k_threshold: kThreshold,
    data: rows,
    note: `Destinations with fewer than ${kThreshold} users are aggregated as 'OTHER_SUPPRESSED'`
  };

  setCache(cacheKey, result);
  
  res.set('X-Query-Time-Ms', String(duration));
  logRequest('/api/capture/destinations', params, duration, rows.length, false);
  res.json(result);
}));

// =============================================================================
// ENDPOINT 7: GET /api/capture/distribution
// =============================================================================
app.get('/api/capture/distribution', asyncHandler(async (req, res) => {
  const startDate = parseDate(req.query.start);
  const endDate = parseDate(req.query.end);
  const issuerRuc = parseString(req.query.issuer_ruc);
  const categoryValue = parseString(req.query.category_value);
  
  if (!startDate) return res.status(400).json({ error: 'Invalid or missing start date' });
  if (!endDate) return res.status(400).json({ error: 'Invalid or missing end date' });
  if (!issuerRuc) return res.status(400).json({ error: 'issuer_ruc is required' });
  if (!categoryValue) return res.status(400).json({ error: 'category_value is required' });

  const storeId = parseString(req.query.store_id);
  const reconcileOk = parseBool(req.query.reconcile_ok, true);
  const categoryLevel = parseCategoryLevel(req.query.category_level);

  const start = Date.now();
  const bypassCache = req.query.cache === '0';
  const params = { start: startDate, end: endDate, issuer_ruc: issuerRuc, category_value: categoryValue, store_id: storeId, reconcile_ok: reconcileOk, category_level: categoryLevel };
  const cacheKey = getCacheKey('/api/capture/distribution', params);
  
  if (!bypassCache) {
    const cached = getFromCache(cacheKey);
    if (cached) {
      res.set('X-Query-Time-Ms', '0');
      logRequest('/api/capture/distribution', params, 0, 1, true);
      return res.json(cached);
    }
  }

  const query = `
    WITH base_data AS (
      SELECT 
        b.user_id, b.cufe, b.issuer_ruc, b.store_id, b.line_total,
        CASE $6
          WHEN 'l1' THEN b.category_l1
          WHEN 'l2' THEN b.category_l2
          WHEN 'l3' THEN b.category_l3
          WHEN 'l4' THEN b.category_l4
          ELSE b.category_l1
        END AS category_value
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date BETWEEN $1 AND $2
        AND b.line_total IS NOT NULL AND b.line_total > 0
        AND ($5::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $5)
    ),
    customers_of_x AS (
      SELECT DISTINCT user_id FROM base_data
      WHERE issuer_ruc = $3 AND ($4::text IS NULL OR store_id = $4)
    ),
    user_spend_x AS (
      SELECT bd.user_id, SUM(bd.line_total) AS spend_user_x
      FROM base_data bd
      INNER JOIN customers_of_x cx ON bd.user_id = cx.user_id
      WHERE bd.issuer_ruc = $3 AND ($4::text IS NULL OR bd.store_id = $4) AND bd.category_value = $7
      GROUP BY bd.user_id
    ),
    user_spend_market AS (
      SELECT bd.user_id, SUM(bd.line_total) AS spend_user_market
      FROM base_data bd
      INNER JOIN customers_of_x cx ON bd.user_id = cx.user_id
      WHERE bd.category_value = $7
      GROUP BY bd.user_id
    ),
    customers_with_x AS (SELECT COUNT(DISTINCT user_id) AS cnt FROM user_spend_x WHERE spend_user_x > 0),
    user_sow AS (
      SELECT 
        usm.user_id,
        COALESCE(usx.spend_user_x, 0) AS spend_in_x,
        usm.spend_user_market,
        100.0 * COALESCE(usx.spend_user_x, 0) / usm.spend_user_market AS sow_user_pct
      FROM user_spend_market usm
      LEFT JOIN user_spend_x usx ON usm.user_id = usx.user_id
      WHERE usm.spend_user_market > 0
    )
    SELECT 
      $3::text AS issuer_ruc,
      $7::text AS category,
      $6::text AS category_level,
      (SELECT COUNT(*) FROM customers_of_x) AS customers_total,
      COUNT(*) AS customers_with_spend_in_category,
      (SELECT cnt FROM customers_with_x) AS customers_with_any_spend_in_x,
      ROUND(100.0 * SUM(spend_in_x) / NULLIF(SUM(spend_user_market), 0), 2) AS aggregate_sow_pct,
      ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY sow_user_pct)::numeric, 2) AS sow_p50,
      ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY sow_user_pct)::numeric, 2) AS sow_p75,
      ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY sow_user_pct)::numeric, 2) AS sow_p90,
      ROUND(AVG(sow_user_pct)::numeric, 2) AS sow_avg,
      ROUND(MIN(sow_user_pct)::numeric, 2) AS sow_min,
      ROUND(MAX(sow_user_pct)::numeric, 2) AS sow_max,
      ROUND(SUM(spend_in_x)::numeric, 2) AS total_spend_in_x,
      ROUND(SUM(spend_user_market)::numeric, 2) AS total_spend_market,
      COUNT(*) FILTER (WHERE sow_user_pct = 0) AS bin_0,
      COUNT(*) FILTER (WHERE sow_user_pct > 0 AND sow_user_pct <= 25) AS bin_1_25,
      COUNT(*) FILTER (WHERE sow_user_pct > 25 AND sow_user_pct <= 50) AS bin_25_50,
      COUNT(*) FILTER (WHERE sow_user_pct > 50 AND sow_user_pct <= 75) AS bin_50_75,
      COUNT(*) FILTER (WHERE sow_user_pct > 75 AND sow_user_pct < 100) AS bin_75_99,
      COUNT(*) FILTER (WHERE sow_user_pct = 100) AS bin_100
    FROM user_sow
  `;

  const { rows } = await pool.query(query, [startDate, endDate, issuerRuc, storeId, reconcileOk, categoryLevel, categoryValue]);
  const duration = Date.now() - start;

  const row = rows[0] || {};
  const result = {
    issuer_ruc: issuerRuc,
    category: categoryValue,
    category_level: categoryLevel,
    customers_total: Number(row.customers_total) || 0,
    customers_with_spend_in_category: Number(row.customers_with_spend_in_category) || 0,
    customers_with_any_spend_in_x: Number(row.customers_with_any_spend_in_x) || 0,
    aggregate_sow_pct: Number(row.aggregate_sow_pct) || 0,
    percentiles: {
      p50: Number(row.sow_p50) || 0,
      p75: Number(row.sow_p75) || 0,
      p90: Number(row.sow_p90) || 0
    },
    stats: {
      avg: Number(row.sow_avg) || 0,
      min: Number(row.sow_min) || 0,
      max: Number(row.sow_max) || 0
    },
    totals: {
      spend_in_x: Number(row.total_spend_in_x) || 0,
      spend_market: Number(row.total_spend_market) || 0
    },
    histogram: {
      bin_0: Number(row.bin_0) || 0,
      bin_1_25: Number(row.bin_1_25) || 0,
      bin_25_50: Number(row.bin_25_50) || 0,
      bin_50_75: Number(row.bin_50_75) || 0,
      bin_75_99: Number(row.bin_75_99) || 0,
      bin_100: Number(row.bin_100) || 0
    }
  };

  setCache(cacheKey, result);
  
  res.set('X-Query-Time-Ms', String(duration));
  logRequest('/api/capture/distribution', params, duration, 1, false);
  res.json(result);
}));

// =============================================================================
// ENDPOINT 8: GET /api/report_pack/sprint1
// =============================================================================
app.get('/api/report_pack/sprint1', asyncHandler(async (req, res) => {
  const startDate = parseDate(req.query.start);
  const endDate = parseDate(req.query.end);
  const issuerRuc = parseString(req.query.issuer_ruc);
  
  if (!startDate) return res.status(400).json({ error: 'Invalid or missing start date' });
  if (!endDate) return res.status(400).json({ error: 'Invalid or missing end date' });
  if (!issuerRuc) return res.status(400).json({ error: 'issuer_ruc is required' });

  const storeId = parseString(req.query.store_id);
  const reconcileOk = parseBool(req.query.reconcile_ok, true);
  const categoryLevel = parseCategoryLevel(req.query.category_level);
  const categoryValue = parseString(req.query.category_value);

  const start = Date.now();

  // Fetch DQ
  const dqResult = await pool.query('SELECT * FROM analytics.radiance_dq_v1 LIMIT 1');
  const dq_latest = dqResult.rows[0] || {};

  // Fetch categories (Query 1)
  const categoriesQuery = `
    WITH base_data AS (
      SELECT b.user_id, b.cufe, b.issuer_ruc, b.retailer_name, b.store_id, b.line_total,
        CASE $6 WHEN 'l1' THEN b.category_l1 WHEN 'l2' THEN b.category_l2 WHEN 'l3' THEN b.category_l3 WHEN 'l4' THEN b.category_l4 ELSE b.category_l1 END AS category_value
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date BETWEEN $1 AND $2 AND b.line_total IS NOT NULL AND b.line_total > 0
        AND ($5::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $5)
    ),
    customers_of_x AS (SELECT DISTINCT user_id FROM base_data WHERE issuer_ruc = $3 AND ($4::text IS NULL OR store_id = $4)),
    total_customers AS (SELECT COUNT(*) AS total_customers_x FROM customers_of_x),
    spend_in_x AS (SELECT category_value, SUM(line_total) AS spend_x, COUNT(DISTINCT user_id) AS users_in_x_cat FROM base_data bd INNER JOIN customers_of_x cx ON bd.user_id = cx.user_id WHERE bd.issuer_ruc = $3 AND ($4::text IS NULL OR bd.store_id = $4) AND bd.category_value IS NOT NULL GROUP BY category_value),
    spend_market AS (SELECT category_value, SUM(line_total) AS spend_market, COUNT(DISTINCT user_id) AS unique_users_cat FROM base_data bd INNER JOIN customers_of_x cx ON bd.user_id = cx.user_id WHERE bd.category_value IS NOT NULL GROUP BY category_value),
    category_coverage AS (SELECT category_value, SUM(CASE WHEN category_value IS NOT NULL AND category_value != 'UNKNOWN' THEN line_total ELSE 0 END) AS covered_sales, SUM(line_total) AS total_sales FROM base_data bd INNER JOIN customers_of_x cx ON bd.user_id = cx.user_id GROUP BY category_value)
    SELECT COALESCE(sm.category_value, sx.category_value) AS category, $6::text AS category_level,
      ROUND(COALESCE(sx.spend_x, 0)::numeric, 2) AS spend_in_x_usd, ROUND(COALESCE(sm.spend_market, 0)::numeric, 2) AS spend_market_usd,
      ROUND(100.0 * COALESCE(sx.spend_x, 0) / NULLIF(sm.spend_market, 0), 2) AS customer_sow_cat_pct,
      ROUND((COALESCE(sm.spend_market, 0) - COALESCE(sx.spend_x, 0))::numeric, 2) AS customer_leakage_cat_usd,
      ROUND(100.0 * (COALESCE(sm.spend_market, 0) - COALESCE(sx.spend_x, 0)) / NULLIF(sm.spend_market, 0), 2) AS leakage_pct,
      COALESCE(sm.unique_users_cat, 0) AS unique_users_cat,
      ROUND(100.0 * cc.covered_sales / NULLIF(cc.total_sales, 0), 2) AS category_coverage_sales_pct,
      CASE WHEN (100.0 * cc.covered_sales / NULLIF(cc.total_sales, 0)) < 30 THEN 'SUPPRESSED'
        WHEN COALESCE(sm.unique_users_cat, 0) >= 10 AND (100.0 * cc.covered_sales / NULLIF(cc.total_sales, 0)) >= 80 THEN 'HIGH'
        WHEN COALESCE(sm.unique_users_cat, 0) >= 5 AND (100.0 * cc.covered_sales / NULLIF(cc.total_sales, 0)) >= 60 THEN 'MEDIUM'
        WHEN COALESCE(sm.unique_users_cat, 0) >= 5 AND (100.0 * cc.covered_sales / NULLIF(cc.total_sales, 0)) >= 30 THEN 'LOW'
        ELSE 'SUPPRESSED' END AS trust_level,
      (SELECT total_customers_x FROM total_customers) AS total_customers_x
    FROM spend_market sm
    FULL OUTER JOIN spend_in_x sx ON sm.category_value = sx.category_value
    LEFT JOIN category_coverage cc ON COALESCE(sm.category_value, sx.category_value) = cc.category_value
    WHERE COALESCE(sm.category_value, sx.category_value) IS NOT NULL AND COALESCE(sm.category_value, sx.category_value) != 'UNKNOWN'
    ORDER BY customer_leakage_cat_usd DESC
  `;
  const categoriesResult = await pool.query(categoriesQuery, [startDate, endDate, issuerRuc, storeId, reconcileOk, categoryLevel]);
  
  const categories = {
    filters: { start: startDate, end: endDate, issuer_ruc: issuerRuc, store_id: storeId, reconcile_ok: reconcileOk, category_level: categoryLevel },
    data: categoriesResult.rows,
    totals: {
      total_spend_in_x: categoriesResult.rows.reduce((sum, r) => sum + Number(r.spend_in_x_usd || 0), 0),
      total_spend_market: categoriesResult.rows.reduce((sum, r) => sum + Number(r.spend_market_usd || 0), 0),
      total_leakage: categoriesResult.rows.reduce((sum, r) => sum + Number(r.customer_leakage_cat_usd || 0), 0),
      total_customers_x: categoriesResult.rows[0]?.total_customers_x || 0
    }
  };

  // Destinations & Distribution (only if category_value provided)
  let destinations = [];
  let distribution = null;

  if (categoryValue) {
    // Query 2: Destinations
    const destQuery = `
      WITH base_data AS (
        SELECT b.user_id, b.cufe, b.issuer_ruc, b.retailer_name, b.store_id, b.line_total, b.issuer_category_l1, b.issuer_category_l2,
          CASE $6 WHEN 'l1' THEN b.category_l1 WHEN 'l2' THEN b.category_l2 WHEN 'l3' THEN b.category_l3 WHEN 'l4' THEN b.category_l4 ELSE b.category_l1 END AS category_value
        FROM analytics.radiance_base_v1 b
        LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
        WHERE b.invoice_date BETWEEN $1 AND $2 AND b.line_total > 0 AND ($5::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $5)
      ),
      customers_of_x AS (SELECT DISTINCT user_id FROM base_data WHERE issuer_ruc = $3 AND ($4::text IS NULL OR store_id = $4)),
      leakage_raw AS (
        SELECT bd.issuer_ruc AS dest_ruc, bd.retailer_name AS dest_name, bd.issuer_category_l1 AS dest_type, bd.issuer_category_l2 AS dest_subtype,
          SUM(bd.line_total) AS leakage_usd, COUNT(DISTINCT bd.user_id) AS users_leaked, COUNT(DISTINCT bd.cufe) AS transactions
        FROM base_data bd INNER JOIN customers_of_x cx ON bd.user_id = cx.user_id
        WHERE bd.issuer_ruc != $3 AND bd.category_value = $7 GROUP BY bd.issuer_ruc, bd.retailer_name, bd.issuer_category_l1, bd.issuer_category_l2
      ),
      leakage_kanon AS (
        SELECT CASE WHEN users_leaked >= 5 THEN dest_ruc ELSE 'OTHER_SUPPRESSED' END AS destination_issuer_ruc,
          CASE WHEN users_leaked >= 5 THEN dest_name ELSE 'Other (suppressed)' END AS destination_retailer_name,
          CASE WHEN users_leaked >= 5 THEN dest_type ELSE NULL END AS destination_type,
          CASE WHEN users_leaked >= 5 THEN dest_subtype ELSE NULL END AS destination_subtype,
          leakage_usd, users_leaked, transactions FROM leakage_raw
      ),
      leakage_agg AS (
        SELECT destination_issuer_ruc, destination_retailer_name, destination_type, destination_subtype,
          SUM(leakage_usd) AS leakage_to_dest_usd, SUM(users_leaked) AS users_leaked_to_dest, SUM(transactions) AS transactions_to_dest
        FROM leakage_kanon GROUP BY destination_issuer_ruc, destination_retailer_name, destination_type, destination_subtype
      ),
      total_leak AS (SELECT SUM(leakage_to_dest_usd) AS total FROM leakage_agg)
      SELECT $7::text AS category, $6::text AS category_level, la.destination_issuer_ruc, la.destination_retailer_name, la.destination_type, la.destination_subtype,
        ROUND(la.leakage_to_dest_usd::numeric, 2) AS leakage_to_dest_usd, ROUND(100.0 * la.leakage_to_dest_usd / NULLIF(tl.total, 0), 2) AS destination_share_of_leakage_pct, la.users_leaked_to_dest, la.transactions_to_dest
      FROM leakage_agg la CROSS JOIN total_leak tl ORDER BY la.leakage_to_dest_usd DESC LIMIT 20
    `;
    const destResult = await pool.query(destQuery, [startDate, endDate, issuerRuc, storeId, reconcileOk, categoryLevel, categoryValue]);
    destinations = destResult.rows;

    // Query 3: Distribution
    const distQuery = `
      WITH base_data AS (
        SELECT b.user_id, b.cufe, b.issuer_ruc, b.store_id, b.line_total,
          CASE $6 WHEN 'l1' THEN b.category_l1 WHEN 'l2' THEN b.category_l2 WHEN 'l3' THEN b.category_l3 WHEN 'l4' THEN b.category_l4 ELSE b.category_l1 END AS category_value
        FROM analytics.radiance_base_v1 b
        LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
        WHERE b.invoice_date BETWEEN $1 AND $2 AND b.line_total > 0 AND ($5::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $5)
      ),
      customers_of_x AS (SELECT DISTINCT user_id FROM base_data WHERE issuer_ruc = $3 AND ($4::text IS NULL OR store_id = $4)),
      user_spend_x AS (SELECT bd.user_id, SUM(bd.line_total) AS spend_user_x FROM base_data bd INNER JOIN customers_of_x cx ON bd.user_id = cx.user_id WHERE bd.issuer_ruc = $3 AND ($4::text IS NULL OR bd.store_id = $4) AND bd.category_value = $7 GROUP BY bd.user_id),
      user_spend_market AS (SELECT bd.user_id, SUM(bd.line_total) AS spend_user_market FROM base_data bd INNER JOIN customers_of_x cx ON bd.user_id = cx.user_id WHERE bd.category_value = $7 GROUP BY bd.user_id),
      customers_with_x AS (SELECT COUNT(DISTINCT user_id) AS cnt FROM user_spend_x WHERE spend_user_x > 0),
      user_sow AS (SELECT usm.user_id, COALESCE(usx.spend_user_x, 0) AS spend_in_x, usm.spend_user_market, 100.0 * COALESCE(usx.spend_user_x, 0) / usm.spend_user_market AS sow_user_pct FROM user_spend_market usm LEFT JOIN user_spend_x usx ON usm.user_id = usx.user_id WHERE usm.spend_user_market > 0)
      SELECT (SELECT COUNT(*) FROM customers_of_x) AS customers_total, COUNT(*) AS customers_with_spend_in_category, (SELECT cnt FROM customers_with_x) AS customers_with_any_spend_in_x,
        ROUND(100.0 * SUM(spend_in_x) / NULLIF(SUM(spend_user_market), 0), 2) AS aggregate_sow_pct,
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY sow_user_pct)::numeric, 2) AS sow_p50,
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY sow_user_pct)::numeric, 2) AS sow_p75,
        ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY sow_user_pct)::numeric, 2) AS sow_p90,
        ROUND(AVG(sow_user_pct)::numeric, 2) AS sow_avg, ROUND(MIN(sow_user_pct)::numeric, 2) AS sow_min, ROUND(MAX(sow_user_pct)::numeric, 2) AS sow_max,
        ROUND(SUM(spend_in_x)::numeric, 2) AS total_spend_in_x, ROUND(SUM(spend_user_market)::numeric, 2) AS total_spend_market,
        COUNT(*) FILTER (WHERE sow_user_pct = 0) AS bin_0, COUNT(*) FILTER (WHERE sow_user_pct > 0 AND sow_user_pct <= 25) AS bin_1_25,
        COUNT(*) FILTER (WHERE sow_user_pct > 25 AND sow_user_pct <= 50) AS bin_25_50, COUNT(*) FILTER (WHERE sow_user_pct > 50 AND sow_user_pct <= 75) AS bin_50_75,
        COUNT(*) FILTER (WHERE sow_user_pct > 75 AND sow_user_pct < 100) AS bin_75_99, COUNT(*) FILTER (WHERE sow_user_pct = 100) AS bin_100
      FROM user_sow
    `;
    const distResult = await pool.query(distQuery, [startDate, endDate, issuerRuc, storeId, reconcileOk, categoryLevel, categoryValue]);
    const dr = distResult.rows[0] || {};
    distribution = {
      category: categoryValue,
      category_level: categoryLevel,
      customers_total: Number(dr.customers_total) || 0,
      customers_with_spend_in_category: Number(dr.customers_with_spend_in_category) || 0,
      customers_with_any_spend_in_x: Number(dr.customers_with_any_spend_in_x) || 0,
      aggregate_sow_pct: Number(dr.aggregate_sow_pct) || 0,
      percentiles: { p50: Number(dr.sow_p50) || 0, p75: Number(dr.sow_p75) || 0, p90: Number(dr.sow_p90) || 0 },
      stats: { avg: Number(dr.sow_avg) || 0, min: Number(dr.sow_min) || 0, max: Number(dr.sow_max) || 0 },
      totals: { spend_in_x: Number(dr.total_spend_in_x) || 0, spend_market: Number(dr.total_spend_market) || 0 },
      histogram: { bin_0: Number(dr.bin_0) || 0, bin_1_25: Number(dr.bin_1_25) || 0, bin_25_50: Number(dr.bin_25_50) || 0, bin_50_75: Number(dr.bin_50_75) || 0, bin_75_99: Number(dr.bin_75_99) || 0, bin_100: Number(dr.bin_100) || 0 }
    };
  }

  const duration = Date.now() - start;

  const result = {
    generated_at: new Date().toISOString(),
    dq_latest,
    disclaimer: DISCLAIMER,
    categories,
    destinations,
    distribution
  };

  res.set('X-Query-Time-Ms', String(duration));
  logRequest('/api/report_pack/sprint1', { start: startDate, end: endDate, issuer_ruc: issuerRuc }, duration, categoriesResult.rows.length, false);
  res.json(result);
}));

// =============================================================================
// ERROR HANDLER
// =============================================================================
app.use((err, req, res, next) => {
  console.error('API Error:', err.message);
  res.status(500).json({ error: err.message });
});

// =============================================================================
// START SERVER
// =============================================================================
app.listen(PORT, '0.0.0.0', () => {
  console.log(`
╔═══════════════════════════════════════════════════════════════╗
║   🌟 RADIANCE SPRINT 1 - CAPTURE EXPLORER                     ║
║   Server running at: http://localhost:${PORT}                    ║
║                                                               ║
║   Endpoints:                                                  ║
║   - GET /api/health                                           ║
║   - GET /api/meta/dq_latest                                   ║
║   - GET /api/meta/issuers                                     ║
║   - GET /api/meta/stores?issuer_ruc=                          ║
║   - GET /api/capture/categories?start&end&issuer_ruc          ║
║   - GET /api/capture/destinations?...&category_value          ║
║   - GET /api/capture/distribution?...&category_value          ║
║   - GET /api/report_pack/sprint1?start&end&issuer_ruc         ║
╚═══════════════════════════════════════════════════════════════╝
  `);
});
