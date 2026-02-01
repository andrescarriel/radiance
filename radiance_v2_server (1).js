// =============================================================================
// RADIANCE DASHBOARD - SERVER v2.1
// LÜM Internal Analytics MVP - Fixed filters
// =============================================================================

require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// Database connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://avalencia:Jacobo236@4.71.179.119:5432/tfactu',
  ssl: false
});

// Test connection on startup
pool.query('SELECT NOW()')
  .then(() => console.log('✅ Database connected'))
  .catch(err => console.error('❌ Database connection failed:', err.message));

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// =============================================================================
// HELPERS
// =============================================================================

function parseParam(val) {
  if (val === undefined || val === null || val === '' || val === 'null' || val === 'undefined' || val === 'all') {
    return null;
  }
  return String(val).trim();
}

function parseBool(val) {
  if (val === undefined || val === null || val === '' || val === 'all') return null;
  if (val === 'true' || val === '1' || val === true) return true;
  if (val === 'false' || val === '0' || val === false) return false;
  return null;
}

function calculateTrustLevel(uniqueUsers, coveragePct, reconcileOkPct) {
  const users = Number(uniqueUsers) || 0;
  const coverage = Number(coveragePct) || 0;
  const reconcile = Number(reconcileOkPct) || 100;
  
  if (users >= 10 && coverage >= 80 && reconcile >= 90) return 'HIGH';
  if (users >= 5 && coverage >= 60) return 'MEDIUM';
  return 'LOW';
}

const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};

// =============================================================================
// API ENDPOINTS
// =============================================================================

// Health check
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// -----------------------------------------------------------------------------
// DATA QUALITY
// -----------------------------------------------------------------------------
app.get('/api/dq', asyncHandler(async (req, res) => {
  const query = `
    SELECT 
      CURRENT_TIMESTAMP AS as_of_ts,
      COALESCE(dq_header_rows, 0) AS dq_header_rows,
      COALESCE(dq_detail_rows, 0) AS dq_detail_rows,
      COALESCE(dq_header_to_detail_coverage_pct, 0) AS dq_header_to_detail_coverage_pct,
      COALESCE(dq_issuer_match_pct, 0) AS dq_issuer_match_pct,
      COALESCE(dq_product_enrichment_match_pct, 0) AS dq_product_enrichment_match_pct,
      COALESCE(dq_numeric_cast_fail_pct, 0) AS dq_numeric_cast_fail_pct,
      COALESCE(dq_reconcile_ok_pct, 0) AS dq_reconcile_ok_pct
    FROM analytics.radiance_dq_v1
    LIMIT 1
  `;
  try {
    const { rows } = await pool.query(query);
    res.json(rows[0] || {
      as_of_ts: new Date().toISOString(),
      dq_header_rows: 0,
      dq_detail_rows: 0,
      dq_reconcile_ok_pct: 0,
      dq_issuer_match_pct: 0,
      dq_product_enrichment_match_pct: 0
    });
  } catch (err) {
    console.error('DQ Error:', err.message);
    res.json({ error: err.message, dq_reconcile_ok_pct: 0 });
  }
}));

// -----------------------------------------------------------------------------
// ISSUERS - Get ALL issuers from the base view
// -----------------------------------------------------------------------------
app.get('/api/issuers', asyncHandler(async (req, res) => {
  const query = `
    SELECT 
      issuer_ruc,
      COALESCE(retailer_name, issuer_ruc) AS retailer_name,
      retailer_category,
      COUNT(DISTINCT cufe) AS receipt_count,
      COUNT(DISTINCT user_id) AS buyer_count,
      SUM(invoice_total) AS total_sales
    FROM analytics.radiance_base_v1
    WHERE issuer_ruc IS NOT NULL 
      AND issuer_ruc != ''
    GROUP BY issuer_ruc, retailer_name, retailer_category
    ORDER BY total_sales DESC NULLS LAST
  `;
  try {
    const { rows } = await pool.query(query);
    console.log(`Found ${rows.length} issuers`);
    res.json(rows);
  } catch (err) {
    console.error('Issuers Error:', err.message);
    res.json([]);
  }
}));

// -----------------------------------------------------------------------------
// CATEGORIES - Get ALL unique categories
// -----------------------------------------------------------------------------
app.get('/api/categories', asyncHandler(async (req, res) => {
  // Try multiple possible category column names
  const queries = [
    `SELECT DISTINCT category_ai_primary AS category, COUNT(*) AS line_count
     FROM analytics.radiance_base_v1 
     WHERE category_ai_primary IS NOT NULL AND category_ai_primary != '' AND category_ai_primary != 'UNKNOWN'
     GROUP BY category_ai_primary ORDER BY line_count DESC`,
    `SELECT DISTINCT category_ai AS category, COUNT(*) AS line_count
     FROM analytics.radiance_base_v1 
     WHERE category_ai IS NOT NULL AND category_ai != '' AND category_ai != 'UNKNOWN'
     GROUP BY category_ai ORDER BY line_count DESC`,
    `SELECT DISTINCT product_category AS category, COUNT(*) AS line_count
     FROM analytics.radiance_base_v1 
     WHERE product_category IS NOT NULL AND product_category != ''
     GROUP BY product_category ORDER BY line_count DESC`
  ];
  
  for (const query of queries) {
    try {
      const { rows } = await pool.query(query);
      if (rows.length > 0) {
        console.log(`Found ${rows.length} categories`);
        return res.json(rows);
      }
    } catch (err) {
      console.log('Trying next category query...');
    }
  }
  
  // Fallback: return empty
  console.log('No categories found');
  res.json([]);
}));

// -----------------------------------------------------------------------------
// BRANDS
// -----------------------------------------------------------------------------
app.get('/api/brands', asyncHandler(async (req, res) => {
  const issuerRuc = parseParam(req.query.issuer_ruc);
  const category = parseParam(req.query.category);
  
  const query = `
    SELECT DISTINCT 
      COALESCE(product_brand, 'UNKNOWN') AS brand,
      COUNT(*) AS line_count,
      SUM(line_total) AS total_sales
    FROM analytics.radiance_base_v1
    WHERE ($1::text IS NULL OR issuer_ruc = $1)
      AND ($2::text IS NULL OR category_ai_primary = $2 OR category_ai = $2)
      AND product_brand IS NOT NULL
      AND product_brand != ''
    GROUP BY product_brand
    ORDER BY total_sales DESC NULLS LAST
    LIMIT 100
  `;
  try {
    const { rows } = await pool.query(query, [issuerRuc, category]);
    res.json(rows);
  } catch (err) {
    console.error('Brands Error:', err.message);
    res.json([]);
  }
}));

// -----------------------------------------------------------------------------
// RETAILER DISTRIBUTION
// -----------------------------------------------------------------------------
app.get('/api/retailer-distribution', asyncHandler(async (req, res) => {
  const start = parseParam(req.query.start) || '2020-01-01';
  const end = parseParam(req.query.end) || '2099-12-31';
  const reconcileOk = parseBool(req.query.reconcile_ok);

  const query = `
    WITH invoice_base AS (
      SELECT DISTINCT ON (b.cufe)
        b.cufe,
        b.issuer_ruc,
        COALESCE(b.retailer_name, b.issuer_ruc) AS retailer_name,
        b.retailer_category,
        b.invoice_total,
        b.user_id,
        b.invoice_date
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date BETWEEN $1::date AND $2::date
        AND ($3::boolean IS NULL OR r.reconcile_ok = $3)
    )
    SELECT 
      retailer_name,
      retailer_category,
      issuer_ruc,
      COUNT(DISTINCT cufe) AS receipts,
      COUNT(DISTINCT user_id) AS buyers,
      COALESCE(SUM(invoice_total), 0) AS gross_sales,
      ROUND(AVG(invoice_total)::numeric, 2) AS avg_ticket,
      MIN(invoice_date) AS first_date,
      MAX(invoice_date) AS last_date
    FROM invoice_base
    WHERE issuer_ruc IS NOT NULL
    GROUP BY retailer_name, retailer_category, issuer_ruc
    ORDER BY gross_sales DESC
    LIMIT 50
  `;
  try {
    const { rows } = await pool.query(query, [start, end, reconcileOk]);
    res.json(rows);
  } catch (err) {
    console.error('Retailer Distribution Error:', err.message);
    res.json([]);
  }
}));

// -----------------------------------------------------------------------------
// KPIs DAILY
// -----------------------------------------------------------------------------
app.get('/api/kpis/daily', asyncHandler(async (req, res) => {
  const start = parseParam(req.query.start) || '2020-01-01';
  const end = parseParam(req.query.end) || '2099-12-31';
  const issuerRuc = parseParam(req.query.issuer_ruc);
  const storeId = parseParam(req.query.store_id);
  const reconcileOk = parseBool(req.query.reconcile_ok);

  const query = `
    WITH invoice_base AS (
      SELECT DISTINCT ON (b.cufe)
        b.cufe,
        b.invoice_date,
        b.invoice_total,
        b.invoice_tax,
        b.user_id
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date BETWEEN $1::date AND $2::date
        AND ($3::text IS NULL OR b.issuer_ruc = $3)
        AND ($4::text IS NULL OR b.store_id = $4)
        AND ($5::boolean IS NULL OR r.reconcile_ok = $5)
    )
    SELECT 
      invoice_date,
      COUNT(*) AS receipts,
      COUNT(DISTINCT user_id) AS buyers,
      COALESCE(SUM(invoice_total), 0) AS gross_sales,
      COALESCE(SUM(invoice_tax), 0) AS tax_collected,
      ROUND(COALESCE(SUM(invoice_total), 0) / NULLIF(COUNT(*), 0), 2) AS aov,
      ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS frequency
    FROM invoice_base
    GROUP BY invoice_date
    ORDER BY invoice_date
  `;
  try {
    const { rows } = await pool.query(query, [start, end, issuerRuc, storeId, reconcileOk]);
    res.json(rows);
  } catch (err) {
    console.error('KPIs Daily Error:', err.message);
    res.json([]);
  }
}));

// -----------------------------------------------------------------------------
// KPIs SUMMARY
// -----------------------------------------------------------------------------
app.get('/api/kpis/summary', asyncHandler(async (req, res) => {
  const start = parseParam(req.query.start) || '2020-01-01';
  const end = parseParam(req.query.end) || '2099-12-31';
  const issuerRuc = parseParam(req.query.issuer_ruc);
  const storeId = parseParam(req.query.store_id);
  const reconcileOk = parseBool(req.query.reconcile_ok);

  const query = `
    WITH invoice_base AS (
      SELECT DISTINCT ON (b.cufe)
        b.cufe,
        b.invoice_date,
        b.invoice_total,
        b.invoice_tax,
        b.user_id
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date BETWEEN $1::date AND $2::date
        AND ($3::text IS NULL OR b.issuer_ruc = $3)
        AND ($4::text IS NULL OR b.store_id = $4)
        AND ($5::boolean IS NULL OR r.reconcile_ok = $5)
    ),
    current_stats AS (
      SELECT 
        COUNT(*) AS receipts,
        COUNT(DISTINCT user_id) AS buyers,
        COALESCE(SUM(invoice_total), 0) AS gross_sales,
        COALESCE(SUM(invoice_tax), 0) AS tax_collected,
        MIN(invoice_date) AS period_start,
        MAX(invoice_date) AS period_end,
        COUNT(DISTINCT invoice_date) AS active_days
      FROM invoice_base
    ),
    prev_invoice_base AS (
      SELECT DISTINCT ON (b.cufe)
        b.cufe,
        b.invoice_total,
        b.user_id
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date BETWEEN ($1::date - ($2::date - $1::date)) AND ($1::date - INTERVAL '1 day')
        AND ($3::text IS NULL OR b.issuer_ruc = $3)
        AND ($4::text IS NULL OR b.store_id = $4)
        AND ($5::boolean IS NULL OR r.reconcile_ok = $5)
    ),
    prev_stats AS (
      SELECT 
        COUNT(*) AS prev_receipts,
        COUNT(DISTINCT user_id) AS prev_buyers,
        COALESCE(SUM(invoice_total), 0) AS prev_gross_sales
      FROM prev_invoice_base
    )
    SELECT 
      c.receipts,
      c.buyers,
      c.gross_sales,
      c.tax_collected,
      c.period_start,
      c.period_end,
      c.active_days,
      ROUND(c.gross_sales / NULLIF(c.receipts, 0), 2) AS aov,
      ROUND(c.receipts::numeric / NULLIF(c.buyers, 0), 2) AS frequency,
      ROUND(c.gross_sales / NULLIF(c.active_days, 0), 2) AS daily_avg_sales,
      p.prev_receipts,
      p.prev_buyers,
      p.prev_gross_sales,
      CASE WHEN p.prev_gross_sales > 0 
        THEN ROUND(100.0 * (c.gross_sales - p.prev_gross_sales) / p.prev_gross_sales, 1)
        ELSE NULL 
      END AS sales_change_pct,
      CASE WHEN p.prev_receipts > 0 
        THEN ROUND(100.0 * (c.receipts - p.prev_receipts) / p.prev_receipts, 1)
        ELSE NULL 
      END AS receipts_change_pct,
      CASE WHEN p.prev_buyers > 0 
        THEN ROUND(100.0 * (c.buyers - p.prev_buyers) / p.prev_buyers, 1)
        ELSE NULL 
      END AS buyers_change_pct
    FROM current_stats c, prev_stats p
  `;
  try {
    const { rows } = await pool.query(query, [start, end, issuerRuc, storeId, reconcileOk]);
    res.json(rows[0] || { receipts: 0, buyers: 0, gross_sales: 0 });
  } catch (err) {
    console.error('KPIs Summary Error:', err.message);
    res.json({ receipts: 0, buyers: 0, gross_sales: 0, error: err.message });
  }
}));

// -----------------------------------------------------------------------------
// SHARE OF WALLET
// -----------------------------------------------------------------------------
app.get('/api/sow', asyncHandler(async (req, res) => {
  const start = parseParam(req.query.start) || '2020-01-01';
  const end = parseParam(req.query.end) || '2099-12-31';
  const issuerRuc = parseParam(req.query.issuer_ruc);
  const storeId = parseParam(req.query.store_id);
  const category = parseParam(req.query.category);
  const reconcileOk = parseBool(req.query.reconcile_ok);

  const query = `
    WITH line_data AS (
      SELECT 
        b.user_id,
        b.cufe,
        COALESCE(b.category_ai_primary, b.category_ai, 'UNKNOWN') AS category,
        COALESCE(b.product_brand, 'UNKNOWN') AS brand,
        b.line_total
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date BETWEEN $1::date AND $2::date
        AND ($3::text IS NULL OR b.issuer_ruc = $3)
        AND ($4::text IS NULL OR b.store_id = $4)
        AND ($5::text IS NULL OR COALESCE(b.category_ai_primary, b.category_ai) = $5)
        AND ($6::boolean IS NULL OR r.reconcile_ok = $6)
        AND b.line_total IS NOT NULL
        AND b.line_total > 0
    ),
    brand_agg AS (
      SELECT 
        category,
        brand,
        SUM(line_total) AS brand_sales,
        COUNT(DISTINCT user_id) AS unique_users,
        COUNT(DISTINCT cufe) AS receipt_count,
        COUNT(*) AS line_count
      FROM line_data
      WHERE category IS NOT NULL AND category != 'UNKNOWN'
      GROUP BY category, brand
    ),
    category_totals AS (
      SELECT 
        category,
        SUM(brand_sales) AS category_total,
        SUM(unique_users) AS category_users
      FROM brand_agg
      GROUP BY category
    )
    SELECT 
      b.category,
      b.brand,
      b.brand_sales,
      b.unique_users,
      b.receipt_count,
      b.line_count,
      c.category_total,
      c.category_users,
      ROUND(100.0 * b.brand_sales / NULLIF(c.category_total, 0), 2) AS sow_pct,
      ROUND(100.0 * b.unique_users / NULLIF(c.category_users, 0), 2) AS penetration_pct,
      ROUND(b.brand_sales / NULLIF(b.unique_users, 0), 2) AS spend_per_buyer
    FROM brand_agg b
    JOIN category_totals c USING (category)
    ORDER BY b.category, b.brand_sales DESC
  `;
  try {
    const { rows } = await pool.query(query, [start, end, issuerRuc, storeId, category, reconcileOk]);
    const enriched = rows.map(row => ({
      ...row,
      trust_level: calculateTrustLevel(row.unique_users, 80, 95)
    }));
    res.json(enriched);
  } catch (err) {
    console.error('SoW Error:', err.message);
    res.json([]);
  }
}));

// -----------------------------------------------------------------------------
// COVERAGE
// -----------------------------------------------------------------------------
app.get('/api/coverage', asyncHandler(async (req, res) => {
  const start = parseParam(req.query.start) || '2020-01-01';
  const end = parseParam(req.query.end) || '2099-12-31';
  const issuerRuc = parseParam(req.query.issuer_ruc);
  const storeId = parseParam(req.query.store_id);
  const reconcileOk = parseBool(req.query.reconcile_ok);

  const query = `
    SELECT 
      COUNT(*) AS total_lines,
      SUM(b.line_total) AS total_sales,
      SUM(CASE WHEN COALESCE(b.category_ai_primary, b.category_ai) IS NOT NULL 
               AND COALESCE(b.category_ai_primary, b.category_ai) != 'UNKNOWN' THEN 1 ELSE 0 END) AS categorized_lines,
      SUM(CASE WHEN COALESCE(b.category_ai_primary, b.category_ai) IS NOT NULL 
               AND COALESCE(b.category_ai_primary, b.category_ai) != 'UNKNOWN' THEN b.line_total ELSE 0 END) AS categorized_sales,
      SUM(CASE WHEN b.product_matched THEN 1 ELSE 0 END) AS enriched_lines,
      SUM(CASE WHEN b.product_matched THEN b.line_total ELSE 0 END) AS enriched_sales,
      ROUND(100.0 * SUM(CASE WHEN COALESCE(b.category_ai_primary, b.category_ai) IS NOT NULL 
                             AND COALESCE(b.category_ai_primary, b.category_ai) != 'UNKNOWN' THEN b.line_total ELSE 0 END) 
            / NULLIF(SUM(b.line_total), 0), 2) AS category_coverage_pct,
      ROUND(100.0 * SUM(CASE WHEN b.product_matched THEN b.line_total ELSE 0 END) 
            / NULLIF(SUM(b.line_total), 0), 2) AS product_coverage_pct
    FROM analytics.radiance_base_v1 b
    LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
    WHERE b.invoice_date BETWEEN $1::date AND $2::date
      AND ($3::text IS NULL OR b.issuer_ruc = $3)
      AND ($4::text IS NULL OR b.store_id = $4)
      AND ($5::boolean IS NULL OR r.reconcile_ok = $5)
      AND b.line_total > 0
  `;
  try {
    const { rows } = await pool.query(query, [start, end, issuerRuc, storeId, reconcileOk]);
    res.json(rows[0] || { category_coverage_pct: 0, product_coverage_pct: 0 });
  } catch (err) {
    console.error('Coverage Error:', err.message);
    res.json({ category_coverage_pct: 0, product_coverage_pct: 0 });
  }
}));

// -----------------------------------------------------------------------------
// TOP PRODUCTS
// -----------------------------------------------------------------------------
app.get('/api/top-products', asyncHandler(async (req, res) => {
  const start = parseParam(req.query.start) || '2020-01-01';
  const end = parseParam(req.query.end) || '2099-12-31';
  const issuerRuc = parseParam(req.query.issuer_ruc);
  const storeId = parseParam(req.query.store_id);
  const category = parseParam(req.query.category);
  const reconcileOk = parseBool(req.query.reconcile_ok);
  const limit = parseInt(req.query.limit) || 50;

  const query = `
    SELECT 
      b.product_code,
      COALESCE(b.product_description, b.product_code, 'Unknown') AS product_description,
      COALESCE(b.product_brand, 'UNKNOWN') AS brand,
      COALESCE(b.category_ai_primary, b.category_ai) AS category,
      SUM(b.line_total) AS total_sales,
      SUM(b.quantity) AS total_units,
      COUNT(DISTINCT b.cufe) AS receipt_count,
      COUNT(DISTINCT b.user_id) AS buyer_count,
      ROUND(SUM(b.line_total) / NULLIF(COUNT(DISTINCT b.user_id), 0), 2) AS sales_per_buyer
    FROM analytics.radiance_base_v1 b
    LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
    WHERE b.invoice_date BETWEEN $1::date AND $2::date
      AND ($3::text IS NULL OR b.issuer_ruc = $3)
      AND ($4::text IS NULL OR b.store_id = $4)
      AND ($5::text IS NULL OR COALESCE(b.category_ai_primary, b.category_ai) = $5)
      AND ($6::boolean IS NULL OR r.reconcile_ok = $6)
      AND b.line_total IS NOT NULL
      AND b.line_total > 0
    GROUP BY b.product_code, b.product_description, b.product_brand, b.category_ai_primary, b.category_ai
    ORDER BY total_sales DESC
    LIMIT $7
  `;
  try {
    const { rows } = await pool.query(query, [start, end, issuerRuc, storeId, category, reconcileOk, limit]);
    res.json(rows);
  } catch (err) {
    console.error('Top Products Error:', err.message);
    res.json([]);
  }
}));

// -----------------------------------------------------------------------------
// BUYER INSIGHTS
// -----------------------------------------------------------------------------
app.get('/api/buyer-insights', asyncHandler(async (req, res) => {
  const start = parseParam(req.query.start) || '2020-01-01';
  const end = parseParam(req.query.end) || '2099-12-31';
  const issuerRuc = parseParam(req.query.issuer_ruc);
  const storeId = parseParam(req.query.store_id);
  const reconcileOk = parseBool(req.query.reconcile_ok);

  const query = `
    WITH buyer_data AS (
      SELECT DISTINCT ON (b.cufe)
        b.user_id,
        b.invoice_total,
        b.invoice_date
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date BETWEEN $1::date AND $2::date
        AND ($3::text IS NULL OR b.issuer_ruc = $3)
        AND ($4::text IS NULL OR b.store_id = $4)
        AND ($5::boolean IS NULL OR r.reconcile_ok = $5)
    ),
    user_stats AS (
      SELECT 
        user_id,
        COUNT(*) AS visit_count,
        SUM(invoice_total) AS total_spend
      FROM buyer_data
      GROUP BY user_id
    ),
    segments AS (
      SELECT 
        CASE 
          WHEN visit_count = 1 THEN 'One-time'
          WHEN visit_count BETWEEN 2 AND 3 THEN 'Occasional'
          WHEN visit_count BETWEEN 4 AND 10 THEN 'Regular'
          ELSE 'Loyal'
        END AS segment,
        COUNT(*) AS buyer_count,
        SUM(total_spend) AS segment_spend,
        ROUND(AVG(total_spend)::numeric, 2) AS avg_spend,
        ROUND(AVG(visit_count)::numeric, 2) AS avg_visits
      FROM user_stats
      GROUP BY 1
    )
    SELECT 
      json_build_object(
        'segments', (SELECT COALESCE(json_agg(s ORDER BY 
          CASE s.segment 
            WHEN 'One-time' THEN 1 
            WHEN 'Occasional' THEN 2 
            WHEN 'Regular' THEN 3 
            ELSE 4 
          END
        ), '[]'::json) FROM segments s),
        'total_buyers', (SELECT COUNT(DISTINCT user_id) FROM user_stats),
        'avg_visits_per_buyer', (SELECT ROUND(AVG(visit_count)::numeric, 2) FROM user_stats),
        'avg_spend_per_buyer', (SELECT ROUND(AVG(total_spend)::numeric, 2) FROM user_stats)
      ) AS insights
  `;
  try {
    const { rows } = await pool.query(query, [start, end, issuerRuc, storeId, reconcileOk]);
    res.json(rows[0]?.insights || { segments: [], total_buyers: 0 });
  } catch (err) {
    console.error('Buyer Insights Error:', err.message);
    res.json({ segments: [], total_buyers: 0 });
  }
}));

// -----------------------------------------------------------------------------
// REPORT PACK
// -----------------------------------------------------------------------------
app.get('/api/report-pack', asyncHandler(async (req, res) => {
  const start = parseParam(req.query.start) || '2020-01-01';
  const end = parseParam(req.query.end) || '2099-12-31';
  const issuerRuc = parseParam(req.query.issuer_ruc);
  const storeId = parseParam(req.query.store_id);
  const category = parseParam(req.query.category);

  try {
    const [dq, summary, coverage, kpisDaily, sow, topProducts] = await Promise.all([
      pool.query('SELECT * FROM analytics.radiance_dq_v1 LIMIT 1'),
      pool.query(`
        WITH invoice_base AS (
          SELECT DISTINCT ON (b.cufe) b.cufe, b.invoice_total, b.invoice_tax, b.user_id, b.invoice_date
          FROM analytics.radiance_base_v1 b
          LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
          WHERE b.invoice_date BETWEEN $1::date AND $2::date
            AND ($3::text IS NULL OR b.issuer_ruc = $3)
            AND ($4::text IS NULL OR b.store_id = $4)
            AND r.reconcile_ok = true
        )
        SELECT COUNT(*) AS receipts, COUNT(DISTINCT user_id) AS buyers,
               COALESCE(SUM(invoice_total), 0) AS gross_sales,
               ROUND(COALESCE(SUM(invoice_total), 0) / NULLIF(COUNT(*), 0), 2) AS aov,
               ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS frequency
        FROM invoice_base
      `, [start, end, issuerRuc, storeId]),
      pool.query(`
        SELECT ROUND(100.0 * SUM(CASE WHEN COALESCE(b.category_ai_primary, b.category_ai) IS NOT NULL 
                                      AND COALESCE(b.category_ai_primary, b.category_ai) != 'UNKNOWN' 
                                 THEN b.line_total ELSE 0 END) / NULLIF(SUM(b.line_total), 0), 2) AS category_coverage_pct,
               ROUND(100.0 * SUM(CASE WHEN b.product_matched THEN b.line_total ELSE 0 END) / NULLIF(SUM(b.line_total), 0), 2) AS product_coverage_pct
        FROM analytics.radiance_base_v1 b
        LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
        WHERE b.invoice_date BETWEEN $1::date AND $2::date
          AND ($3::text IS NULL OR b.issuer_ruc = $3)
          AND r.reconcile_ok = true AND b.line_total > 0
      `, [start, end, issuerRuc]),
      pool.query(`
        WITH invoice_base AS (
          SELECT DISTINCT ON (b.cufe) b.cufe, b.invoice_date, b.invoice_total, b.user_id
          FROM analytics.radiance_base_v1 b
          LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
          WHERE b.invoice_date BETWEEN $1::date AND $2::date
            AND ($3::text IS NULL OR b.issuer_ruc = $3)
            AND r.reconcile_ok = true
        )
        SELECT invoice_date, COUNT(*) AS receipts, SUM(invoice_total) AS gross_sales
        FROM invoice_base GROUP BY invoice_date ORDER BY invoice_date
      `, [start, end, issuerRuc]),
      pool.query(`
        WITH brand_data AS (
          SELECT COALESCE(b.category_ai_primary, b.category_ai) AS category,
                 COALESCE(b.product_brand, 'UNKNOWN') AS brand,
                 SUM(b.line_total) AS brand_sales, COUNT(DISTINCT b.user_id) AS unique_users
          FROM analytics.radiance_base_v1 b
          LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
          WHERE b.invoice_date BETWEEN $1::date AND $2::date
            AND ($3::text IS NULL OR b.issuer_ruc = $3)
            AND r.reconcile_ok = true AND b.line_total > 0
            AND COALESCE(b.category_ai_primary, b.category_ai) IS NOT NULL 
            AND COALESCE(b.category_ai_primary, b.category_ai) != 'UNKNOWN'
            AND ($4::text IS NULL OR COALESCE(b.category_ai_primary, b.category_ai) = $4)
          GROUP BY 1, 2
        ),
        cat_totals AS (SELECT category, SUM(brand_sales) AS cat_total FROM brand_data GROUP BY category)
        SELECT b.category, b.brand, b.brand_sales, b.unique_users,
               ROUND(100.0 * b.brand_sales / c.cat_total, 2) AS sow_pct
        FROM brand_data b JOIN cat_totals c USING(category)
        ORDER BY b.category, sow_pct DESC
      `, [start, end, issuerRuc, category]),
      pool.query(`
        SELECT COALESCE(b.product_description, 'Unknown') AS product_description,
               COALESCE(b.product_brand, 'UNKNOWN') AS brand,
               SUM(b.line_total) AS total_sales, COUNT(DISTINCT b.user_id) AS buyers
        FROM analytics.radiance_base_v1 b
        LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
        WHERE b.invoice_date BETWEEN $1::date AND $2::date
          AND ($3::text IS NULL OR b.issuer_ruc = $3)
          AND r.reconcile_ok = true AND b.line_total > 0
        GROUP BY b.product_description, b.product_brand
        ORDER BY total_sales DESC LIMIT 10
      `, [start, end, issuerRuc])
    ]);

    const dqData = dq.rows[0] || {};
    const summaryData = summary.rows[0] || {};
    const coverageData = coverage.rows[0] || {};

    res.json({
      generated_at: new Date().toISOString(),
      filters: { start, end, issuer_ruc: issuerRuc, store_id: storeId, category },
      data_quality: dqData,
      summary: summaryData,
      coverage: coverageData,
      kpis_daily: kpisDaily.rows,
      sow: sow.rows,
      top_products: topProducts.rows,
      trust_level: calculateTrustLevel(
        summaryData.buyers || 0,
        coverageData.category_coverage_pct || 0,
        dqData.dq_reconcile_ok_pct || 0
      )
    });
  } catch (err) {
    console.error('Report Pack Error:', err.message);
    res.status(500).json({ error: err.message });
  }
}));

// -----------------------------------------------------------------------------
// DEBUG: Schema info
// -----------------------------------------------------------------------------
app.get('/api/debug/schema', asyncHandler(async (req, res) => {
  const query = `
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema = 'analytics' AND table_name = 'radiance_base_v1'
    ORDER BY ordinal_position
  `;
  try {
    const { rows } = await pool.query(query);
    res.json({ columns: rows });
  } catch (err) {
    res.json({ error: err.message });
  }
}));

// -----------------------------------------------------------------------------
// ERROR HANDLER
// -----------------------------------------------------------------------------
app.use((err, req, res, next) => {
  console.error('API Error:', err);
  res.status(500).json({ error: err.message, hint: 'Check database connection and schema' });
});

// -----------------------------------------------------------------------------
// START SERVER
// -----------------------------------------------------------------------------
app.listen(PORT, '0.0.0.0', () => {
  console.log(`
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║   🌟 RADIANCE DASHBOARD v2.1                                  ║
║   LÜM Internal Analytics MVP                                  ║
║                                                               ║
║   Server running at: http://localhost:${PORT}                    ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
  `);
});
