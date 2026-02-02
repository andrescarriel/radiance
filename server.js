// =============================================================================
// RADIANCE DASHBOARD - SERVER v4.0 FINAL
// Uses correct dim_product join (code_cleaned + issuer_ruc)
// =============================================================================

require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3000;

// Database connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

pool.query('SELECT NOW()')
  .then(() => console.log('✅ Database connected'))
  .catch(err => console.error('❌ Database connection failed:', err.message));

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
      COALESCE(dq_category_match_pct, 0) AS dq_category_match_pct,
      COALESCE(dq_numeric_cast_fail_pct, 0) AS dq_numeric_cast_fail_pct,
      COALESCE(dq_reconcile_ok_pct, 0) AS dq_reconcile_ok_pct
    FROM analytics.radiance_dq_v1
    LIMIT 1
  `;
  try {
    const { rows } = await pool.query(query);
    res.json(rows[0] || { dq_reconcile_ok_pct: 0 });
  } catch (err) {
    console.error('DQ Error:', err.message);
    res.json({ error: err.message, dq_reconcile_ok_pct: 0 });
  }
}));

// -----------------------------------------------------------------------------
// ISSUERS (Retailers)
// -----------------------------------------------------------------------------
app.get('/api/issuers', asyncHandler(async (req, res) => {
  const query = `
    SELECT 
      b.issuer_ruc,
      COALESCE(b.retailer_name, b.issuer_ruc) AS retailer_name,
      b.issuer_category_l1 AS retailer_category,
      b.issuer_category_l2 AS retailer_subcategory,
      COUNT(DISTINCT b.cufe) AS receipt_count,
      COUNT(DISTINCT b.user_id) AS buyer_count,
      ROUND(SUM(b.invoice_total)::numeric, 2) AS total_sales
    FROM analytics.radiance_base_v1 b
    WHERE b.issuer_ruc IS NOT NULL AND b.issuer_ruc != ''
    GROUP BY b.issuer_ruc, b.retailer_name, b.issuer_category_l1, b.issuer_category_l2
    ORDER BY total_sales DESC NULLS LAST
    LIMIT 500
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
// CATEGORIES (Product categories from dim_product)
// -----------------------------------------------------------------------------
app.get('/api/categories', asyncHandler(async (req, res) => {
  const query = `
    SELECT 
      category_l1 AS category,
      category_l2 AS subcategory,
      COUNT(*) AS line_count,
      ROUND(SUM(line_total)::numeric, 2) AS total_sales
    FROM analytics.radiance_base_v1
    WHERE category_l1 IS NOT NULL 
      AND category_l1 != 'UNKNOWN' 
      AND category_l1 != ''
    GROUP BY category_l1, category_l2
    ORDER BY total_sales DESC NULLS LAST
  `;
  try {
    const { rows } = await pool.query(query);
    console.log(`Found ${rows.length} product categories`);
    res.json(rows);
  } catch (err) {
    console.error('Categories Error:', err.message);
    res.json([]);
  }
}));

// -----------------------------------------------------------------------------
// RETAILER CATEGORIES (from dim_issuer - type of store)
// -----------------------------------------------------------------------------
app.get('/api/retailer-categories', asyncHandler(async (req, res) => {
  const query = `
    SELECT 
      issuer_category_l1 AS category,
      issuer_category_l2 AS subcategory,
      COUNT(DISTINCT cufe) AS invoice_count,
      COUNT(DISTINCT issuer_ruc) AS retailer_count,
      ROUND(SUM(line_total)::numeric, 2) AS total_sales
    FROM analytics.radiance_base_v1
    WHERE issuer_category_l1 IS NOT NULL 
      AND issuer_category_l1 != 'UNKNOWN'
    GROUP BY issuer_category_l1, issuer_category_l2
    ORDER BY total_sales DESC
  `;
  try {
    const { rows } = await pool.query(query);
    console.log(`Found ${rows.length} retailer categories`);
    res.json(rows);
  } catch (err) {
    console.error('Retailer Categories Error:', err.message);
    res.json([]);
  }
}));

// -----------------------------------------------------------------------------
// BRANDS (from dim_product)
// -----------------------------------------------------------------------------
app.get('/api/brands', asyncHandler(async (req, res) => {
  const category = parseParam(req.query.category);
  const issuerRuc = parseParam(req.query.issuer_ruc);
  
  const query = `
    SELECT 
      product_brand AS brand,
      category_l1 AS category,
      COUNT(*) AS line_count,
      ROUND(SUM(line_total)::numeric, 2) AS total_sales,
      COUNT(DISTINCT user_id) AS buyer_count
    FROM analytics.radiance_base_v1
    WHERE product_brand IS NOT NULL 
      AND product_brand != 'UNKNOWN'
      AND product_brand != ''
      AND ($1::text IS NULL OR category_l1 = $1)
      AND ($2::text IS NULL OR issuer_ruc = $2)
    GROUP BY product_brand, category_l1
    ORDER BY total_sales DESC NULLS LAST
    LIMIT 100
  `;
  try {
    const { rows } = await pool.query(query, [category, issuerRuc]);
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
        b.cufe, b.issuer_ruc, b.retailer_name,
        b.issuer_category_l1 AS retailer_category,
        b.issuer_category_l2 AS retailer_subcategory,
        b.invoice_total, b.user_id, b.invoice_date
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date BETWEEN $1::date AND $2::date
        AND ($3::boolean IS NULL OR r.reconcile_ok = $3)
    )
    SELECT 
      retailer_name,
      retailer_category,
      retailer_subcategory,
      issuer_ruc,
      COUNT(DISTINCT cufe) AS receipts,
      COUNT(DISTINCT user_id) AS buyers,
      ROUND(SUM(invoice_total)::numeric, 2) AS gross_sales,
      ROUND(AVG(invoice_total)::numeric, 2) AS avg_ticket
    FROM invoice_base
    WHERE issuer_ruc IS NOT NULL
    GROUP BY retailer_name, retailer_category, retailer_subcategory, issuer_ruc
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
  const category = parseParam(req.query.category);
  const reconcileOk = parseBool(req.query.reconcile_ok);

  const query = `
    WITH invoice_base AS (
      SELECT DISTINCT ON (b.cufe)
        b.cufe, b.invoice_date, b.invoice_total, b.invoice_tax, b.user_id
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date BETWEEN $1::date AND $2::date
        AND ($3::text IS NULL OR b.issuer_ruc = $3)
        AND ($4::text IS NULL OR b.category_l1 = $4)
        AND ($5::boolean IS NULL OR r.reconcile_ok = $5)
    )
    SELECT 
      invoice_date,
      COUNT(*) AS receipts,
      COUNT(DISTINCT user_id) AS buyers,
      ROUND(SUM(invoice_total)::numeric, 2) AS gross_sales,
      ROUND((SUM(invoice_total) / NULLIF(COUNT(*), 0))::numeric, 2) AS aov
    FROM invoice_base
    GROUP BY invoice_date
    ORDER BY invoice_date
  `;
  try {
    const { rows } = await pool.query(query, [start, end, issuerRuc, category, reconcileOk]);
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
  const category = parseParam(req.query.category);
  const reconcileOk = parseBool(req.query.reconcile_ok);

  const query = `
    WITH invoice_base AS (
      SELECT DISTINCT ON (b.cufe)
        b.cufe, b.invoice_date, b.invoice_total, b.user_id
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date BETWEEN $1::date AND $2::date
        AND ($3::text IS NULL OR b.issuer_ruc = $3)
        AND ($4::text IS NULL OR b.category_l1 = $4)
        AND ($5::boolean IS NULL OR r.reconcile_ok = $5)
    )
    SELECT 
      COUNT(*) AS receipts,
      COUNT(DISTINCT user_id) AS buyers,
      ROUND(SUM(invoice_total)::numeric, 2) AS gross_sales,
      COUNT(DISTINCT invoice_date) AS active_days,
      ROUND((SUM(invoice_total) / NULLIF(COUNT(*), 0))::numeric, 2) AS aov,
      ROUND((COUNT(*)::numeric / NULLIF(COUNT(DISTINCT user_id), 0)), 2) AS frequency
    FROM invoice_base
  `;
  try {
    const { rows } = await pool.query(query, [start, end, issuerRuc, category, reconcileOk]);
    res.json(rows[0] || { receipts: 0, buyers: 0, gross_sales: 0 });
  } catch (err) {
    console.error('KPIs Summary Error:', err.message);
    res.json({ receipts: 0, buyers: 0, gross_sales: 0, error: err.message });
  }
}));

// -----------------------------------------------------------------------------
// SHARE OF WALLET - By Product Category and Brand
// -----------------------------------------------------------------------------
app.get('/api/sow', asyncHandler(async (req, res) => {
  const start = parseParam(req.query.start) || '2020-01-01';
  const end = parseParam(req.query.end) || '2099-12-31';
  const issuerRuc = parseParam(req.query.issuer_ruc);
  const category = parseParam(req.query.category);
  const reconcileOk = parseBool(req.query.reconcile_ok);

  const query = `
    WITH line_data AS (
      SELECT 
        b.user_id,
        b.cufe,
        COALESCE(b.category_l1, 'UNKNOWN') AS category,
        COALESCE(b.product_brand, b.category_l2, 'UNKNOWN') AS brand,
        b.line_total
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date BETWEEN $1::date AND $2::date
        AND ($3::text IS NULL OR b.issuer_ruc = $3)
        AND ($4::text IS NULL OR b.category_l1 = $4)
        AND ($5::boolean IS NULL OR r.reconcile_ok = $5)
        AND b.line_total IS NOT NULL AND b.line_total > 0
    ),
    brand_agg AS (
      SELECT 
        category,
        brand,
        SUM(line_total) AS brand_sales,
        COUNT(DISTINCT user_id) AS unique_users,
        COUNT(DISTINCT cufe) AS receipt_count
      FROM line_data
      WHERE category != 'UNKNOWN'
      GROUP BY category, brand
    ),
    category_totals AS (
      SELECT category, SUM(brand_sales) AS category_total
      FROM brand_agg
      GROUP BY category
    )
    SELECT 
      b.category,
      b.brand,
      ROUND(b.brand_sales::numeric, 2) AS brand_sales,
      b.unique_users,
      b.receipt_count,
      ROUND(c.category_total::numeric, 2) AS category_total,
      ROUND(100.0 * b.brand_sales / NULLIF(c.category_total, 0), 2) AS sow_pct,
      ROUND((b.brand_sales / NULLIF(b.unique_users, 0))::numeric, 2) AS spend_per_buyer
    FROM brand_agg b
    JOIN category_totals c USING (category)
    ORDER BY b.category, b.brand_sales DESC
  `;
  try {
    const { rows } = await pool.query(query, [start, end, issuerRuc, category, reconcileOk]);
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
  const reconcileOk = parseBool(req.query.reconcile_ok);

  const query = `
    SELECT 
      COUNT(*) AS total_lines,
      SUM(CASE WHEN category_l1 IS NOT NULL AND category_l1 != 'UNKNOWN' THEN 1 ELSE 0 END) AS categorized_lines,
      SUM(CASE WHEN product_matched THEN 1 ELSE 0 END) AS enriched_lines,
      SUM(CASE WHEN product_brand IS NOT NULL AND product_brand != 'UNKNOWN' THEN 1 ELSE 0 END) AS branded_lines,
      ROUND(100.0 * SUM(CASE WHEN category_l1 IS NOT NULL AND category_l1 != 'UNKNOWN' THEN line_total ELSE 0 END) 
            / NULLIF(SUM(line_total), 0), 2) AS category_coverage_pct,
      ROUND(100.0 * SUM(CASE WHEN product_matched THEN line_total ELSE 0 END) 
            / NULLIF(SUM(line_total), 0), 2) AS product_coverage_pct,
      ROUND(100.0 * SUM(CASE WHEN product_brand IS NOT NULL AND product_brand != 'UNKNOWN' THEN line_total ELSE 0 END) 
            / NULLIF(SUM(line_total), 0), 2) AS brand_coverage_pct
    FROM analytics.radiance_base_v1 b
    LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
    WHERE b.invoice_date BETWEEN $1::date AND $2::date
      AND ($3::text IS NULL OR b.issuer_ruc = $3)
      AND ($4::boolean IS NULL OR r.reconcile_ok = $4)
      AND b.line_total > 0
  `;
  try {
    const { rows } = await pool.query(query, [start, end, issuerRuc, reconcileOk]);
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
  const category = parseParam(req.query.category);
  const reconcileOk = parseBool(req.query.reconcile_ok);
  const limit = parseInt(req.query.limit) || 50;

  const query = `
    SELECT 
      b.product_code,
      COALESCE(b.product_description, 'Unknown') AS product_description,
      COALESCE(b.product_brand, 'UNKNOWN') AS brand,
      b.category_l1 AS category,
      b.category_l2 AS subcategory,
      ROUND(SUM(b.line_total)::numeric, 2) AS total_sales,
      ROUND(SUM(b.quantity)::numeric, 2) AS total_units,
      COUNT(DISTINCT b.cufe) AS receipt_count,
      COUNT(DISTINCT b.user_id) AS buyer_count
    FROM analytics.radiance_base_v1 b
    LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
    WHERE b.invoice_date BETWEEN $1::date AND $2::date
      AND ($3::text IS NULL OR b.issuer_ruc = $3)
      AND ($4::text IS NULL OR b.category_l1 = $4)
      AND ($5::boolean IS NULL OR r.reconcile_ok = $5)
      AND b.line_total > 0
    GROUP BY b.product_code, b.product_description, b.product_brand, b.category_l1, b.category_l2
    ORDER BY total_sales DESC
    LIMIT $6
  `;
  try {
    const { rows } = await pool.query(query, [start, end, issuerRuc, category, reconcileOk, limit]);
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
  const reconcileOk = parseBool(req.query.reconcile_ok);

  const query = `
    WITH buyer_data AS (
      SELECT DISTINCT ON (b.cufe)
        b.user_id, b.invoice_total
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date BETWEEN $1::date AND $2::date
        AND ($3::text IS NULL OR b.issuer_ruc = $3)
        AND ($4::boolean IS NULL OR r.reconcile_ok = $4)
    ),
    user_stats AS (
      SELECT user_id, COUNT(*) AS visit_count, SUM(invoice_total) AS total_spend
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
        ROUND(SUM(total_spend)::numeric, 2) AS segment_spend,
        ROUND(AVG(total_spend)::numeric, 2) AS avg_spend
      FROM user_stats
      GROUP BY 1
    )
    SELECT json_build_object(
      'segments', (SELECT COALESCE(json_agg(s ORDER BY 
        CASE s.segment WHEN 'One-time' THEN 1 WHEN 'Occasional' THEN 2 WHEN 'Regular' THEN 3 ELSE 4 END
      ), '[]'::json) FROM segments s),
      'total_buyers', (SELECT COUNT(DISTINCT user_id) FROM user_stats),
      'avg_visits_per_buyer', (SELECT ROUND(AVG(visit_count)::numeric, 2) FROM user_stats),
      'avg_spend_per_buyer', (SELECT ROUND(AVG(total_spend)::numeric, 2) FROM user_stats)
    ) AS insights
  `;
  try {
    const { rows } = await pool.query(query, [start, end, issuerRuc, reconcileOk]);
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
  const category = parseParam(req.query.category);

  try {
    const [dq, summary, coverage, kpisDaily, sow, topProducts] = await Promise.all([
      pool.query('SELECT * FROM analytics.radiance_dq_v1 LIMIT 1'),
      pool.query(`
        WITH ib AS (
          SELECT DISTINCT ON (b.cufe) b.cufe, b.invoice_total, b.user_id, b.invoice_date
          FROM analytics.radiance_base_v1 b
          LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
          WHERE b.invoice_date BETWEEN $1::date AND $2::date
            AND ($3::text IS NULL OR b.issuer_ruc = $3)
            AND r.reconcile_ok = true
        )
        SELECT COUNT(*) AS receipts, COUNT(DISTINCT user_id) AS buyers,
               ROUND(SUM(invoice_total)::numeric, 2) AS gross_sales,
               ROUND((SUM(invoice_total) / NULLIF(COUNT(*), 0))::numeric, 2) AS aov,
               ROUND((COUNT(*)::numeric / NULLIF(COUNT(DISTINCT user_id), 0)), 2) AS frequency
        FROM ib
      `, [start, end, issuerRuc]),
      pool.query(`
        SELECT ROUND(100.0 * SUM(CASE WHEN category_l1 IS NOT NULL AND category_l1 != 'UNKNOWN' THEN line_total ELSE 0 END) / NULLIF(SUM(line_total), 0), 2) AS category_coverage_pct,
               ROUND(100.0 * SUM(CASE WHEN product_matched THEN line_total ELSE 0 END) / NULLIF(SUM(line_total), 0), 2) AS product_coverage_pct
        FROM analytics.radiance_base_v1 b
        LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
        WHERE b.invoice_date BETWEEN $1::date AND $2::date AND ($3::text IS NULL OR b.issuer_ruc = $3) AND r.reconcile_ok = true AND b.line_total > 0
      `, [start, end, issuerRuc]),
      pool.query(`
        WITH ib AS (
          SELECT DISTINCT ON (b.cufe) b.cufe, b.invoice_date, b.invoice_total
          FROM analytics.radiance_base_v1 b
          LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
          WHERE b.invoice_date BETWEEN $1::date AND $2::date AND ($3::text IS NULL OR b.issuer_ruc = $3) AND r.reconcile_ok = true
        )
        SELECT invoice_date, COUNT(*) AS receipts, ROUND(SUM(invoice_total)::numeric, 2) AS gross_sales
        FROM ib GROUP BY invoice_date ORDER BY invoice_date
      `, [start, end, issuerRuc]),
      pool.query(`
        WITH bd AS (
          SELECT COALESCE(b.category_l1, 'UNKNOWN') AS category, 
                 COALESCE(b.product_brand, b.category_l2, 'UNKNOWN') AS brand,
                 SUM(b.line_total) AS brand_sales, COUNT(DISTINCT b.user_id) AS unique_users
          FROM analytics.radiance_base_v1 b
          LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
          WHERE b.invoice_date BETWEEN $1::date AND $2::date AND ($3::text IS NULL OR b.issuer_ruc = $3)
            AND r.reconcile_ok = true AND b.line_total > 0 AND b.category_l1 IS NOT NULL AND b.category_l1 != 'UNKNOWN'
          GROUP BY 1, 2
        ),
        ct AS (SELECT category, SUM(brand_sales) AS cat_total FROM bd GROUP BY category)
        SELECT bd.category, bd.brand, ROUND(bd.brand_sales::numeric, 2) AS brand_sales, bd.unique_users, 
               ROUND(100.0 * bd.brand_sales / ct.cat_total, 2) AS sow_pct
        FROM bd JOIN ct USING(category) ORDER BY bd.category, sow_pct DESC
      `, [start, end, issuerRuc]),
      pool.query(`
        SELECT COALESCE(b.product_description, 'Unknown') AS product_description, 
               COALESCE(b.product_brand, 'UNKNOWN') AS brand,
               ROUND(SUM(b.line_total)::numeric, 2) AS total_sales, COUNT(DISTINCT b.user_id) AS buyers
        FROM analytics.radiance_base_v1 b
        LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
        WHERE b.invoice_date BETWEEN $1::date AND $2::date AND ($3::text IS NULL OR b.issuer_ruc = $3)
          AND r.reconcile_ok = true AND b.line_total > 0
        GROUP BY b.product_description, b.product_brand ORDER BY total_sales DESC LIMIT 10
      `, [start, end, issuerRuc])
    ]);

    const dqData = dq.rows[0] || {};
    const summaryData = summary.rows[0] || {};
    const coverageData = coverage.rows[0] || {};

    res.json({
      generated_at: new Date().toISOString(),
      filters: { start, end, issuer_ruc: issuerRuc, category },
      data_quality: dqData,
      summary: summaryData,
      coverage: coverageData,
      kpis_daily: kpisDaily.rows,
      sow: sow.rows,
      top_products: topProducts.rows,
      trust_level: calculateTrustLevel(summaryData.buyers || 0, coverageData.category_coverage_pct || 0, dqData.dq_reconcile_ok_pct || 0)
    });
  } catch (err) {
    console.error('Report Pack Error:', err.message);
    res.status(500).json({ error: err.message });
  }
}));

// Error handler
app.use((err, req, res, next) => {
  console.error('API Error:', err);
  res.status(500).json({ error: err.message });
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`
╔═══════════════════════════════════════════════════════════════╗
║   🌟 RADIANCE DASHBOARD v4.0                                  ║
║   Server running at: http://localhost:${PORT}                    ║
╚═══════════════════════════════════════════════════════════════╝
  `);
});
