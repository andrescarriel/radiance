// =============================================================================
// RADIANCE API SERVER v2.1.0 — COMPLETE CONSOLIDATED
// =============================================================================
// Sprints: 1-Capture, 2-Switching, 3-Leakage, 4-Basket, 5-Loyalty, 6-Panel, 7-Deck
// DOD-67 Compliant: ≥8 charts, disclaimers, UNKNOWN handling, projections labeled
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
const VERSION = '2.1.0';
const SPRINTS = ['1-capture', '2-switching', '3-leakage', '4-basket', '5-loyalty', '6-panel', '7-deck'];
const VALID_CAT_COLS = { 'l1': 'category_l1', 'l2': 'category_l2', 'l3': 'category_l3', 'l4': 'category_l4' };

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
// HELPERS
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
  return isNaN(k) || k < 1 ? 5 : k;
};

const asyncHandler = (fn) => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);

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
// LANDING PAGE HTML
// =============================================================================
function generateLandingHTML() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>LÜM Radiance API</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #0b0f19;
      --card: #12172a;
      --border: rgba(99,102,241,0.2);
      --text: #f1f5f9;
      --muted: #64748b;
      --cyan: #06b6d4;
      --purple: #8b5cf6;
      --green: #10b981;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: 'Inter', sans-serif; background: var(--bg); color: var(--text); min-height: 100vh; padding: 40px 20px; }
    .container { max-width: 900px; margin: 0 auto; }
    .header { text-align: center; margin-bottom: 48px; }
    .logo { font-size: 12px; font-weight: 700; letter-spacing: 4px; color: var(--cyan); margin-bottom: 8px; }
    h1 { font-size: 36px; font-weight: 800; margin-bottom: 8px; }
    .version { font-family: 'JetBrains Mono', monospace; font-size: 14px; color: var(--purple); margin-bottom: 16px; }
    .status { display: inline-flex; align-items: center; gap: 8px; background: rgba(16,185,129,0.1); border: 1px solid rgba(16,185,129,0.3); padding: 8px 16px; border-radius: 20px; font-size: 13px; color: var(--green); }
    .status::before { content: ''; width: 8px; height: 8px; background: var(--green); border-radius: 50%; animation: pulse 2s infinite; }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
    .section { margin-bottom: 32px; }
    .section h2 { font-size: 14px; color: var(--muted); text-transform: uppercase; letter-spacing: 1px; margin-bottom: 16px; padding-bottom: 8px; border-bottom: 1px solid var(--border); }
    .endpoints { display: grid; gap: 12px; }
    .endpoint { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 16px 20px; display: flex; align-items: center; gap: 16px; transition: all 0.2s; }
    .endpoint:hover { border-color: var(--cyan); transform: translateX(4px); }
    .method { font-family: 'JetBrains Mono', monospace; font-size: 11px; font-weight: 600; padding: 4px 8px; border-radius: 4px; background: rgba(6,182,212,0.2); color: var(--cyan); }
    .path { font-family: 'JetBrains Mono', monospace; font-size: 14px; color: var(--text); flex: 1; }
    .desc { font-size: 12px; color: var(--muted); max-width: 280px; text-align: right; }
    .sprint-badge { font-size: 10px; padding: 2px 8px; border-radius: 10px; background: rgba(139,92,246,0.2); color: var(--purple); }
    .quick-links { display: flex; gap: 12px; flex-wrap: wrap; justify-content: center; margin-top: 32px; }
    .quick-link { background: linear-gradient(135deg, var(--cyan), var(--purple)); color: white; padding: 12px 24px; border-radius: 8px; text-decoration: none; font-weight: 600; font-size: 14px; transition: transform 0.2s, box-shadow 0.2s; }
    .quick-link:hover { transform: translateY(-2px); box-shadow: 0 8px 24px rgba(6,182,212,0.3); }
    .footer { text-align: center; margin-top: 48px; color: var(--muted); font-size: 12px; }
    .footer a { color: var(--cyan); text-decoration: none; }
  </style>
</head>
<body>
<div class="container">
  <header class="header">
    <div class="logo">LÜM RADIANCE</div>
    <h1>Panel Analytics API</h1>
    <div class="version">v${VERSION}</div>
    <div class="status">API Online · Database Connected</div>
  </header>

  <section class="section">
    <h2>Quick Start</h2>
    <div class="quick-links">
  <a href="/index.html" class="quick-link">Dashboard</a>
  <a href="/api/health" class="quick-link">Health Check</a>
  <a href="/api/deck/commerce?start=2025-01-01&end=2025-07-01&issuer_ruc=DEMO" class="quick-link">Demo Deck</a>
</div>
  </section>

  <section class="section">
    <h2>Available Endpoints</h2>
    <div class="endpoints">
      <div class="endpoint">
        <span class="method">GET</span>
        <span class="path">/api/health</span>
        <span class="desc">Database connection check</span>
      </div>
      <div class="endpoint">
        <span class="method">GET</span>
        <span class="path">/api/sow_leakage/by_category</span>
        <span class="sprint-badge">Sprint 1</span>
        <span class="desc">SoW and leakage by category</span>
      </div>
      <div class="endpoint">
        <span class="method">GET</span>
        <span class="path">/api/switching/destinations</span>
        <span class="sprint-badge">Sprint 2</span>
        <span class="desc">Where customers shop elsewhere</span>
      </div>
      <div class="endpoint">
        <span class="method">GET</span>
        <span class="path">/api/leakage/tree</span>
        <span class="sprint-badge">Sprint 3</span>
        <span class="desc">6-bucket leakage waterfall</span>
      </div>
      <div class="endpoint">
        <span class="method">GET</span>
        <span class="path">/api/basket/breadth</span>
        <span class="sprint-badge">Sprint 4</span>
        <span class="desc">Basket breadth analysis</span>
      </div>
      <div class="endpoint">
        <span class="method">GET</span>
        <span class="path">/api/loyalty/brands</span>
        <span class="sprint-badge">Sprint 5</span>
        <span class="desc">Brand loyalty metrics</span>
      </div>
      <div class="endpoint">
        <span class="method">GET</span>
        <span class="path">/api/panel/summary</span>
        <span class="sprint-badge">Sprint 6</span>
        <span class="desc">Panel stats + projection</span>
      </div>
      <div class="endpoint">
        <span class="method">GET</span>
        <span class="path">/api/deck/commerce</span>
        <span class="sprint-badge">Sprint 7</span>
        <span class="desc">Full HTML deck with charts</span>
      </div>
    </div>
  </section>

  <section class="section">
    <h2>Required Parameters</h2>
    <div class="endpoints">
      <div class="endpoint">
        <span class="path">start</span>
        <span class="desc">Start date (YYYY-MM-DD)</span>
      </div>
      <div class="endpoint">
        <span class="path">end</span>
        <span class="desc">End date (YYYY-MM-DD, exclusive)</span>
      </div>
      <div class="endpoint">
        <span class="path">issuer_ruc</span>
        <span class="desc">Retailer identifier</span>
      </div>
    </div>
  </section>

  <footer class="footer">
    <p>LÜM Radiance · Panel Analytics Platform · <a href="https://github.com/lum-analytics">GitHub</a></p>
    <p style="margin-top: 8px;">Built with ❤️ for retail intelligence</p>
  </footer>
</div>
</body>
</html>`;
}

// =============================================================================
// DECK HTML GENERATOR v2.0 (DOD-67 Compliant)
// =============================================================================
function generateDeckHTML(data) {
  const { panel, panelTrend, capture, switching, leakage, basket, loyalty, filters, generatedAt } = data;
  
  const formatNum = (n) => n != null ? n.toLocaleString('en-US') : '—';
  const formatPct = (n) => n != null ? `${Number(n).toFixed(1)}%` : '—';
  const formatUSD = (n) => n != null ? `$${Number(n).toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 })}` : '—';
  
  const startD = new Date(filters.start);
  const endD = new Date(filters.end);
  const windowDays = Math.round((endD - startD) / (1000 * 60 * 60 * 24));

  // Prepare chart data as JSON strings
  const trendLabels = JSON.stringify(panelTrend?.map(t => t.month) || []);
  const trendSpend = JSON.stringify(panelTrend?.map(t => t.spend) || []);
  const trendCustomers = JSON.stringify(panelTrend?.map(t => t.customers) || []);
  
  const captureLabels = JSON.stringify(capture?.data?.slice(0, 10).map(c => (c.category_value || '').substring(0, 15)) || []);
  const captureSoW = JSON.stringify(capture?.data?.slice(0, 10).map(c => c.sow_pct) || []);
  const captureInX = JSON.stringify(capture?.data?.slice(0, 10).map(c => Math.round(c.spend_in_x_usd || 0)) || []);
  const captureLeakage = JSON.stringify(capture?.data?.slice(0, 10).map(c => Math.round(c.leakage_usd || 0)) || []);
  
  const switchLabels = JSON.stringify(switching?.data?.slice(0, 8).map(s => (s.destination || '').substring(0, 12)) || []);
  const switchPcts = JSON.stringify(switching?.data?.slice(0, 8).map(s => s.pct) || []);
  
  const leakageLabels = JSON.stringify(['RETAINED', 'CAT_GONE', 'RED_BASKET', 'RED_FREQ', 'DELAYED', 'CHURN']);
  const leakageData = leakage?.waterfall ? JSON.stringify(leakage.waterfall.map(w => w.pct)) : JSON.stringify([0,0,0,0,0,0]);
  const hasLeakageData = leakage?.waterfall?.some(w => w.pct > 0);
  
  const basketLabels = JSON.stringify(basket?.data?.slice(0, 6).map(b => b.origin_month) || []);
  const basketMarket = JSON.stringify(basket?.data?.slice(0, 6).map(b => b.avg_breadth_market) || []);
  const basketInX = JSON.stringify(basket?.data?.slice(0, 6).map(b => b.avg_breadth_in_x) || []);
  
  const loyaltyTiers = loyalty?.tiers || { exclusive: 0, loyal: 0, prefer: 0, light: 0 };
  const loyaltyTierData = JSON.stringify([loyaltyTiers.exclusive || 0, loyaltyTiers.loyal || 0, loyaltyTiers.prefer || 0, loyaltyTiers.light || 0]);
  const loyaltyDist = loyalty?.distribution || { p10: 0, p25: 0, p50: 0, p75: 0, p90: 0 };
  const loyaltyDistData = JSON.stringify([loyaltyDist.p10 || 0, loyaltyDist.p25 || 0, loyaltyDist.p50 || 0, loyaltyDist.p75 || 0, loyaltyDist.p90 || 0]);
  const hasLoyaltyData = loyalty?.data?.length > 0;

  const trustBadge = (level) => {
    const colors = { HIGH: '#10b981', MEDIUM: '#f59e0b', LOW: '#f97316', SUPPRESSED: '#ef4444' };
    return level ? `<span class="trust-badge" style="background:${colors[level] || '#475569'}">${level}</span>` : '';
  };

  return `<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>LÜM Radiance | Commerce Deck</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg-primary: #0b0f19;
      --bg-card: #12172a;
      --bg-card-alt: #161d32;
      --border: rgba(99, 102, 241, 0.15);
      --border-accent: rgba(99, 102, 241, 0.4);
      --text-primary: #f1f5f9;
      --text-secondary: #94a3b8;
      --text-muted: #64748b;
      --accent-cyan: #06b6d4;
      --accent-purple: #8b5cf6;
      --accent-green: #10b981;
      --gradient-accent: linear-gradient(135deg, #06b6d4, #8b5cf6);
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: 'Inter', -apple-system, sans-serif; background: var(--bg-primary); color: var(--text-primary); line-height: 1.5; font-size: 14px; }
    .container { max-width: 1400px; margin: 0 auto; padding: 24px; }
    .header { text-align: center; margin-bottom: 32px; padding-bottom: 24px; border-bottom: 1px solid var(--border); }
    .logo { font-size: 11px; font-weight: 700; letter-spacing: 4px; color: var(--accent-cyan); margin-bottom: 4px; }
    .header h1 { font-size: 28px; font-weight: 800; color: var(--text-primary); margin-bottom: 8px; }
    .header .meta { font-size: 13px; color: var(--text-secondary); margin-bottom: 16px; }
    .header .meta strong { color: var(--text-primary); }
    .disclaimer-banner { background: rgba(234, 179, 8, 0.1); border: 1px solid rgba(234, 179, 8, 0.3); border-radius: 8px; padding: 12px 16px; font-size: 11px; color: #fbbf24; margin-bottom: 24px; }
    .disclaimer-banner strong { color: #fcd34d; }
    .chips { display: flex; justify-content: center; gap: 10px; flex-wrap: wrap; }
    .chip { background: rgba(99, 102, 241, 0.08); border: 1px solid var(--border); padding: 6px 14px; border-radius: 20px; font-size: 12px; font-family: 'JetBrains Mono', monospace; }
    .chip .label { color: var(--text-muted); }
    .chip .value { color: var(--accent-cyan); font-weight: 500; margin-left: 4px; }
    .hero-section { margin-bottom: 32px; }
    .hero-kpi { background: var(--bg-card); border: 1px solid var(--border-accent); border-radius: 16px; padding: 32px; text-align: center; position: relative; overflow: hidden; }
    .hero-kpi::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px; background: var(--gradient-accent); }
    .hero-kpi .label { font-size: 13px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px; }
    .hero-kpi .value { font-size: 64px; font-weight: 800; font-family: 'JetBrains Mono', monospace; background: var(--gradient-accent); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; line-height: 1; }
    .hero-kpi .sub { font-size: 14px; color: var(--text-secondary); margin-top: 8px; }
    .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 16px; margin-bottom: 32px; }
    .kpi-card { background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px; padding: 20px; text-align: center; }
    .kpi-card .value { font-size: 28px; font-weight: 700; font-family: 'JetBrains Mono', monospace; color: var(--text-primary); margin-bottom: 4px; }
    .kpi-card .label { font-size: 11px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.5px; }
    .projection-box { background: linear-gradient(135deg, rgba(6,182,212,0.1), rgba(139,92,246,0.1)); border: 1px solid rgba(139,92,246,0.3); border-radius: 12px; padding: 20px; text-align: center; margin-bottom: 32px; }
    .projection-box .tag { font-size: 10px; color: var(--accent-purple); text-transform: uppercase; letter-spacing: 2px; margin-bottom: 4px; }
    .projection-box .big { font-size: 36px; font-weight: 800; font-family: 'JetBrains Mono', monospace; color: var(--accent-purple); }
    .projection-box .note { font-size: 11px; color: var(--text-muted); margin-top: 4px; }
    .section { margin-bottom: 40px; }
    .section-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 16px; }
    .section-title { display: flex; align-items: center; gap: 10px; }
    .section-title .icon { font-size: 20px; }
    .section-title h2 { font-size: 18px; font-weight: 700; }
    .section-title .question { font-size: 12px; color: var(--text-muted); font-style: italic; margin-left: 8px; }
    .trust-badge { padding: 4px 10px; border-radius: 12px; font-size: 10px; font-weight: 600; text-transform: uppercase; color: white; }
    .charts-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px; }
    .charts-grid.single { grid-template-columns: 1fr; }
    @media (max-width: 900px) { .charts-grid { grid-template-columns: 1fr; } }
    .chart-card { background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px; padding: 20px; }
    .chart-card .chart-header { margin-bottom: 12px; }
    .chart-card .chart-title { font-size: 13px; font-weight: 600; color: var(--text-primary); }
    .chart-card .chart-meta { font-size: 10px; color: var(--text-muted); margin-top: 2px; }
    .chart-card canvas { max-height: 220px; }
    .table-card { background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px; overflow: hidden; margin-top: 16px; }
    .table-card .table-title { padding: 12px 16px; font-size: 12px; font-weight: 600; color: var(--text-secondary); border-bottom: 1px solid var(--border); }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th { text-align: left; padding: 10px 14px; background: var(--bg-card-alt); color: var(--text-muted); font-size: 10px; text-transform: uppercase; font-weight: 600; }
    td { padding: 10px 14px; border-bottom: 1px solid rgba(255,255,255,0.03); color: var(--text-secondary); }
    tr:hover td { background: rgba(99,102,241,0.05); }
    .text-right { text-align: right; }
    .mono { font-family: 'JetBrains Mono', monospace; }
    .highlight { color: var(--accent-cyan); font-weight: 600; }
    .suppressed { color: var(--text-muted); font-style: italic; }
    .empty-state { padding: 40px; text-align: center; color: var(--text-muted); font-style: italic; background: rgba(0,0,0,0.2); border-radius: 8px; border: 1px dashed var(--border); }
    .footer-disclaimers { background: rgba(234, 179, 8, 0.08); border: 1px solid rgba(234, 179, 8, 0.2); border-radius: 12px; padding: 20px; margin-top: 40px; }
    .footer-disclaimers h3 { font-size: 12px; color: #fbbf24; margin-bottom: 12px; }
    .footer-disclaimers ul { margin-left: 16px; font-size: 11px; color: var(--text-muted); }
    .footer-disclaimers li { margin-bottom: 4px; }
    .footer { text-align: center; margin-top: 32px; padding-top: 20px; border-top: 1px solid var(--border); }
    .footer .brand { font-size: 11px; font-weight: 700; letter-spacing: 3px; color: var(--accent-cyan); }
    .footer .timestamp { font-size: 10px; color: var(--text-muted); font-family: 'JetBrains Mono', monospace; margin-top: 4px; }
  </style>
</head>
<body>
<div class="container">
  <header class="header">
    <div class="logo">LÜM RADIANCE</div>
    <h1>Commerce Insights Deck</h1>
    <p class="meta">Retailer: <strong>${filters.issuer_ruc}</strong> · Window: <strong>${filters.start}</strong> → <strong>${filters.end}</strong> (${windowDays} days)</p>
    <div class="chips">
      <span class="chip"><span class="label">reconcile_ok</span><span class="value">${filters.reconcile_ok ?? 'all'}</span></span>
      <span class="chip"><span class="label">k_threshold</span><span class="value">${filters.k_threshold || 5}</span></span>
      <span class="chip"><span class="label">source</span><span class="value">Observed Panel</span></span>
    </div>
  </header>
  
  <div class="disclaimer-banner">
    <strong>⚠️ DISCLAIMER:</strong> Panel observado LÜM. Proyección REFERENCIAL / ILUSTRATIVA. Sin inferencia causal. UNKNOWN siempre incluido.
  </div>

  <section class="hero-section">
    <div class="hero-kpi">
      <div class="label">Share of Wallet en X</div>
      <div class="value">${formatPct(panel?.sow_pct)}</div>
      <div class="sub">${formatUSD(panel?.spend_in_x_usd)} de ${formatUSD(panel?.spend_market_usd)} capturados</div>
    </div>
  </section>

  <div class="kpi-grid">
    <div class="kpi-card"><div class="value mono">${formatNum(panel?.customers_n)}</div><div class="label">Clientes Panel</div></div>
    <div class="kpi-card"><div class="value mono">${formatNum(panel?.invoices_n)}</div><div class="label">Facturas</div></div>
    <div class="kpi-card"><div class="value mono">${formatNum(panel?.active_months)}</div><div class="label">Meses Activos</div></div>
    <div class="kpi-card"><div class="value mono">${formatUSD(panel?.avg_spend_per_customer)}</div><div class="label">Gasto Prom/Cliente</div></div>
  </div>

  <div class="projection-box">
    <div class="tag">⚡ Proyección REFERENCIAL / ILUSTRATIVA</div>
    <div class="big">≈ ${formatNum(panel?.projection?.projected_households)} hogares</div>
    <div class="note">Factor: ${panel?.projection?.expansion_factor || 100}x · Source: ${panel?.projection?.expansion_source || 'default'} · NO causal</div>
  </div>

  <!-- PANEL TRENDS -->
  <section class="section">
    <div class="section-header">
      <div class="section-title"><span class="icon">📈</span><h2>Panel Trends</h2><span class="question">¿Cómo evoluciona el panel mes a mes?</span></div>
    </div>
    <div class="charts-grid">
      <div class="chart-card">
        <div class="chart-header"><div class="chart-title">Gasto en X por Mes</div><div class="chart-meta">Unidad: USD · Rango: ${filters.start} → ${filters.end} · Fuente: Observed Panel</div></div>
        <canvas id="chartSpendTrend"></canvas>
      </div>
      <div class="chart-card">
        <div class="chart-header"><div class="chart-title">Clientes Únicos por Mes</div><div class="chart-meta">Unidad: Usuarios · Rango: ${filters.start} → ${filters.end} · Fuente: Observed Panel</div></div>
        <canvas id="chartCustomersTrend"></canvas>
      </div>
    </div>
  </section>

  <!-- COMMERCE CAPTURE -->
  <section class="section">
    <div class="section-header">
      <div class="section-title"><span class="icon">🎯</span><h2>Commerce Capture</h2><span class="question">¿Cuánto captura X vs mercado por categoría?</span></div>
    </div>
    ${capture?.data?.length > 0 ? `
    <div class="charts-grid">
      <div class="chart-card">
        <div class="chart-header"><div class="chart-title">Share of Wallet por Categoría (Top 10)</div><div class="chart-meta">Unidad: % · Fuente: Observed Panel</div></div>
        <canvas id="chartCaptureSoW"></canvas>
      </div>
      <div class="chart-card">
        <div class="chart-header"><div class="chart-title">In-X vs Leakage por Categoría</div><div class="chart-meta">Unidad: USD · Fuente: Observed Panel</div></div>
        <canvas id="chartCaptureStacked"></canvas>
      </div>
    </div>
    <div class="table-card">
      <div class="table-title">Detalle por Categoría</div>
      <table>
        <thead><tr><th>Categoría</th><th class="text-right">Users</th><th class="text-right">Gasto X</th><th class="text-right">Leakage</th><th class="text-right">SoW %</th></tr></thead>
        <tbody>
          ${capture.data.slice(0, 10).map(c => `
          <tr>
            <td class="${c.category_value === 'UNKNOWN' ? 'suppressed' : ''}">${c.category_value}${c.category_value === 'UNKNOWN' ? ' (SUPPRESSED)' : ''}</td>
            <td class="text-right mono">${formatNum(c.users)}</td>
            <td class="text-right mono">${formatUSD(c.spend_in_x_usd)}</td>
            <td class="text-right mono">${formatUSD(c.leakage_usd)}</td>
            <td class="text-right mono highlight">${formatPct(c.sow_pct)}</td>
          </tr>`).join('')}
        </tbody>
      </table>
    </div>` : '<div class="empty-state">Sin datos de capture. Verifique filtros.</div>'}
  </section>

  <!-- SWITCHING -->
  <section class="section">
    <div class="section-header">
      <div class="section-title"><span class="icon">🔄</span><h2>Switching Destinations</h2><span class="question">¿A dónde van los clientes cuando no compran en X?</span></div>
    </div>
    ${switching?.data?.length > 0 ? `
    <div class="charts-grid single">
      <div class="chart-card">
        <div class="chart-header"><div class="chart-title">Top Destinos (% del Cohort)</div><div class="chart-meta">Unidad: % de usuarios · Fuente: Observed Panel</div></div>
        <canvas id="chartSwitching"></canvas>
      </div>
    </div>` : '<div class="empty-state">Sin datos de switching. Verifique filtros.</div>'}
  </section>

  <!-- LEAKAGE WATERFALL -->
  <section class="section">
    <div class="section-header">
      <div class="section-title"><span class="icon">🌊</span><h2>Leakage Waterfall</h2><span class="question">¿Por qué los usuarios no recompran en X?</span></div>
      ${trustBadge(leakage?.trust_level)}
    </div>
    ${hasLeakageData ? `
    <div class="charts-grid single">
      <div class="chart-card">
        <div class="chart-header"><div class="chart-title">Waterfall 6-Bucket (${leakage.category || 'Aggregated'})</div><div class="chart-meta">Unidad: % del cohorte · Buckets: Retained → Full Churn · Fuente: Observed Panel</div></div>
        <canvas id="chartLeakage"></canvas>
      </div>
    </div>` : '<div class="empty-state">Leakage tree requiere category_value. Auto-select aplicado pero sin datos suficientes.</div>'}
  </section>

  <!-- BASKET BREADTH -->
  <section class="section">
    <div class="section-header">
      <div class="section-title"><span class="icon">🛒</span><h2>Basket Breadth</h2><span class="question">¿Qué tan completo es el basket en X vs mercado?</span></div>
    </div>
    ${basket?.data?.length > 0 ? `
    <div class="charts-grid single">
      <div class="chart-card">
        <div class="chart-header"><div class="chart-title">Breadth: Market vs In-X por Mes</div><div class="chart-meta">Unidad: # categorías promedio · Fuente: Observed Panel</div></div>
        <canvas id="chartBasket"></canvas>
      </div>
    </div>` : '<div class="empty-state">Sin datos de basket breadth para el período.</div>'}
  </section>

  <!-- BRAND LOYALTY -->
  <section class="section">
    <div class="section-header">
      <div class="section-title"><span class="icon">💎</span><h2>Brand Loyalty</h2><span class="question">¿A qué marcas son leales?${loyalty?.category ? ` (${loyalty.category})` : ''}</span></div>
      ${trustBadge(loyalty?.trust_level)}
    </div>
    ${hasLoyaltyData ? `
    <div class="charts-grid">
      <div class="chart-card">
        <div class="chart-header"><div class="chart-title">Distribución de Lealtad (Tiers)</div><div class="chart-meta">Exclusive≥95% · Loyal≥80% · Prefer≥50% · Light&lt;50% · Fuente: Observed Panel</div></div>
        <canvas id="chartLoyaltyTiers"></canvas>
      </div>
      <div class="chart-card">
        <div class="chart-header"><div class="chart-title">User Brand Share Distribution (Percentiles)</div><div class="chart-meta">Unidad: % share · p10 → p90 · Fuente: Observed Panel</div></div>
        <canvas id="chartLoyaltyDist"></canvas>
      </div>
    </div>
    <div class="table-card">
      <div class="table-title">Top Marcas</div>
      <table>
        <thead><tr><th>Marca</th><th class="text-right">Buyers</th><th class="text-right">Penetración</th><th class="text-right">p75 Share</th><th class="text-right">Loyalty Rate</th></tr></thead>
        <tbody>
          ${loyalty.data.filter(l => l.brand !== 'OTHER_SUPPRESSED').slice(0, 8).map(l => `
          <tr>
            <td class="${l.brand === 'UNKNOWN' ? 'suppressed' : ''}">${l.brand}${l.brand === 'UNKNOWN' ? ' (SUPPRESSED)' : ''}</td>
            <td class="text-right mono">${formatNum(l.brand_buyers)}</td>
            <td class="text-right mono">${formatPct(l.penetration_pct)}</td>
            <td class="text-right mono">${formatPct(l.p75)}</td>
            <td class="text-right mono highlight">${formatPct(l.loyalty_rate_pct)}</td>
          </tr>`).join('')}
        </tbody>
      </table>
    </div>` : '<div class="empty-state">Loyalty requiere category_value. Auto-select aplicado pero sin datos suficientes.</div>'}
  </section>

  <div class="footer-disclaimers">
    <h3>⚠️ Disclaimers Obligatorios</h3>
    <ul>
      ${DECK_DISCLAIMERS.map(d => `<li>${d}</li>`).join('')}
    </ul>
  </div>

  <footer class="footer">
    <div class="brand">LÜM RADIANCE</div>
    <div class="timestamp">Generated: ${generatedAt} · v${VERSION} · DOD-67 Compliant</div>
  </footer>
</div>

<script>
  Chart.defaults.color = '#94a3b8';
  Chart.defaults.borderColor = 'rgba(99, 102, 241, 0.1)';
  Chart.defaults.font.family = "'Inter', sans-serif";
  Chart.defaults.font.size = 11;

  // Chart 1: Spend Trend
  const trendLabels = ${trendLabels};
  if (trendLabels.length > 0) {
    new Chart(document.getElementById('chartSpendTrend'), {
      type: 'line',
      data: { labels: trendLabels, datasets: [{ label: 'Gasto (USD)', data: ${trendSpend}, borderColor: '#06b6d4', backgroundColor: 'rgba(6,182,212,0.1)', fill: true, tension: 0.3, pointRadius: 4 }] },
      options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: false } } }
    });
  }

  // Chart 2: Customers Trend
  if (trendLabels.length > 0) {
    new Chart(document.getElementById('chartCustomersTrend'), {
      type: 'line',
      data: { labels: trendLabels, datasets: [{ label: 'Clientes', data: ${trendCustomers}, borderColor: '#8b5cf6', backgroundColor: 'rgba(139,92,246,0.1)', fill: true, tension: 0.3, pointRadius: 4 }] },
      options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: false } } }
    });
  }

  // Chart 3: Capture SoW
  const captureLabels = ${captureLabels};
  if (captureLabels.length > 0) {
    new Chart(document.getElementById('chartCaptureSoW'), {
      type: 'bar',
      data: { labels: captureLabels, datasets: [{ label: 'SoW %', data: ${captureSoW}, backgroundColor: 'rgba(6,182,212,0.7)', borderRadius: 4 }] },
      options: { indexAxis: 'y', responsive: true, plugins: { legend: { display: false } }, scales: { x: { max: 100 } } }
    });
  }

  // Chart 4: Capture Stacked
  if (captureLabels.length > 0) {
    new Chart(document.getElementById('chartCaptureStacked'), {
      type: 'bar',
      data: { labels: captureLabels, datasets: [
        { label: 'In-X', data: ${captureInX}, backgroundColor: 'rgba(16,185,129,0.7)', borderRadius: 4 },
        { label: 'Leakage', data: ${captureLeakage}, backgroundColor: 'rgba(239,68,68,0.7)', borderRadius: 4 }
      ]},
      options: { indexAxis: 'y', responsive: true, plugins: { legend: { position: 'bottom' } }, scales: { x: { stacked: true }, y: { stacked: true } } }
    });
  }

  // Chart 5: Switching
  const switchLabels = ${switchLabels};
  if (switchLabels.length > 0) {
    new Chart(document.getElementById('chartSwitching'), {
      type: 'bar',
      data: { labels: switchLabels, datasets: [{ label: '% Cohort', data: ${switchPcts}, backgroundColor: 'rgba(236,72,153,0.7)', borderRadius: 4 }] },
      options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { max: 100 } } }
    });
  }

  // Chart 6: Leakage Waterfall
  ${hasLeakageData ? `
  new Chart(document.getElementById('chartLeakage'), {
    type: 'bar',
    data: { labels: ${leakageLabels}, datasets: [{ label: '% Cohorte', data: ${leakageData}, backgroundColor: ['#10b981', '#f59e0b', '#f97316', '#f97316', '#ef4444', '#64748b'], borderRadius: 4 }] },
    options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { max: 100 } } }
  });` : ''}

  // Chart 7: Basket Breadth
  const basketLabels = ${basketLabels};
  if (basketLabels.length > 0) {
    new Chart(document.getElementById('chartBasket'), {
      type: 'bar',
      data: { labels: basketLabels, datasets: [
        { label: 'Market', data: ${basketMarket}, backgroundColor: 'rgba(148,163,184,0.6)', borderRadius: 4 },
        { label: 'In-X', data: ${basketInX}, backgroundColor: 'rgba(6,182,212,0.7)', borderRadius: 4 }
      ]},
      options: { responsive: true, plugins: { legend: { position: 'bottom' } } }
    });
  }

  // Chart 8: Loyalty Tiers
  ${hasLoyaltyData ? `
  new Chart(document.getElementById('chartLoyaltyTiers'), {
    type: 'doughnut',
    data: { labels: ['EXCLUSIVE (≥95%)', 'LOYAL (≥80%)', 'PREFER (≥50%)', 'LIGHT (<50%)'], datasets: [{ data: ${loyaltyTierData}, backgroundColor: ['#10b981', '#06b6d4', '#8b5cf6', '#64748b'], borderWidth: 0 }] },
    options: { responsive: true, plugins: { legend: { position: 'bottom' } } }
  });` : ''}

  // Chart 9: Loyalty Distribution
  ${hasLoyaltyData ? `
  new Chart(document.getElementById('chartLoyaltyDist'), {
    type: 'bar',
    data: { labels: ['p10', 'p25', 'p50', 'p75', 'p90'], datasets: [{ label: 'User Brand Share %', data: ${loyaltyDistData}, backgroundColor: 'rgba(139,92,246,0.7)', borderRadius: 4 }] },
    options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { max: 100 } } }
  });` : ''}
</script>
</body>
</html>`;
}

// =============================================================================
// ROUTES: LANDING & HEALTH
// =============================================================================
app.get('/', (req, res) => {
  res.set('Content-Type', 'text/html; charset=utf-8');
  res.send(generateLandingHTML());
});

app.get('/api/health', asyncHandler(async (req, res) => {
  const start = Date.now();
  const result = await pool.query('SELECT NOW() as db_time, current_database() as db_name');
  res.json({
    status: 'ok',
    version: VERSION,
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
    data: rows.map(r => ({
      issuer_ruc: r.issuer_ruc,
      issuer_name: r.issuer_name,
      users: Number(r.users),
      invoices: Number(r.invoices)
    }))
  });
}));
// =============================================================================
// CATEGORIES LIST (Products + Commerce)
// =============================================================================
app.get('/api/categories', asyncHandler(async (req, res) => {
const categoryLevel = (req.query.category_level || 'l1').toLowerCase();
  const peerScope = (req.query.peer_scope || 'all').toLowerCase();

  const errors = [];
  const type = req.query.type || 'all'; // 'product', 'commerce', 'all'
  
  const productCol = VALID_CAT_COLS[categoryLevel] || 'category_l1';
  const commerceCol = `commerce_${categoryLevel}`; // commerce_l1, commerce_l2, etc.
  
  let query;
  
  if (type === 'product') {
    query = `
      SELECT DISTINCT COALESCE(b.${productCol}, 'UNKNOWN') AS category_value,
        'product' AS category_type,
        COUNT(DISTINCT b.user_id) AS users,
        SUM(COALESCE(b.line_total, 0)) AS spend
      FROM analytics.radiance_base_v1 b
      WHERE b.${productCol} IS NOT NULL AND b.${productCol} != 'UNKNOWN'
      GROUP BY 1
      HAVING COUNT(DISTINCT b.user_id) >= 1
      ORDER BY spend DESC
      LIMIT 100
    `;
  } else if (type === 'commerce') {
    query = `
      SELECT DISTINCT COALESCE(b.${commerceCol}, 'UNKNOWN') AS category_value,
        'commerce' AS category_type,
        COUNT(DISTINCT b.user_id) AS users,
        SUM(COALESCE(b.line_total, 0)) AS spend
      FROM analytics.radiance_base_v1 b
      WHERE b.${commerceCol} IS NOT NULL AND b.${commerceCol} != 'UNKNOWN'
      GROUP BY 1
      HAVING COUNT(DISTINCT b.user_id) >= 1
      ORDER BY spend DESC
      LIMIT 100
    `;
  } else {
    // Both
    query = `
      (SELECT DISTINCT COALESCE(b.${productCol}, 'UNKNOWN') AS category_value,
        'product' AS category_type,
        COUNT(DISTINCT b.user_id) AS users,
        SUM(COALESCE(b.line_total, 0)) AS spend
      FROM analytics.radiance_base_v1 b
      WHERE b.${productCol} IS NOT NULL AND b.${productCol} != 'UNKNOWN'
      GROUP BY 1
      HAVING COUNT(DISTINCT b.user_id) >= 1
      ORDER BY spend DESC
      LIMIT 50)
      UNION ALL
      (SELECT DISTINCT COALESCE(b.${commerceCol}, 'UNKNOWN') AS category_value,
        'commerce' AS category_type,
        COUNT(DISTINCT b.user_id) AS users,
        SUM(COALESCE(b.line_total, 0)) AS spend
      FROM analytics.radiance_base_v1 b
      WHERE b.${commerceCol} IS NOT NULL AND b.${commerceCol} != 'UNKNOWN'
      GROUP BY 1
      HAVING COUNT(DISTINCT b.user_id) >= 1
      ORDER BY spend DESC
      LIMIT 50)
    `;
  }
  
  const { rows } = await pool.query(query);
  res.json({
    data: rows.map(r => ({
      category_value: r.category_value,
      category_type: r.category_type,
      users: Number(r.users),
      spend: Number(r.spend)
    }))
  });
}));
// =============================================================================
// SPRINT 0: KPIs SUMMARY (Overview Dashboard)
// =============================================================================
app.get('/api/kpis/summary', asyncHandler(async (req, res) => {
  const startDate = parseDate(req.query.start);
  const endDate = parseDate(req.query.end);
  const issuerRuc = parseString(req.query.issuer_ruc);
  const reconcileOk = parseBool(req.query.reconcile_ok, null); // FIX: default NULL
const categoryLevel = (req.query.category_level || 'l1').toLowerCase();
  const peerScope = (req.query.peer_scope || 'all').toLowerCase();

  const errors = [];
  if (!startDate) errors.push('Invalid or missing start date');
  if (!endDate) errors.push('Invalid or missing end date');
  if (!issuerRuc) errors.push('issuer_ruc is required');
  if (errors.length > 0) return res.status(400).json({ error: errors.join('; ') });

  const catCol = VALID_CAT_COLS[categoryLevel] || 'category_l1';
  const start = Date.now();

  // Calculate previous period (same duration, immediately before)
  const startD = new Date(startDate);
  const endD = new Date(endDate);
  const daysDiff = Math.round((endD - startD) / (1000 * 60 * 60 * 24));
  const prevEnd = startDate;
  const prevStart = new Date(startD);
  prevStart.setDate(prevStart.getDate() - daysDiff);
  const prevStartStr = prevStart.toISOString().split('T')[0];

  // Main KPIs query
  const kpisQuery = `
    WITH base AS (
      SELECT 
        b.user_id,
        b.cufe,
        b.invoice_date,
        COALESCE(b.${catCol}, 'UNKNOWN') AS category,
        SUM(COALESCE(b.line_total, 0)) AS line_total
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.issuer_ruc = $4
        AND b.user_id IS NOT NULL
      GROUP BY b.user_id, b.cufe, b.invoice_date, COALESCE(b.${catCol}, 'UNKNOWN')
    ),
    kpis AS (
      SELECT
        SUM(line_total) AS ventas,
        COUNT(DISTINCT cufe) AS transacciones,
        COUNT(DISTINCT user_id) AS clientes,
        COUNT(DISTINCT category) AS categorias
      FROM base
    ),
    market AS (
      SELECT SUM(COALESCE(b.line_total, 0)) AS spend_market
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.user_id IN (SELECT DISTINCT user_id FROM base)
    )
    SELECT 
      k.*,
      CASE WHEN k.transacciones > 0 THEN k.ventas / k.transacciones ELSE 0 END AS ticket_promedio,
      CASE WHEN k.clientes > 0 THEN k.transacciones::float / k.clientes ELSE 0 END AS frecuencia,
      CASE WHEN m.spend_market > 0 THEN 100.0 * k.ventas / m.spend_market ELSE 0 END AS sow_pct
    FROM kpis k CROSS JOIN market m
  `;

  // Previous period KPIs
  const prevKpisQuery = `
    WITH base AS (
      SELECT b.user_id, b.cufe, SUM(COALESCE(b.line_total, 0)) AS line_total
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.issuer_ruc = $4
        AND b.user_id IS NOT NULL
		GROUP BY b.user_id, b.cufe
    )
    SELECT
      SUM(line_total) AS ventas,
      COUNT(DISTINCT cufe) AS transacciones,
      COUNT(DISTINCT user_id) AS clientes,
      CASE WHEN COUNT(DISTINCT cufe) > 0 THEN SUM(line_total) / COUNT(DISTINCT cufe) ELSE 0 END AS ticket_promedio
    FROM base
  `;

  // Monthly trends
  const trendsQuery = `
    SELECT 
      DATE_TRUNC('month', b.invoice_date)::date AS month,
      SUM(COALESCE(b.line_total, 0)) AS ventas,
      COUNT(DISTINCT b.user_id) AS clientes,
      COUNT(DISTINCT b.cufe) AS transacciones
    FROM analytics.radiance_base_v1 b
    LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
    WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
      AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
      AND b.issuer_ruc = $4
      AND b.user_id IS NOT NULL
    GROUP BY DATE_TRUNC('month', b.invoice_date)
    ORDER BY month
  `;

  // By day of week
  const weekdayQuery = `
    WITH base AS (
      SELECT EXTRACT(DOW FROM b.invoice_date) AS dow, COUNT(DISTINCT b.cufe) AS txn
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.issuer_ruc = $4
        AND b.user_id IS NOT NULL
      GROUP BY dow
    ),
    total AS (SELECT SUM(txn) AS total FROM base)
    SELECT 
      dow,
      CASE dow 
        WHEN 0 THEN 'Dom' WHEN 1 THEN 'Lun' WHEN 2 THEN 'Mar' 
        WHEN 3 THEN 'Mié' WHEN 4 THEN 'Jue' WHEN 5 THEN 'Vie' WHEN 6 THEN 'Sáb' 
      END AS day_name,
      txn,
      ROUND(100.0 * txn / NULLIF(total, 0), 1) AS pct
    FROM base CROSS JOIN total
    ORDER BY dow
  `;

  // Top categories
  const topCatQuery = `
    SELECT 
      COALESCE(b.${catCol}, 'UNKNOWN') AS category,
      SUM(COALESCE(b.line_total, 0)) AS ventas,
      COUNT(DISTINCT b.user_id) AS clientes
    FROM analytics.radiance_base_v1 b
    LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
    WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
      AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
      AND b.issuer_ruc = $4
      AND b.user_id IS NOT NULL
    GROUP BY COALESCE(b.${catCol}, 'UNKNOWN')
    ORDER BY ventas DESC
    LIMIT 10
  `;

  // Execute all queries
  const [kpisResult, prevResult, trendsResult, weekdayResult, topCatResult] = await Promise.all([
    pool.query(kpisQuery, [startDate, endDate, reconcileOk, issuerRuc]),
    pool.query(prevKpisQuery, [prevStartStr, prevEnd, reconcileOk, issuerRuc]),
    pool.query(trendsQuery, [startDate, endDate, reconcileOk, issuerRuc]),
    pool.query(weekdayQuery, [startDate, endDate, reconcileOk, issuerRuc]),
    pool.query(topCatQuery, [startDate, endDate, reconcileOk, issuerRuc])
  ]);

  const current = kpisResult.rows[0] || {};
  const previous = prevResult.rows[0] || null;

  res.set('X-Query-Time-Ms', String(Date.now() - start));
  res.json({
    filters: { start: startDate, end: endDate, issuer_ruc: issuerRuc, reconcile_ok: reconcileOk },
    current: {
      ventas: Number(current.ventas || 0),
      transacciones: Number(current.transacciones || 0),
      clientes: Number(current.clientes || 0),
      ticket_promedio: Number(current.ticket_promedio || 0),
      frecuencia: Number(current.frecuencia || 0),
      categorias: Number(current.categorias || 0),
      sow_pct: Number(current.sow_pct || 0)
    },
    previous: previous ? {
      ventas: Number(previous.ventas || 0),
      transacciones: Number(previous.transacciones || 0),
      clientes: Number(previous.clientes || 0),
      ticket_promedio: Number(previous.ticket_promedio || 0)
    } : null,
    trends: trendsResult.rows.map(r => ({
      month: r.month.toISOString().split('T')[0].substring(0, 7),
      ventas: Number(r.ventas || 0),
      clientes: Number(r.clientes || 0),
      transacciones: Number(r.transacciones || 0)
    })),
    by_weekday: weekdayResult.rows.map(r => ({
      dow: Number(r.dow),
      day_name: r.day_name,
      txn: Number(r.txn || 0),
      pct: Number(r.pct || 0)
    })),
    top_categories: topCatResult.rows.map(r => ({
      category: r.category,
      ventas: Number(r.ventas || 0),
      clientes: Number(r.clientes || 0)
    }))
  });
}));



// =============================================================================
// SPRINT 1: CAPTURE (SoW & Leakage by Category)
// =============================================================================
app.get('/api/sow_leakage/by_category', asyncHandler(async (req, res) => {
  const startDate = parseDate(req.query.start);
  const endDate = parseDate(req.query.end);
  const issuerRuc = parseString(req.query.issuer_ruc);
  const storeId = parseString(req.query.store_id);
  const reconcileOk = parseBool(req.query.reconcile_ok, null);
  const kThreshold = parseKThreshold(req.query.k_threshold);
  const categoryLevel = (req.query.category_level || 'l1').toLowerCase();

  const errors = [];
  if (!startDate) errors.push('Invalid or missing start date');
  if (!endDate) errors.push('Invalid or missing end date');
  if (!issuerRuc) errors.push('issuer_ruc is required');
  if (!VALID_CAT_COLS[categoryLevel]) errors.push('Invalid category_level');
  if (errors.length > 0) return res.status(400).json({ error: errors.join('; ') });

  const catCol = VALID_CAT_COLS[categoryLevel];
  const start = Date.now();

  // Build peer scope filter for market denominator
  let peerScopeJoin = '';
  let peerScopeWhere = '';
  
  if (peerScope === 'peers') {
    // Solo comercios del mismo tipo (issuer_l1)
    peerScopeJoin = `
      LEFT JOIN public.dim_issuer di_x ON di_x.issuer_ruc = $4
      LEFT JOIN public.dim_issuer di_b ON di_b.issuer_ruc = b.issuer_ruc`;
    peerScopeWhere = `AND (b.issuer_ruc = $4 OR di_b.issuer_l1 = di_x.issuer_l1)`;
  } else if (peerScope === 'extended') {
    // Comercios que venden la categoría de producto (requiere category_value en filtro)
    // Por ahora igual que ALL, se refina después
    peerScopeJoin = '';
    peerScopeWhere = '';
  }
  // 'all' = sin filtro adicional (default)

  const query = `
    WITH cohort AS (
      SELECT DISTINCT b.user_id FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.issuer_ruc = $4 AND b.user_id IS NOT NULL
    ),
    peer_spend AS (
      SELECT b.user_id, COALESCE(b.${catCol}, 'UNKNOWN') AS category_value, b.issuer_ruc, 
        SUM(COALESCE(b.line_total, 0)) AS spend
      FROM analytics.radiance_base_v1 b
      INNER JOIN cohort c ON b.user_id = c.user_id
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      ${peerScopeJoin}
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        ${peerScopeWhere}
      GROUP BY b.user_id, COALESCE(b.${catCol}, 'UNKNOWN'), b.issuer_ruc
    )
    SELECT category_value, COUNT(DISTINCT user_id) AS users,
      SUM(CASE WHEN issuer_ruc = $4 THEN spend ELSE 0 END) AS spend_in_x_usd,
      SUM(spend) AS spend_market_usd,
      SUM(spend) - SUM(CASE WHEN issuer_ruc = $4 THEN spend ELSE 0 END) AS leakage_usd,
      ROUND(100.0 * SUM(CASE WHEN issuer_ruc = $4 THEN spend ELSE 0 END) / NULLIF(SUM(spend), 0), 2) AS sow_pct
    FROM peer_spend GROUP BY category_value HAVING COUNT(DISTINCT user_id) >= $5
    ORDER BY spend_in_x_usd DESC
  `;
  const { rows } = await pool.query(query, [startDate, endDate, reconcileOk, issuerRuc, kThreshold]);

  res.set('X-Query-Time-Ms', String(Date.now() - start));
  res.json({
    filters: { start: startDate, end: endDate, issuer_ruc: issuerRuc, store_id: storeId, reconcile_ok: reconcileOk, k_threshold: kThreshold, category_level: categoryLevel, peer_scope: peerScope },
    data: rows.map(r => ({
      category_value: r.category_value,
      users: Number(r.users),
      spend_in_x_usd: Math.round(Number(r.spend_in_x_usd) * 100) / 100,
      spend_market_usd: Math.round(Number(r.spend_market_usd) * 100) / 100,
      leakage_usd: Math.round(Number(r.leakage_usd) * 100) / 100,
      sow_pct: Number(r.sow_pct),
      trust_level: r.category_value === 'UNKNOWN' ? 'SUPPRESSED' : null
    })),
    disclaimers: CAPTURE_DISCLAIMERS
  });
}));

// =============================================================================
// SPRINT 2: SWITCHING DESTINATIONS
// =============================================================================
app.get('/api/switching/destinations', asyncHandler(async (req, res) => {
  const startDate = parseDate(req.query.start);
  const endDate = parseDate(req.query.end);
  const issuerRuc = parseString(req.query.issuer_ruc);
  const reconcileOk = parseBool(req.query.reconcile_ok, null);
  const kThreshold = parseKThreshold(req.query.k_threshold);

  const errors = [];
  if (!startDate) errors.push('Invalid or missing start date');
  if (!endDate) errors.push('Invalid or missing end date');
  if (!issuerRuc) errors.push('issuer_ruc is required');
  if (errors.length > 0) return res.status(400).json({ error: errors.join('; ') });

  const start = Date.now();
  const query = `
    WITH cohort AS (
      SELECT DISTINCT user_id FROM analytics.radiance_base_v1
      WHERE invoice_date >= $1::date AND invoice_date < $2::date AND issuer_ruc = $4 AND user_id IS NOT NULL
    ),
    elsewhere AS (
      SELECT b.user_id, COALESCE(b.issuer_name, b.issuer_ruc) AS destination
      FROM analytics.radiance_base_v1 b
      INNER JOIN cohort c ON b.user_id = c.user_id
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3) AND b.issuer_ruc != $4
    )
    SELECT destination, COUNT(DISTINCT user_id) AS users,
      ROUND(100.0 * COUNT(DISTINCT user_id) / NULLIF((SELECT COUNT(*) FROM cohort), 0), 2) AS pct
    FROM elsewhere GROUP BY destination HAVING COUNT(DISTINCT user_id) >= $5
    ORDER BY users DESC LIMIT 15
  `;
  const { rows } = await pool.query(query, [startDate, endDate, reconcileOk, issuerRuc, kThreshold]);

  res.set('X-Query-Time-Ms', String(Date.now() - start));
  res.json({
    filters: { start: startDate, end: endDate, issuer_ruc: issuerRuc, reconcile_ok: reconcileOk, k_threshold: kThreshold },
    data: rows.map(r => ({ destination: r.destination, users: Number(r.users), pct: Number(r.pct) })),
    disclaimers: SWITCHING_DISCLAIMERS
  });
}));

// =============================================================================
// SPRINT 3: LEAKAGE TREE (6-Bucket Waterfall)
// =============================================================================
app.get('/api/leakage/tree', asyncHandler(async (req, res) => {
  const startDate = parseDate(req.query.start);
  const endDate = parseDate(req.query.end);
  const issuerRuc = parseString(req.query.issuer_ruc);
  const categoryValue = parseString(req.query.category_value);
  const reconcileOk = parseBool(req.query.reconcile_ok, null);
  const kThreshold = parseKThreshold(req.query.k_threshold);
const categoryLevel = (req.query.category_level || 'l1').toLowerCase();
  const peerScope = (req.query.peer_scope || 'all').toLowerCase();

  const errors = [];
  if (!startDate) errors.push('Invalid or missing start date');
  if (!endDate) errors.push('Invalid or missing end date');
  if (!issuerRuc) errors.push('issuer_ruc is required');
  if (!categoryValue) errors.push('category_value is required');
  if (errors.length > 0) return res.status(400).json({ error: errors.join('; ') });

  const catCol = VALID_CAT_COLS[categoryLevel] || 'category_l1';
  const start = Date.now();

 const query = `
    WITH base_txn AS (
      SELECT b.user_id, DATE_TRUNC('month', b.invoice_date)::date AS txn_month, b.issuer_ruc,
        SUM(COALESCE(b.line_total, 0)) AS line_total, COUNT(DISTINCT b.cufe) AS visits
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.user_id IS NOT NULL
        AND COALESCE(b.${catCol}, 'UNKNOWN') = $5
      GROUP BY b.user_id, DATE_TRUNC('month', b.invoice_date)::date, b.issuer_ruc
    ),
    months AS (SELECT DISTINCT txn_month FROM base_txn ORDER BY txn_month),
    month_pairs AS (SELECT txn_month AS origin, LEAD(txn_month) OVER (ORDER BY txn_month) AS next FROM months),
    user_month AS (
      SELECT user_id, txn_month,
        SUM(CASE WHEN issuer_ruc = $4 THEN visits ELSE 0 END) AS visits_x,
        SUM(CASE WHEN issuer_ruc = $4 THEN line_total ELSE 0 END) AS spend_x,
        SUM(visits) AS visits_total
      FROM base_txn GROUP BY user_id, txn_month
    ),
    cohort AS (SELECT user_id, txn_month AS origin FROM user_month WHERE visits_x > 0),
    transitions AS (
      SELECT c.user_id, c.origin, mp.next,
        COALESCE(um_o.visits_x, 0) AS vx_o, COALESCE(um_o.spend_x, 0) AS sx_o,
        COALESCE(um_n.visits_x, 0) AS vx_n, COALESCE(um_n.spend_x, 0) AS sx_n,
        COALESCE(um_n.visits_total, 0) AS vt_n
      FROM cohort c
      INNER JOIN month_pairs mp ON c.origin = mp.origin
      LEFT JOIN user_month um_o ON c.user_id = um_o.user_id AND c.origin = um_o.txn_month
      LEFT JOIN user_month um_n ON c.user_id = um_n.user_id AND mp.next = um_n.txn_month
      WHERE mp.next IS NOT NULL
    ),
    buckets AS (
      SELECT origin,
        CASE
          WHEN vx_n > 0 AND sx_n >= sx_o * 0.9 THEN 'RETAINED'
          WHEN vt_n > 0 AND vx_n = 0 THEN 'CATEGORY_GONE'
          WHEN vx_n > 0 AND sx_n < sx_o * 0.5 THEN 'REDUCED_BASKET'
          WHEN vx_n > 0 AND vx_n < vx_o THEN 'REDUCED_FREQ'
          WHEN vt_n = 0 THEN 'FULL_CHURN'
          ELSE 'DELAYED_ONLY'
        END AS bucket
      FROM transitions
    )
    SELECT bucket, COUNT(*) AS users, ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER(), 0), 2) AS pct
    FROM buckets GROUP BY bucket
  `;
  const { rows } = await pool.query(query, [startDate, endDate, reconcileOk, issuerRuc, categoryValue]);

  const bucketOrder = ['RETAINED', 'CATEGORY_GONE', 'REDUCED_BASKET', 'REDUCED_FREQ', 'DELAYED_ONLY', 'FULL_CHURN'];
  const bucketMap = {};
  rows.forEach(r => { bucketMap[r.bucket] = { users: Number(r.users), pct: Number(r.pct) }; });

  res.set('X-Query-Time-Ms', String(Date.now() - start));
  res.json({
    filters: { start: startDate, end: endDate, issuer_ruc: issuerRuc, category_value: categoryValue, reconcile_ok: reconcileOk, k_threshold: kThreshold },
    waterfall: bucketOrder.map(b => ({ bucket: b, users: bucketMap[b]?.users || 0, pct: bucketMap[b]?.pct || 0 })),
    disclaimers: LEAKAGE_DISCLAIMERS
  });
}));

// =============================================================================
// SPRINT 4: BASKET BREADTH
// =============================================================================
app.get('/api/basket/breadth', asyncHandler(async (req, res) => {
  const startDate = parseDate(req.query.start);
  const endDate = parseDate(req.query.end);
  const issuerRuc = parseString(req.query.issuer_ruc);
  const reconcileOk = parseBool(req.query.reconcile_ok, null);
  const kThreshold = parseKThreshold(req.query.k_threshold);
const categoryLevel = (req.query.category_level || 'l1').toLowerCase();
  const peerScope = (req.query.peer_scope || 'all').toLowerCase();

  const errors = [];
  if (!startDate) errors.push('Invalid or missing start date');
  if (!endDate) errors.push('Invalid or missing end date');
  if (!issuerRuc) errors.push('issuer_ruc is required');
  if (errors.length > 0) return res.status(400).json({ error: errors.join('; ') });

  const catCol = VALID_CAT_COLS[categoryLevel] || 'category_l1';
  const start = Date.now();

 const query = `
    WITH base_txn AS (
      SELECT b.user_id, DATE_TRUNC('month', b.invoice_date)::date AS txn_month, b.issuer_ruc,
        COALESCE(b.${catCol}, 'UNKNOWN') AS cat_val
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.user_id IS NOT NULL
    ),
    cohort AS (SELECT DISTINCT user_id, txn_month FROM base_txn WHERE issuer_ruc = $4),
    user_breadth AS (
      SELECT c.user_id, c.txn_month,
        COUNT(DISTINCT bt.cat_val) AS breadth_total,
        COUNT(DISTINCT CASE WHEN bt.issuer_ruc = $4 THEN bt.cat_val END) AS breadth_in_x
      FROM cohort c
      INNER JOIN base_txn bt ON c.user_id = bt.user_id AND c.txn_month = bt.txn_month
      GROUP BY c.user_id, c.txn_month
    )
    SELECT txn_month AS origin_month, COUNT(DISTINCT user_id) AS users,
      ROUND(AVG(breadth_total), 2) AS avg_breadth_market,
      ROUND(AVG(breadth_in_x), 2) AS avg_breadth_in_x
    FROM user_breadth GROUP BY txn_month HAVING COUNT(DISTINCT user_id) >= $5
    ORDER BY txn_month
  `;
  const { rows } = await pool.query(query, [startDate, endDate, reconcileOk, issuerRuc, kThreshold]);

  res.set('X-Query-Time-Ms', String(Date.now() - start));
  res.json({
    filters: { start: startDate, end: endDate, issuer_ruc: issuerRuc, reconcile_ok: reconcileOk, k_threshold: kThreshold, category_level: categoryLevel },
    data: rows.map(r => ({
      origin_month: r.origin_month.toISOString().split('T')[0].substring(0, 7),
      users: Number(r.users),
      avg_breadth_market: Number(r.avg_breadth_market),
      avg_breadth_in_x: Number(r.avg_breadth_in_x)
    })),
    disclaimers: BASKET_DISCLAIMERS
  });
}));

// =============================================================================
// SPRINT 5: BRAND LOYALTY
// =============================================================================
app.get('/api/loyalty/brands', asyncHandler(async (req, res) => {
  const startDate = parseDate(req.query.start);
  const endDate = parseDate(req.query.end);
  const issuerRuc = parseString(req.query.issuer_ruc);
  const categoryValue = parseString(req.query.category_value);
  const reconcileOk = parseBool(req.query.reconcile_ok, null);
  const kThreshold = parseKThreshold(req.query.k_threshold);
const categoryLevel = (req.query.category_level || 'l1').toLowerCase();
  const peerScope = (req.query.peer_scope || 'all').toLowerCase();

  const errors = [];
  if (!startDate) errors.push('Invalid or missing start date');
  if (!endDate) errors.push('Invalid or missing end date');
  if (!issuerRuc) errors.push('issuer_ruc is required');
  if (!categoryValue) errors.push('category_value is required');
  if (errors.length > 0) return res.status(400).json({ error: errors.join('; ') });

  const catCol = VALID_CAT_COLS[categoryLevel] || 'category_l1';
  const start = Date.now();

  const query = `
    WITH base AS (
      SELECT b.user_id, COALESCE(b.product_brand, 'UNKNOWN') AS brand, SUM(COALESCE(b.line_total, 0)) AS brand_spend
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.issuer_ruc = $4 AND b.user_id IS NOT NULL AND COALESCE(b.${catCol}, 'UNKNOWN') = $5
      GROUP BY b.user_id, COALESCE(b.product_brand, 'UNKNOWN')
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
    )
    SELECT brand, brand_buyers, penetration_pct, p75, loyal_users,
      ROUND(100.0 * loyal_users / NULLIF(brand_buyers, 0), 2) AS loyalty_rate_pct,
      (SELECT COUNT(*) FROM user_cat_spend) AS eligible_users,
      (SELECT row_to_json(tiers.*) FROM tiers) AS tiers_json,
      (SELECT row_to_json(dist.*) FROM dist) AS dist_json
    FROM brand_agg WHERE brand_buyers >= $6 OR brand = 'UNKNOWN'
    ORDER BY CASE brand WHEN 'UNKNOWN' THEN 2 ELSE 1 END, brand_buyers DESC
  `;
  const { rows } = await pool.query(query, [startDate, endDate, reconcileOk, issuerRuc, categoryValue, kThreshold]);

  const first = rows[0] || {};
  res.set('X-Query-Time-Ms', String(Date.now() - start));
  res.json({
    filters: { start: startDate, end: endDate, issuer_ruc: issuerRuc, category_value: categoryValue, reconcile_ok: reconcileOk, k_threshold: kThreshold },
    tiers: first.tiers_json || { exclusive: 0, loyal: 0, prefer: 0, light: 0 },
    distribution: first.dist_json || { p10: 0, p25: 0, p50: 0, p75: 0, p90: 0 },
    data: rows.map(r => ({
      brand: r.brand,
      brand_buyers: Number(r.brand_buyers),
      penetration_pct: Number(r.penetration_pct),
      p75: Number(r.p75),
      loyalty_rate_pct: Number(r.loyalty_rate_pct),
      trust_level: r.brand === 'UNKNOWN' ? 'SUPPRESSED' : null
    })),
    disclaimers: LOYALTY_DISCLAIMERS
  });
}));

// =============================================================================
// SPRINT 6: PANEL SUMMARY
// =============================================================================
app.get('/api/panel/summary', asyncHandler(async (req, res) => {
  const startDate = parseDate(req.query.start);
  const endDate = parseDate(req.query.end);
  const issuerRuc = parseString(req.query.issuer_ruc);
  const storeId = parseString(req.query.store_id);
  const reconcileOk = parseBool(req.query.reconcile_ok, null);

  const errors = [];
  if (!startDate) errors.push('Invalid or missing start date');
  if (!endDate) errors.push('Invalid or missing end date');
  if (!issuerRuc) errors.push('issuer_ruc is required');
  if (errors.length > 0) return res.status(400).json({ error: errors.join('; ') });

  const start = Date.now();
  const query = `
 WITH base_txn AS (
      SELECT b.user_id, b.cufe, b.invoice_date, b.issuer_ruc, SUM(COALESCE(b.line_total, 0)) AS line_total
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3) AND b.user_id IS NOT NULL
      GROUP BY b.user_id, b.cufe, b.invoice_date, b.issuer_ruc
    ),
    panel_x AS (
      SELECT COUNT(DISTINCT user_id) AS customers_n, SUM(line_total) AS spend_in_x_usd,
        COUNT(DISTINCT cufe) AS invoices_n, COUNT(DISTINCT DATE_TRUNC('month', invoice_date)) AS active_months
      FROM base_txn WHERE issuer_ruc = $4 -- store_id filter removed
    ),
    panel_market AS (
      SELECT SUM(line_total) AS spend_market_usd FROM base_txn
      WHERE user_id IN (SELECT DISTINCT user_id FROM base_txn WHERE issuer_ruc = $4)
    )
    SELECT px.*, pm.spend_market_usd, ROUND(100.0 * px.spend_in_x_usd / NULLIF(pm.spend_market_usd, 0), 2) AS sow_pct,
      CASE WHEN px.customers_n > 0 THEN ROUND(px.spend_in_x_usd / px.customers_n, 2) ELSE 0 END AS avg_spend
    FROM panel_x px CROSS JOIN panel_market pm
  `;
  const { rows } = await pool.query(query, [startDate, endDate, reconcileOk, issuerRuc]);
  const r = rows[0] || {};
  const customersN = Number(r.customers_n || 0);
  const expansion = getExpansionFactor(issuerRuc, null);

  res.set('X-Query-Time-Ms', String(Date.now() - start));
  res.json({
    filters: { start: startDate, end: endDate, issuer_ruc: issuerRuc, store_id: storeId, reconcile_ok: reconcileOk },
    panel: {
      customers_n: customersN,
      spend_in_x_usd: Math.round(Number(r.spend_in_x_usd || 0) * 100) / 100,
      spend_market_usd: Math.round(Number(r.spend_market_usd || 0) * 100) / 100,
      sow_pct: Number(r.sow_pct || 0),
      invoices_n: Number(r.invoices_n || 0),
      active_months: Number(r.active_months || 0)
    },
    projection: {
      method: 'expansion_factor',
      expansion_factor: expansion.factor,
      expansion_source: expansion.source,
      projected_households: Math.round(customersN * expansion.factor),
      notes: ['REFERENTIAL', 'ILLUSTRATIVE']
    },
    disclaimers: PANEL_DISCLAIMERS
  });
}));

app.get('/api/panel/config', (req, res) => {
  const config = getExpansionConfig();
  res.json({ method: 'expansion_factor', default_factor: config.defaultFactor, overrides_count: Object.keys(config.overrides).length });
});

// DECK FILTER PAGE
app.get('/deck', (req, res) => {
  res.set('Content-Type', 'text/html; charset=utf-8');
  res.send(`<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>LÜM Radiance | Generate Deck</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
  <style>
    :root { --bg: #0b0f19; --card: #12172a; --border: rgba(99,102,241,0.2); --text: #f1f5f9; --muted: #64748b; --cyan: #06b6d4; --purple: #8b5cf6; }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: 'Inter', sans-serif; background: var(--bg); color: var(--text); min-height: 100vh; padding: 40px 20px; }
    .container { max-width: 500px; margin: 0 auto; }
    .logo { font-size: 11px; font-weight: 700; letter-spacing: 4px; color: var(--cyan); text-align: center; margin-bottom: 8px; }
    h1 { font-size: 28px; font-weight: 800; text-align: center; margin-bottom: 32px; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 16px; padding: 32px; }
    .field { margin-bottom: 20px; }
    label { display: block; font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; }
    input, select { width: 100%; padding: 12px 16px; background: var(--bg); border: 1px solid var(--border); border-radius: 8px; color: var(--text); font-size: 14px; font-family: 'JetBrains Mono', monospace; }
    input:focus, select:focus { outline: none; border-color: var(--cyan); }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    button { width: 100%; padding: 14px; background: linear-gradient(135deg, var(--cyan), var(--purple)); border: none; border-radius: 8px; color: white; font-size: 14px; font-weight: 600; cursor: pointer; margin-top: 12px; transition: transform 0.2s, box-shadow 0.2s; }
    button:hover { transform: translateY(-2px); box-shadow: 0 8px 24px rgba(6,182,212,0.3); }
    .formats { display: flex; gap: 12px; margin-top: 16px; }
    .formats button { flex: 1; background: var(--card); border: 1px solid var(--border); }
    .formats button:hover { border-color: var(--cyan); background: var(--card); }
    .back { display: block; text-align: center; margin-top: 24px; color: var(--muted); font-size: 13px; text-decoration: none; }
    .back:hover { color: var(--cyan); }
  </style>
</head>
<body>
<div class="container">
  <div class="logo">LÜM RADIANCE</div>
  <h1>Generate Deck</h1>
  <div class="card">
    <form id="deckForm">
      <div class="field">
        <label>Issuer RUC *</label>
        <input type="text" name="issuer_ruc" placeholder="ej: 8-NT-2-12345" required>
      </div>
      <div class="row">
        <div class="field">
          <label>Start Date *</label>
          <input type="date" name="start" value="2025-01-01" required>
        </div>
        <div class="field">
          <label>End Date *</label>
          <input type="date" name="end" value="2025-07-01" required>
        </div>
      </div>
      <div class="row">
        <div class="field">
          <label>Category Level</label>
          <select name="category_level">
            <option value="l1">L1 (default)</option>
            <option value="l2">L2</option>
            <option value="l3">L3</option>
            <option value="l4">L4</option>
          </select>
        </div>
        <div class="field">
          <label>K-Threshold</label>
          <input type="number" name="k_threshold" value="5" min="1">
        </div>
      </div>
      <button type="submit">Generate HTML Deck</button>
      <div class="formats">
        <button type="button" onclick="openJSON()">Get JSON</button>
      </div>
    </form>
  </div>
  <a href="/" class="back">← Back to API</a>
</div>
<script>
  const form = document.getElementById('deckForm');
  form.addEventListener('submit', (e) => {
    e.preventDefault();
    const params = new URLSearchParams(new FormData(form)).toString();
    window.open('/api/deck/commerce?' + params, '_blank');
  });
  function openJSON() {
    const params = new URLSearchParams(new FormData(form)).toString();
    window.open('/api/deck/commerce?' + params + '&format=json', '_blank');
  }
</script>
</body>
</html>`);
});

// =============================================================================
// SPRINT 7: DECK (Full HTML with Charts)
// =============================================================================
app.get('/api/deck/commerce', asyncHandler(async (req, res) => {
  const startDate = parseDate(req.query.start);
  const endDate = parseDate(req.query.end);
  const issuerRuc = parseString(req.query.issuer_ruc);
  const storeId = parseString(req.query.store_id);
  const reconcileOk = parseBool(req.query.reconcile_ok, null);
  const kThreshold = parseKThreshold(req.query.k_threshold);
  const categoryLevel = (req.query.category_level || 'l1').toLowerCase();
  const format = req.query.format || 'html';

  const errors = [];
  if (!startDate) errors.push('Invalid or missing start date');
  if (!endDate) errors.push('Invalid or missing end date');
  if (!issuerRuc) errors.push('issuer_ruc is required');
  if (errors.length > 0) return res.status(400).json({ error: errors.join('; ') });

  const filters = { start: startDate, end: endDate, issuer_ruc: issuerRuc, store_id: storeId, reconcile_ok: reconcileOk, k_threshold: kThreshold, category_level: categoryLevel };
  const catCol = VALID_CAT_COLS[categoryLevel] || 'category_l1';
  const start = Date.now();

  // PANEL SUMMARY
  const panelQuery = `
    WITH base_txn AS (
      SELECT b.user_id, b.cufe, b.invoice_date, b.issuer_ruc, SUM(COALESCE(b.line_total, 0)) AS line_total
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3) AND b.user_id IS NOT NULL
      GROUP BY b.user_id, b.cufe, b.invoice_date, b.issuer_ruc
    ),
    panel_x AS (
      SELECT COUNT(DISTINCT user_id) AS customers_n, SUM(line_total) AS spend_in_x_usd,
        COUNT(DISTINCT cufe) AS invoices_n, COUNT(DISTINCT DATE_TRUNC('month', invoice_date)) AS active_months
      FROM base_txn WHERE issuer_ruc = $4 -- store_id filter removed
    ),
    panel_market AS (SELECT SUM(line_total) AS spend_market_usd FROM base_txn WHERE user_id IN (SELECT DISTINCT user_id FROM base_txn WHERE issuer_ruc = $4))
    SELECT px.*, pm.spend_market_usd, ROUND(100.0 * px.spend_in_x_usd / NULLIF(pm.spend_market_usd, 0), 2) AS sow_pct,
      CASE WHEN px.customers_n > 0 THEN ROUND(px.spend_in_x_usd / px.customers_n, 2) ELSE 0 END AS avg_spend_per_customer
    FROM panel_x px CROSS JOIN panel_market pm
  `;
  const panelResult = await pool.query(panelQuery, [startDate, endDate, reconcileOk, issuerRuc]);
  const panelRow = panelResult.rows[0] || {};
  const expansion = getExpansionFactor(issuerRuc, null);
  const panel = {
    customers_n: Number(panelRow.customers_n || 0),
    spend_in_x_usd: Number(panelRow.spend_in_x_usd || 0),
    spend_market_usd: Number(panelRow.spend_market_usd || 0),
    sow_pct: Number(panelRow.sow_pct || 0),
    invoices_n: Number(panelRow.invoices_n || 0),
    active_months: Number(panelRow.active_months || 0),
    avg_spend_per_customer: Number(panelRow.avg_spend_per_customer || 0),
    projection: { expansion_factor: expansion.factor, expansion_source: expansion.source, projected_households: Math.round(Number(panelRow.customers_n || 0) * expansion.factor) }
  };

// PANEL TREND
  const trendQuery = `
    WITH base_txn AS (
      SELECT b.user_id, DATE_TRUNC('month', b.invoice_date)::date AS month, SUM(COALESCE(b.line_total, 0)) AS spend
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
        AND b.issuer_ruc = $4
        AND b.user_id IS NOT NULL
      GROUP BY b.user_id, DATE_TRUNC('month', b.invoice_date)::date
    )
    SELECT month, COUNT(DISTINCT user_id) AS customers, SUM(spend) AS spend FROM base_txn GROUP BY month ORDER BY month
  `;
  const trendResult = await pool.query(trendQuery, [startDate, endDate, reconcileOk, issuerRuc]);
  const panelTrend = trendResult.rows.map(r => ({ month: r.month.toISOString().split('T')[0].substring(0, 7), customers: Number(r.customers), spend: Math.round(Number(r.spend)) }));

  // CAPTURE
  const captureQuery = `
    WITH cohort AS (
      SELECT DISTINCT b.user_id FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3) AND b.issuer_ruc = $4 AND b.user_id IS NOT NULL
    ),
    user_cat AS (
      SELECT b.user_id, COALESCE(b.${catCol}, 'UNKNOWN') AS category_value, b.issuer_ruc, SUM(COALESCE(b.line_total, 0)) AS spend
      FROM analytics.radiance_base_v1 b INNER JOIN cohort c ON b.user_id = c.user_id
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3)
      GROUP BY b.user_id, COALESCE(b.${catCol}, 'UNKNOWN'), b.issuer_ruc
    )
    SELECT category_value, COUNT(DISTINCT user_id) AS users,
      SUM(CASE WHEN issuer_ruc = $4 THEN spend ELSE 0 END) AS spend_in_x_usd, SUM(spend) AS spend_market_usd,
      SUM(spend) - SUM(CASE WHEN issuer_ruc = $4 THEN spend ELSE 0 END) AS leakage_usd,
      ROUND(100.0 * SUM(CASE WHEN issuer_ruc = $4 THEN spend ELSE 0 END) / NULLIF(SUM(spend), 0), 2) AS sow_pct
    FROM user_cat GROUP BY category_value HAVING COUNT(DISTINCT user_id) >= $5 ORDER BY spend_in_x_usd DESC LIMIT 15
  `;
  const captureResult = await pool.query(captureQuery, [startDate, endDate, reconcileOk, issuerRuc, kThreshold]);
  const capture = { data: captureResult.rows.map(r => ({ category_value: r.category_value, users: Number(r.users), spend_in_x_usd: Number(r.spend_in_x_usd), spend_market_usd: Number(r.spend_market_usd), leakage_usd: Number(r.leakage_usd), sow_pct: Number(r.sow_pct) })) };

  // SWITCHING
  const switchingQuery = `
    WITH cohort AS (SELECT DISTINCT user_id FROM analytics.radiance_base_v1 WHERE invoice_date >= $1::date AND invoice_date < $2::date AND issuer_ruc = $4 AND user_id IS NOT NULL),
    elsewhere AS (
      SELECT b.user_id, COALESCE(b.issuer_name, b.issuer_ruc) AS destination
      FROM analytics.radiance_base_v1 b INNER JOIN cohort c ON b.user_id = c.user_id
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3) AND b.issuer_ruc != $4
    )
    SELECT destination, COUNT(DISTINCT user_id) AS users, ROUND(100.0 * COUNT(DISTINCT user_id) / NULLIF((SELECT COUNT(*) FROM cohort), 0), 2) AS pct
    FROM elsewhere GROUP BY destination HAVING COUNT(DISTINCT user_id) >= $5 ORDER BY users DESC LIMIT 10
  `;
  const switchingResult = await pool.query(switchingQuery, [startDate, endDate, reconcileOk, issuerRuc, kThreshold]);
  const switching = { data: switchingResult.rows.map(r => ({ destination: r.destination, users: Number(r.users), pct: Number(r.pct) })) };

 // AUTO-SELECT TOP CATEGORY
  const topCatQuery = `
    SELECT COALESCE(b.${catCol}, 'UNKNOWN') AS cat_val, SUM(COALESCE(b.line_total, 0)) AS spend
    FROM analytics.radiance_base_v1 b
    LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
    WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
      AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3) AND b.issuer_ruc = $4 AND b.user_id IS NOT NULL
    GROUP BY COALESCE(b.${catCol}, 'UNKNOWN')
    HAVING COALESCE(b.${catCol}, 'UNKNOWN') NOT IN ('UNKNOWN', 'OTHER_SUPPRESSED')
    ORDER BY spend DESC LIMIT 1
  `;
  const topCatResult = await pool.query(topCatQuery, [startDate, endDate, reconcileOk, issuerRuc]);
  const topCategory = topCatResult.rows[0]?.cat_val || null;

  // LEAKAGE (for top category)
  let leakage = { waterfall: [], category: topCategory, trust_level: null };
  if (topCategory) {
    const leakageQuery = `
      WITH base_txn AS (
        SELECT b.user_id, DATE_TRUNC('month', b.invoice_date)::date AS txn_month, b.issuer_ruc,
          SUM(COALESCE(b.line_total, 0))AS line_total, COUNT(DISTINCT b.cufe) AS visits
        FROM analytics.radiance_base_v1 b
        LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
        WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
          AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3) AND b.user_id IS NOT NULL AND COALESCE(b.${catCol}, 'UNKNOWN') = $5
        GROUP BY b.user_id, txn_month, b.issuer_ruc
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
    const leakageResult = await pool.query(leakageQuery, [startDate, endDate, reconcileOk, issuerRuc, topCategory]);
    const bucketOrder = ['RETAINED', 'CATEGORY_GONE', 'REDUCED_BASKET', 'REDUCED_FREQ', 'DELAYED_ONLY', 'FULL_CHURN'];
    const bucketMap = {};
    leakageResult.rows.forEach(r => { bucketMap[r.bucket] = { users: Number(r.users), pct: Number(r.pct) }; });
    leakage = {
      waterfall: bucketOrder.map(b => ({ bucket: b, users: bucketMap[b]?.users || 0, pct: bucketMap[b]?.pct || 0 })),
      category: topCategory,
      trust_level: leakageResult.rows.length > 0 ? 'MEDIUM' : 'SUPPRESSED'
    };
  }

  // BASKET
  const basketQuery = `
    WITH base_txn AS (
      SELECT b.user_id, DATE_TRUNC('month', b.invoice_date)::date AS txn_month, b.issuer_ruc, COALESCE(b.${catCol}, 'UNKNOWN') AS cat_val
      FROM analytics.radiance_base_v1 b
      LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
      WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
        AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3) AND b.user_id IS NOT NULL
    ),
    cohort AS (SELECT DISTINCT user_id, txn_month FROM base_txn WHERE issuer_ruc = $4),
    user_breadth AS (
      SELECT c.user_id, c.txn_month, COUNT(DISTINCT bt.cat_val) AS breadth_total,
        COUNT(DISTINCT CASE WHEN bt.issuer_ruc = $4 THEN bt.cat_val END) AS breadth_in_x
      FROM cohort c INNER JOIN base_txn bt ON c.user_id = bt.user_id AND c.txn_month = bt.txn_month GROUP BY c.user_id, c.txn_month
    )
    SELECT txn_month AS origin_month, COUNT(DISTINCT user_id) AS users, ROUND(AVG(breadth_total), 2) AS avg_breadth_market, ROUND(AVG(breadth_in_x), 2) AS avg_breadth_in_x
    FROM user_breadth GROUP BY txn_month HAVING COUNT(DISTINCT user_id) >= $5 ORDER BY txn_month
  `;
  const basketResult = await pool.query(basketQuery, [startDate, endDate, reconcileOk, issuerRuc, kThreshold]);
  const basket = { data: basketResult.rows.map(r => ({ origin_month: r.origin_month.toISOString().split('T')[0].substring(0, 7), users: Number(r.users), avg_breadth_market: Number(r.avg_breadth_market), avg_breadth_in_x: Number(r.avg_breadth_in_x) })) };

  // LOYALTY (for top category)
  let loyalty = { data: [], tiers: null, distribution: null, category: topCategory, trust_level: null };
  if (topCategory) {
    const loyaltyQuery = `
      WITH base AS (
        SELECT b.user_id, COALESCE(b.product_brand, 'UNKNOWN') AS brand, SUM(COALESCE(b.line_total, 0)) AS brand_spend
        FROM analytics.radiance_base_v1 b
        LEFT JOIN analytics.radiance_invoice_reconcile_v1 r ON b.cufe = r.cufe
        WHERE b.invoice_date >= $1::date AND b.invoice_date < $2::date
          AND ($3::boolean IS NULL OR COALESCE(r.reconcile_ok, false) = $3) AND b.issuer_ruc = $4 AND b.user_id IS NOT NULL AND COALESCE(b.${catCol}, 'UNKNOWN') = $5
        GROUP BY b.user_id, COALESCE(b.product_brand, 'UNKNOWN')
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
        SELECT COUNT(*) FILTER (WHERE share_pct >= 95) AS exclusive, COUNT(*) FILTER (WHERE share_pct >= 80 AND share_pct < 95) AS loyal,
          COUNT(*) FILTER (WHERE share_pct >= 50 AND share_pct < 80) AS prefer, COUNT(*) FILTER (WHERE share_pct < 50) AS light
        FROM shares
      ),
      dist AS (
        SELECT ROUND(PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY share_pct)::numeric, 2) AS p10,
          ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY share_pct)::numeric, 2) AS p25,
          ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY share_pct)::numeric, 2) AS p50,
          ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY share_pct)::numeric, 2) AS p75,
          ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY share_pct)::numeric, 2) AS p90
        FROM shares
      )
      SELECT brand, brand_buyers, penetration_pct, p75, loyal_users, ROUND(100.0 * loyal_users / NULLIF(brand_buyers, 0), 2) AS loyalty_rate_pct,
        (SELECT COUNT(*) FROM user_cat_spend) AS eligible_users,
        (SELECT row_to_json(tiers.*) FROM tiers) AS tiers_json, (SELECT row_to_json(dist.*) FROM dist) AS dist_json
      FROM brand_agg WHERE brand_buyers >= $6 OR brand = 'UNKNOWN' ORDER BY CASE brand WHEN 'UNKNOWN' THEN 2 ELSE 1 END, brand_buyers DESC LIMIT 10
    `;
    const loyaltyResult = await pool.query(loyaltyQuery, [startDate, endDate, reconcileOk, issuerRuc, topCategory, kThreshold]);
    if (loyaltyResult.rows.length > 0) {
      const first = loyaltyResult.rows[0];
      const eligibleUsers = Number(first.eligible_users || 0);
      loyalty = {
        data: loyaltyResult.rows.map(r => ({ brand: r.brand, brand_buyers: Number(r.brand_buyers), penetration_pct: Number(r.penetration_pct), p75: Number(r.p75), loyalty_rate_pct: Number(r.loyalty_rate_pct), trust_level: r.brand === 'UNKNOWN' ? 'SUPPRESSED' : null })),
        tiers: first.tiers_json || { exclusive: 0, loyal: 0, prefer: 0, light: 0 },
        distribution: first.dist_json || { p10: 0, p25: 0, p50: 0, p75: 0, p90: 0 },
        category: topCategory,
        trust_level: eligibleUsers < 10 ? 'SUPPRESSED' : eligibleUsers < 30 ? 'LOW' : eligibleUsers < 100 ? 'MEDIUM' : 'HIGH'
      };
    }
  }

  const deckData = { filters: { ...filters, auto_selected_category: topCategory }, generatedAt: new Date().toISOString(), panel, panelTrend, capture, switching, leakage, basket, loyalty };

  res.set('X-Query-Time-Ms', String(Date.now() - start));
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
      console.log(`   Sprints: ${SPRINTS.join(', ')}`);
    });
  } catch (err) {
    console.error('❌ Database connection failed:', err.message);
    process.exit(1);
  }
}

startServer();
