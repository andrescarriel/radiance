// =============================================================================
// RADIANCE DASHBOARD — APP.JS
// =============================================================================

// -----------------------------------------------------------------------------
// GLOBALS & CONFIG
// -----------------------------------------------------------------------------
const API_BASE = '';  // Same origin
let charts = {};      // Store Chart.js instances for cleanup

// Chart.js defaults
Chart.defaults.color = '#94a3b8';
Chart.defaults.borderColor = 'rgba(99, 102, 241, 0.1)';
Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.font.size = 11;

// Colors
const COLORS = {
  cyan: '#06b6d4',
  purple: '#8b5cf6',
  green: '#10b981',
  yellow: '#f59e0b',
  orange: '#f97316',
  red: '#ef4444',
  pink: '#ec4899',
  gray: '#64748b'
};

// -----------------------------------------------------------------------------
// HELPERS
// -----------------------------------------------------------------------------
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

const formatUSD = (n) => {
  if (n == null || isNaN(n)) return '—';
  return '$' + Number(n).toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
};

const formatNum = (n) => {
  if (n == null || isNaN(n)) return '—';
  return Number(n).toLocaleString('en-US');
};

const formatPct = (n) => {
  if (n == null || isNaN(n)) return '—';
  return Number(n).toFixed(1) + '%';
};

const formatDelta = (current, previous) => {
  if (!previous || previous === 0) return { text: '', class: '' };
  const delta = ((current - previous) / previous) * 100;
  const sign = delta >= 0 ? '+' : '';
  return {
    text: `${sign}${delta.toFixed(1)}%`,
    class: delta >= 0 ? 'positive' : 'negative'
  };
};

// Get filter values
const getFilters = () => ({
  issuer_ruc: $('#filterIssuerRuc').value.trim(),
  start: $('#filterStart').value,
  end: $('#filterEnd').value,
  category_level: $('#filterCategoryLevel').value,
  reconcile_ok: $('#filterReconcile').value,
  k_threshold: $('#filterKThreshold').value
});

// Build query string
const buildQuery = (filters) => {
  const params = new URLSearchParams();
  Object.entries(filters).forEach(([k, v]) => {
    if (v !== '' && v != null) params.append(k, v);
  });
  return params.toString();
};

// Fetch with error handling
const fetchAPI = async (endpoint, filters = {}) => {
  const query = buildQuery({ ...getFilters(), ...filters });
  const url = `${API_BASE}${endpoint}?${query}`;
  
  try {
    const res = await fetch(url);
    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.message || `HTTP ${res.status}`);
    }
    return await res.json();
  } catch (e) {
    showToast(`Error: ${e.message}`, 'error');
    throw e;
  }
};

// Toast notifications
const showToast = (message, type = 'error') => {
  const container = $('#toastContainer');
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.textContent = message;
  container.appendChild(toast);
  setTimeout(() => toast.remove(), 5000);
};

// Loading state
const showLoading = () => $('#loadingOverlay').classList.remove('hidden');
const hideLoading = () => $('#loadingOverlay').classList.add('hidden');

// Destroy chart if exists
const destroyChart = (id) => {
  if (charts[id]) {
    charts[id].destroy();
    delete charts[id];
  }
};

// Create empty state
const showEmptyState = (container, message = 'Sin datos para mostrar') => {
  container.innerHTML = `
    <div class="empty-state">
      <div class="empty-state-icon">📭</div>
      <div class="empty-state-text">${message}</div>
    </div>
  `;
};

// -----------------------------------------------------------------------------
// NAVIGATION
// -----------------------------------------------------------------------------
const initNavigation = () => {
  $$('.nav-item').forEach(item => {
    item.addEventListener('click', (e) => {
      e.preventDefault();
      const module = item.dataset.module;
      
      // Update nav active state
      $$('.nav-item').forEach(n => n.classList.remove('active'));
      item.classList.add('active');
      
      // Show/hide modules
      $$('.module').forEach(m => m.classList.add('hidden'));
      $(`#module${capitalize(module)}`).classList.remove('hidden');
      
      // Load module data
      loadModule(module);
    });
  });
};

const capitalize = (s) => s.charAt(0).toUpperCase() + s.slice(1);

// -----------------------------------------------------------------------------
// MODULE LOADERS
// -----------------------------------------------------------------------------
const loadModule = async (module) => {
  const filters = getFilters();
  if (!filters.issuer_ruc) {
    showToast('Ingresa un issuer_ruc', 'error');
    return;
  }

  showLoading();
  
  try {
    switch (module) {
      case 'overview': await loadOverview(); break;
      case 'capture': await loadCapture(); break;
      case 'switching': await loadSwitching(); break;
      case 'leakage': await loadLeakage(); break;
      case 'basket': await loadBasket(); break;
      case 'loyalty': await loadLoyalty(); break;
    }
  } catch (e) {
    console.error('Module load error:', e);
  } finally {
    hideLoading();
  }
};

// -----------------------------------------------------------------------------
// OVERVIEW MODULE
// -----------------------------------------------------------------------------
const loadOverview = async () => {
  try {
    const data = await fetchAPI('/api/kpis/summary');
    renderOverviewKPIs(data);
    renderOverviewCharts(data);
  } catch (e) {
    console.error('Overview error:', e);
  }
};

const renderOverviewKPIs = (data) => {
  const { current, previous } = data;
  
  // Row 1
  $('#kpiVentas').textContent = formatUSD(current.ventas);
  const ventasDelta = formatDelta(current.ventas, previous?.ventas);
  $('#kpiVentasDelta').textContent = ventasDelta.text;
  $('#kpiVentasDelta').className = `kpi-delta ${ventasDelta.class}`;
  
  $('#kpiTxn').textContent = formatNum(current.transacciones);
  const txnDelta = formatDelta(current.transacciones, previous?.transacciones);
  $('#kpiTxnDelta').textContent = txnDelta.text;
  $('#kpiTxnDelta').className = `kpi-delta ${txnDelta.class}`;
  
  $('#kpiClientes').textContent = formatNum(current.clientes);
  const clientesDelta = formatDelta(current.clientes, previous?.clientes);
  $('#kpiClientesDelta').textContent = clientesDelta.text;
  $('#kpiClientesDelta').className = `kpi-delta ${clientesDelta.class}`;
  
  $('#kpiTicket').textContent = formatUSD(current.ticket_promedio);
  const ticketDelta = formatDelta(current.ticket_promedio, previous?.ticket_promedio);
  $('#kpiTicketDelta').textContent = ticketDelta.text;
  $('#kpiTicketDelta').className = `kpi-delta ${ticketDelta.class}`;
  
  // Row 2
  $('#kpiFrecuencia').textContent = current.frecuencia?.toFixed(2) || '—';
  $('#kpiProductos').textContent = formatNum(current.productos);
  $('#kpiCategorias').textContent = formatNum(current.categorias);
  $('#kpiSoW').textContent = formatPct(current.sow_pct);
};

const renderOverviewCharts = (data) => {
  const { trends, by_weekday, top_categories } = data;
  
  // Ventas por Mes
  destroyChart('chartVentasMes');
  const ctxVentas = $('#chartVentasMes');
  if (trends?.length > 0) {
    charts['chartVentasMes'] = new Chart(ctxVentas, {
      type: 'line',
      data: {
        labels: trends.map(t => t.month),
        datasets: [{
          label: 'Ventas',
          data: trends.map(t => t.ventas),
          borderColor: COLORS.cyan,
          backgroundColor: 'rgba(6, 182, 212, 0.1)',
          fill: true,
          tension: 0.3
        }]
      },
      options: { responsive: true, plugins: { legend: { display: false } } }
    });
  } else {
    showEmptyState(ctxVentas.parentElement);
  }
  
  // Clientes por Mes
  destroyChart('chartClientesMes');
  const ctxClientes = $('#chartClientesMes');
  if (trends?.length > 0) {
    charts['chartClientesMes'] = new Chart(ctxClientes, {
      type: 'line',
      data: {
        labels: trends.map(t => t.month),
        datasets: [{
          label: 'Clientes',
          data: trends.map(t => t.clientes),
          borderColor: COLORS.purple,
          backgroundColor: 'rgba(139, 92, 246, 0.1)',
          fill: true,
          tension: 0.3
        }]
      },
      options: { responsive: true, plugins: { legend: { display: false } } }
    });
  } else {
    showEmptyState(ctxClientes.parentElement);
  }
  
  // Por día de semana
  destroyChart('chartTxnDia');
  const ctxDia = $('#chartTxnDia');
  if (by_weekday?.length > 0) {
    charts['chartTxnDia'] = new Chart(ctxDia, {
      type: 'bar',
      data: {
        labels: by_weekday.map(d => d.day_name),
        datasets: [{
          label: '% Txn',
          data: by_weekday.map(d => d.pct),
          backgroundColor: COLORS.pink,
          borderRadius: 4
        }]
      },
      options: { responsive: true, plugins: { legend: { display: false } } }
    });
  } else {
    showEmptyState(ctxDia.parentElement);
  }
  
  // Top Categorías
  destroyChart('chartTopCategorias');
  const ctxCat = $('#chartTopCategorias');
  if (top_categories?.length > 0) {
    charts['chartTopCategorias'] = new Chart(ctxCat, {
      type: 'bar',
      data: {
        labels: top_categories.map(c => c.category.substring(0, 15)),
        datasets: [{
          label: 'Ventas',
          data: top_categories.map(c => c.ventas),
          backgroundColor: COLORS.cyan,
          borderRadius: 4
        }]
      },
      options: { 
        indexAxis: 'y',
        responsive: true, 
        plugins: { legend: { display: false } } 
      }
    });
  } else {
    showEmptyState(ctxCat.parentElement);
  }
};

// -----------------------------------------------------------------------------
// CAPTURE MODULE
// -----------------------------------------------------------------------------
const loadCapture = async () => {
  try {
    const data = await fetchAPI('/api/sow_leakage/by_category');
    renderCaptureKPIs(data);
    renderCaptureCharts(data);
    renderCaptureTable(data);
  } catch (e) {
    console.error('Capture error:', e);
  }
};

const renderCaptureKPIs = (data) => {
  const totals = data.data.reduce((acc, row) => {
    acc.inX += row.spend_in_x_usd || 0;
    acc.market += row.spend_market_usd || 0;
    acc.leakage += row.leakage_usd || 0;
    return acc;
  }, { inX: 0, market: 0, leakage: 0 });
  
  const sow = totals.market > 0 ? (totals.inX / totals.market) * 100 : 0;
  
  $('#captureSoW').textContent = formatPct(sow);
  $('#captureInX').textContent = formatUSD(totals.inX);
  $('#captureLeakage').textContent = formatUSD(totals.leakage);
};

const renderCaptureCharts = (data) => {
  const rows = data.data.slice(0, 10);
  
  // SoW Chart
  destroyChart('chartCaptureSoW');
  const ctxSoW = $('#chartCaptureSoW');
  if (rows.length > 0) {
    charts['chartCaptureSoW'] = new Chart(ctxSoW, {
      type: 'bar',
      data: {
        labels: rows.map(r => r.category_value.substring(0, 15)),
        datasets: [{
          label: 'SoW %',
          data: rows.map(r => r.sow_pct),
          backgroundColor: COLORS.cyan,
          borderRadius: 4
        }]
      },
      options: { indexAxis: 'y', responsive: true, plugins: { legend: { display: false } }, scales: { x: { max: 100 } } }
    });
  } else {
    showEmptyState(ctxSoW.parentElement);
  }
  
  // Stacked Chart
  destroyChart('chartCaptureStacked');
  const ctxStacked = $('#chartCaptureStacked');
  if (rows.length > 0) {
    charts['chartCaptureStacked'] = new Chart(ctxStacked, {
      type: 'bar',
      data: {
        labels: rows.map(r => r.category_value.substring(0, 15)),
        datasets: [
          { label: 'In-X', data: rows.map(r => r.spend_in_x_usd), backgroundColor: COLORS.green, borderRadius: 4 },
          { label: 'Leakage', data: rows.map(r => r.leakage_usd), backgroundColor: COLORS.red, borderRadius: 4 }
        ]
      },
      options: { indexAxis: 'y', responsive: true, plugins: { legend: { position: 'bottom' } }, scales: { x: { stacked: true }, y: { stacked: true } } }
    });
  } else {
    showEmptyState(ctxStacked.parentElement);
  }
};

const renderCaptureTable = (data) => {
  const tbody = $('#tableCaptureDetail tbody');
  tbody.innerHTML = data.data.map(r => `
    <tr>
      <td class="${r.category_value === 'UNKNOWN' ? 'suppressed' : ''}">${r.category_value}</td>
      <td class="text-right mono">${formatNum(r.users)}</td>
      <td class="text-right mono">${formatUSD(r.spend_in_x_usd)}</td>
      <td class="text-right mono">${formatUSD(r.spend_market_usd)}</td>
      <td class="text-right mono">${formatUSD(r.leakage_usd)}</td>
      <td class="text-right mono highlight">${formatPct(r.sow_pct)}</td>
    </tr>
  `).join('');
};

// -----------------------------------------------------------------------------
// SWITCHING MODULE
// -----------------------------------------------------------------------------
const loadSwitching = async () => {
  try {
    const data = await fetchAPI('/api/switching/destinations');
    renderSwitchingChart(data);
    renderSwitchingTable(data);
  } catch (e) {
    console.error('Switching error:', e);
  }
};

const renderSwitchingChart = (data) => {
  destroyChart('chartSwitching');
  const ctx = $('#chartSwitching');
  const rows = data.data.slice(0, 10);
  
  if (rows.length > 0) {
    charts['chartSwitching'] = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: rows.map(r => r.destination.substring(0, 20)),
        datasets: [{
          label: '% Cohort',
          data: rows.map(r => r.pct),
          backgroundColor: COLORS.pink,
          borderRadius: 4
        }]
      },
      options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { max: 100 } } }
    });
  } else {
    showEmptyState(ctx.parentElement);
  }
};

const renderSwitchingTable = (data) => {
  const tbody = $('#tableSwitching tbody');
  tbody.innerHTML = data.data.map(r => `
    <tr>
      <td>${r.destination}</td>
      <td class="text-right mono">${formatNum(r.users)}</td>
      <td class="text-right mono highlight">${formatPct(r.pct)}</td>
    </tr>
  `).join('');
};

// -----------------------------------------------------------------------------
// LEAKAGE MODULE
// -----------------------------------------------------------------------------
const loadLeakage = async () => {
  // First load categories for dropdown
  try {
    const captureData = await fetchAPI('/api/sow_leakage/by_category');
    populateCategorySelect('#leakageCategorySelect', captureData.data);
    
    // Load leakage if category selected
    const category = $('#leakageCategorySelect').value;
    if (category) {
      const data = await fetchAPI('/api/leakage/tree', { category_value: category });
      renderLeakageChart(data);
    }
  } catch (e) {
    console.error('Leakage error:', e);
  }
};

const populateCategorySelect = (selector, categories) => {
  const select = $(selector);
  const current = select.value;
  select.innerHTML = '<option value="">Selecciona categoría...</option>';
  
  categories
    .filter(c => c.category_value !== 'UNKNOWN' && c.category_value !== 'OTHER_SUPPRESSED')
    .forEach(c => {
      const opt = document.createElement('option');
      opt.value = c.category_value;
      opt.textContent = c.category_value;
      select.appendChild(opt);
    });
  
  if (current) select.value = current;
};

const renderLeakageChart = (data) => {
  destroyChart('chartLeakage');
  const ctx = $('#chartLeakage');
  const waterfall = data.waterfall;
  
  if (waterfall?.some(w => w.pct > 0)) {
    const colors = [COLORS.green, COLORS.yellow, COLORS.orange, COLORS.orange, COLORS.red, COLORS.gray];
    charts['chartLeakage'] = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: waterfall.map(w => w.bucket.replace('_', '\n')),
        datasets: [{
          label: '% Cohort',
          data: waterfall.map(w => w.pct),
          backgroundColor: colors,
          borderRadius: 4
        }]
      },
      options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { max: 100 } } }
    });
  } else {
    showEmptyState(ctx.parentElement, 'Sin datos de leakage para esta categoría');
  }
};

// -----------------------------------------------------------------------------
// BASKET MODULE
// -----------------------------------------------------------------------------
const loadBasket = async () => {
  try {
    const data = await fetchAPI('/api/basket/breadth');
    renderBasketKPIs(data);
    renderBasketChart(data);
  } catch (e) {
    console.error('Basket error:', e);
  }
};

const renderBasketKPIs = (data) => {
  if (data.data.length > 0) {
    const avgMarket = data.data.reduce((sum, d) => sum + d.avg_breadth_market, 0) / data.data.length;
    const avgInX = data.data.reduce((sum, d) => sum + d.avg_breadth_in_x, 0) / data.data.length;
    $('#basketBreadthMarket').textContent = avgMarket.toFixed(2);
    $('#basketBreadthInX').textContent = avgInX.toFixed(2);
  } else {
    $('#basketBreadthMarket').textContent = '—';
    $('#basketBreadthInX').textContent = '—';
  }
};

const renderBasketChart = (data) => {
  destroyChart('chartBasket');
  const ctx = $('#chartBasket');
  const rows = data.data;
  
  if (rows.length > 0) {
    charts['chartBasket'] = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: rows.map(r => r.origin_month),
        datasets: [
          { label: 'Market', data: rows.map(r => r.avg_breadth_market), backgroundColor: COLORS.gray, borderRadius: 4 },
          { label: 'In-X', data: rows.map(r => r.avg_breadth_in_x), backgroundColor: COLORS.cyan, borderRadius: 4 }
        ]
      },
      options: { responsive: true, plugins: { legend: { position: 'bottom' } } }
    });
  } else {
    showEmptyState(ctx.parentElement);
  }
};

// -----------------------------------------------------------------------------
// LOYALTY MODULE
// -----------------------------------------------------------------------------
const loadLoyalty = async () => {
  try {
    // Load categories
    const captureData = await fetchAPI('/api/sow_leakage/by_category');
    populateCategorySelect('#loyaltyCategorySelect', captureData.data);
    
    const category = $('#loyaltyCategorySelect').value;
    if (category) {
      const data = await fetchAPI('/api/loyalty/brands', { category_value: category });
      renderLoyaltyKPIs(data);
      renderLoyaltyCharts(data);
      renderLoyaltyTable(data);
    }
  } catch (e) {
    console.error('Loyalty error:', e);
  }
};

const renderLoyaltyKPIs = (data) => {
  const tiers = data.tiers || {};
  $('#loyaltyExclusive').textContent = formatNum(tiers.exclusive);
  $('#loyaltyLoyal').textContent = formatNum(tiers.loyal);
  $('#loyaltyPrefer').textContent = formatNum(tiers.prefer);
  $('#loyaltyLight').textContent = formatNum(tiers.light);
};

const renderLoyaltyCharts = (data) => {
  const tiers = data.tiers || {};
  const dist = data.distribution || {};
  
  // Tiers Donut
  destroyChart('chartLoyaltyTiers');
  const ctxTiers = $('#chartLoyaltyTiers');
  if (Object.values(tiers).some(v => v > 0)) {
    charts['chartLoyaltyTiers'] = new Chart(ctxTiers, {
      type: 'doughnut',
      data: {
        labels: ['Exclusive (≥95%)', 'Loyal (≥80%)', 'Prefer (≥50%)', 'Light (<50%)'],
        datasets: [{
          data: [tiers.exclusive, tiers.loyal, tiers.prefer, tiers.light],
          backgroundColor: [COLORS.green, COLORS.cyan, COLORS.purple, COLORS.gray],
          borderWidth: 0
        }]
      },
      options: { responsive: true, plugins: { legend: { position: 'bottom' } } }
    });
  } else {
    showEmptyState(ctxTiers.parentElement);
  }
  
  // Distribution Bar
  destroyChart('chartLoyaltyDist');
  const ctxDist = $('#chartLoyaltyDist');
  if (Object.values(dist).some(v => v > 0)) {
    charts['chartLoyaltyDist'] = new Chart(ctxDist, {
      type: 'bar',
      data: {
        labels: ['p10', 'p25', 'p50', 'p75', 'p90'],
        datasets: [{
          label: 'User Brand Share %',
          data: [dist.p10, dist.p25, dist.p50, dist.p75, dist.p90],
          backgroundColor: COLORS.purple,
          borderRadius: 4
        }]
      },
      options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { max: 100 } } }
    });
  } else {
    showEmptyState(ctxDist.parentElement);
  }
};

const renderLoyaltyTable = (data) => {
  const tbody = $('#tableLoyalty tbody');
  tbody.innerHTML = data.data
    .filter(r => r.brand !== 'OTHER_SUPPRESSED')
    .map(r => `
    <tr>
      <td class="${r.brand === 'UNKNOWN' ? 'suppressed' : ''}">${r.brand}</td>
      <td class="text-right mono">${formatNum(r.brand_buyers)}</td>
      <td class="text-right mono">${formatPct(r.penetration_pct)}</td>
      <td class="text-right mono">${formatPct(r.p75)}</td>
      <td class="text-right mono highlight">${formatPct(r.loyalty_rate_pct)}</td>
    </tr>
  `).join('');
};

// -----------------------------------------------------------------------------
// EVENT LISTENERS
// -----------------------------------------------------------------------------
const initEventListeners = () => {
  // Apply filters button
  $('#btnApplyFilters').addEventListener('click', () => {
    const activeModule = $('.nav-item.active')?.dataset.module || 'overview';
    loadModule(activeModule);
  });
  
  // Leakage category change
  $('#leakageCategorySelect').addEventListener('change', async (e) => {
    if (e.target.value) {
      showLoading();
      try {
        const data = await fetchAPI('/api/leakage/tree', { category_value: e.target.value });
        renderLeakageChart(data);
      } finally {
        hideLoading();
      }
    }
  });
  
  // Loyalty category change
  $('#loyaltyCategorySelect').addEventListener('change', async (e) => {
    if (e.target.value) {
      showLoading();
      try {
        const data = await fetchAPI('/api/loyalty/brands', { category_value: e.target.value });
        renderLoyaltyKPIs(data);
        renderLoyaltyCharts(data);
        renderLoyaltyTable(data);
      } finally {
        hideLoading();
      }
    }
  });
  
  // Enter key on filters
  $$('.filters-bar input').forEach(input => {
    input.addEventListener('keypress', (e) => {
      if (e.key === 'Enter') $('#btnApplyFilters').click();
    });
  });
};

// -----------------------------------------------------------------------------
// INIT
// -----------------------------------------------------------------------------
document.addEventListener('DOMContentLoaded', () => {
  initNavigation();
  initEventListeners();
  
  // Check for issuer_ruc and auto-load
  const params = new URLSearchParams(window.location.search);
  if (params.get('issuer_ruc')) {
    $('#filterIssuerRuc').value = params.get('issuer_ruc');
    if (params.get('start')) $('#filterStart').value = params.get('start');
    if (params.get('end')) $('#filterEnd').value = params.get('end');
    loadModule('overview');
  }
});
