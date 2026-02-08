// =============================================================================
// RADIANCE DASHBOARD — APP.JS v2.2.0
// Filter Contract v1 Compliant
// =============================================================================

// -----------------------------------------------------------------------------
// GLOBALS & CONFIG
// -----------------------------------------------------------------------------
const API_BASE = '';
let charts = {};
let abortController = null;  // For cancelling stale requests
let childrenCache = {};      // Cache for category children

// Chart.js defaults
Chart.defaults.color = '#94a3b8';
Chart.defaults.borderColor = 'rgba(99, 102, 241, 0.1)';
Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.font.size = 11;

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
// GLOBAL FILTERS STATE (Single Source of Truth)
// -----------------------------------------------------------------------------
const globalFilters = {
  start: '2025-01-01',
  end: '2025-07-01',
  issuer_ruc: null,
  reconcile_ok: 'all',        // 'all' | 'true' | 'false'
  k_threshold: 5,
  peer_scope: 'all',          // 'peers' | 'extended' | 'all'
  category_domain: 'product', // 'product' | 'commerce'
  category_level: 'l1',       // 'l1'..'l4'
  
  // Hierarchical filter (what user selected)
  category_path: { l1: null, l2: null, l3: null, l4: null },
  
  // For category-specific endpoints
  category_value: null,
  brand: null
};

// Last applied filters (for filter echo comparison)
let lastAppliedFilters = null;

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

// -----------------------------------------------------------------------------
// FILTER CONTRACT v1: Unified Params Builder
// -----------------------------------------------------------------------------
function syncFiltersFromUI() {
  globalFilters.issuer_ruc = $('#filterIssuerRuc').value || null;
  globalFilters.start = $('#filterStart').value;
  globalFilters.end = $('#filterEnd').value;
  globalFilters.category_level = $('#filterCategoryLevel').value;
  globalFilters.category_domain = $('#filterCategoryDomain').value;
  globalFilters.peer_scope = $('#filterPeerScope').value;
  
  const reconcile = $('#filterReconcile').value;
  globalFilters.reconcile_ok = reconcile;
  
  globalFilters.k_threshold = parseInt($('#filterKThreshold').value) || 5;
  
  // Category path from dropdowns
  globalFilters.category_path = {
    l1: $('#filterCatL1')?.value || null,
    l2: $('#filterCatL2')?.value || null,
    l3: $('#filterCatL3')?.value || null,
    l4: $('#filterCatL4')?.value || null
  };
}

function buildParams(overrides = {}) {
  const f = { ...globalFilters, ...overrides };
  const p = new URLSearchParams();

  // Required
  p.set('start', f.start);
  p.set('end', f.end);
  if (f.issuer_ruc) p.set('issuer_ruc', f.issuer_ruc);

  // Optional with defaults
  if (f.reconcile_ok && f.reconcile_ok !== 'all') {
    p.set('reconcile_ok', f.reconcile_ok);
  }
  
  p.set('k_threshold', String(f.k_threshold ?? 5));
  p.set('peer_scope', f.peer_scope ?? 'all');
  p.set('category_domain', f.category_domain ?? 'product');
  p.set('category_level', f.category_level ?? 'l1');

  // Category path (send only defined levels)
  const path = f.category_path || {};
  if (path.l1) p.set('category_l1', path.l1);
  if (path.l2) p.set('category_l2', path.l2);
  if (path.l3) p.set('category_l3', path.l3);
  if (path.l4) p.set('category_l4', path.l4);

  // Category value (for leakage/loyalty)
  if (f.category_value) p.set('category_value', f.category_value);
  
  // Brand filter
  if (f.brand) p.set('brand', f.brand);

  return p.toString();
}

// -----------------------------------------------------------------------------
// FETCH WITH ABORT CONTROLLER (Anti-stale)
// -----------------------------------------------------------------------------
async function fetchAPI(endpoint, extraParams = {}) {
  const params = buildParams(extraParams);
  const url = `${API_BASE}${endpoint}?${params}`;
  
  try {
    const res = await fetch(url, { 
      signal: abortController?.signal 
    });
    
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.message || err.error || `HTTP ${res.status}`);
    }
    
    const data = await res.json();
    
    // Process filter echo from backend
    if (data.filters) {
      processFilterEcho(data.filters);
    }
    
    return data;
  } catch (e) {
    if (e.name === 'AbortError') {
      console.log('Request aborted (stale)');
      return null;
    }
    showToast(`Error: ${e.message}`, 'error');
    throw e;
  }
}

// Resilient fetch for multiple endpoints
async function fetchDashboard(endpoints) {
  // Cancel any pending requests
  if (abortController) {
    abortController.abort();
  }
  abortController = new AbortController();
  
  const results = await Promise.allSettled(
    endpoints.map(ep => fetchAPI(ep.url, ep.params || {}))
  );
  
  return results.map((result, i) => ({
    endpoint: endpoints[i].url,
    status: result.status,
    data: result.status === 'fulfilled' ? result.value : null,
    error: result.status === 'rejected' ? result.reason : null
  }));
}

// -----------------------------------------------------------------------------
// FILTER ECHO PROCESSING
// -----------------------------------------------------------------------------
function processFilterEcho(backendFilters) {
  if (!backendFilters) return;
  
  const { applied, ignored, defaults } = backendFilters;
  
  // Update status strip with actual applied values
  if (applied) {
    $('#statusKThreshold').textContent = applied.k_threshold ?? defaults?.k_threshold ?? 5;
    $('#statusReconcile').textContent = applied.reconcile_ok === null ? 'All' : applied.reconcile_ok;
    $('#statusPeerScope').textContent = applied.peer_scope ?? 'all';
  }
  
  // Show ignored filters as warning
  if (ignored && ignored.length > 0) {
    console.log('Ignored filters:', ignored);
    // Could show a subtle indicator in UI
  }
}

// -----------------------------------------------------------------------------
// HIERARCHICAL CATEGORY DROPDOWNS
// -----------------------------------------------------------------------------
async function fetchCategoryChildren(level, parentPath) {
  const cacheKey = `${level}:${JSON.stringify(parentPath)}`;
  
  if (childrenCache[cacheKey]) {
    return childrenCache[cacheKey];
  }
  
  const params = {
    start: globalFilters.start,
    end: globalFilters.end,
    level: level,
    domain: globalFilters.category_domain
  };
  
  // Add parent path
  if (parentPath.l1) params.category_l1 = parentPath.l1;
  if (parentPath.l2) params.category_l2 = parentPath.l2;
  if (parentPath.l3) params.category_l3 = parentPath.l3;
  
  if (globalFilters.issuer_ruc) {
    params.issuer_ruc = globalFilters.issuer_ruc;
  }
  
  try {
    const queryStr = new URLSearchParams(params).toString();
    const res = await fetch(`/api/categories/children?${queryStr}`);
    const data = await res.json();
    
    childrenCache[cacheKey] = data.data || [];
    return childrenCache[cacheKey];
  } catch (e) {
    console.error('Error fetching children:', e);
    return [];
  }
}

async function populateCategoryDropdown(selectId, level, parentPath) {
  const select = $(selectId);
  if (!select) return;
  
  select.innerHTML = '<option value="">Cargando...</option>';
  select.disabled = true;
  
  const children = await fetchCategoryChildren(level, parentPath);
  
  select.innerHTML = '<option value="">Todas</option>';
  children.forEach(c => {
    const opt = document.createElement('option');
    opt.value = c.category_value;
    const icon = c.is_unknown ? '❓' : (globalFilters.category_domain === 'commerce' ? '🏪' : '📦');
    opt.textContent = `${icon} ${c.category_value} (${formatNum(c.users)})`;
    if (c.is_unknown) opt.classList.add('suppressed');
    select.appendChild(opt);
  });
  
  select.disabled = false;
}

function initCategoryHierarchy() {
  // L1 change -> reset L2, L3, L4 and load L2 children
  $('#filterCatL1')?.addEventListener('change', async (e) => {
    const val = e.target.value;
    globalFilters.category_path.l1 = val || null;
    globalFilters.category_path.l2 = null;
    globalFilters.category_path.l3 = null;
    globalFilters.category_path.l4 = null;
    
    // Reset downstream dropdowns
    if ($('#filterCatL2')) $('#filterCatL2').innerHTML = '<option value="">—</option>';
    if ($('#filterCatL3')) $('#filterCatL3').innerHTML = '<option value="">—</option>';
    if ($('#filterCatL4')) $('#filterCatL4').innerHTML = '<option value="">—</option>';
    
    if (val) {
      await populateCategoryDropdown('#filterCatL2', 'l2', { l1: val });
    }
  });
  
  // L2 change -> reset L3, L4 and load L3 children
  $('#filterCatL2')?.addEventListener('change', async (e) => {
    const val = e.target.value;
    globalFilters.category_path.l2 = val || null;
    globalFilters.category_path.l3 = null;
    globalFilters.category_path.l4 = null;
    
    if ($('#filterCatL3')) $('#filterCatL3').innerHTML = '<option value="">—</option>';
    if ($('#filterCatL4')) $('#filterCatL4').innerHTML = '<option value="">—</option>';
    
    if (val) {
      await populateCategoryDropdown('#filterCatL3', 'l3', { 
        l1: globalFilters.category_path.l1, 
        l2: val 
      });
    }
  });
  
  // L3 change -> reset L4 and load L4 children
  $('#filterCatL3')?.addEventListener('change', async (e) => {
    const val = e.target.value;
    globalFilters.category_path.l3 = val || null;
    globalFilters.category_path.l4 = null;
    
    if ($('#filterCatL4')) $('#filterCatL4').innerHTML = '<option value="">—</option>';
    
    if (val) {
      await populateCategoryDropdown('#filterCatL4', 'l4', { 
        l1: globalFilters.category_path.l1, 
        l2: globalFilters.category_path.l2,
        l3: val 
      });
    }
  });
  
  // L4 change
  $('#filterCatL4')?.addEventListener('change', (e) => {
    globalFilters.category_path.l4 = e.target.value || null;
  });
  
  // Domain change -> reset all and reload L1
  $('#filterCategoryDomain')?.addEventListener('change', async (e) => {
    globalFilters.category_domain = e.target.value;
    globalFilters.category_path = { l1: null, l2: null, l3: null, l4: null };
    childrenCache = {}; // Clear cache on domain change
    
    // Reset all dropdowns
    ['#filterCatL1', '#filterCatL2', '#filterCatL3', '#filterCatL4'].forEach(sel => {
      if ($(sel)) $(sel).innerHTML = '<option value="">—</option>';
    });
    
    await populateCategoryDropdown('#filterCatL1', 'l1', {});
  });
}

// -----------------------------------------------------------------------------
// TOAST & LOADING
// -----------------------------------------------------------------------------
const showToast = (message, type = 'error') => {
  const container = $('#toastContainer');
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.textContent = message;
  container.appendChild(toast);
  setTimeout(() => toast.remove(), 5000);
};

const showLoading = () => {
  $('#loadingOverlay')?.classList.remove('hidden');
  $('#btnApplyFilters').disabled = true;
};

const hideLoading = () => {
  $('#loadingOverlay')?.classList.add('hidden');
  $('#btnApplyFilters').disabled = false;
};

// -----------------------------------------------------------------------------
// CHART HELPERS
// -----------------------------------------------------------------------------
const destroyChart = (id) => {
  if (charts[id]) {
    charts[id].destroy();
    delete charts[id];
  }
};

const showEmptyState = (container, message = 'Sin datos para mostrar') => {
  const canvas = container.querySelector('canvas');
  if (canvas) canvas.classList.add('hidden');
  
  let empty = container.querySelector('.empty-state');
  if (!empty) {
    empty = document.createElement('div');
    empty.className = 'empty-state';
    empty.innerHTML = `
      <div class="empty-state-icon">📭</div>
      <div class="empty-state-text"></div>
    `;
    container.appendChild(empty);
  }
  empty.querySelector('.empty-state-text').textContent = message;
  empty.classList.remove('hidden');
};

const clearEmptyState = (container) => {
  const empty = container.querySelector('.empty-state');
  if (empty) empty.classList.add('hidden');
  const canvas = container.querySelector('canvas');
  if (canvas) canvas.classList.remove('hidden');
};

// -----------------------------------------------------------------------------
// NAVIGATION
// -----------------------------------------------------------------------------
const initNavigation = () => {
  $$('.nav-item').forEach(item => {
    item.addEventListener('click', () => {
      const module = item.dataset.module;
      
      $$('.nav-item').forEach(n => n.classList.remove('active'));
      item.classList.add('active');
      
      $$('.module').forEach(m => m.classList.add('hidden'));
      $(`#module${capitalize(module)}`)?.classList.remove('hidden');
      
      loadModule(module);
    });
  });
};

const capitalize = (s) => s.charAt(0).toUpperCase() + s.slice(1);

// -----------------------------------------------------------------------------
// MODULE LOADERS
// -----------------------------------------------------------------------------
const loadModule = async (module) => {
  syncFiltersFromUI();
  
  if (!globalFilters.issuer_ruc) {
    showToast('Selecciona un comercio', 'warning');
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
    if (!data) return; // Aborted
    
    renderOverviewKPIs(data);
    renderOverviewCharts(data);
  } catch (e) {
    console.error('Overview error:', e);
  }
};

const renderOverviewKPIs = (data) => {
  const { current, previous } = data;
  
  $('#kpiVentas').textContent = formatUSD(current.ventas);
  const ventasDelta = formatDelta(current.ventas, previous?.ventas);
  $('#kpiVentasDelta').textContent = ventasDelta.text;
  $('#kpiVentasDelta').className = `kpi-delta ${ventasDelta.class}`;
  
  $('#kpiTxn').textContent = formatNum(current.transacciones);
  $('#kpiClientes').textContent = formatNum(current.clientes);
  $('#kpiTicket').textContent = formatUSD(current.ticket_promedio);
  
  $('#kpiFreq').textContent = current.frecuencia?.toFixed(2) || '—';
  $('#kpiMeses').textContent = formatNum(data.trends?.length || 0);
  $('#kpiCategorias').textContent = formatNum(current.categorias);
  $('#kpiSoW').textContent = formatPct(current.sow_pct);
};

const renderOverviewCharts = (data) => {
  const { trends, by_weekday, top_categories } = data;
  
  // Ventas por Mes
  destroyChart('chartVentasMes');
  const ctxVentas = $('#chartVentasMes');
  if (trends?.length > 0) {
    clearEmptyState(ctxVentas.parentElement);
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
    clearEmptyState(ctxClientes.parentElement);
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
    clearEmptyState(ctxDia.parentElement);
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
    clearEmptyState(ctxCat.parentElement);
    charts['chartTopCategorias'] = new Chart(ctxCat, {
      type: 'bar',
      data: {
        labels: top_categories.map(c => c.category.substring(0, 15)),
        datasets: [{
          label: 'Ventas',
          data: top_categories.map(c => c.ventas),
          backgroundColor: top_categories.map(c => c.is_unknown ? COLORS.gray : COLORS.cyan),
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
    if (!data) return;
    
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
  
  destroyChart('chartCaptureSoW');
  const ctxSoW = $('#chartCaptureSoW');
  if (rows.length > 0) {
    clearEmptyState(ctxSoW.parentElement);
    charts['chartCaptureSoW'] = new Chart(ctxSoW, {
      type: 'bar',
      data: {
        labels: rows.map(r => r.category_value.substring(0, 15)),
        datasets: [{
          label: 'SoW %',
          data: rows.map(r => r.sow_pct),
          backgroundColor: rows.map(r => r.is_unknown ? COLORS.gray : COLORS.cyan),
          borderRadius: 4
        }]
      },
      options: { indexAxis: 'y', responsive: true, plugins: { legend: { display: false } }, scales: { x: { max: 100 } } }
    });
  } else {
    showEmptyState(ctxSoW.parentElement);
  }
  
  destroyChart('chartCaptureStacked');
  const ctxStacked = $('#chartCaptureStacked');
  if (rows.length > 0) {
    clearEmptyState(ctxStacked.parentElement);
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
    <tr class="${r.is_unknown ? 'row-suppressed' : ''}">
      <td class="${r.is_unknown ? 'suppressed' : ''}">${r.category_value}${r.is_unknown ? ' ⚠️' : ''}</td>
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
    if (!data) return;
    
    renderSwitchingChart(data);
    renderSwitchingTable(data);
  } catch (e) {
    console.error('Switching error:', e);
  }
};

const renderSwitchingChart = (data) => {
  destroyChart('chartSwitching');
  const ctx = $('#chartSwitching');
  
  if (data.data?.length > 0) {
    clearEmptyState(ctx.parentElement);
    charts['chartSwitching'] = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: data.data.map(r => r.destination.substring(0, 20)),
        datasets: [{
          label: '% Cohort',
          data: data.data.map(r => r.pct),
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
  try {
    // Load categories for dropdown
    const captureData = await fetchAPI('/api/sow_leakage/by_category');
    if (!captureData) return;
    
    populateCategoryValueSelect('#leakageCategorySelect', captureData.data);
    
    const category = $('#leakageCategorySelect').value;
    if (category) {
      const data = await fetchAPI('/api/leakage/tree', { category_value: category });
      if (data) renderLeakageChart(data);
    }
  } catch (e) {
    console.error('Leakage error:', e);
  }
};

const renderLeakageChart = (data) => {
  destroyChart('chartLeakage');
  const ctx = $('#chartLeakage');
  
  const hasData = data.waterfall?.some(b => b.users > 0);
  
  if (hasData) {
    clearEmptyState(ctx.parentElement);
    const bucketColors = {
      'RETAINED': COLORS.green,
      'CATEGORY_GONE': COLORS.yellow,
      'REDUCED_BASKET': COLORS.orange,
      'REDUCED_FREQ': COLORS.orange,
      'DELAYED_ONLY': COLORS.red,
      'FULL_CHURN': COLORS.gray
    };
    
    charts['chartLeakage'] = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: data.waterfall.map(b => b.bucket),
        datasets: [{
          label: '% Cohorte',
          data: data.waterfall.map(b => b.pct),
          backgroundColor: data.waterfall.map(b => bucketColors[b.bucket] || COLORS.gray),
          borderRadius: 4
        }]
      },
      options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { max: 100 } } }
    });
  } else {
    showEmptyState(ctx.parentElement, 'Sin datos de transiciones para esta categoría');
  }
};

// -----------------------------------------------------------------------------
// BASKET MODULE
// -----------------------------------------------------------------------------
const loadBasket = async () => {
  try {
    const data = await fetchAPI('/api/basket/breadth');
    if (!data) return;
    
    renderBasketKPIs(data);
    renderBasketChart(data);
  } catch (e) {
    console.error('Basket error:', e);
  }
};

const renderBasketKPIs = (data) => {
  if (data.data?.length > 0) {
    const avg = arr => arr.reduce((a, b) => a + b, 0) / arr.length;
    $('#basketBreadthMarket').textContent = avg(data.data.map(d => d.avg_breadth_market)).toFixed(2);
    $('#basketBreadthInX').textContent = avg(data.data.map(d => d.avg_breadth_in_x)).toFixed(2);
  } else {
    $('#basketBreadthMarket').textContent = '—';
    $('#basketBreadthInX').textContent = '—';
  }
};

const renderBasketChart = (data) => {
  destroyChart('chartBasket');
  const ctx = $('#chartBasket');
  
  if (data.data?.length > 0) {
    clearEmptyState(ctx.parentElement);
    charts['chartBasket'] = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: data.data.map(d => d.origin_month),
        datasets: [
          { label: 'Market', data: data.data.map(d => d.avg_breadth_market), backgroundColor: COLORS.gray, borderRadius: 4 },
          { label: 'In-X', data: data.data.map(d => d.avg_breadth_in_x), backgroundColor: COLORS.cyan, borderRadius: 4 }
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
    const captureData = await fetchAPI('/api/sow_leakage/by_category');
    if (!captureData) return;
    
    populateCategoryValueSelect('#loyaltyCategorySelect', captureData.data);
    
    const category = $('#loyaltyCategorySelect').value;
    if (category) {
      const data = await fetchAPI('/api/loyalty/brands', { category_value: category });
      if (data) {
        renderLoyaltyKPIs(data);
        renderLoyaltyCharts(data);
        renderLoyaltyTable(data);
      }
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
  
  destroyChart('chartLoyaltyTiers');
  const ctxTiers = $('#chartLoyaltyTiers');
  if (Object.values(tiers).some(v => v > 0)) {
    clearEmptyState(ctxTiers.parentElement);
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
  
  destroyChart('chartLoyaltyDist');
  const ctxDist = $('#chartLoyaltyDist');
  if (Object.values(dist).some(v => v > 0)) {
    clearEmptyState(ctxDist.parentElement);
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
    <tr class="${r.is_unknown ? 'row-suppressed' : ''}">
      <td class="${r.is_unknown ? 'suppressed' : ''}">${r.brand}${r.is_unknown ? ' ⚠️' : ''}</td>
      <td class="text-right mono">${formatNum(r.brand_buyers)}</td>
      <td class="text-right mono">${formatPct(r.penetration_pct)}</td>
      <td class="text-right mono">${formatPct(r.p75)}</td>
      <td class="text-right mono highlight">${formatPct(r.loyalty_rate_pct)}</td>
    </tr>
  `).join('');
};

// -----------------------------------------------------------------------------
// CATEGORY SELECT HELPERS
// -----------------------------------------------------------------------------
function populateCategoryValueSelect(selectId, data) {
  const select = $(selectId);
  if (!select) return;
  
  const currentVal = select.value;
  select.innerHTML = '<option value="">Selecciona categoría...</option>';
  
  data.filter(r => !r.is_unknown).forEach(r => {
    const opt = document.createElement('option');
    opt.value = r.category_value;
    opt.textContent = `${r.category_value} (${formatNum(r.users)} users)`;
    select.appendChild(opt);
  });
  
  // Restore previous selection if still valid
  if (currentVal && select.querySelector(`option[value="${currentVal}"]`)) {
    select.value = currentVal;
  } else if (data.length > 0) {
    // Auto-select first non-unknown
    const first = data.find(r => !r.is_unknown);
    if (first) select.value = first.category_value;
  }
}

// -----------------------------------------------------------------------------
// EVENT LISTENERS
// -----------------------------------------------------------------------------
const initEventListeners = () => {
  // Apply filters
  $('#btnApplyFilters').addEventListener('click', () => {
    syncFiltersFromUI();
    updateFilterStatus();
    const activeModule = $('.nav-item.active')?.dataset.module || 'overview';
    loadModule(activeModule);
  });
  
  // Reset filters
  $('#btnResetFilters').addEventListener('click', () => {
    $('#filterIssuerRuc').value = '';
    $('#filterStart').value = '2025-01-01';
    $('#filterEnd').value = '2025-07-01';
    $('#filterCategoryLevel').value = 'l1';
    $('#filterCategoryDomain').value = 'product';
    $('#filterPeerScope').value = 'all';
    $('#filterReconcile').value = 'all';
    $('#filterKThreshold').value = '5';
    
    // Reset category path dropdowns
    ['#filterCatL1', '#filterCatL2', '#filterCatL3', '#filterCatL4'].forEach(sel => {
      if ($(sel)) $(sel).value = '';
    });
    
    // Reset global state
    Object.assign(globalFilters, {
      issuer_ruc: null,
      start: '2025-01-01',
      end: '2025-07-01',
      category_level: 'l1',
      category_domain: 'product',
      peer_scope: 'all',
      reconcile_ok: 'all',
      k_threshold: 5,
      category_path: { l1: null, l2: null, l3: null, l4: null }
    });
    
    childrenCache = {};
    updateFilterStatus();
  });
  
  // Export Deck (disabled - endpoint removed)
  $('#btnExportDeck')?.addEventListener('click', () => {
    showToast('Export Deck temporalmente deshabilitado', 'warning');
  });
  
  // Leakage category change
  $('#leakageCategorySelect').addEventListener('change', async (e) => {
    if (e.target.value) {
      showLoading();
      try {
        const data = await fetchAPI('/api/leakage/tree', { category_value: e.target.value });
        if (data) renderLeakageChart(data);
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
        if (data) {
          renderLoyaltyKPIs(data);
          renderLoyaltyCharts(data);
          renderLoyaltyTable(data);
        }
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

// Update filter status strip
const updateFilterStatus = () => {
  $('#statusKThreshold').textContent = globalFilters.k_threshold || '5';
  $('#statusReconcile').textContent = globalFilters.reconcile_ok === 'all' ? 'All' : globalFilters.reconcile_ok;
  if ($('#statusPeerScope')) {
    $('#statusPeerScope').textContent = globalFilters.peer_scope || 'all';
  }
};

// -----------------------------------------------------------------------------
// LOAD RETAILERS
// -----------------------------------------------------------------------------
const loadRetailers = async () => {
  try {
    const res = await fetch('/api/retailers');
    const data = await res.json();
    const select = $('#filterIssuerRuc');
    select.innerHTML = '<option value="">Selecciona comercio...</option>';
    data.data.forEach(r => {
      const opt = document.createElement('option');
      opt.value = r.issuer_ruc;
      opt.textContent = `${r.issuer_name} (${r.users.toLocaleString()} users)`;
      select.appendChild(opt);
    });
  } catch (e) {
    console.error('Error loading retailers:', e);
    $('#filterIssuerRuc').innerHTML = '<option value="">Error cargando</option>';
  }
};

// -----------------------------------------------------------------------------
// INIT
// -----------------------------------------------------------------------------
document.addEventListener('DOMContentLoaded', async () => {
  initNavigation();
  initEventListeners();
  initCategoryHierarchy();
  
  await loadRetailers();
  await populateCategoryDropdown('#filterCatL1', 'l1', {});
  
  // Check URL params for auto-load
  const params = new URLSearchParams(window.location.search);
  if (params.get('issuer_ruc')) {
    $('#filterIssuerRuc').value = params.get('issuer_ruc');
    if (params.get('start')) $('#filterStart').value = params.get('start');
    if (params.get('end')) $('#filterEnd').value = params.get('end');
    syncFiltersFromUI();
    loadModule('overview');
  }
});
