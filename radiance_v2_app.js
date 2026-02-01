// =============================================================================
// RADIANCE DASHBOARD - CLIENT v2.1
// LÜM Internal Analytics MVP - Fixed filters
// =============================================================================

const API_BASE = '';
let currentView = 'overview';
let currentFilters = {
  start: getDefaultStart(),
  end: getToday(),
  issuer_ruc: null,
  category: null,
  reconcile_ok: 'true'
};
let filterCache = { issuers: [], categories: [] };

// =============================================================================
// UTILITIES
// =============================================================================

function getToday() {
  return new Date().toISOString().split('T')[0];
}

function getDefaultStart() {
  const d = new Date();
  d.setMonth(d.getMonth() - 6); // 6 months back
  return d.toISOString().split('T')[0];
}

function formatCurrency(value) {
  if (value === null || value === undefined) return '$0';
  const num = Number(value);
  if (num >= 1000000) return '$' + (num / 1000000).toFixed(2) + 'M';
  if (num >= 1000) return '$' + (num / 1000).toFixed(1) + 'K';
  return '$' + num.toFixed(2);
}

function formatNumber(value) {
  if (value === null || value === undefined) return '0';
  const num = Number(value);
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
  return new Intl.NumberFormat('en-US').format(num);
}

function formatPercent(value) {
  if (value === null || value === undefined) return '0%';
  return `${Number(value).toFixed(1)}%`;
}

function formatChange(value) {
  if (value === null || value === undefined) return '';
  const sign = value >= 0 ? '+' : '';
  return `${sign}${Number(value).toFixed(1)}%`;
}

function getTrustBadge(level) {
  const config = {
    'HIGH': { class: 'trust-high', icon: '●' },
    'MEDIUM': { class: 'trust-medium', icon: '◐' },
    'LOW': { class: 'trust-low', icon: '○' }
  };
  const c = config[level] || config['LOW'];
  return `<span class="trust-badge ${c.class}">${c.icon} ${level || 'LOW'}</span>`;
}

function getDqStatus(value, threshold = 90) {
  if (value >= threshold) return 'good';
  if (value >= threshold - 10) return 'warning';
  return 'bad';
}

function calculateTrustLevel(users, coverage, reconcile) {
  const u = Number(users) || 0;
  const c = Number(coverage) || 0;
  const r = Number(reconcile) || 100;
  if (u >= 10 && c >= 80 && r >= 90) return 'HIGH';
  if (u >= 5 && c >= 60) return 'MEDIUM';
  return 'LOW';
}

// =============================================================================
// API
// =============================================================================

async function fetchAPI(endpoint, params = {}) {
  const url = new URL(endpoint, window.location.origin);
  Object.entries(params).forEach(([k, v]) => {
    if (v !== null && v !== undefined && v !== '' && v !== 'all') {
      url.searchParams.set(k, v);
    }
  });
  console.log('Fetching:', url.toString());
  const response = await fetch(url);
  if (!response.ok) {
    const err = await response.json().catch(() => ({}));
    throw new Error(err.error || 'API Error');
  }
  return response.json();
}

async function loadFiltersData() {
  if (filterCache.issuers.length > 0) return filterCache;
  try {
    const [issuers, categories] = await Promise.all([
      fetchAPI('/api/issuers').catch(() => []),
      fetchAPI('/api/categories').catch(() => [])
    ]);
    filterCache = { issuers: issuers || [], categories: categories || [] };
    console.log(`Loaded ${filterCache.issuers.length} issuers, ${filterCache.categories.length} categories`);
    return filterCache;
  } catch (err) {
    console.error('Failed to load filters:', err);
    return { issuers: [], categories: [] };
  }
}

// =============================================================================
// COMPONENTS
// =============================================================================

function renderFiltersBar(issuers = [], categories = []) {
  return `
    <div class="filters-bar">
      <div class="filter-group">
        <label class="filter-label">Fecha Inicio</label>
        <input type="date" class="filter-input" id="filter-start" value="${currentFilters.start}">
      </div>
      <div class="filter-group">
        <label class="filter-label">Fecha Fin</label>
        <input type="date" class="filter-input" id="filter-end" value="${currentFilters.end}">
      </div>
      <div class="filter-group">
        <label class="filter-label">Comercio (${issuers.length})</label>
        <select class="filter-input" id="filter-issuer" style="min-width: 200px;">
          <option value="">— Todos los comercios —</option>
          ${issuers.map(i => `
            <option value="${i.issuer_ruc}" ${currentFilters.issuer_ruc === i.issuer_ruc ? 'selected' : ''}>
              ${(i.retailer_name || i.issuer_ruc || 'Unknown').substring(0, 35)} (${formatNumber(i.receipt_count || 0)})
            </option>
          `).join('')}
        </select>
      </div>
      <div class="filter-group">
        <label class="filter-label">Categoría (${categories.length})</label>
        <select class="filter-input" id="filter-category" style="min-width: 180px;">
          <option value="">— Todas las categorías —</option>
          ${categories.map(c => `
            <option value="${c.category}" ${currentFilters.category === c.category ? 'selected' : ''}>
              ${(c.category || 'Unknown').substring(0, 30)} (${formatNumber(c.line_count || 0)})
            </option>
          `).join('')}
        </select>
      </div>
      <div class="filter-group">
        <label class="filter-label">Calidad</label>
        <select class="filter-input" id="filter-reconcile">
          <option value="true" ${currentFilters.reconcile_ok === 'true' ? 'selected' : ''}>Solo Validados</option>
          <option value="" ${currentFilters.reconcile_ok === '' ? 'selected' : ''}>Todos</option>
        </select>
      </div>
      <div class="filter-group" style="align-self: flex-end;">
        <button class="btn btn-primary" onclick="applyFilters()">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>
          Aplicar
        </button>
      </div>
    </div>
  `;
}

function renderKPICard(data) {
  const changeClass = data.change > 0 ? 'positive' : data.change < 0 ? 'negative' : '';
  return `
    <div class="kpi-card ${data.color}">
      <div class="kpi-header">
        <div class="kpi-icon ${data.color}">${data.icon}</div>
        ${data.change !== null && data.change !== undefined ? `<div class="kpi-change ${changeClass}">${formatChange(data.change)}</div>` : ''}
      </div>
      <div class="kpi-value">${data.value}</div>
      <div class="kpi-label">${data.label}</div>
    </div>
  `;
}

// =============================================================================
// VIEW: OVERVIEW
// =============================================================================

async function renderOverview() {
  const main = document.getElementById('main-content');
  main.innerHTML = '<div class="loading"><div class="spinner"></div><div>Cargando Overview...</div></div>';

  try {
    const { issuers, categories } = await loadFiltersData();
    const [summary, dq, coverage, retailers] = await Promise.all([
      fetchAPI('/api/kpis/summary', currentFilters),
      fetchAPI('/api/dq'),
      fetchAPI('/api/coverage', currentFilters),
      fetchAPI('/api/retailer-distribution', currentFilters)
    ]);

    const kpis = [
      { icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>', value: formatCurrency(summary.gross_sales), label: 'Ventas Brutas', change: summary.sales_change_pct, color: 'amber' },
      { icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/></svg>', value: formatNumber(summary.buyers), label: 'Compradores', change: summary.buyers_change_pct, color: 'teal' },
      { icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/></svg>', value: formatNumber(summary.receipts), label: 'Recibos', change: summary.receipts_change_pct, color: 'violet' },
      { icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>', value: formatCurrency(summary.aov), label: 'Ticket Promedio', change: null, color: 'cyan' },
      { icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>', value: (summary.frequency || 0).toFixed(1) + 'x', label: 'Frecuencia', change: null, color: 'rose' },
      { icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="4" width="18" height="18" rx="2"/></svg>', value: summary.active_days || '0', label: 'Días Activos', change: null, color: 'emerald' }
    ];

    const trustLevel = calculateTrustLevel(summary.buyers, coverage.category_coverage_pct, dq.dq_reconcile_ok_pct);

    main.innerHTML = `
      <div class="header">
        <div>
          <h1>📊 Overview</h1>
          <p>Panel principal · ${currentFilters.start} a ${currentFilters.end}</p>
        </div>
        <div>${getTrustBadge(trustLevel)}</div>
      </div>

      ${renderFiltersBar(issuers, categories)}

      <div class="kpi-grid">${kpis.map(k => renderKPICard(k)).join('')}</div>

      <div class="charts-grid">
        <div class="chart-card">
          <div class="chart-header">
            <div>
              <div class="chart-title">Top Comercios</div>
              <div class="chart-subtitle">${retailers.length} retailers por ventas</div>
            </div>
          </div>
          <div class="chart-container"><canvas id="retailerChart"></canvas></div>
        </div>

        <div class="chart-card">
          <div class="chart-header">
            <div>
              <div class="chart-title">Data Quality</div>
              <div class="chart-subtitle">Métricas de cobertura</div>
            </div>
          </div>
          <div style="padding: 20px 0;">
            ${[
              { label: 'Cobertura Productos', value: coverage.product_coverage_pct, color: 'amber' },
              { label: 'Cobertura Categorías', value: coverage.category_coverage_pct, color: 'teal' },
              { label: 'Reconciliación OK', value: dq.dq_reconcile_ok_pct, color: 'emerald' },
              { label: 'Issuer Match', value: dq.dq_issuer_match_pct, color: 'cyan' }
            ].map(m => `
              <div style="margin-bottom: 20px;">
                <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
                  <span style="font-size: 13px; color: var(--text-secondary);">${m.label}</span>
                  <span style="font-weight: 700;">${formatPercent(m.value)}</span>
                </div>
                <div class="progress-bar">
                  <div class="progress-fill ${m.color}" style="width: ${m.value || 0}%"></div>
                </div>
              </div>
            `).join('')}
          </div>
        </div>
      </div>

      <div class="data-table-container">
        <div class="table-header">
          <div class="table-title">🏪 Distribución por Comercio</div>
          <div style="font-size: 12px; color: var(--text-muted);">${retailers.length} comercios</div>
        </div>
        <table class="data-table">
          <thead>
            <tr>
              <th>Comercio</th>
              <th>Categoría</th>
              <th>Ventas</th>
              <th>Recibos</th>
              <th>Buyers</th>
              <th>Ticket Prom.</th>
            </tr>
          </thead>
          <tbody>
            ${retailers.slice(0, 15).map(r => `
              <tr>
                <td style="font-weight: 600;">${(r.retailer_name || r.issuer_ruc || 'Unknown').substring(0, 30)}</td>
                <td style="color: var(--text-secondary); font-size: 12px;">${(r.retailer_category || '-').substring(0, 25)}</td>
                <td style="font-weight: 700; color: var(--glow-amber);">${formatCurrency(r.gross_sales)}</td>
                <td>${formatNumber(r.receipts)}</td>
                <td>${formatNumber(r.buyers)}</td>
                <td>${formatCurrency(r.avg_ticket)}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    `;

    // Render chart
    if (retailers.length > 0) {
      new Chart(document.getElementById('retailerChart').getContext('2d'), {
        type: 'bar',
        data: {
          labels: retailers.slice(0, 10).map(r => (r.retailer_name || 'Unknown').substring(0, 18)),
          datasets: [{
            data: retailers.slice(0, 10).map(r => r.gross_sales),
            backgroundColor: 'rgba(255, 176, 32, 0.8)',
            borderRadius: 6
          }]
        },
        options: {
          indexAxis: 'y',
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: {
            x: { grid: { color: 'rgba(255,255,255,0.04)' }, ticks: { color: '#5c5c6d' } },
            y: { grid: { display: false }, ticks: { color: '#9898a8', font: { size: 11 } } }
          }
        }
      });
    }

  } catch (err) {
    console.error('Overview error:', err);
    main.innerHTML = `<div class="empty-state">
      <h3>Error cargando datos</h3>
      <p>${err.message}</p>
      <br>
      <button class="btn btn-primary" onclick="renderView('overview')">Reintentar</button>
    </div>`;
  }
}

// =============================================================================
// VIEW: PERFORMANCE
// =============================================================================

async function renderPerformance() {
  const main = document.getElementById('main-content');
  main.innerHTML = '<div class="loading"><div class="spinner"></div><div>Cargando Performance...</div></div>';

  try {
    const { issuers, categories } = await loadFiltersData();
    const [kpisDaily, summary] = await Promise.all([
      fetchAPI('/api/kpis/daily', currentFilters),
      fetchAPI('/api/kpis/summary', currentFilters)
    ]);

    main.innerHTML = `
      <div class="header">
        <div>
          <h1>📈 Performance</h1>
          <p>Tendencias diarias · ${kpisDaily.length} días</p>
        </div>
        <div style="text-align: right;">
          <div style="font-size: 32px; font-weight: 800; color: var(--glow-amber);">${formatCurrency(summary.gross_sales)}</div>
          <div style="font-size: 12px; color: var(--text-muted);">Total del período</div>
        </div>
      </div>

      ${renderFiltersBar(issuers, categories)}

      <div class="chart-card" style="margin-bottom: 24px;">
        <div class="chart-header">
          <div>
            <div class="chart-title">Ventas Diarias</div>
            <div class="chart-subtitle">Evolución de ingresos</div>
          </div>
        </div>
        <div class="chart-container" style="height: 320px;"><canvas id="salesChart"></canvas></div>
      </div>

      <div class="charts-grid">
        <div class="chart-card">
          <div class="chart-header"><div><div class="chart-title">Recibos por Día</div></div></div>
          <div class="chart-container"><canvas id="receiptsChart"></canvas></div>
        </div>
        <div class="chart-card">
          <div class="chart-header"><div><div class="chart-title">Ticket Promedio</div></div></div>
          <div class="chart-container"><canvas id="aovChart"></canvas></div>
        </div>
      </div>

      <div class="data-table-container">
        <div class="table-header"><div class="table-title">📅 Detalle Diario (últimos 30)</div></div>
        <div style="max-height: 400px; overflow-y: auto;">
          <table class="data-table">
            <thead><tr><th>Fecha</th><th>Ventas</th><th>Recibos</th><th>Buyers</th><th>AOV</th></tr></thead>
            <tbody>
              ${kpisDaily.slice(-30).reverse().map(d => `
                <tr>
                  <td style="font-family: 'JetBrains Mono', monospace; font-size: 12px;">${d.invoice_date}</td>
                  <td style="font-weight: 700; color: var(--glow-amber);">${formatCurrency(d.gross_sales)}</td>
                  <td>${formatNumber(d.receipts)}</td>
                  <td>${formatNumber(d.buyers)}</td>
                  <td>${formatCurrency(d.aov)}</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      </div>
    `;

    // Sales chart
    new Chart(document.getElementById('salesChart').getContext('2d'), {
      type: 'line',
      data: {
        labels: kpisDaily.map(d => d.invoice_date),
        datasets: [{
          data: kpisDaily.map(d => d.gross_sales),
          borderColor: '#ffb020',
          backgroundColor: 'rgba(255, 176, 32, 0.1)',
          fill: true, tension: 0.4, pointRadius: 0, borderWidth: 2
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { grid: { color: 'rgba(255,255,255,0.04)' }, ticks: { color: '#5c5c6d', maxTicksLimit: 12 } },
          y: { grid: { color: 'rgba(255,255,255,0.04)' }, ticks: { color: '#5c5c6d' } }
        }
      }
    });

    // Receipts chart
    new Chart(document.getElementById('receiptsChart').getContext('2d'), {
      type: 'bar',
      data: {
        labels: kpisDaily.map(d => d.invoice_date),
        datasets: [{ data: kpisDaily.map(d => d.receipts), backgroundColor: 'rgba(0, 212, 170, 0.7)', borderRadius: 4 }]
      },
      options: {
        responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } },
        scales: { x: { grid: { display: false }, ticks: { color: '#5c5c6d', maxTicksLimit: 10 } }, y: { grid: { color: 'rgba(255,255,255,0.04)' }, ticks: { color: '#5c5c6d' } } }
      }
    });

    // AOV chart
    new Chart(document.getElementById('aovChart').getContext('2d'), {
      type: 'line',
      data: {
        labels: kpisDaily.map(d => d.invoice_date),
        datasets: [{ data: kpisDaily.map(d => d.aov), borderColor: '#a78bfa', backgroundColor: 'rgba(167, 139, 250, 0.1)', fill: true, tension: 0.4, pointRadius: 0, borderWidth: 2 }]
      },
      options: {
        responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } },
        scales: { x: { grid: { display: false }, ticks: { color: '#5c5c6d', maxTicksLimit: 8 } }, y: { grid: { color: 'rgba(255,255,255,0.04)' }, ticks: { color: '#5c5c6d' } } }
      }
    });

  } catch (err) {
    main.innerHTML = `<div class="empty-state">Error: ${err.message}</div>`;
  }
}

// =============================================================================
// VIEW: SHARE OF WALLET
// =============================================================================

async function renderSoW() {
  const main = document.getElementById('main-content');
  main.innerHTML = '<div class="loading"><div class="spinner"></div><div>Cargando Share of Wallet...</div></div>';

  try {
    const { issuers, categories } = await loadFiltersData();
    const [sow, coverage] = await Promise.all([
      fetchAPI('/api/sow', currentFilters),
      fetchAPI('/api/coverage', currentFilters)
    ]);

    // Group by category
    const categoryGroups = {};
    sow.forEach(row => {
      const cat = row.category || 'UNKNOWN';
      if (!categoryGroups[cat]) categoryGroups[cat] = { category: cat, total: 0, brands: [] };
      categoryGroups[cat].total += Number(row.brand_sales) || 0;
      categoryGroups[cat].brands.push(row);
    });

    const sortedCategories = Object.values(categoryGroups)
      .filter(c => c.category !== 'UNKNOWN')
      .sort((a, b) => b.total - a.total);

    const colors = ['#ffb020', '#00d4aa', '#a78bfa', '#22d3ee', '#fb7185', '#34d399', '#f97316', '#8b5cf6'];

    main.innerHTML = `
      <div class="header">
        <div>
          <h1>🥧 Share of Wallet</h1>
          <p>Participación de marca por categoría · Panel Observado</p>
        </div>
        <div><span class="trust-badge trust-medium">◐ PANEL OBSERVADO</span></div>
      </div>

      ${renderFiltersBar(issuers, categories)}

      <div class="dq-grid">
        <div class="dq-card">
          <div class="dq-value" style="color: var(--glow-amber);">${sortedCategories.length}</div>
          <div class="dq-label">Categorías</div>
        </div>
        <div class="dq-card">
          <div class="dq-value" style="color: var(--glow-teal);">${sow.length}</div>
          <div class="dq-label">Combinaciones</div>
        </div>
        <div class="dq-card">
          <div class="dq-value" style="color: var(--glow-violet);">${formatPercent(coverage.category_coverage_pct)}</div>
          <div class="dq-label">Cobertura</div>
        </div>
        <div class="dq-card">
          <div class="dq-value" style="color: var(--glow-cyan);">${formatCurrency(sortedCategories.reduce((a, c) => a + c.total, 0))}</div>
          <div class="dq-label">Ventas Mapeadas</div>
        </div>
      </div>

      ${sortedCategories.length === 0 ? '<div class="empty-state">No hay datos de categorías. Verifica que las columnas category_ai_primary o category_ai existan en la base.</div>' : ''}

      ${sortedCategories.slice(0, 12).map(cat => `
        <div class="chart-card" style="margin-bottom: 20px;">
          <div class="chart-header">
            <div>
              <div class="chart-title">${cat.category}</div>
              <div class="chart-subtitle">Total: ${formatCurrency(cat.total)} · ${cat.brands.length} marcas</div>
            </div>
          </div>
          
          <div class="sow-bar" style="margin-bottom: 20px;">
            ${cat.brands.slice(0, 8).map((b, i) => `
              <div class="sow-segment" 
                   style="width: ${Math.max(b.sow_pct || 0, 1)}%; background: ${colors[i % colors.length]};" 
                   title="${b.brand}: ${formatPercent(b.sow_pct)}">
              </div>
            `).join('')}
          </div>

          <table class="data-table" style="font-size: 13px;">
            <thead>
              <tr>
                <th style="width: 25%;">Marca</th>
                <th>Ventas</th>
                <th>SoW</th>
                <th>Buyers</th>
                <th>$/Buyer</th>
                <th>Trust</th>
              </tr>
            </thead>
            <tbody>
              ${cat.brands.slice(0, 8).map((b, i) => `
                <tr>
                  <td>
                    <div style="display: flex; align-items: center; gap: 10px;">
                      <div style="width: 12px; height: 12px; border-radius: 3px; background: ${colors[i % colors.length]};"></div>
                      <span style="font-weight: 600;">${(b.brand || 'UNKNOWN').substring(0, 20)}</span>
                    </div>
                  </td>
                  <td style="color: var(--glow-amber);">${formatCurrency(b.brand_sales)}</td>
                  <td>
                    <div style="display: flex; align-items: center; gap: 8px;">
                      <div class="progress-bar" style="width: 50px; height: 6px;">
                        <div class="progress-fill amber" style="width: ${Math.min(b.sow_pct || 0, 100)}%;"></div>
                      </div>
                      <span style="font-weight: 700; font-family: monospace; font-size: 12px;">${formatPercent(b.sow_pct)}</span>
                    </div>
                  </td>
                  <td>${formatNumber(b.unique_users)}</td>
                  <td>${formatCurrency(b.spend_per_buyer)}</td>
                  <td>${getTrustBadge(b.trust_level || calculateTrustLevel(b.unique_users, 80, 95))}</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      `).join('')}
    `;

  } catch (err) {
    main.innerHTML = `<div class="empty-state">Error: ${err.message}</div>`;
  }
}

// =============================================================================
// VIEW: PRODUCTS
// =============================================================================

async function renderProducts() {
  const main = document.getElementById('main-content');
  main.innerHTML = '<div class="loading"><div class="spinner"></div><div>Cargando Productos...</div></div>';

  try {
    const { issuers, categories } = await loadFiltersData();
    const products = await fetchAPI('/api/top-products', { ...currentFilters, limit: 50 });

    const totalSales = products.reduce((a, p) => a + Number(p.total_sales || 0), 0);
    const totalUnits = products.reduce((a, p) => a + Number(p.total_units || 0), 0);
    const uniqueBrands = new Set(products.map(p => p.brand)).size;

    main.innerHTML = `
      <div class="header">
        <div>
          <h1>🏷️ Top Productos</h1>
          <p>Productos con mayor volumen de ventas</p>
        </div>
      </div>

      ${renderFiltersBar(issuers, categories)}

      <div class="kpi-grid" style="grid-template-columns: repeat(4, 1fr);">
        ${renderKPICard({ icon: '$', value: formatCurrency(totalSales), label: 'Ventas Top 50', change: null, color: 'amber' })}
        ${renderKPICard({ icon: '#', value: formatNumber(totalUnits), label: 'Unidades', change: null, color: 'teal' })}
        ${renderKPICard({ icon: '◇', value: formatNumber(products.length), label: 'Productos', change: null, color: 'violet' })}
        ${renderKPICard({ icon: '★', value: formatNumber(uniqueBrands), label: 'Marcas', change: null, color: 'cyan' })}
      </div>

      <div class="data-table-container">
        <div class="table-header"><div class="table-title">📦 Ranking de Productos</div></div>
        <div style="max-height: 600px; overflow-y: auto;">
          <table class="data-table">
            <thead>
              <tr>
                <th style="width: 40px;">#</th>
                <th style="width: 35%;">Producto</th>
                <th>Marca</th>
                <th>Categoría</th>
                <th>Ventas</th>
                <th>Units</th>
                <th>Buyers</th>
              </tr>
            </thead>
            <tbody>
              ${products.map((p, i) => `
                <tr>
                  <td style="color: var(--text-muted); font-weight: 700;">${i + 1}</td>
                  <td style="font-weight: 500;">${(p.product_description || '-').substring(0, 45)}</td>
                  <td>
                    <span style="background: var(--glow-amber-dim); color: var(--glow-amber); padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: 600;">
                      ${(p.brand || 'UNKNOWN').substring(0, 15)}
                    </span>
                  </td>
                  <td style="color: var(--text-secondary); font-size: 12px;">${(p.category || '-').substring(0, 20)}</td>
                  <td style="font-weight: 700; color: var(--glow-amber);">${formatCurrency(p.total_sales)}</td>
                  <td>${formatNumber(p.total_units)}</td>
                  <td>${formatNumber(p.buyer_count)}</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      </div>
    `;

  } catch (err) {
    main.innerHTML = `<div class="empty-state">Error: ${err.message}</div>`;
  }
}

// =============================================================================
// VIEW: BUYERS
// =============================================================================

async function renderBuyers() {
  const main = document.getElementById('main-content');
  main.innerHTML = '<div class="loading"><div class="spinner"></div><div>Cargando Buyer Insights...</div></div>';

  try {
    const { issuers, categories } = await loadFiltersData();
    const insights = await fetchAPI('/api/buyer-insights', currentFilters);

    const segments = insights.segments || [];
    const colors = ['#ffb020', '#00d4aa', '#a78bfa', '#22d3ee'];

    main.innerHTML = `
      <div class="header">
        <div>
          <h1>👥 Buyer Insights</h1>
          <p>Segmentación y patrones de comportamiento</p>
        </div>
      </div>

      ${renderFiltersBar(issuers, categories)}

      <div class="kpi-grid" style="grid-template-columns: repeat(3, 1fr);">
        ${renderKPICard({ icon: '👤', value: formatNumber(insights.total_buyers), label: 'Total Buyers', change: null, color: 'amber' })}
        ${renderKPICard({ icon: '↻', value: (insights.avg_visits_per_buyer || 0).toFixed(1), label: 'Visitas/Buyer', change: null, color: 'teal' })}
        ${renderKPICard({ icon: '$', value: formatCurrency(insights.avg_spend_per_buyer), label: 'Gasto/Buyer', change: null, color: 'violet' })}
      </div>

      <div class="charts-grid">
        <div class="chart-card">
          <div class="chart-header"><div><div class="chart-title">Segmentación por Frecuencia</div></div></div>
          <div class="chart-container"><canvas id="segmentsChart"></canvas></div>
        </div>
        <div class="chart-card">
          <div class="chart-header"><div><div class="chart-title">Valor por Segmento</div></div></div>
          <table class="data-table" style="margin-top: 16px;">
            <thead><tr><th>Segmento</th><th>Buyers</th><th>Gasto Total</th><th>Gasto Prom</th></tr></thead>
            <tbody>
              ${segments.map((s, i) => `
                <tr>
                  <td>
                    <div style="display: flex; align-items: center; gap: 10px;">
                      <div style="width: 12px; height: 12px; border-radius: 3px; background: ${colors[i % colors.length]};"></div>
                      <span style="font-weight: 600;">${s.segment}</span>
                    </div>
                  </td>
                  <td>${formatNumber(s.buyer_count)}</td>
                  <td style="color: var(--glow-amber);">${formatCurrency(s.segment_spend)}</td>
                  <td>${formatCurrency(s.avg_spend)}</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      </div>
    `;

    if (segments.length > 0) {
      new Chart(document.getElementById('segmentsChart').getContext('2d'), {
        type: 'doughnut',
        data: {
          labels: segments.map(s => s.segment),
          datasets: [{ data: segments.map(s => s.buyer_count), backgroundColor: colors, borderWidth: 0, spacing: 2 }]
        },
        options: {
          responsive: true, maintainAspectRatio: false, cutout: '65%',
          plugins: { legend: { position: 'right', labels: { color: '#9898a8', padding: 20 } } }
        }
      });
    }

  } catch (err) {
    main.innerHTML = `<div class="empty-state">Error: ${err.message}</div>`;
  }
}

// =============================================================================
// VIEW: HEALTH
// =============================================================================

async function renderHealth() {
  const main = document.getElementById('main-content');
  main.innerHTML = '<div class="loading"><div class="spinner"></div><div>Cargando Data Health...</div></div>';

  try {
    const dq = await fetchAPI('/api/dq');

    main.innerHTML = `
      <div class="header">
        <div>
          <h1>🏥 Data Health Gate</h1>
          <p>Métricas de calidad y reconciliación</p>
        </div>
        <div style="text-align: right;">
          <div style="font-size: 11px; color: var(--text-muted);">Última actualización</div>
          <div style="font-size: 13px; color: var(--text-secondary);">${new Date(dq.as_of_ts || Date.now()).toLocaleString()}</div>
        </div>
      </div>

      <div class="dq-grid" style="grid-template-columns: repeat(5, 1fr);">
        <div class="dq-card">
          <span class="dq-status ${getDqStatus(dq.dq_reconcile_ok_pct, 90)}"></span>
          <div class="dq-value" style="color: var(--glow-emerald);">${formatPercent(dq.dq_reconcile_ok_pct)}</div>
          <div class="dq-label">Reconciliación OK</div>
        </div>
        <div class="dq-card">
          <span class="dq-status ${getDqStatus(dq.dq_issuer_match_pct, 90)}"></span>
          <div class="dq-value" style="color: var(--glow-cyan);">${formatPercent(dq.dq_issuer_match_pct)}</div>
          <div class="dq-label">Issuer Match</div>
        </div>
        <div class="dq-card">
          <span class="dq-status ${getDqStatus(dq.dq_product_enrichment_match_pct, 80)}"></span>
          <div class="dq-value" style="color: var(--glow-amber);">${formatPercent(dq.dq_product_enrichment_match_pct)}</div>
          <div class="dq-label">Product Match</div>
        </div>
        <div class="dq-card">
          <span class="dq-status ${getDqStatus(dq.dq_header_to_detail_coverage_pct, 95)}"></span>
          <div class="dq-value" style="color: var(--glow-violet);">${formatPercent(dq.dq_header_to_detail_coverage_pct)}</div>
          <div class="dq-label">Header→Detail</div>
        </div>
        <div class="dq-card">
          <span class="dq-status ${getDqStatus(100 - (dq.dq_numeric_cast_fail_pct || 0), 99)}"></span>
          <div class="dq-value" style="color: var(--glow-rose);">${formatPercent(dq.dq_numeric_cast_fail_pct)}</div>
          <div class="dq-label">Cast Failures</div>
        </div>
      </div>

      <div class="charts-grid">
        <div class="chart-card">
          <div class="chart-header"><div><div class="chart-title">Volumen de Datos</div></div></div>
          <div style="padding: 30px 0; display: flex; gap: 60px;">
            <div>
              <div style="font-size: 48px; font-weight: 800; color: var(--glow-amber);">${formatNumber(dq.dq_header_rows)}</div>
              <div style="font-size: 14px; color: var(--text-secondary); margin-top: 8px;">Invoice Headers</div>
            </div>
            <div>
              <div style="font-size: 48px; font-weight: 800; color: var(--glow-teal);">${formatNumber(dq.dq_detail_rows)}</div>
              <div style="font-size: 14px; color: var(--text-secondary); margin-top: 8px;">Invoice Details</div>
            </div>
            <div>
              <div style="font-size: 48px; font-weight: 800; color: var(--glow-violet);">${((dq.dq_detail_rows || 0) / (dq.dq_header_rows || 1)).toFixed(1)}</div>
              <div style="font-size: 14px; color: var(--text-secondary); margin-top: 8px;">Líneas/Factura</div>
            </div>
          </div>
        </div>

        <div class="chart-card">
          <div class="chart-header"><div><div class="chart-title">Trust Level Guide</div></div></div>
          <div style="padding: 16px 0;">
            <div style="margin-bottom: 16px; padding: 14px; background: var(--glow-emerald-dim); border-radius: 10px; border-left: 3px solid var(--glow-emerald);">
              <div style="font-weight: 700; color: var(--glow-emerald);">● HIGH</div>
              <div style="font-size: 12px; color: var(--text-secondary);">users≥10 AND coverage≥80% AND reconcile≥90%</div>
            </div>
            <div style="margin-bottom: 16px; padding: 14px; background: var(--glow-amber-dim); border-radius: 10px; border-left: 3px solid var(--glow-amber);">
              <div style="font-weight: 700; color: var(--glow-amber);">◐ MEDIUM</div>
              <div style="font-size: 12px; color: var(--text-secondary);">users≥5 AND coverage≥60%</div>
            </div>
            <div style="padding: 14px; background: var(--glow-rose-dim); border-radius: 10px; border-left: 3px solid var(--glow-rose);">
              <div style="font-weight: 700; color: var(--glow-rose);">○ LOW</div>
              <div style="font-size: 12px; color: var(--text-secondary);">No cumple criterios anteriores</div>
            </div>
          </div>
        </div>
      </div>
    `;

  } catch (err) {
    main.innerHTML = `<div class="empty-state">Error: ${err.message}</div>`;
  }
}

// =============================================================================
// VIEW: REPORT PACK
// =============================================================================

async function renderReportPack() {
  const main = document.getElementById('main-content');
  main.innerHTML = '<div class="loading"><div class="spinner"></div><div>Generando Report Pack...</div></div>';

  try {
    const report = await fetchAPI('/api/report-pack', currentFilters);
    const { data_quality: dq, summary, coverage, kpis_daily, sow, top_products, trust_level } = report;

    main.innerHTML = `
      <div style="max-width: 1100px; margin: 0 auto; background: var(--abyss); border-radius: 24px; padding: 32px; border: 1px solid var(--border-subtle);">
        
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 32px; padding-bottom: 24px; border-bottom: 2px solid var(--glow-amber);">
          <div style="display: flex; align-items: center; gap: 16px;">
            <div style="width: 56px; height: 56px; background: var(--gradient-amber); border-radius: 16px; display: flex; align-items: center; justify-content: center; font-weight: 900; font-size: 26px; color: var(--void);">L</div>
            <div>
              <h1 style="font-size: 28px; font-weight: 800; margin: 0;">Radiance Report</h1>
              <p style="color: var(--text-secondary); font-size: 14px; margin: 4px 0 0 0;">LÜM Analytics · Panel Observado</p>
            </div>
          </div>
          <div style="text-align: right;">
            <div style="font-size: 12px; color: var(--text-muted);">${currentFilters.start} → ${currentFilters.end}</div>
            <div style="font-size: 12px; color: var(--text-muted); margin-bottom: 8px;">${new Date().toLocaleString()}</div>
            ${getTrustBadge(trust_level)}
          </div>
        </div>

        <div style="display: grid; grid-template-columns: repeat(5, 1fr); gap: 16px; margin-bottom: 32px;">
          <div style="background: var(--depth); padding: 20px; border-radius: 16px; border-left: 4px solid var(--glow-amber);">
            <div style="font-size: 28px; font-weight: 800;">${formatCurrency(summary.gross_sales)}</div>
            <div style="font-size: 12px; color: var(--text-secondary); margin-top: 4px;">Ventas</div>
          </div>
          <div style="background: var(--depth); padding: 20px; border-radius: 16px; border-left: 4px solid var(--glow-teal);">
            <div style="font-size: 28px; font-weight: 800;">${formatNumber(summary.receipts)}</div>
            <div style="font-size: 12px; color: var(--text-secondary); margin-top: 4px;">Recibos</div>
          </div>
          <div style="background: var(--depth); padding: 20px; border-radius: 16px; border-left: 4px solid var(--glow-violet);">
            <div style="font-size: 28px; font-weight: 800;">${formatNumber(summary.buyers)}</div>
            <div style="font-size: 12px; color: var(--text-secondary); margin-top: 4px;">Buyers</div>
          </div>
          <div style="background: var(--depth); padding: 20px; border-radius: 16px; border-left: 4px solid var(--glow-cyan);">
            <div style="font-size: 28px; font-weight: 800;">${formatCurrency(summary.aov)}</div>
            <div style="font-size: 12px; color: var(--text-secondary); margin-top: 4px;">Ticket</div>
          </div>
          <div style="background: var(--depth); padding: 20px; border-radius: 16px; border-left: 4px solid var(--glow-emerald);">
            <div style="font-size: 28px; font-weight: 800;">${(summary.frequency || 0).toFixed(1)}x</div>
            <div style="font-size: 12px; color: var(--text-secondary); margin-top: 4px;">Frecuencia</div>
          </div>
        </div>

        <div style="display: grid; grid-template-columns: 2fr 1fr; gap: 20px; margin-bottom: 32px;">
          <div style="background: var(--depth); padding: 24px; border-radius: 16px;">
            <div style="font-size: 16px; font-weight: 700; margin-bottom: 16px;">📈 Tendencia</div>
            <div style="height: 180px;"><canvas id="reportSalesChart"></canvas></div>
          </div>
          <div style="background: var(--depth); padding: 24px; border-radius: 16px;">
            <div style="font-size: 16px; font-weight: 700; margin-bottom: 16px;">🏥 Data Quality</div>
            <div style="display: flex; flex-direction: column; gap: 14px;">
              ${[
                { label: 'Reconcile', value: dq.dq_reconcile_ok_pct, color: 'emerald' },
                { label: 'Product Match', value: dq.dq_product_enrichment_match_pct, color: 'amber' },
                { label: 'Coverage', value: coverage.category_coverage_pct, color: 'teal' }
              ].map(m => `
                <div>
                  <div style="display: flex; justify-content: space-between; font-size: 12px; margin-bottom: 6px;">
                    <span style="color: var(--text-secondary);">${m.label}</span>
                    <span style="font-weight: 700;">${formatPercent(m.value)}</span>
                  </div>
                  <div class="progress-bar"><div class="progress-fill ${m.color}" style="width: ${m.value || 0}%;"></div></div>
                </div>
              `).join('')}
            </div>
          </div>
        </div>

        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
          <div style="background: var(--depth); padding: 24px; border-radius: 16px;">
            <div style="font-size: 16px; font-weight: 700; margin-bottom: 16px;">🥧 Top SoW</div>
            <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
              <thead>
                <tr style="border-bottom: 1px solid var(--border-subtle);">
                  <th style="text-align: left; padding: 10px 8px; color: var(--text-muted); font-size: 10px;">Categoría</th>
                  <th style="text-align: left; padding: 10px 8px; color: var(--text-muted); font-size: 10px;">Marca</th>
                  <th style="text-align: right; padding: 10px 8px; color: var(--text-muted); font-size: 10px;">SoW</th>
                </tr>
              </thead>
              <tbody>
                ${(sow || []).slice(0, 7).map(s => `
                  <tr style="border-bottom: 1px solid var(--border-subtle);">
                    <td style="padding: 10px 8px; color: var(--text-secondary);">${(s.category || '-').substring(0, 18)}</td>
                    <td style="padding: 10px 8px; font-weight: 600;">${(s.brand || '-').substring(0, 15)}</td>
                    <td style="padding: 10px 8px; text-align: right; color: var(--glow-amber); font-weight: 700;">${formatPercent(s.sow_pct)}</td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
          <div style="background: var(--depth); padding: 24px; border-radius: 16px;">
            <div style="font-size: 16px; font-weight: 700; margin-bottom: 16px;">🏷️ Top Productos</div>
            <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
              <thead>
                <tr style="border-bottom: 1px solid var(--border-subtle);">
                  <th style="text-align: left; padding: 10px 8px; color: var(--text-muted); font-size: 10px;">Producto</th>
                  <th style="text-align: right; padding: 10px 8px; color: var(--text-muted); font-size: 10px;">Ventas</th>
                </tr>
              </thead>
              <tbody>
                ${(top_products || []).slice(0, 7).map(p => `
                  <tr style="border-bottom: 1px solid var(--border-subtle);">
                    <td style="padding: 10px 8px;">${(p.product_description || '-').substring(0, 32)}</td>
                    <td style="padding: 10px 8px; text-align: right; color: var(--glow-amber); font-weight: 700;">${formatCurrency(p.total_sales)}</td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
        </div>

        <div style="margin-top: 32px; padding-top: 20px; border-top: 1px solid var(--border-subtle); display: flex; justify-content: space-between; font-size: 11px; color: var(--text-muted);">
          <div>LÜM Radiance · Panel Observado · Datos no extrapolados</div>
          <div>Headers: ${formatNumber(dq.dq_header_rows)} · Details: ${formatNumber(dq.dq_detail_rows)}</div>
        </div>
      </div>
    `;

    // Mini chart
    if (kpis_daily && kpis_daily.length > 0) {
      new Chart(document.getElementById('reportSalesChart').getContext('2d'), {
        type: 'line',
        data: {
          labels: kpis_daily.map(d => d.invoice_date),
          datasets: [{
            data: kpis_daily.map(d => d.gross_sales),
            borderColor: '#ffb020',
            backgroundColor: 'rgba(255, 176, 32, 0.1)',
            fill: true, tension: 0.4, pointRadius: 0, borderWidth: 2
          }]
        },
        options: {
          responsive: true, maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: { x: { display: false }, y: { display: false } }
        }
      });
    }

  } catch (err) {
    main.innerHTML = `<div class="empty-state">Error: ${err.message}</div>`;
  }
}

// =============================================================================
// NAVIGATION
// =============================================================================

function applyFilters() {
  currentFilters.start = document.getElementById('filter-start')?.value || currentFilters.start;
  currentFilters.end = document.getElementById('filter-end')?.value || currentFilters.end;
  currentFilters.issuer_ruc = document.getElementById('filter-issuer')?.value || null;
  currentFilters.category = document.getElementById('filter-category')?.value || null;
  currentFilters.reconcile_ok = document.getElementById('filter-reconcile')?.value || 'true';
  
  console.log('Applying filters:', currentFilters);
  renderView(currentView);
}

function renderView(view) {
  currentView = view;
  document.querySelectorAll('.nav-item').forEach(item => {
    item.classList.toggle('active', item.dataset.view === view);
  });

  switch (view) {
    case 'overview': renderOverview(); break;
    case 'performance': renderPerformance(); break;
    case 'sow': renderSoW(); break;
    case 'products': renderProducts(); break;
    case 'buyers': renderBuyers(); break;
    case 'health': renderHealth(); break;
    case 'report': renderReportPack(); break;
    default: renderOverview();
  }
}

// Navigation click handlers
document.querySelectorAll('.nav-item').forEach(item => {
  item.addEventListener('click', () => renderView(item.dataset.view));
});

// URL params
function parseURLParams() {
  const params = new URLSearchParams(window.location.search);
  if (params.get('start')) currentFilters.start = params.get('start');
  if (params.get('end')) currentFilters.end = params.get('end');
  if (params.get('issuer_ruc')) currentFilters.issuer_ruc = params.get('issuer_ruc');
  if (params.get('category')) currentFilters.category = params.get('category');
  if (params.get('view')) currentView = params.get('view');
}

// Initialize
parseURLParams();
renderView(currentView);
