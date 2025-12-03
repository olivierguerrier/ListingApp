const API_BASE = '/api';

// State management
let items = [];
let skus = [];
let currentItem = null;
let currentView = 'products'; // 'products', 'skus', or 'database'
let currentTable = 'products';

// Pagination state
let productsPage = 1;
let productsLimit = 50;
let productsTotalPages = 1;

let skusPage = 1;
let skusLimit = 50;
let skusTotalPages = 1;

// Initialize app
document.addEventListener('DOMContentLoaded', () => {
    loadItems();
    loadSkus();
    loadCountries();
    setupEventListeners();
});

// Event Listeners
function setupEventListeners() {
    // View Tab Switching
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const view = btn.dataset.view;
            switchView(view);
        });
    });

    // Database Table Switching
    document.querySelectorAll('.table-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const table = btn.dataset.table;
            loadDatabaseTable(table);
            
            // Update active state
            document.querySelectorAll('.table-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
        });
    });

    // Add Item Button
    document.getElementById('addItemBtn').addEventListener('click', () => {
        openItemModal();
    });

    // Export Button
    document.getElementById('exportBtn').addEventListener('click', openExportModal);

    // Sync PIM Button
    document.getElementById('syncPimBtn').addEventListener('click', syncPimData);

    // Import QPI Button
    document.getElementById('importQpiBtn').addEventListener('click', importQpiData);

    // Sync QPI Button
    document.getElementById('syncQpiBtn').addEventListener('click', syncQpiData);

    // Sync VC Button
    document.getElementById('syncVcBtn').addEventListener('click', syncVcData);

    // Item Form Submit
    document.getElementById('itemForm').addEventListener('submit', handleItemSubmit);

    // Pricing Form Submit
    document.getElementById('pricingForm').addEventListener('submit', handlePricingSubmit);

    // Auto-fill currency based on country selection
    document.getElementById('countryCode').addEventListener('change', function() {
        const selectedOption = this.options[this.selectedIndex];
        const currency = selectedOption.getAttribute('data-currency');
        if (currency) {
            document.getElementById('currency').value = currency;
            document.getElementById('retailCurrencyHint').textContent = `In ${currency}`;
        }
    });

    // Search and Filter
    document.getElementById('searchInput').addEventListener('input', filterItems);
    document.getElementById('statusFilter').addEventListener('change', filterItems);
    document.getElementById('countryFilter').addEventListener('change', filterItems);

    // Modal close buttons
    document.querySelectorAll('.close').forEach(closeBtn => {
        closeBtn.addEventListener('click', (e) => {
            e.target.closest('.modal').style.display = 'none';
        });
    });

    // Cancel button
    document.getElementById('cancelBtn').addEventListener('click', () => {
        document.getElementById('itemModal').style.display = 'none';
    });

    // Close modal on outside click
    window.addEventListener('click', (e) => {
        if (e.target.classList.contains('modal')) {
            e.target.style.display = 'none';
        }
    });

    // Flow stage checkboxes
    document.querySelectorAll('.stage-check').forEach(checkbox => {
        checkbox.addEventListener('change', handleStageChange);
    });
}

// Load all items
async function loadItems(page = 1) {
    try {
        productsPage = page;
        const offset = (page - 1) * productsLimit;
        const response = await fetch(`${API_BASE}/items?limit=${productsLimit}&offset=${offset}`);
        const result = await response.json();
        // Handle both paginated and non-paginated responses
        items = result.data || result;
        
        // Update pagination info
        if (result.pagination) {
            productsTotalPages = result.pagination.total_pages;
            updatePaginationControls('products', page, productsTotalPages);
        }
        
        if (currentView === 'products') {
            renderItems(items);
        }
    } catch (error) {
        console.error('Error loading items:', error);
        showError('Failed to load items');
    }
}

// Load SKUs
async function loadSkus(page = 1) {
    try {
        skusPage = page;
        const offset = (page - 1) * skusLimit;
        const response = await fetch(`${API_BASE}/skus?limit=${skusLimit}&offset=${offset}`);
        const result = await response.json();
        // Handle both paginated and non-paginated responses
        skus = result.data || result;
        
        // Update pagination info
        if (result.pagination) {
            skusTotalPages = result.pagination.total_pages;
            updatePaginationControls('skus', page, skusTotalPages);
        }
        
        if (currentView === 'skus') {
            renderSkus(skus);
        }
    } catch (error) {
        console.error('Error loading SKUs:', error);
        showError('Failed to load SKUs');
    }
}

// Load available countries from ASIN status
async function loadCountries() {
    try {
        const response = await fetch(`${API_BASE}/asin-status/summary/all?limit=10000`);
        const result = await response.json();
        const data = result.data || result;
        
        // Extract unique countries from country_status field
        const countriesSet = new Set();
        data.forEach(item => {
            if (item.country_status) {
                // Parse country_status like "Canada:DISCOVERABLE,United States:DISCOVERABLE"
                const countries = item.country_status.split(',');
                countries.forEach(countryStatus => {
                    const country = countryStatus.split(':')[0];
                    if (country) {
                        countriesSet.add(country.trim());
                    }
                });
            }
        });
        
        // Sort countries alphabetically
        const countries = Array.from(countriesSet).sort();
        
        // Populate the country filter dropdown
        const countryFilter = document.getElementById('countryFilter');
        countryFilter.innerHTML = '<option value="all">All Countries</option>';
        
        countries.forEach(country => {
            const option = document.createElement('option');
            option.value = country;
            option.textContent = country;
            countryFilter.appendChild(option);
        });
        
    } catch (error) {
        console.error('Error loading countries:', error);
        const countryFilter = document.getElementById('countryFilter');
        countryFilter.innerHTML = '<option value="all">All Countries (Error loading)</option>';
    }
}

// Switch between views
function switchView(view) {
    currentView = view;
    
    // Update tab buttons
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.view === view);
    });
    
    // Update view containers
    document.getElementById('productsView').classList.toggle('active', view === 'products');
    document.getElementById('skusView').classList.toggle('active', view === 'skus');
    document.getElementById('databaseView').classList.toggle('active', view === 'database');
    document.getElementById('productsView').style.display = view === 'products' ? 'block' : 'none';
    document.getElementById('skusView').style.display = view === 'skus' ? 'block' : 'none';
    document.getElementById('databaseView').style.display = view === 'database' ? 'block' : 'none';
    
    // Hide filters for database view
    document.querySelector('.filters').style.display = view === 'database' ? 'none' : 'flex';
    
    // Render appropriate view
    if (view === 'products') {
        renderItems(items);
    } else if (view === 'skus') {
        renderSkus(skus);
    } else if (view === 'database') {
        loadDatabaseTable(currentTable);
    }
}

// Load database table
async function loadDatabaseTable(tableName) {
    currentTable = tableName;
    
    try {
        const response = await fetch(`${API_BASE}/database/${tableName}?limit=100`);
        const data = await response.json();
        
        renderDatabaseTable(data);
    } catch (error) {
        console.error('Error loading database table:', error);
        showError('Failed to load table data');
    }
}

// Render database table
function renderDatabaseTable(data) {
    document.getElementById('dbTableName').textContent = data.table;
    document.getElementById('dbRowCount').textContent = `${data.rows.length} of ${data.total} rows`;
    
    const thead = document.getElementById('dbTableHead');
    const tbody = document.getElementById('dbTableBody');
    
    if (data.rows.length === 0) {
        thead.innerHTML = '';
        tbody.innerHTML = '<tr><td colspan="100" style="text-align: center; padding: 40px; color: var(--text-secondary);">No data in this table</td></tr>';
        return;
    }
    
    // Render table headers
    const columns = data.columns.map(col => col.name);
    thead.innerHTML = `
        <tr>
            ${columns.map(col => `<th>${escapeHtml(col)}</th>`).join('')}
        </tr>
    `;
    
    // Render table rows
    tbody.innerHTML = data.rows.map(row => `
        <tr>
            ${columns.map(col => {
                let value = row[col];
                if (value === null) {
                    return '<td style="color: #999; font-style: italic;">null</td>';
                }
                if (typeof value === 'boolean') {
                    return `<td>${value ? '✓' : ''}</td>`;
                }
                if (col.includes('_at') && value) {
                    // Format timestamps
                    try {
                        const date = new Date(value);
                        return `<td><small>${date.toLocaleString()}</small></td>`;
                    } catch (e) {
                        return `<td>${escapeHtml(String(value))}</td>`;
                    }
                }
                return `<td>${escapeHtml(String(value))}</td>`;
            }).join('')}
        </tr>
    `).join('');
}

// Render items
function renderItems(itemsToRender) {
    const tbody = document.getElementById('itemsTableBody');
    
    if (itemsToRender.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="11" style="text-align: center; padding: 40px; color: var(--text-secondary);">
                    <h3>No products found</h3>
                    <p>Click "Add New Item" to get started</p>
                </td>
            </tr>
        `;
        return;
    }

    tbody.innerHTML = itemsToRender.map(item => {
        // Handle SKUs
        const skus = item.skus || [];
        const displaySku = skus.length > 0 ? skus.join(', ') : '-';
        const isTempAsin = item.is_temp_asin === 1;
        
        return `
            <tr data-item-id="${item.id}">
                <td class="item-name" title="${escapeHtml(item.name || item.asin)}">
                    ${escapeHtml(item.name || item.asin)}
                    ${isTempAsin ? '<span class="temp-badge">TEMP</span>' : ''}
                </td>
                <td class="item-asin">${escapeHtml(item.asin)}</td>
                <td class="item-skus" title="${escapeHtml(displaySku)}">${escapeHtml(displaySku)}</td>
                <td><span class="stage-badge ${item.stage_1_idea_considered ? 'completed' : 'pending'}" onclick="openWorkflowModal('${item.asin}', 1)" title="Stage 1: Ideation">${item.stage_1_idea_considered ? '✓' : '○'}</span></td>
                <td><span class="stage-badge ${item.stage_2_product_finalized ? 'completed' : 'pending'}" onclick="openWorkflowModal('${item.asin}', 2)" title="Stage 2: PIM Finalized">${item.stage_2_product_finalized ? '✓' : '○'}</span></td>
                <td><span class="stage-badge ${item.stage_3a_pricing_submitted ? 'completed' : 'pending'}" onclick="openWorkflowModal('${item.asin}', 3)" title="Stage 3a: Pricing Submitted">${item.stage_3a_pricing_submitted ? '✓' : '○'}</span></td>
                <td><span class="stage-badge ${item.stage_3b_pricing_approved ? 'completed' : 'pending'}" onclick="openWorkflowModal('${item.asin}', 4)" title="Stage 3b: Pricing Approved">${item.stage_3b_pricing_approved ? '✓' : '○'}</span></td>
                <td><span class="stage-badge ${item.stage_4_product_listed ? 'completed' : 'pending'}" onclick="openWorkflowModal('${item.asin}', 5)" title="Stage 4: VC Listed">${item.stage_4_product_listed ? '✓' : '○'}</span></td>
                <td><span class="stage-badge ${item.stage_5_product_ordered ? 'completed' : 'pending'}" onclick="openWorkflowModal('${item.asin}', 6)" title="Stage 5: Ordered">${item.stage_5_product_ordered ? '✓' : '○'}</span></td>
                <td><span class="stage-badge ${item.stage_6_product_online ? 'completed' : 'pending'}" onclick="openWorkflowModal('${item.asin}', 7)" title="Stage 6: Online">${item.stage_6_product_online ? '✓' : '○'}</span></td>
                <td>
                    <div class="action-btns">
                        <button class="btn-icon" onclick="editItem('${item.asin}')" title="Edit">✏️</button>
                        <button class="btn-icon" onclick="deleteItem('${item.asin}')" title="Delete">🗑️</button>
                    </div>
                </td>
            </tr>
        `;
    }).join('');
}

function getCurrentStage(item) {
    // Determine the highest completed stage
    if (item.stage_6_product_online) {
        return { stage: 6, name: 'Available Online', badge: '<span class="status-badge" style="background-color: #10b981;">Stage 6: Online</span>' };
    } else if (item.stage_5_product_ordered) {
        return { stage: 5, name: 'Ordered [In QPI]', badge: '<span class="status-badge" style="background-color: #3b82f6;">Stage 5: Ordered</span>' };
    } else if (item.stage_4_product_listed) {
        return { stage: 4, name: 'VC Listed', badge: '<span class="status-badge" style="background-color: #8b5cf6;">Stage 4: VC Listed</span>' };
    } else if (item.stage_3b_pricing_approved) {
        return { stage: 3, name: 'Pricing Approved', badge: '<span class="status-badge approved">Stage 3b: Approved</span>' };
    } else if (item.stage_3a_pricing_submitted) {
        return { stage: 3, name: 'Pricing Submitted', badge: '<span class="status-badge pending">Stage 3a: Submitted</span>' };
    } else if (item.stage_2_product_finalized) {
        const newBadge = item.stage_2_newly_finalized ? ' <span style="background-color: #ef4444; color: white; padding: 2px 6px; border-radius: 4px; font-size: 10px; margin-left: 4px;">NEW</span>' : '';
        return { stage: 2, name: 'PIM Finalized', badge: '<span class="status-badge" style="background-color: #f59e0b;">Stage 2: Finalized' + newBadge + '</span>' };
    } else if (item.stage_1_idea_considered) {
        return { stage: 1, name: 'Ideation', badge: '<span class="status-badge" style="background-color: #6b7280;">Stage 1: Ideation</span>' };
    } else {
        return { stage: 0, name: 'Not Started', badge: '<span class="status-badge" style="background-color: #999;">Not Started</span>' };
    }
}

function renderStageDisplay(number, label, completed, isOptional = false, itemId = null, isNewlyFinalized = false, isPricingStage = false) {
    const clickHandler = isPricingStage && itemId ? `onclick="openPricingModal(${itemId})" style="cursor: pointer;"` : '';
    const newBadge = isNewlyFinalized ? '<span style="background-color: #ef4444; color: white; padding: 2px 4px; border-radius: 3px; font-size: 9px; margin-left: 3px;">NEW</span>' : '';
    
    return `
        <div class="stage-display ${completed ? 'completed' : ''} ${isPricingStage ? 'pricing-stage' : ''}" ${clickHandler}>
            <div class="stage-icon">${completed ? '✓' : number}</div>
            <div class="stage-label">
                ${label}${newBadge}
                ${isOptional ? '<br><span style="font-size: 10px;">(Optional)</span>' : ''}
            </div>
        </div>
    `;
}

// Render SKUs view
function renderSkus(skusToRender) {
    const container = document.getElementById('skusContainer');
    
    if (skusToRender.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <h3>No SKUs found</h3>
                <p>SKUs will appear here once data is synced</p>
            </div>
        `;
        return;
    }

    // Group SKUs by ASIN
    const skusByAsin = {};
    skusToRender.forEach(sku => {
        if (!skusByAsin[sku.asin]) {
            skusByAsin[sku.asin] = {
                asin: sku.asin,
                product_name: sku.product_name,
                is_temp_asin: sku.is_temp_asin,
                skus: []
            };
        }
        skusByAsin[sku.asin].skus.push(sku);
    });

    // Render grouped by ASIN
    container.innerHTML = Object.values(skusByAsin).map(group => {
        const isTempAsin = group.is_temp_asin === 1;
        const hasMultipleSkus = group.skus.length > 1;
        const hasInconsistencies = hasMultipleSkus && checkForInconsistencies(group.skus);
        
        return `
            <div class="item-card ${hasInconsistencies ? 'has-inconsistency' : ''}">
                <div class="item-header">
                    <div class="item-info">
                        <h3>
                            ASIN: ${escapeHtml(group.asin)}
                            ${isTempAsin ? '<span style="background-color: #f59e0b; color: white; padding: 2px 6px; border-radius: 4px; font-size: 11px; margin-left: 8px;">TEMP</span>' : ''}
                            ${hasInconsistencies ? '<span style="background-color: #ef4444; color: white; padding: 2px 6px; border-radius: 4px; font-size: 11px; margin-left: 8px;">⚠ INCONSISTENT</span>' : ''}
                        </h3>
                        <div class="item-sku">
                            Product: ${escapeHtml(group.product_name || 'N/A')}
                        </div>
                        <div class="item-sku-list">
                            <small style="color: var(--text-secondary);">${group.skus.length} SKU${group.skus.length > 1 ? 's' : ''}</small>
                        </div>
                    </div>
                </div>

                <div class="skus-table-container">
                    <table class="skus-table">
                        <thead>
                            <tr>
                                <th>SKU / Item Number</th>
                                <th>Source</th>
                                <th>Primary</th>
                                <th>Country</th>
                                <th>Status</th>
                                <th>Added</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${group.skus.map(sku => {
                                const currentStage = getCurrentStageForSku(sku);
                                return `
                                    <tr>
                                        <td><strong>${escapeHtml(sku.sku)}</strong></td>
                                        <td><span class="source-badge">${sku.source || 'Unknown'}</span></td>
                                        <td>${sku.is_primary ? '✓' : ''}</td>
                                        <td>${sku.stage_1_country || '-'}</td>
                                        <td>${currentStage.badge}</td>
                                        <td><small>${new Date(sku.created_at).toLocaleDateString()}</small></td>
                                    </tr>
                                `;
                            }).join('')}
                        </tbody>
                    </table>
                </div>
            </div>
        `;
    }).join('');
}

// Check for inconsistencies in SKUs under same ASIN
function checkForInconsistencies(skus) {
    if (skus.length <= 1) return false;
    
    // Check if different SKUs have different countries or sources
    const countries = new Set(skus.map(s => s.stage_1_country).filter(c => c));
    const sources = new Set(skus.map(s => s.source).filter(s => s));
    
    return countries.size > 1 || sources.size > 1;
}

// Get current stage for a SKU
function getCurrentStageForSku(sku) {
    if (sku.stage_6_product_online) {
        return { stage: 6, badge: '<span class="status-badge" style="background-color: #10b981; font-size: 11px;">Stage 6</span>' };
    } else if (sku.stage_5_product_ordered) {
        return { stage: 5, badge: '<span class="status-badge" style="background-color: #3b82f6; font-size: 11px;">Stage 5</span>' };
    } else if (sku.stage_4_product_listed) {
        return { stage: 4, badge: '<span class="status-badge" style="background-color: #8b5cf6; font-size: 11px;">Stage 4</span>' };
    } else if (sku.stage_3b_pricing_approved) {
        return { stage: 3, badge: '<span class="status-badge approved" style="font-size: 11px;">Stage 3b</span>' };
    } else if (sku.stage_3a_pricing_submitted) {
        return { stage: 3, badge: '<span class="status-badge pending" style="font-size: 11px;">Stage 3a</span>' };
    } else if (sku.stage_2_product_finalized) {
        return { stage: 2, badge: '<span class="status-badge" style="background-color: #f59e0b; font-size: 11px;">Stage 2</span>' };
    } else {
        return { stage: 0, badge: '<span class="status-badge" style="background-color: #999; font-size: 11px;">-</span>' };
    }
}

// Filter items
async function filterItems() {
    const searchTerm = document.getElementById('searchInput').value.toLowerCase();
    const statusFilter = document.getElementById('statusFilter').value;
    const countryFilter = document.getElementById('countryFilter').value;

    // If filtering by country, we need to fetch ASIN status data
    let asinsByCountry = new Set();
    if (countryFilter !== 'all') {
        try {
            const response = await fetch(`${API_BASE}/asin-status?country=${encodeURIComponent(countryFilter)}&limit=10000`);
            const result = await response.json();
            const data = result.data || result;
            data.forEach(item => {
                if (item.asin) {
                    asinsByCountry.add(item.asin);
                }
            });
        } catch (error) {
            console.error('Error fetching country filter data:', error);
        }
    }

    const filtered = items.filter(item => {
        const matchesSearch = item.name.toLowerCase().includes(searchTerm) || 
                            item.sku.toLowerCase().includes(searchTerm) ||
                            item.asin.toLowerCase().includes(searchTerm);
        
        if (!matchesSearch) return false;

        // Filter by country
        if (countryFilter !== 'all') {
            if (!asinsByCountry.has(item.asin)) {
                return false;
            }
        }

        // Filter by stage
        if (statusFilter === 'all') return true;

        if (statusFilter === 'stage_1') {
            return item.stage_1_idea_considered === 1;
        } else if (statusFilter === 'stage_2') {
            return item.stage_2_product_finalized === 1;
        } else if (statusFilter === 'stage_3a') {
            return item.stage_3a_pricing_submitted === 1;
        } else if (statusFilter === 'stage_3b') {
            return item.stage_3b_pricing_approved === 1;
        } else if (statusFilter === 'stage_4') {
            return item.stage_4_product_listed === 1;
        } else if (statusFilter === 'stage_5') {
            return item.stage_5_product_ordered === 1;
        } else if (statusFilter === 'stage_6') {
            return item.stage_6_product_online === 1;
        }

        return true;
    });

    renderItems(filtered);
}

// Modal functions
function openItemModal(itemId = null) {
    const modal = document.getElementById('itemModal');
    const form = document.getElementById('itemForm');
    const title = document.getElementById('modalTitle');
    const advancedFields = document.getElementById('advancedFields');
    const toggleBtn = document.getElementById('toggleAdvancedBtn');

    form.reset();

    if (itemId) {
        // Edit mode - show all fields
        title.textContent = 'Edit Product';
        advancedFields.style.display = 'block';
        toggleBtn.style.display = 'none';
        
        const item = items.find(i => i.asin === itemId || i.id === itemId);
        if (item) {
            document.getElementById('itemId').value = item.asin; // Use ASIN as identifier
            document.getElementById('sku').value = item.skus && item.skus.length > 0 ? item.skus[0] : '';
            document.getElementById('sku').disabled = true; // Don't allow SKU changes
            document.getElementById('country').value = item.stage_1_country || '';
            document.getElementById('itemNumber').value = '';
            document.getElementById('brand').value = item.stage_1_brand || item.brand || '';
            document.getElementById('description').value = item.stage_1_description || item.product_description || '';
            document.getElementById('seasonLaunch').value = item.stage_1_season_launch || '';
            document.getElementById('asin').value = item.asin || '';
            document.getElementById('name').value = item.name;
        }
    } else {
        // Add mode - simple fields only
        title.textContent = 'Add New Product - Ideation Stage';
        advancedFields.style.display = 'none';
        toggleBtn.style.display = 'inline-block';
        toggleBtn.textContent = 'Show Advanced Fields';
        document.getElementById('sku').disabled = false;
    }

    modal.style.display = 'block';
}

// Toggle advanced fields
document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('toggleAdvancedBtn')?.addEventListener('click', function() {
        const advancedFields = document.getElementById('advancedFields');
        if (advancedFields.style.display === 'none') {
            advancedFields.style.display = 'block';
            this.textContent = 'Hide Advanced Fields';
        } else {
            advancedFields.style.display = 'none';
            this.textContent = 'Show Advanced Fields';
        }
    });
});

async function openPricingModal(itemId) {
    const item = items.find(i => i.asin === itemId || i.id === itemId);
    if (!item) return;

    document.getElementById('pricingItemId').value = item.asin; // Use ASIN
    document.getElementById('pricingItemName').textContent = item.name;
    document.getElementById('pricingSku').textContent = `ASIN: ${item.asin} | SKUs: ${item.skus ? item.skus.join(', ') : 'None'}`;

    // Load detailed pricing and VC status
    try {
        const [pricingResponse, vcStatusResponse] = await Promise.all([
            fetch(`${API_BASE}/items/${item.asin}`),
            fetch(`${API_BASE}/items/${item.asin}/vc-status`)
        ]);
        
        const itemData = await pricingResponse.json();
        const vcData = await vcStatusResponse.json();
        
        renderPricingTable(itemData.pricing || [], vcData.vc_status || []);
        document.getElementById('pricingModal').style.display = 'block';
    } catch (error) {
        console.error('Error loading pricing:', error);
        showError('Failed to load pricing data');
    }
}

function renderPricingTable(pricingData, vcStatusData) {
    const tbody = document.getElementById('pricingTableBody');
    const asin = document.getElementById('pricingItemId').value; // Get ASIN from modal
    
    if (pricingData.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: var(--text-secondary);">No pricing data yet</td></tr>';
        return;
    }

    // Create a map of country to VC status for quick lookup
    const vcStatusMap = {};
    vcStatusData.forEach(vc => {
        const countryCode = vc.country ? vc.country.toUpperCase() : null;
        if (countryCode) {
            vcStatusMap[countryCode] = {
                status: vc.status,
                asin: vc.asin
            };
        }
    });

    tbody.innerHTML = pricingData.map(pricing => {
        const countryCode = pricing.country_code.toUpperCase();
        const vcInfo = vcStatusMap[countryCode] || null;
        const vcStatusBadge = vcInfo 
            ? (vcInfo.status === 'SUCCESS' || vcInfo.status === 'COMPLETE') 
                ? '<span class="status-badge approved">✓ Listed</span>' 
                : `<span class="status-badge pending">${vcInfo.status || 'Unknown'}</span>`
            : '<span class="status-badge" style="background-color: #999;">Not Listed</span>';
        
        return `
        <tr>
            <td>${escapeHtml(pricing.country_code)}</td>
            <td>USD ${parseFloat(pricing.sell_price).toFixed(2)}</td>
            <td>${pricing.currency} ${parseFloat(pricing.retail_price).toFixed(2)}</td>
            <td><span class="status-badge ${pricing.approval_status}">${pricing.approval_status}</span></td>
            <td>${vcStatusBadge}</td>
            <td>
                <div class="action-buttons">
                    ${pricing.approval_status !== 'approved' ? 
                        `<button class="btn btn-success" onclick="approvePricing('${asin}', '${pricing.country_code}')">Approve</button>` : 
                        '<span style="color: var(--success-color); font-weight: 600;">✓ Approved</span>'}
                    ${pricing.approval_status !== 'rejected' ? 
                        `<button class="btn btn-danger" onclick="rejectPricing('${asin}', '${pricing.country_code}')">Reject</button>` : ''}
                </div>
            </td>
        </tr>
    `;
    }).join('');
}

async function openFlowModal(itemId) {
    const item = items.find(i => i.asin === itemId || i.id === itemId);
    if (!item) return;

    document.getElementById('flowItemId').value = item.asin; // Use ASIN
    document.getElementById('flowItemName').textContent = item.name;
    document.getElementById('flowSku').textContent = `ASIN: ${item.asin} | SKUs: ${item.skus ? item.skus.join(', ') : 'None'}`;

    // Set checkbox states for new 6-stage flow
    document.querySelector('[data-stage="stage_1_idea_considered"]').checked = item.stage_1_idea_considered || false;
    document.querySelector('[data-stage="stage_2_product_finalized"]').checked = item.stage_2_product_finalized || false;
    document.querySelector('[data-stage="stage_3a_pricing_submitted"]').checked = item.stage_3a_pricing_submitted || false;
    document.querySelector('[data-stage="stage_3b_pricing_approved"]').checked = item.stage_3b_pricing_approved || false;
    document.querySelector('[data-stage="stage_4_product_listed"]').checked = item.stage_4_product_listed || false;
    document.querySelector('[data-stage="stage_5_product_ordered"]').checked = item.stage_5_product_ordered || false;
    document.querySelector('[data-stage="stage_6_product_online"]').checked = item.stage_6_product_online || false;

    document.getElementById('flowModal').style.display = 'block';
}

// Form handlers
async function handleItemSubmit(e) {
    e.preventDefault();

    const itemId = document.getElementById('itemId').value; // This is ASIN for existing items
    const brand = document.getElementById('brand').value;
    const description = document.getElementById('description').value;
    const seasonLaunch = document.getElementById('seasonLaunch').value;
    const country = document.getElementById('country').value;
    const itemNumber = document.getElementById('itemNumber').value;
    
    const itemData = {
        sku: document.getElementById('sku').value,
        name: document.getElementById('name').value || document.getElementById('sku').value, // Use SKU as name if not provided
        asin: document.getElementById('asin').value || null
    };

    try {
        const url = itemId ? `${API_BASE}/items/${itemId}` : `${API_BASE}/items`;
        const method = itemId ? 'PUT' : 'POST';

        const response = await fetch(url, {
            method: method,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(itemData)
        });

        if (response.ok) {
            const result = await response.json();
            const savedAsin = itemId || result.asin;
            
            // Mark as Stage 1 (Ideation) with brand/description/season/country/item_number
            if (brand || description || seasonLaunch || country || itemNumber) {
                await fetch(`${API_BASE}/items/${savedAsin}/stage1`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        brand: brand,
                        description: description,
                        season_launch: seasonLaunch,
                        country: country,
                        item_number: itemNumber
                    })
                });
            }
            
            document.getElementById('itemModal').style.display = 'none';
            loadItems();
            showSuccess(itemId ? 'Product updated successfully' : 'Product created at Ideation stage');
        } else {
            throw new Error('Failed to save product');
        }
    } catch (error) {
        console.error('Error saving product:', error);
        showError('Failed to save product');
    }
}

async function handlePricingSubmit(e) {
    e.preventDefault();

    const itemId = document.getElementById('pricingItemId').value;
    const pricingData = {
        country_code: document.getElementById('countryCode').value,
        currency: document.getElementById('currency').value,
        retail_price: document.getElementById('retailPrice').value,
        sell_price: document.getElementById('sellPrice').value
    };

    try {
        const response = await fetch(`${API_BASE}/items/${itemId}/pricing`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(pricingData)
        });

        if (response.ok) {
            document.getElementById('pricingForm').reset();
            // Reload pricing table
            openPricingModal(itemId);
            loadItems(); // Refresh main view
            showSuccess('Pricing added successfully');
        } else {
            throw new Error('Failed to add pricing');
        }
    } catch (error) {
        console.error('Error adding pricing:', error);
        showError('Failed to add pricing');
    }
}

async function handleStageChange(e) {
    const stage = e.target.dataset.stage;
    const completed = e.target.checked;
    const itemId = document.getElementById('flowItemId').value;

    try {
        const response = await fetch(`${API_BASE}/items/${itemId}/stage`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ stage, completed })
        });

        if (response.ok) {
            loadItems(); // Refresh main view
            showSuccess('Progress updated');
        } else {
            throw new Error('Failed to update stage');
        }
    } catch (error) {
        console.error('Error updating stage:', error);
        showError('Failed to update progress');
        e.target.checked = !completed; // Revert checkbox
    }
}

// Action functions
function editItem(itemId) {
    openItemModal(itemId);
}

// Edit product name (for temp ASINs)
async function editProductName(asin, currentName) {
    const newName = prompt('Enter new product name:', currentName);
    if (!newName || newName === currentName) {
        return;
    }

    try {
        const response = await fetch(`${API_BASE}/items/${asin}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: newName })
        });

        if (response.ok) {
            loadItems();
            showSuccess('Product name updated');
        } else {
            throw new Error('Failed to update name');
        }
    } catch (error) {
        console.error('Error updating name:', error);
        showError('Failed to update product name');
    }
}

async function deleteItem(itemId) {
    if (!confirm('Are you sure you want to delete this item? This action cannot be undone.')) {
        return;
    }

    try {
        const response = await fetch(`${API_BASE}/items/${itemId}`, {
            method: 'DELETE'
        });

        if (response.ok) {
            loadItems();
            showSuccess('Item deleted successfully');
        } else {
            throw new Error('Failed to delete item');
        }
    } catch (error) {
        console.error('Error deleting item:', error);
        showError('Failed to delete item');
    }
}

async function approvePricing(itemId, countryCode) {
    try {
        const response = await fetch(`${API_BASE}/items/${itemId}/pricing/${countryCode}/approve`, {
            method: 'PUT'
        });

        if (response.ok) {
            openPricingModal(itemId); // Refresh pricing modal
            loadItems(); // Refresh main view
            showSuccess('Pricing approved');
        } else {
            throw new Error('Failed to approve pricing');
        }
    } catch (error) {
        console.error('Error approving pricing:', error);
        showError('Failed to approve pricing');
    }
}

async function rejectPricing(itemId, countryCode) {
    try {
        const response = await fetch(`${API_BASE}/items/${itemId}/pricing/${countryCode}/reject`, {
            method: 'PUT'
        });

        if (response.ok) {
            openPricingModal(itemId); // Refresh pricing modal
            loadItems(); // Refresh main view
            showSuccess('Pricing rejected');
        } else {
            throw new Error('Failed to reject pricing');
        }
    } catch (error) {
        console.error('Error rejecting pricing:', error);
        showError('Failed to reject pricing');
    }
}

// Utility functions
function escapeHtml(text) {
    const map = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#039;'
    };
    return text.replace(/[&<>"']/g, m => map[m]);
}

function showSuccess(message) {
    // Simple alert for now - you can replace with a toast notification
    console.log('Success:', message);
}

function showError(message) {
    alert('Error: ' + message);
}

// QPI Import and Sync functions
async function importQpiData() {
    if (!confirm('This will import all items from the QPI CSV file. Items with existing SKUs will be skipped. Continue?')) {
        return;
    }

    try {
        const response = await fetch(`${API_BASE}/import/qpi`, {
            method: 'POST'
        });

        const result = await response.json();
        
        if (response.ok) {
            alert(`QPI Import Complete!\n\nTotal items: ${result.total}\nImported: ${result.imported}\nSkipped (duplicates): ${result.skipped}\nErrors: ${result.errors}`);
            loadItems();
        } else {
            throw new Error(result.error || 'Import failed');
        }
    } catch (error) {
        console.error('Error importing QPI data:', error);
        showError('Failed to import QPI data: ' + error.message);
    }
}

async function syncQpiData() {
    if (!confirm('This will sync order received status from the QPI CSV file. Items found in QPI will be marked as "Order Received". Continue?')) {
        return;
    }

    try {
        const response = await fetch(`${API_BASE}/sync/qpi`, {
            method: 'POST'
        });

        const result = await response.json();
        
        if (response.ok) {
            alert(`QPI Sync Complete!\n\nItems updated: ${result.updated}\nTotal in QPI: ${result.total_in_qpi}`);
            loadItems();
        } else {
            throw new Error(result.error || 'Sync failed');
        }
    } catch (error) {
        console.error('Error syncing QPI data:', error);
        showError('Failed to sync QPI data: ' + error.message);
    }
}

async function syncVcData() {
    if (!confirm('This will sync Vendor Central setup status from the latest VC extract parquet file. Items found in VC will be marked as "Vendor Central Setup" complete. Continue?')) {
        return;
    }

    try {
        const response = await fetch(`${API_BASE}/sync/vc`, {
            method: 'POST'
        });

        const result = await response.json();
        
        if (response.ok) {
            alert(`VC Sync Complete!\n\nFile: ${result.file}\nTotal in VC: ${result.total_in_vc}\nItems updated: ${result.updated}\nErrors: ${result.errors}`);
            loadItems();
        } else {
            throw new Error(result.error || 'Sync failed');
        }
    } catch (error) {
        console.error('Error syncing VC data:', error);
        showError('Failed to sync VC data: ' + error.message);
    }
}

async function syncPimData() {
    if (!confirm('This will sync item data from the PIM Extract Excel file. Item names, descriptions, dimensions, and other details will be updated. Continue?')) {
        return;
    }

    try {
        const response = await fetch(`${API_BASE}/sync/pim`, {
            method: 'POST'
        });

        const result = await response.json();
        
        if (response.ok) {
            alert(`PIM Sync Complete!\n\nTotal in PIM: ${result.total_in_pim}\nItems updated: ${result.updated}\nNot found in DB: ${result.not_found}\nErrors: ${result.errors}`);
            loadItems();
        } else {
            throw new Error(result.error || 'Sync failed');
        }
    } catch (error) {
        console.error('Error syncing PIM data:', error);
        showError('Failed to sync PIM data: ' + error.message);
    }
}

// Export functionality
function openExportModal() {
    document.getElementById('exportModal').style.display = 'block';
}

document.getElementById('exportModalClose').addEventListener('click', () => {
    document.getElementById('exportModal').style.display = 'none';
});

document.getElementById('cancelExportBtn').addEventListener('click', () => {
    document.getElementById('exportModal').style.display = 'none';
});

document.getElementById('downloadExportBtn').addEventListener('click', async () => {
    const fields = [];
    
    if (document.getElementById('exportSku').checked) fields.push('sku');
    if (document.getElementById('exportAsin').checked) fields.push('asin');
    if (document.getElementById('exportName').checked) fields.push('name');
    if (document.getElementById('exportVcStatus').checked) fields.push('vendor_central_setup');
    if (document.getElementById('exportOrderStatus').checked) fields.push('order_received');
    
    if (fields.length === 0) {
        alert('Please select at least one field to export');
        return;
    }
    
    const format = document.getElementById('exportFormat').value;
    
    try {
        const url = `${API_BASE}/export/items?format=${format}&fields=${fields.join(',')}`;
        
        if (format === 'json') {
            // For JSON, fetch and display
            const response = await fetch(url);
            const data = await response.json();
            
            // Create a blob and download
            const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
            const downloadUrl = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = downloadUrl;
            a.download = 'items_export.json';
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            window.URL.revokeObjectURL(downloadUrl);
        } else {
            // For CSV and TXT, just download directly
            const a = document.createElement('a');
            a.href = url;
            a.download = `items_export.${format}`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
        }
        
        document.getElementById('exportModal').style.display = 'none';
        alert('Export complete!');
        
    } catch (error) {
        console.error('Error exporting data:', error);
        showError('Failed to export data: ' + error.message);
    }
});

// Pagination Controls
function updatePaginationControls(view, currentPage, totalPages) {
    const pageInfo = document.getElementById(`${view}PageInfo`);
    const prevBtn = document.getElementById(`${view}PrevBtn`);
    const nextBtn = document.getElementById(`${view}NextBtn`);
    
    pageInfo.textContent = `Page ${currentPage} of ${totalPages}`;
    prevBtn.disabled = currentPage <= 1;
    nextBtn.disabled = currentPage >= totalPages;
}

// Products pagination
document.getElementById('productsPrevBtn').addEventListener('click', () => {
    if (productsPage > 1) {
        loadItems(productsPage - 1);
    }
});

document.getElementById('productsNextBtn').addEventListener('click', () => {
    if (productsPage < productsTotalPages) {
        loadItems(productsPage + 1);
    }
});

// SKUs pagination
document.getElementById('skusPrevBtn').addEventListener('click', () => {
    if (skusPage > 1) {
        loadSkus(skusPage - 1);
    }
});

document.getElementById('skusNextBtn').addEventListener('click', () => {
    if (skusPage < skusTotalPages) {
        loadSkus(skusPage + 1);
    }
});

// Workflow Modal
async function openWorkflowModal(asin, stageNumber) {
    const modal = document.getElementById('workflowModal');
    const title = document.getElementById('workflowModalTitle');
    const content = document.getElementById('workflowModalContent');
    
    const stageNames = {
        1: 'Stage 1: Ideation',
        2: 'Stage 2: PIM Finalized',
        3: 'Stage 3a: Pricing Submitted',
        4: 'Stage 3b: Pricing Approved',
        5: 'Stage 4: VC Listed',
        6: 'Stage 5: Ordered',
        7: 'Stage 6: Online'
    };
    
    title.textContent = `${stageNames[stageNumber]} - ${asin}`;
    
    // If it's Stage 5 (VC Listed), show country status
    if (stageNumber === 5) {
        content.innerHTML = '<p style="text-align: center; color: var(--text-secondary);">Loading country status...</p>';
        modal.style.display = 'block';
        
        try {
            // Fetch ASIN status by country
            const response = await fetch(`${API_BASE}/asin-status/${asin}`);
            const data = await response.json();
            
            if (data.countries && data.countries.length > 0) {
                const countriesWithStatus = data.countries;
                
                // Get all unique countries from the filter dropdown
                const countryFilter = document.getElementById('countryFilter');
                const allCountries = Array.from(countryFilter.options)
                    .map(opt => opt.value)
                    .filter(val => val !== 'all' && val !== 'loading');
                
                content.innerHTML = `
                    <div style="margin-bottom: 20px;">
                        <h3>VC Listing Status by Country</h3>
                        <p>ASIN: <strong>${asin}</strong></p>
                    </div>
                    
                    <div class="country-status-table-wrapper">
                        <table class="country-status-table">
                            <thead>
                                <tr>
                                    <th>Country</th>
                                    <th>Status</th>
                                    <th>VC Status</th>
                                    <th>Last Synced</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${allCountries.map(country => {
                                    const countryData = countriesWithStatus.find(c => c.country_code === country);
                                    const isListed = !!countryData;
                                    const vcStatus = countryData ? countryData.vc_status : '-';
                                    const lastSynced = countryData ? new Date(countryData.last_synced).toLocaleString() : '-';
                                    
                                    return `
                                        <tr class="${isListed ? 'listed' : 'not-listed'}">
                                            <td><strong>${escapeHtml(country)}</strong></td>
                                            <td>
                                                ${isListed 
                                                    ? '<span class="status-badge listed">✓ Listed</span>' 
                                                    : '<span class="status-badge not-listed">✗ Not Listed</span>'}
                                            </td>
                                            <td>${escapeHtml(vcStatus)}</td>
                                            <td><small>${escapeHtml(lastSynced)}</small></td>
                                        </tr>
                                    `;
                                }).join('')}
                            </tbody>
                        </table>
                    </div>
                    
                    <div style="margin-top: 20px; text-align: right;">
                        <button class="btn btn-primary" onclick="document.getElementById('workflowModal').style.display='none'">Close</button>
                    </div>
                `;
            } else {
                content.innerHTML = `
                    <p>No VC listing data found for ASIN: <strong>${asin}</strong></p>
                    <div style="margin-top: 20px;">
                        <button class="btn btn-primary" onclick="document.getElementById('workflowModal').style.display='none'">Close</button>
                    </div>
                `;
            }
        } catch (error) {
            console.error('Error loading country status:', error);
            content.innerHTML = `
                <p style="color: var(--danger-color);">Error loading country status</p>
                <div style="margin-top: 20px;">
                    <button class="btn btn-primary" onclick="document.getElementById('workflowModal').style.display='none'">Close</button>
                </div>
            `;
        }
    } else {
        // For other stages, show basic info
        content.innerHTML = `
            <p>Workflow details for ASIN: <strong>${asin}</strong></p>
            <p>Stage: <strong>${stageNames[stageNumber]}</strong></p>
            <div style="margin-top: 20px;">
                <button class="btn btn-primary" onclick="document.getElementById('workflowModal').style.display='none'">Close</button>
            </div>
        `;
        modal.style.display = 'block';
    }
}

document.getElementById('workflowModalClose').addEventListener('click', () => {
    document.getElementById('workflowModal').style.display = 'none';
});


