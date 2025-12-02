const API_BASE = '/api';

// State management
let items = [];
let currentItem = null;

// Initialize app
document.addEventListener('DOMContentLoaded', () => {
    loadItems();
    setupEventListeners();
});

// Event Listeners
function setupEventListeners() {
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

    // Search and Filter
    document.getElementById('searchInput').addEventListener('input', filterItems);
    document.getElementById('statusFilter').addEventListener('change', filterItems);

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
async function loadItems() {
    try {
        const response = await fetch(`${API_BASE}/items`);
        items = await response.json();
        renderItems(items);
    } catch (error) {
        console.error('Error loading items:', error);
        showError('Failed to load items');
    }
}

// Render items
function renderItems(itemsToRender) {
    const container = document.getElementById('itemsContainer');
    
    if (itemsToRender.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <h3>No items found</h3>
                <p>Click "Add New Item" to get started</p>
            </div>
        `;
        return;
    }

    container.innerHTML = itemsToRender.map(item => {
        const pricingData = item.pricing_data || [];
        const allApproved = pricingData.length > 0 && pricingData.every(p => p.approval_status === 'approved');
        const hasRejected = pricingData.some(p => p.approval_status === 'rejected');
        
        // Determine current stage
        const currentStage = getCurrentStage(item);
        
        return `
            <div class="item-card" data-item-id="${item.id}">
                <div class="item-header">
                    <div class="item-info">
                        <h3>${escapeHtml(item.name)}</h3>
                        <div class="item-sku">
                            SKU: ${escapeHtml(item.sku)}
                            ${item.asin ? ` | ASIN: ${escapeHtml(item.asin)}` : ''}
                        </div>
                    </div>
                    <div class="item-actions">
                        <button class="btn btn-secondary btn-small" onclick="editItem(${item.id})">Edit</button>
                        <button class="btn btn-danger btn-small" onclick="deleteItem(${item.id})">Delete</button>
                    </div>
                </div>

                <div class="item-details">
                    <div class="detail-item">
                        <div class="detail-label">Dimensions</div>
                        <div class="detail-value">${item.dimensions || 'N/A'}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Case Pack</div>
                        <div class="detail-value">${item.case_pack || 'N/A'}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">SIOC Status</div>
                        <div class="detail-value">${item.sioc_status || 'N/A'}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Current Stage</div>
                        <div class="detail-value">
                            ${currentStage.badge}
                        </div>
                    </div>
                </div>

                <div class="pricing-section">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                        <h3>Country Pricing</h3>
                        <button class="btn btn-primary btn-small" onclick="openPricingModal(${item.id})">
                            Manage Pricing
                        </button>
                    </div>
                    
                    ${pricingData.length > 0 ? `
                        <div class="pricing-grid">
                            ${pricingData.map(pricing => `
                                <div class="pricing-card ${pricing.approval_status}">
                                    <div class="pricing-header">
                                        <span class="country-name">${pricing.country}</span>
                                        <span class="status-badge ${pricing.approval_status}">
                                            ${pricing.approval_status}
                                        </span>
                                    </div>
                                    <div class="pricing-prices">
                                        Retail: ${pricing.currency} ${parseFloat(pricing.retail_price).toFixed(2)}<br>
                                        Sell: ${pricing.currency} ${parseFloat(pricing.sell_price).toFixed(2)}
                                    </div>
                                </div>
                            `).join('')}
                        </div>
                    ` : '<p style="color: var(--text-secondary); font-size: 14px;">No pricing data available</p>'}
                </div>

                <div class="flow-progress">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                        <h3>Flow Progress</h3>
                        <button class="btn btn-secondary btn-small" onclick="openFlowModal(${item.id})">
                            Update Progress
                        </button>
                    </div>
                    
                    <div class="flow-stages-display">
                        ${renderStageDisplay('1', 'Ideation', item.stage_1_idea_considered, true)}
                        ${renderStageDisplay('2', 'PIM Finalized', item.stage_2_product_finalized)}
                        ${renderStageDisplay('3a', 'Price Submit', item.stage_3a_pricing_submitted)}
                        ${renderStageDisplay('3b', 'Price Approved', item.stage_3b_pricing_approved)}
                        ${renderStageDisplay('4', 'VC Listed', item.stage_4_product_listed)}
                        ${renderStageDisplay('5', 'Ordered [In QPI]', item.stage_5_product_ordered)}
                        ${renderStageDisplay('6', 'Online', item.stage_6_product_online)}
                    </div>
                </div>
            </div>
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
        return { stage: 2, name: 'PIM Finalized', badge: '<span class="status-badge" style="background-color: #f59e0b;">Stage 2: Finalized</span>' };
    } else if (item.stage_1_idea_considered) {
        return { stage: 1, name: 'Ideation', badge: '<span class="status-badge" style="background-color: #6b7280;">Stage 1: Ideation</span>' };
    } else {
        return { stage: 0, name: 'Not Started', badge: '<span class="status-badge" style="background-color: #999;">Not Started</span>' };
    }
}

function renderStageDisplay(number, label, completed, isOptional = false) {
    return `
        <div class="stage-display ${completed ? 'completed' : ''}">
            <div class="stage-icon">${completed ? '✓' : number}</div>
            <div class="stage-label">
                ${label}
                ${isOptional ? '<br><span style="font-size: 10px;">(Optional)</span>' : ''}
            </div>
        </div>
    `;
}

// Filter items
function filterItems() {
    const searchTerm = document.getElementById('searchInput').value.toLowerCase();
    const statusFilter = document.getElementById('statusFilter').value;

    const filtered = items.filter(item => {
        const matchesSearch = item.name.toLowerCase().includes(searchTerm) || 
                            item.sku.toLowerCase().includes(searchTerm);
        
        if (!matchesSearch) return false;

        if (statusFilter === 'all') return true;

        // Filter by stage
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
        title.textContent = 'Edit Item';
        advancedFields.style.display = 'block';
        toggleBtn.style.display = 'none';
        
        const item = items.find(i => i.id === itemId);
        if (item) {
            document.getElementById('itemId').value = item.id;
            document.getElementById('sku').value = item.sku;
            document.getElementById('sku').disabled = true; // Don't allow SKU changes
            document.getElementById('brand').value = item.stage_1_brand || item.brand || '';
            document.getElementById('description').value = item.stage_1_description || item.product_description || '';
            document.getElementById('seasonLaunch').value = item.stage_1_season_launch || '';
            document.getElementById('asin').value = item.asin || '';
            document.getElementById('name').value = item.name;
            document.getElementById('dimensions').value = item.dimensions || '';
            document.getElementById('casePack').value = item.case_pack || '';
            document.getElementById('siocStatus').value = item.sioc_status || '';
        }
    } else {
        // Add mode - simple fields only
        title.textContent = 'Add New Item - Ideation Stage';
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
    const item = items.find(i => i.id === itemId);
    if (!item) return;

    document.getElementById('pricingItemId').value = itemId;
    document.getElementById('pricingItemName').textContent = item.name;
    document.getElementById('pricingSku').textContent = item.sku;

    // Load detailed pricing
    try {
        const response = await fetch(`${API_BASE}/items/${itemId}`);
        const itemData = await response.json();
        
        renderPricingTable(itemData.pricing || []);
        document.getElementById('pricingModal').style.display = 'block';
    } catch (error) {
        console.error('Error loading pricing:', error);
        showError('Failed to load pricing data');
    }
}

function renderPricingTable(pricingData) {
    const tbody = document.getElementById('pricingTableBody');
    
    if (pricingData.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" style="text-align: center; color: var(--text-secondary);">No pricing data yet</td></tr>';
        return;
    }

    tbody.innerHTML = pricingData.map(pricing => `
        <tr>
            <td>${escapeHtml(pricing.country_code)}</td>
            <td>${pricing.currency} ${parseFloat(pricing.retail_price).toFixed(2)}</td>
            <td>${pricing.currency} ${parseFloat(pricing.sell_price).toFixed(2)}</td>
            <td><span class="status-badge ${pricing.approval_status}">${pricing.approval_status}</span></td>
            <td>
                <div class="action-buttons">
                    ${pricing.approval_status !== 'approved' ? 
                        `<button class="btn btn-success" onclick="approvePricing(${pricing.item_id}, '${pricing.country_code}')">Approve</button>` : 
                        '<span style="color: var(--success-color); font-weight: 600;">✓ Approved</span>'}
                    ${pricing.approval_status !== 'rejected' ? 
                        `<button class="btn btn-danger" onclick="rejectPricing(${pricing.item_id}, '${pricing.country_code}')">Reject</button>` : ''}
                </div>
            </td>
        </tr>
    `).join('');
}

async function openFlowModal(itemId) {
    const item = items.find(i => i.id === itemId);
    if (!item) return;

    document.getElementById('flowItemId').value = itemId;
    document.getElementById('flowItemName').textContent = item.name;
    document.getElementById('flowSku').textContent = item.sku;

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

    const itemId = document.getElementById('itemId').value;
    const brand = document.getElementById('brand').value;
    const description = document.getElementById('description').value;
    const seasonLaunch = document.getElementById('seasonLaunch').value;
    
    const itemData = {
        sku: document.getElementById('sku').value,
        name: document.getElementById('name').value || document.getElementById('sku').value, // Use SKU as name if not provided
        asin: document.getElementById('asin').value || null,
        dimensions: document.getElementById('dimensions').value || null,
        case_pack: document.getElementById('casePack').value || null,
        sioc_status: document.getElementById('siocStatus').value || null
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
            const savedItemId = itemId || result.id;
            
            // If new item, mark as Stage 1 (Ideation) with brand/description/season
            if (!itemId && (brand || description || seasonLaunch)) {
                await fetch(`${API_BASE}/items/${savedItemId}/stage1`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        brand: brand,
                        description: description,
                        season_launch: seasonLaunch
                    })
                });
            }
            
            document.getElementById('itemModal').style.display = 'none';
            loadItems();
            showSuccess(itemId ? 'Item updated successfully' : 'Item created at Ideation stage');
        } else {
            throw new Error('Failed to save item');
        }
    } catch (error) {
        console.error('Error saving item:', error);
        showError('Failed to save item');
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


