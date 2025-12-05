const API_BASE = '/api';

// State management
let currentUser = null;
let currentView = 'products';

// Products state
let productsPage = 1;
let productsLimit = 50;
let productsTotalPages = 1;

// Item Numbers state
let itemsPage = 1;
let itemsLimit = 50;
let itemsTotalPages = 1;

// Database explorer state
let dbPage = 1;
let dbLimit = 100;
let dbTotalPages = 1;
let currentTable = 'products';

// Filter data
let customerGroups = [];
let customers = [];

// ============================================================================
// AUTHENTICATION
// ============================================================================

function checkAuth() {
    const token = localStorage.getItem('token');
    const user = JSON.parse(localStorage.getItem('user') || '{}');
    
    if (!token || !user.role) {
        window.location.href = '/login.html';
        return false;
    }
    
    currentUser = user;
    setupUserUI();
    return true;
}

function setupUserUI() {
    // Show user info in header
    const header = document.querySelector('header');
    const userInfoDiv = document.createElement('div');
    userInfoDiv.style.cssText = 'display: flex; align-items: center; gap: 10px; margin-left: auto;';
    userInfoDiv.innerHTML = `
        <span style="color: var(--text-secondary); font-size: 14px;">${currentUser.full_name || currentUser.username}</span>
        <span style="background: var(--primary); color: white; padding: 4px 12px; border-radius: 12px; font-size: 12px; font-weight: 600;">
            ${currentUser.role.toUpperCase()}
        </span>
        <button class="btn btn-secondary btn-sm" onclick="logout()">Logout</button>
    `;
    header.querySelector('div:last-child').appendChild(userInfoDiv);
    
    // Show admin sections based on role
    if (['salesperson', 'approver', 'admin'].includes(currentUser.role)) {
        document.getElementById('customerAdminSection').style.display = 'block';
        document.getElementById('customerAdminViewTab').style.display = 'inline-block';
    }
    
    if (currentUser.role === 'admin') {
        document.getElementById('adminConsoleSection').style.display = 'block';
    }
}

function logout() {
    fetch(`${API_BASE}/auth/logout`, { method: 'POST' });
    localStorage.removeItem('token');
    localStorage.removeItem('user');
    window.location.href = '/login.html';
}

function getAuthHeaders() {
    return {
        'Authorization': `Bearer ${localStorage.getItem('token')}`,
        'Content-Type': 'application/json'
    };
}

// ============================================================================
// NOTIFICATIONS
// ============================================================================

function showSuccess(message) {
    showToast(message, 'success');
}

function showError(message) {
    showToast(message, 'error');
}

function showToast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 15px 25px;
        border-radius: 8px;
        color: white;
        font-weight: 500;
        z-index: 10000;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        animation: slideIn 0.3s ease;
    `;
    toast.style.background = type === 'success' ? '#10b981' : type === 'error' ? '#ef4444' : '#3b82f6';
    toast.textContent = message;
    document.body.appendChild(toast);
    setTimeout(() => {
        toast.style.animation = 'slideOut 0.3s ease';
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

// ============================================================================
// DATA LOADING
// ============================================================================

async function loadCustomerGroups() {
    try {
        const response = await fetch(`${API_BASE}/customer-groups`, {
            headers: getAuthHeaders()
        });
        if (response.ok) {
            customerGroups = await response.json();
            populateCustomerGroupFilters();
        }
    } catch (error) {
        console.error('Error loading customer groups:', error);
    }
}

async function loadCustomers() {
    try {
        const response = await fetch(`${API_BASE}/customers`, {
            headers: getAuthHeaders()
        });
        if (response.ok) {
            customers = await response.json();
            populateCustomerFilters();
        }
    } catch (error) {
        console.error('Error loading customers:', error);
    }
}

function populateCustomerGroupFilters() {
    const select = document.getElementById('customerGroupFilter');
    select.innerHTML = '<option value="">All Customer Groups</option>';
    customerGroups.forEach(cg => {
        select.innerHTML += `<option value="${cg.id}">${cg.name} (${cg.product_count} products)</option>`;
    });
    
    // Also populate customer group select in modal
    const modalSelect = document.getElementById('customerGroupSelect');
    if (modalSelect) {
        modalSelect.innerHTML = '<option value="">Select Customer Group</option>';
        customerGroups.forEach(cg => {
            modalSelect.innerHTML += `<option value="${cg.id}">${cg.name}</option>`;
        });
    }
}

function populateCustomerFilters() {
    const select = document.getElementById('customerFilter');
    const customerGroupId = document.getElementById('customerGroupFilter').value;
    
    select.innerHTML = '<option value="">All Customers</option>';
    
    let filteredCustomers = customers;
    if (customerGroupId) {
        filteredCustomers = customers.filter(c => c.customer_group_id == customerGroupId);
    }
    
    filteredCustomers.forEach(c => {
        select.innerHTML += `<option value="${c.id}">${c.name} (${c.product_count} products)</option>`;
    });
}

// ============================================================================
// PRODUCTS
// ============================================================================

async function loadProducts(page = 1, maintainFilters = true) {
    productsPage = page;
    const offset = (page - 1) * productsLimit;
    
    let url = `${API_BASE}/products?limit=${productsLimit}&offset=${offset}`;
    
    if (maintainFilters) {
        const search = document.getElementById('searchInput').value;
        const customerGroupId = document.getElementById('customerGroupFilter').value;
        const customerId = document.getElementById('customerFilter').value;
        const status = document.getElementById('statusFilter').value;
        const stage = document.getElementById('stageFilter')?.value;
        
        if (search) url += `&search=${encodeURIComponent(search)}`;
        if (customerGroupId) url += `&customer_group_id=${customerGroupId}`;
        if (customerId) url += `&customer_id=${customerId}`;
        if (status) url += `&status=${status}`;
        if (stage) url += `&stage=${stage}`;
    }
    
    try {
        const response = await fetch(url, { headers: getAuthHeaders() });
        if (!response.ok) throw new Error('Failed to load products');
        
        const result = await response.json();
        renderProductsTable(result.data);
        updateProductsPagination(result.pagination);
    } catch (error) {
        console.error('Error loading products:', error);
        showError('Failed to load products');
    }
}

function renderProductsTable(products) {
    const tbody = document.getElementById('productsTableBody');
    
    if (!products || products.length === 0) {
        tbody.innerHTML = '<tr><td colspan="14" style="text-align: center; padding: 40px;">No products found</td></tr>';
        return;
    }
    
    tbody.innerHTML = products.map(p => {
        // Render stage circles
        const renderStage = (stageNum, isComplete, isManual, stageName, stageKey) => {
            const colorClass = isComplete ? 'stage-complete' : 'stage-incomplete';
            const clickable = isManual ? 'stage-clickable' : '';
            const onclick = isManual && stageKey ? `onclick="toggleStage(${p.id}, '${stageKey}', ${!isComplete})"` : '';
            const displayNum = stageNum === 2.5 ? '💰' : (isComplete ? '✓' : stageNum);
            return `<div class="stage-circle ${colorClass} ${clickable}" title="${stageName}" ${onclick}>${displayNum}</div>`;
        };
        
        return `
            <tr>
                <td><strong>${p.customer_group_name || '-'}</strong></td>
                <td>${p.customer_name || '-'}</td>
                <td><code>${p.customer_number}</code></td>
                <td>${p.item_number ? `<code>${p.item_number}</code>` : '-'}</td>
                <td title="${p.description || ''}">${truncate(p.description, 25)}</td>
                <td style="text-align: center;">${renderStage(1, p.stage_1_ideation, true, 'Stage 1: Ideation / Product Dev', 'stage_1_ideation')}</td>
                <td style="text-align: center;">${renderStage(2, p.stage_2_pim, false, 'Stage 2: PIM Listed', null)}</td>
                <td style="text-align: center;">${renderStage(2.5, p.stage_2_5_pricing, true, 'Stage 2.5: Pricing Approved', 'stage_2_5_pricing')}</td>
                <td style="text-align: center;">${renderStage(3, p.stage_3_qpi, false, 'Stage 3: In QPI', null)}</td>
                <td style="text-align: center;">${renderStage(4, p.stage_4_listed_with_customer, true, 'Stage 4: Listed with Customer', 'stage_4_listed_with_customer')}</td>
                <td style="text-align: center;">${renderStage(5, p.stage_5_available, true, 'Stage 5: Available Online/In-Store', 'stage_5_available')}</td>
                <td style="text-align: center;">${renderStage(6, p.stage_6_eol, false, 'Stage 6: End of Life', null)}</td>
                <td>${p.sell_price ? `$${parseFloat(p.sell_price).toFixed(2)}` : '-'}</td>
                <td>
                    <button class="btn-sm btn-primary" onclick="viewProduct(${p.id})">View</button>
                </td>
            </tr>
        `;
    }).join('');
}

// Toggle a manual stage
async function toggleStage(productId, stageKey, newValue) {
    try {
        const response = await fetch(`${API_BASE}/products/${productId}/stage`, {
            method: 'PUT',
            headers: getAuthHeaders(),
            body: JSON.stringify({ stage: stageKey, value: newValue })
        });
        
        if (response.ok) {
            loadProducts(productsPage);
        } else {
            const error = await response.json();
            showError(error.error || 'Failed to update stage');
        }
    } catch (error) {
        showError('Failed to update stage: ' + error.message);
    }
}

function updateProductsPagination(pagination) {
    productsTotalPages = pagination.total_pages;
    
    document.getElementById('pageInfo').textContent = `Page ${pagination.page} of ${pagination.total_pages} (${pagination.total} total)`;
    document.getElementById('prevPage').disabled = pagination.page <= 1;
    document.getElementById('nextPage').disabled = pagination.page >= pagination.total_pages;
}

async function viewProduct(id) {
    try {
        const response = await fetch(`${API_BASE}/products/${id}`, { headers: getAuthHeaders() });
        if (!response.ok) throw new Error('Failed to load product');
        
        const product = await response.json();
        
        const content = document.getElementById('productDetailContent');
        content.innerHTML = `
            <div class="product-detail-grid">
                <div class="detail-section">
                    <h3>Product Information</h3>
                    <table class="detail-table">
                        <tr><td>Customer Group</td><td><strong>${product.customer_group_name}</strong></td></tr>
                        <tr><td>Customer</td><td>${product.customer_name}</td></tr>
                        <tr><td>Customer Number</td><td><code>${product.customer_number}</code></td></tr>
                        <tr><td>Item Number</td><td>${product.item_number ? `<code>${product.item_number}</code>` : '-'}</td></tr>
                        <tr><td>Description</td><td>${product.description || '-'}</td></tr>
                        <tr><td>FCL/LCL</td><td>${product.fcl_lcl || '-'}</td></tr>
                        <tr><td>Status</td><td><span class="status-badge status-${(product.status || '').toLowerCase()}">${product.status || '-'}</span></td></tr>
                        <tr><td>Sell Price</td><td>${product.sell_price ? `$${parseFloat(product.sell_price).toFixed(2)}` : '-'}</td></tr>
                    </table>
                </div>
                
                ${product.item_number ? `
                <div class="detail-section">
                    <h3>PIM Data (from Item Number)</h3>
                    <table class="detail-table">
                        <tr><td>Legal Name</td><td>${product.pim_legal_name || '-'}</td></tr>
                        <tr><td>Brand</td><td>${product.pim_brand || '-'}</td></tr>
                        <tr><td>Series</td><td>${product.pim_series || '-'}</td></tr>
                        <tr><td>Taxonomy</td><td>${product.pim_taxonomy || '-'}</td></tr>
                        <tr><td>Age Grade</td><td>${product.pim_age_grade || '-'}</td></tr>
                        <tr><td>UPC</td><td>${product.pim_upc || '-'}</td></tr>
                        <tr><td>Dev Status</td><td>${product.pim_dev_status || '-'}</td></tr>
                        <tr><td>Spec Status</td><td>${product.pim_spec_status || '-'}</td></tr>
                        <tr><td>Dimensions</td><td>${product.pim_length && product.pim_width && product.pim_height ? 
                            `${product.pim_length} × ${product.pim_width} × ${product.pim_height} cm` : '-'}</td></tr>
                        <tr><td>Weight</td><td>${product.pim_weight ? `${product.pim_weight} kg` : '-'}</td></tr>
                    </table>
                </div>
                ` : ''}
            </div>
        `;
        
        document.getElementById('productModal').style.display = 'block';
    } catch (error) {
        console.error('Error loading product:', error);
        showError('Failed to load product details');
    }
}

function closeProductModal() {
    document.getElementById('productModal').style.display = 'none';
}

// ============================================================================
// ITEM NUMBERS (PIM)
// ============================================================================

async function loadItemNumbers(page = 1) {
    itemsPage = page;
    const offset = (page - 1) * itemsLimit;
    
    let url = `${API_BASE}/item-numbers?limit=${itemsLimit}&offset=${offset}`;
    
    const search = document.getElementById('itemSearchInput').value;
    const series = document.getElementById('itemSeriesFilter').value;
    const brand = document.getElementById('itemBrandFilter').value;
    const devStatus = document.getElementById('itemDevStatusFilter').value;
    
    if (search) url += `&search=${encodeURIComponent(search)}`;
    if (series) url += `&series=${encodeURIComponent(series)}`;
    if (brand) url += `&brand=${encodeURIComponent(brand)}`;
    if (devStatus) url += `&dev_status=${encodeURIComponent(devStatus)}`;
    
    try {
        const response = await fetch(url, { headers: getAuthHeaders() });
        if (!response.ok) throw new Error('Failed to load item numbers');
        
        const result = await response.json();
        renderItemsTable(result.data);
        updateItemsPagination(result.pagination);
    } catch (error) {
        console.error('Error loading item numbers:', error);
        showError('Failed to load item numbers');
    }
}

function renderItemsTable(items) {
    const tbody = document.getElementById('itemsTableBody');
    
    if (!items || items.length === 0) {
        tbody.innerHTML = '<tr><td colspan="9" style="text-align: center; padding: 40px;">No item numbers found</td></tr>';
        return;
    }
    
    tbody.innerHTML = items.map(i => `
        <tr>
            <td><code>${i.item_number}</code></td>
            <td title="${i.legal_name || ''}">${truncate(i.legal_name, 30)}</td>
            <td>${i.brand_product_line || '-'}</td>
            <td>${i.series || '-'}</td>
            <td>${i.product_taxonomy_category || '-'}</td>
            <td>${i.age_grade || '-'}</td>
            <td><span class="pim-badge ${i.product_development_status === 'Finalized' ? 'pim-finalized' : ''}">${i.product_development_status || '-'}</span></td>
            <td>${i.item_spec_sheet_status || '-'}</td>
            <td>${i.upc_number || '-'}</td>
        </tr>
    `).join('');
}

function updateItemsPagination(pagination) {
    itemsTotalPages = pagination.total_pages;
    
    document.getElementById('itemPageInfo').textContent = `Page ${pagination.page} of ${pagination.total_pages} (${pagination.total} total)`;
    document.getElementById('itemPrevPage').disabled = pagination.page <= 1;
    document.getElementById('itemNextPage').disabled = pagination.page >= pagination.total_pages;
}

async function loadItemFilters() {
    try {
        const response = await fetch(`${API_BASE}/item-numbers/filters`, { headers: getAuthHeaders() });
        if (!response.ok) return;
        
        const filters = await response.json();
        
        const seriesSelect = document.getElementById('itemSeriesFilter');
        seriesSelect.innerHTML = '<option value="">All Series</option>';
        filters.series.forEach(s => {
            seriesSelect.innerHTML += `<option value="${s}">${s}</option>`;
        });
        
        const brandSelect = document.getElementById('itemBrandFilter');
        brandSelect.innerHTML = '<option value="">All Brands</option>';
        filters.brands.forEach(b => {
            brandSelect.innerHTML += `<option value="${b}">${b}</option>`;
        });
        
        const devStatusSelect = document.getElementById('itemDevStatusFilter');
        devStatusSelect.innerHTML = '<option value="">All Dev Statuses</option>';
        filters.dev_statuses.forEach(s => {
            devStatusSelect.innerHTML += `<option value="${s}">${s}</option>`;
        });
    } catch (error) {
        console.error('Error loading item filters:', error);
    }
}

// ============================================================================
// CUSTOMER ADMIN
// ============================================================================

async function loadCustomerGroupsTable() {
    try {
        const response = await fetch(`${API_BASE}/customer-groups`, { headers: getAuthHeaders() });
        if (!response.ok) throw new Error('Failed to load customer groups');
        
        const groups = await response.json();
        const tbody = document.getElementById('customerGroupsTableBody');
        
        if (groups.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" style="text-align: center;">No customer groups found</td></tr>';
            return;
        }
        
        tbody.innerHTML = groups.map(g => `
            <tr>
                <td><strong>${g.name}</strong></td>
                <td>${g.customer_count}</td>
                <td>${g.product_count}</td>
                <td>${formatDate(g.created_at)}</td>
                <td>
                    <button class="btn-sm btn-secondary" onclick="editCustomerGroup(${g.id}, '${escapeHtml(g.name)}')">Edit</button>
                    ${currentUser.role === 'admin' ? `<button class="btn-sm btn-danger" onclick="deleteCustomerGroup(${g.id})">Delete</button>` : ''}
                </td>
            </tr>
        `).join('');
    } catch (error) {
        console.error('Error loading customer groups:', error);
    }
}

async function loadCustomersTable() {
    try {
        const response = await fetch(`${API_BASE}/customers`, { headers: getAuthHeaders() });
        if (!response.ok) throw new Error('Failed to load customers');
        
        const customersList = await response.json();
        const tbody = document.getElementById('customersTableBody');
        
        if (customersList.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" style="text-align: center;">No customers found</td></tr>';
            return;
        }
        
        tbody.innerHTML = customersList.map(c => `
            <tr>
                <td><strong>${c.name}</strong></td>
                <td>${c.customer_group_name}</td>
                <td>${c.product_count}</td>
                <td>${formatDate(c.created_at)}</td>
                <td>
                    <button class="btn-sm btn-secondary" onclick="editCustomer(${c.id}, '${escapeHtml(c.name)}', ${c.customer_group_id})">Edit</button>
                    ${currentUser.role === 'admin' ? `<button class="btn-sm btn-danger" onclick="deleteCustomer(${c.id})">Delete</button>` : ''}
                </td>
            </tr>
        `).join('');
    } catch (error) {
        console.error('Error loading customers:', error);
    }
}

// Customer Group Modal
function openAddCustomerGroupModal() {
    document.getElementById('customerGroupId').value = '';
    document.getElementById('customerGroupName').value = '';
    document.getElementById('customerGroupModalTitle').textContent = 'Add Customer Group';
    document.getElementById('customerGroupModal').style.display = 'block';
}

function editCustomerGroup(id, name) {
    document.getElementById('customerGroupId').value = id;
    document.getElementById('customerGroupName').value = name;
    document.getElementById('customerGroupModalTitle').textContent = 'Edit Customer Group';
    document.getElementById('customerGroupModal').style.display = 'block';
}

function closeCustomerGroupModal() {
    document.getElementById('customerGroupModal').style.display = 'none';
}

async function saveCustomerGroup(e) {
    e.preventDefault();
    
    const id = document.getElementById('customerGroupId').value;
    const name = document.getElementById('customerGroupName').value;
    
    try {
        const url = id ? `${API_BASE}/customer-groups/${id}` : `${API_BASE}/customer-groups`;
        const method = id ? 'PUT' : 'POST';
        
        const response = await fetch(url, {
            method,
            headers: getAuthHeaders(),
            body: JSON.stringify({ name })
        });
        
        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error || 'Failed to save customer group');
        }
        
        closeCustomerGroupModal();
        showSuccess(id ? 'Customer group updated' : 'Customer group created');
        loadCustomerGroupsTable();
        loadCustomerGroups();
    } catch (error) {
        showError(error.message);
    }
}

async function deleteCustomerGroup(id) {
    if (!confirm('Are you sure? This will delete all customers and products in this group.')) return;
    
    try {
        const response = await fetch(`${API_BASE}/customer-groups/${id}`, {
            method: 'DELETE',
            headers: getAuthHeaders()
        });
        
        if (!response.ok) throw new Error('Failed to delete customer group');
        
        showSuccess('Customer group deleted');
        loadCustomerGroupsTable();
        loadCustomerGroups();
    } catch (error) {
        showError(error.message);
    }
}

// Customer Modal
function openAddCustomerModal() {
    document.getElementById('customerId').value = '';
    document.getElementById('customerName').value = '';
    document.getElementById('customerGroupSelect').value = '';
    document.getElementById('customerModalTitle').textContent = 'Add Customer';
    document.getElementById('customerModal').style.display = 'block';
}

function editCustomer(id, name, customerGroupId) {
    document.getElementById('customerId').value = id;
    document.getElementById('customerName').value = name;
    document.getElementById('customerGroupSelect').value = customerGroupId;
    document.getElementById('customerModalTitle').textContent = 'Edit Customer';
    document.getElementById('customerModal').style.display = 'block';
}

function closeCustomerModal() {
    document.getElementById('customerModal').style.display = 'none';
}

async function saveCustomer(e) {
    e.preventDefault();
    
    const id = document.getElementById('customerId').value;
    const name = document.getElementById('customerName').value;
    const customer_group_id = document.getElementById('customerGroupSelect').value;
    
    try {
        const url = id ? `${API_BASE}/customers/${id}` : `${API_BASE}/customers`;
        const method = id ? 'PUT' : 'POST';
        
        const response = await fetch(url, {
            method,
            headers: getAuthHeaders(),
            body: JSON.stringify({ name, customer_group_id })
        });
        
        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error || 'Failed to save customer');
        }
        
        closeCustomerModal();
        showSuccess(id ? 'Customer updated' : 'Customer created');
        loadCustomersTable();
        loadCustomers();
    } catch (error) {
        showError(error.message);
    }
}

async function deleteCustomer(id) {
    if (!confirm('Are you sure? This will delete all products for this customer.')) return;
    
    try {
        const response = await fetch(`${API_BASE}/customers/${id}`, {
            method: 'DELETE',
            headers: getAuthHeaders()
        });
        
        if (!response.ok) throw new Error('Failed to delete customer');
        
        showSuccess('Customer deleted');
        loadCustomersTable();
        loadCustomers();
    } catch (error) {
        showError(error.message);
    }
}

// ============================================================================
// DATABASE EXPLORER
// ============================================================================

async function loadDatabaseTable(table = null, page = 1) {
    if (table) currentTable = table;
    dbPage = page;
    const offset = (page - 1) * dbLimit;
    
    try {
        const response = await fetch(`${API_BASE}/database/${currentTable}?limit=${dbLimit}&offset=${offset}`, {
            headers: getAuthHeaders()
        });
        if (!response.ok) throw new Error('Failed to load table');
        
        const result = await response.json();
        
        // Render header
        const thead = document.getElementById('databaseTableHead');
        thead.innerHTML = `<tr>${result.columns.map(c => `<th>${c.name}</th>`).join('')}</tr>`;
        
        // Render body
        const tbody = document.getElementById('databaseTableBody');
        if (result.rows.length === 0) {
            tbody.innerHTML = `<tr><td colspan="${result.columns.length}" style="text-align: center;">No data</td></tr>`;
        } else {
            tbody.innerHTML = result.rows.map(row => 
                `<tr>${result.columns.map(c => `<td>${formatCell(row[c.name])}</td>`).join('')}</tr>`
            ).join('');
        }
        
        // Update pagination
        dbTotalPages = Math.ceil(result.total / dbLimit);
        document.getElementById('dbPageInfo').textContent = `Page ${page} of ${dbTotalPages} (${result.total} total)`;
        document.getElementById('dbPrevPage').disabled = page <= 1;
        document.getElementById('dbNextPage').disabled = page >= dbTotalPages;
        document.getElementById('databasePagination').style.display = 'flex';
    } catch (error) {
        console.error('Error loading table:', error);
        showError('Failed to load table');
    }
}

// ============================================================================
// UPLOAD
// ============================================================================

function openUploadModal() {
    document.getElementById('uploadModal').style.display = 'block';
    document.getElementById('uploadForm').reset();
    document.getElementById('uploadProgress').style.display = 'none';
    document.getElementById('uploadResult').style.display = 'none';
}

function closeUploadModal() {
    document.getElementById('uploadModal').style.display = 'none';
}

async function handleUpload(e) {
    e.preventDefault();
    
    const fileInput = document.getElementById('uploadFile');
    if (!fileInput.files.length) {
        showError('Please select a file');
        return;
    }
    
    const formData = new FormData();
    formData.append('file', fileInput.files[0]);
    
    document.getElementById('uploadProgress').style.display = 'block';
    document.getElementById('uploadResult').style.display = 'none';
    
    try {
        const response = await fetch(`${API_BASE}/products/upload`, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${localStorage.getItem('token')}` },
            body: formData
        });
        
        const result = await response.json();
        
        document.getElementById('uploadProgress').style.display = 'none';
        document.getElementById('uploadResult').style.display = 'block';
        
        if (response.ok) {
            document.getElementById('uploadResult').innerHTML = `
                <div style="background: #d1fae5; padding: 15px; border-radius: 8px;">
                    <h4 style="color: #059669; margin-bottom: 10px;">✓ Upload Complete</h4>
                    <ul style="margin: 0; padding-left: 20px;">
                        <li>Customer Groups Created: ${result.summary.customer_groups_created}</li>
                        <li>Customers Created: ${result.summary.customers_created}</li>
                        <li>Products Created: ${result.summary.products_created}</li>
                        <li>Products Updated: ${result.summary.products_updated}</li>
                        ${result.summary.errors > 0 ? `<li style="color: #dc2626;">Errors: ${result.summary.errors}</li>` : ''}
                    </ul>
                    ${result.errors && result.errors.length > 0 ? `
                        <details style="margin-top: 10px;">
                            <summary style="cursor: pointer; color: #dc2626;">View Errors</summary>
                            <ul style="margin-top: 5px; font-size: 12px; max-height: 200px; overflow-y: auto;">
                                ${result.errors.map(e => `<li>${e}</li>`).join('')}
                            </ul>
                        </details>
                    ` : ''}
                </div>
            `;
            
            // Reload data
            loadCustomerGroups();
            loadCustomers();
            loadProducts();
        } else {
            document.getElementById('uploadResult').innerHTML = `
                <div style="background: #fee2e2; padding: 15px; border-radius: 8px; color: #dc2626;">
                    <h4>✗ Upload Failed</h4>
                    <p>${result.error || 'Unknown error'}</p>
                </div>
            `;
        }
    } catch (error) {
        document.getElementById('uploadProgress').style.display = 'none';
        document.getElementById('uploadResult').style.display = 'block';
        document.getElementById('uploadResult').innerHTML = `
            <div style="background: #fee2e2; padding: 15px; border-radius: 8px; color: #dc2626;">
                <h4>✗ Upload Failed</h4>
                <p>${error.message}</p>
            </div>
        `;
    }
}

// ============================================================================
// SYNC
// ============================================================================

async function syncPIM() {
    showToast('Syncing PIM data...');
    
    try {
        const response = await fetch(`${API_BASE}/sync/pim`, {
            method: 'POST',
            headers: getAuthHeaders()
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showSuccess(`PIM sync complete: ${result.created} created, ${result.updated} updated`);
            loadItemNumbers();
        } else {
            showError(result.error || 'PIM sync failed');
        }
    } catch (error) {
        showError('PIM sync failed: ' + error.message);
    }
}

// ============================================================================
// UTILITIES
// ============================================================================

function truncate(str, maxLength) {
    if (!str) return '-';
    return str.length > maxLength ? str.substring(0, maxLength) + '...' : str;
}

function formatDate(dateStr) {
    if (!dateStr) return '-';
    return new Date(dateStr).toLocaleDateString();
}

function formatCell(value) {
    if (value === null || value === undefined) return '-';
    if (typeof value === 'string' && value.length > 50) {
        return `<span title="${escapeHtml(value)}">${escapeHtml(value.substring(0, 50))}...</span>`;
    }
    return escapeHtml(String(value));
}

function escapeHtml(str) {
    if (!str) return '';
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

// ============================================================================
// VIEW SWITCHING
// ============================================================================

function switchView(viewName) {
    currentView = viewName;
    
    // Hide all views
    document.querySelectorAll('.view-container').forEach(v => v.classList.remove('active'));
    
    // Show selected view
    const viewElement = document.getElementById(viewName + 'View');
    if (viewElement) {
        viewElement.classList.add('active');
    }
    
    // Update tab buttons
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    document.querySelector(`.tab-btn[data-view="${viewName}"]`)?.classList.add('active');
    
    // Load data for the view
    if (viewName === 'products') {
        loadProducts();
    } else if (viewName === 'items') {
        loadItemNumbers();
        loadItemFilters();
    } else if (viewName === 'customer-admin') {
        loadCustomerGroupsTable();
        loadCustomersTable();
    }
}

// ============================================================================
// SIDEBAR
// ============================================================================

function setupSidebar() {
    const sidebar = document.getElementById('sidebar');
    const overlay = document.getElementById('sidebarOverlay');
    const toggleBtn = document.getElementById('sidebarToggle');
    const closeBtn = document.getElementById('sidebarClose');
    
    toggleBtn.addEventListener('click', () => {
        sidebar.classList.add('active');
        overlay.classList.add('active');
    });
    
    closeBtn.addEventListener('click', closeSidebar);
    overlay.addEventListener('click', closeSidebar);
    
    function closeSidebar() {
        sidebar.classList.remove('active');
        overlay.classList.remove('active');
    }
    
    // Sidebar submenu toggles
    document.querySelectorAll('.sidebar-toggle').forEach(btn => {
        btn.addEventListener('click', () => {
            const targetId = btn.getAttribute('data-target');
            const submenu = document.getElementById(targetId);
            const arrow = btn.querySelector('.sidebar-arrow');
            
            if (submenu.classList.contains('active')) {
                submenu.classList.remove('active');
                arrow.textContent = '▼';
            } else {
                submenu.classList.add('active');
                arrow.textContent = '▲';
            }
        });
    });
    
    // Sidebar buttons
    document.getElementById('sidebarUploadBtn')?.addEventListener('click', () => {
        closeSidebar();
        openUploadModal();
    });
    
    document.getElementById('sidebarCustomerAdminBtn')?.addEventListener('click', () => {
        closeSidebar();
        switchView('customer-admin');
    });
    
    document.getElementById('sidebarSyncPimBtn')?.addEventListener('click', () => {
        closeSidebar();
        syncPIM();
    });
    
    document.getElementById('sidebarExportBtn')?.addEventListener('click', () => {
        closeSidebar();
        exportProducts();
    });
}

// ============================================================================
// EXPORT
// ============================================================================

async function exportProducts() {
    showToast('Preparing export...');
    
    try {
        const response = await fetch(`${API_BASE}/products?limit=10000`, { headers: getAuthHeaders() });
        if (!response.ok) throw new Error('Failed to fetch products');
        
        const result = await response.json();
        const products = result.data;
        
        // Create CSV content
        const headers = ['Customer Group', 'Customer', 'Customer Number', 'Item Number', 'Description', 'FCL/LCL', 'Status', 'Sell Price'];
        const csvContent = [
            headers.join(','),
            ...products.map(p => [
                `"${p.customer_group_name || ''}"`,
                `"${p.customer_name || ''}"`,
                `"${p.customer_number || ''}"`,
                `"${p.item_number || ''}"`,
                `"${(p.description || '').replace(/"/g, '""')}"`,
                `"${p.fcl_lcl || ''}"`,
                `"${p.status || ''}"`,
                p.sell_price || ''
            ].join(','))
        ].join('\n');
        
        // Download
        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        link.href = URL.createObjectURL(blob);
        link.download = `products_export_${new Date().toISOString().split('T')[0]}.csv`;
        link.click();
        
        showSuccess('Export complete');
    } catch (error) {
        showError('Export failed: ' + error.message);
    }
}

// ============================================================================
// EVENT LISTENERS
// ============================================================================

document.addEventListener('DOMContentLoaded', () => {
    if (!checkAuth()) return;
    
    setupSidebar();
    
    // Load initial data
    loadCustomerGroups();
    loadCustomers();
    loadProducts();
    
    // Tab switching
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            switchView(btn.dataset.view);
        });
    });
    
    // Products filters
    document.getElementById('applyFiltersBtn').addEventListener('click', () => loadProducts(1));
    document.getElementById('clearFiltersBtn').addEventListener('click', () => {
        document.getElementById('searchInput').value = '';
        document.getElementById('customerGroupFilter').value = '';
        document.getElementById('customerFilter').value = '';
        document.getElementById('statusFilter').value = '';
        if (document.getElementById('stageFilter')) {
            document.getElementById('stageFilter').value = '';
        }
        loadProducts(1, false);
    });
    
    // Auto-apply filters when changed
    document.getElementById('customerGroupFilter').addEventListener('change', () => {
        populateCustomerFilters();
        loadProducts(1); // Auto-reload with new filter
    });
    
    document.getElementById('customerFilter').addEventListener('change', () => {
        loadProducts(1); // Auto-reload with new filter
    });
    
    document.getElementById('statusFilter').addEventListener('change', () => {
        loadProducts(1); // Auto-reload with new filter
    });
    
    if (document.getElementById('stageFilter')) {
        document.getElementById('stageFilter').addEventListener('change', () => {
            loadProducts(1); // Auto-reload with new filter
        });
    }
    
    document.getElementById('searchInput').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') loadProducts(1);
    });
    
    // Also trigger search on input after a short delay (debounce)
    let searchTimeout;
    document.getElementById('searchInput').addEventListener('input', () => {
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => loadProducts(1), 500);
    });
    
    // Products pagination
    document.getElementById('prevPage').addEventListener('click', () => {
        if (productsPage > 1) loadProducts(productsPage - 1);
    });
    document.getElementById('nextPage').addEventListener('click', () => {
        if (productsPage < productsTotalPages) loadProducts(productsPage + 1);
    });
    
    // Item Numbers filters
    document.getElementById('applyItemFiltersBtn').addEventListener('click', () => loadItemNumbers(1));
    document.getElementById('clearItemFiltersBtn').addEventListener('click', () => {
        document.getElementById('itemSearchInput').value = '';
        document.getElementById('itemSeriesFilter').value = '';
        document.getElementById('itemBrandFilter').value = '';
        document.getElementById('itemDevStatusFilter').value = '';
        loadItemNumbers(1);
    });
    
    document.getElementById('itemSearchInput').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') loadItemNumbers(1);
    });
    
    // Item Numbers pagination
    document.getElementById('itemPrevPage').addEventListener('click', () => {
        if (itemsPage > 1) loadItemNumbers(itemsPage - 1);
    });
    document.getElementById('itemNextPage').addEventListener('click', () => {
        if (itemsPage < itemsTotalPages) loadItemNumbers(itemsPage + 1);
    });
    
    // Database explorer
    document.getElementById('loadTableBtn').addEventListener('click', () => {
        const table = document.getElementById('tableSelect').value;
        loadDatabaseTable(table, 1);
    });
    
    document.getElementById('dbPrevPage').addEventListener('click', () => {
        if (dbPage > 1) loadDatabaseTable(null, dbPage - 1);
    });
    document.getElementById('dbNextPage').addEventListener('click', () => {
        if (dbPage < dbTotalPages) loadDatabaseTable(null, dbPage + 1);
    });
    
    // Upload
    document.getElementById('uploadBtn').addEventListener('click', openUploadModal);
    document.getElementById('uploadForm').addEventListener('submit', handleUpload);
    
    // Forms
    document.getElementById('customerGroupForm').addEventListener('submit', saveCustomerGroup);
    document.getElementById('customerForm').addEventListener('submit', saveCustomer);
    
    // Close modals on outside click
    window.addEventListener('click', (e) => {
        if (e.target.classList.contains('modal')) {
            e.target.style.display = 'none';
        }
    });
});

// Make functions global for onclick handlers
window.openAddCustomerGroupModal = openAddCustomerGroupModal;
window.editCustomerGroup = editCustomerGroup;
window.closeCustomerGroupModal = closeCustomerGroupModal;
window.deleteCustomerGroup = deleteCustomerGroup;
window.openAddCustomerModal = openAddCustomerModal;
window.editCustomer = editCustomer;
window.closeCustomerModal = closeCustomerModal;
window.deleteCustomer = deleteCustomer;
window.viewProduct = viewProduct;
window.closeProductModal = closeProductModal;
window.closeUploadModal = closeUploadModal;
window.logout = logout;
window.toggleStage = toggleStage;
