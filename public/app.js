const API_BASE = '/api';

// State management
let items = [];
let skus = [];
let vendorMappingData = [];
let currentItem = null;
let currentView = 'products'; // 'products', 'skus', or 'database'
let currentTable = 'products';
let currentUser = null;

// Pagination state
let productsPage = 1;
let productsLimit = 50;
let productsTotalPages = 1;

let skusPage = 1;
let skusLimit = 50;
let skusTotalPages = 1;

// ============================================================================
// Global Functions for Customer Admin (must be at top for onclick handlers)
// ============================================================================

// Store available QPI files for dropdowns
let availableQPIFiles = [];

// Edit vendor mapping row - enables editing mode
window.editVendorMappingRow = async function(id) {
    // Load QPI files if not already loaded
    if (availableQPIFiles.length === 0) {
        try {
            const response = await fetch('/api/qpi-files', {
                headers: { 'Authorization': `Bearer ${localStorage.getItem('token')}` }
            });
            if (response.ok) {
                availableQPIFiles = await response.json();
                console.log('[Customer Admin] Loaded', availableQPIFiles.length, 'QPI files');
            }
        } catch (error) {
            console.error('[Customer Admin] Error loading QPI files:', error);
        }
    }
    
    const row = document.querySelector(`tr[data-id="${id}"]`);
    if (!row) {
        alert('Row not found');
        return;
    }
    
    const cells = row.querySelectorAll('td');
    
    // Customer Code (cell 3) - text input
    const customerCodeCell = cells[3];
    const customerCodeValue = customerCodeCell.textContent.trim().replace(/<\/?code>/g, '').replace('-', '');
    customerCodeCell.innerHTML = `<input type="text" value="${customerCodeValue}" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;" data-field="customer_code">`;
    
    // QPI Source File (cell 5) - dropdown
    const qpiCell = cells[5];
    const qpiValue = qpiCell.textContent.trim().replace(/<\/?small>/g, '').replace('-', '');
    let qpiOptions = '<option value="">(None)</option>';
    availableQPIFiles.forEach(file => {
        const selected = file === qpiValue ? 'selected' : '';
        qpiOptions += `<option value="${file}" ${selected}>${file}</option>`;
    });
    qpiCell.innerHTML = `<select style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;" data-field="qpi_source_file">${qpiOptions}</select>`;
    
    // Language (cell 7) - text input
    const languageCell = cells[7];
    const languageValue = languageCell.textContent.trim().replace('-', '');
    languageCell.innerHTML = `<input type="text" value="${languageValue}" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;" data-field="language">`;
    
    // Currency (cell 8) - text input
    const currencyCell = cells[8];
    const currencyValue = currencyCell.textContent.trim().replace('-', '');
    currencyCell.innerHTML = `<input type="text" value="${currencyValue}" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;" data-field="currency">`;
    
    // Replace Edit button with Save and Cancel buttons
    const actionCell = cells[9];
    actionCell.innerHTML = `
        <button onclick="saveVendorMappingRow(${id})" class="btn-sm" style="padding: 4px 8px; font-size: 12px; background: #10b981; color: white; margin-right: 4px;">💾 Save</button>
        <button onclick="loadVendorMappingFromMain()" class="btn-sm" style="padding: 4px 8px; font-size: 12px; background: #6b7280; color: white;">✖ Cancel</button>
    `;
};

// Save vendor mapping row
window.saveVendorMappingRow = async function(id) {
    const row = document.querySelector(`tr[data-id="${id}"]`);
    if (!row) {
        alert('Row not found');
        return;
    }
    
    const cells = row.querySelectorAll('td');
    
    // Get values from inputs/selects if in edit mode, otherwise from text
    const getFieldValue = (cell, fallbackText) => {
        const input = cell.querySelector('input');
        const select = cell.querySelector('select');
        if (input) return input.value.trim();
        if (select) return select.value.trim();
        return fallbackText.replace(/<\/?code>/g, '').replace(/<\/?small>/g, '').trim();
    };
    
    const customerCode = getFieldValue(cells[3], cells[3].textContent);
    const vendorCode = cells[4].textContent.trim();
    const qpiSourceFile = getFieldValue(cells[5], cells[5].textContent);
    const vcFile = cells[6].textContent.trim();
    const language = getFieldValue(cells[7], cells[7].textContent);
    const currency = getFieldValue(cells[8], cells[8].textContent);
    
    const data = {
        customer_code: customerCode === '-' ? null : customerCode,
        vendor_code: vendorCode === '-' ? null : vendorCode,
        qpi_source_file: qpiSourceFile === '-' ? null : qpiSourceFile,
        vc_file: vcFile === '-' ? null : vcFile,
        language: language === '-' ? null : language,
        currency: currency === '-' ? null : currency
    };
    
    try {
        const response = await fetch(`/api/vendor-mapping/${id}`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${localStorage.getItem('token')}`
            },
            body: JSON.stringify(data)
        });
        
        if (response.ok) {
            const result = await response.json();
            // Show success toast
            const toast = document.createElement('div');
            toast.style.position = 'fixed';
            toast.style.top = '20px';
            toast.style.right = '20px';
            toast.style.padding = '12px 24px';
            toast.style.borderRadius = '8px';
            toast.style.backgroundColor = '#10b981';
            toast.style.color = 'white';
            toast.style.fontWeight = 'bold';
            toast.style.zIndex = '10000';
            toast.style.boxShadow = '0 4px 6px rgba(0,0,0,0.1)';
            toast.textContent = `✓ Updated ${result.changes} record(s) for ${result.country}`;
            document.body.appendChild(toast);
            setTimeout(() => toast.remove(), 3000);
            
            // Reload data
            loadVendorMappingFromMain();
        } else {
            const error = await response.json();
            alert('Error updating: ' + error.error);
        }
    } catch (error) {
        console.error('Error saving vendor mapping:', error);
        alert('Error saving changes');
    }
};

// ============================================================================
// End Global Functions
// ============================================================================

// Check authentication
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

// Setup user interface based on role
function setupUserUI() {
    // Show user info
    const userInfoHTML = `
        <div style="display: flex; align-items: center; gap: 10px; margin-left: auto;">
            ${currentUser.role === 'approver' || currentUser.role === 'admin' ? 
                '<button class="btn btn-warning btn-sm" onclick="openApprovalsModal()" id="approvalsBtn" style="position: relative;">💰 Approvals <span id="approvalsBadge" class="badge" style="display: none;"></span></button>' : ''}
            <span style="color: var(--text-secondary); font-size: 14px;">${currentUser.full_name || currentUser.username}</span>
            <span style="background: var(--primary); color: white; padding: 4px 12px; border-radius: 12px; font-size: 12px; font-weight: 600;">
                ${currentUser.role.toUpperCase()}
            </span>
            ${currentUser.role === 'admin' ? '<button class="btn btn-secondary btn-sm" onclick="window.location.href=\'/admin.html\'">👥 Admin</button>' : ''}
            <button class="btn btn-secondary btn-sm" onclick="logout()">Logout</button>
        </div>
    `;
    
    const header = document.querySelector('header');
    if (header) {
        const userDiv = document.createElement('div');
        userDiv.innerHTML = userInfoHTML;
        header.appendChild(userDiv.firstElementChild);
    }
    
    // Load notifications count for approvers
    if (currentUser.role === 'approver' || currentUser.role === 'admin') {
        loadPendingApprovalsCount();
    }
    
    // Show pricing approvals tab for approvers/admins
    if (currentUser.role === 'approver' || currentUser.role === 'admin') {
        const pricingTab = document.getElementById('pricingApprovalsTab');
        if (pricingTab) {
            pricingTab.style.display = 'inline-block';
        }
    }
    
    // Show Customer Admin for Sales Person, Approver, and Admin
    if (currentUser.role === 'sales person' || currentUser.role === 'approver' || currentUser.role === 'admin') {
        const customerAdminSection = document.getElementById('customerAdminSection');
        const customerAdminViewTab = document.getElementById('customerAdminViewTab');
        if (customerAdminSection) {
            customerAdminSection.style.display = 'block';
        }
        if (customerAdminViewTab) {
            customerAdminViewTab.style.display = 'inline-block';
        }
    }
    
    // Show Admin Console only for Admins
    if (currentUser.role === 'admin') {
        const adminConsoleSection = document.getElementById('adminConsoleSection');
        if (adminConsoleSection) {
            adminConsoleSection.style.display = 'block';
        }
    }
    
    // Hide features based on role
    if (currentUser.role === 'viewer') {
        // Viewers can only view and export
        hideElement('addItemBtn');
        hideElement('syncPimBtn');
        hideElement('importQpiBtn');
        hideElement('syncQpiBtn');
        hideElement('syncVcBtn');
        hideElement('syncVariationsBtn');
        hideElement('syncOnlineBtn');
        disableEditing();
    }
}

async function loadPendingApprovalsCount() {
    const token = localStorage.getItem('token');
    
    try {
        const response = await fetch(`${API_BASE}/pricing/submissions?status=pending`, {
            headers: {
                'Authorization': `Bearer ${token}`
            }
        });
        
        const submissions = await response.json();
        const badge = document.getElementById('approvalsBadge');
        
        if (badge && submissions.length > 0) {
            badge.textContent = submissions.length;
            badge.style.display = 'inline-block';
            badge.style.cssText = `
                display: inline-block;
                position: absolute;
                top: -8px;
                right: -8px;
                background: var(--danger);
                color: white;
                border-radius: 50%;
                padding: 2px 6px;
                font-size: 11px;
                font-weight: 600;
                min-width: 18px;
                text-align: center;
            `;
        }
    } catch (error) {
        console.error('Error loading approvals count:', error);
    }
}

function hideElement(id) {
    const element = document.getElementById(id);
    if (element) element.style.display = 'none';
}

function disableEditing() {
    // Disable delete and edit buttons for viewers
    document.addEventListener('click', (e) => {
        if (currentUser.role === 'viewer' && 
            (e.target.classList.contains('btn-danger') || 
             e.target.textContent.includes('Delete') ||
             e.target.textContent.includes('Edit'))) {
            e.preventDefault();
            e.stopPropagation();
            alert('You do not have permission to perform this action.');
            return false;
        }
    }, true);
}

function logout() {
    fetch(`${API_BASE}/auth/logout`, { method: 'POST' });
    localStorage.clear();
    window.location.href = '/login.html';
}

// Initialize app
document.addEventListener('DOMContentLoaded', () => {
    if (!checkAuth()) return;
    
    initSidebar();
    loadItems();
    loadSkus();
    loadCountries();
    loadVariationFilters();
    setupFilterSearch();
    setupEventListeners();
    setupCustomerAdminFilters();
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

    // Item Form Submit
    document.getElementById('itemForm').addEventListener('submit', handleItemSubmit);
    
    // Pricing Submission Form Submit
    document.getElementById('pricingSubmissionForm').addEventListener('submit', handlePricingSubmissionSubmit);

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
    document.getElementById('tempFilter').addEventListener('change', filterItems);
    document.getElementById('missingFilter').addEventListener('change', filterItems);
    document.getElementById('brandFilter').addEventListener('change', () => {
        loadVariationFilters(); // Reload filters for cross-filtering
        filterItems();
    });
    document.getElementById('bundleFilter').addEventListener('change', () => {
        loadVariationFilters(); // Reload filters for cross-filtering
        filterItems();
    });
    document.getElementById('ppgFilter').addEventListener('change', () => {
        loadVariationFilters(); // Reload filters for cross-filtering
        filterItems();
    });

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
async function loadItems(page = 1, applyFilters = false) {
    try {
        productsPage = page;
        const offset = (page - 1) * productsLimit;
        
        // Build query parameters
        let queryParams = `limit=${productsLimit}&offset=${offset}`;
        
        // Add filters if requested
        if (applyFilters) {
            const searchTerm = document.getElementById('searchInput').value;
            const statusFilter = document.getElementById('statusFilter').value;
            const countryFilter = document.getElementById('countryFilter').value;
            const tempFilter = document.getElementById('tempFilter').value;
            const missingFilter = document.getElementById('missingFilter').value;
            const brandFilter = document.getElementById('brandFilter');
            const bundleFilter = document.getElementById('bundleFilter');
            const ppgFilter = document.getElementById('ppgFilter');
            
            if (searchTerm) {
                queryParams += `&search=${encodeURIComponent(searchTerm)}`;
            }
            if (statusFilter !== 'all') {
                queryParams += `&stage=${statusFilter}`;
            }
            if (countryFilter !== 'all') {
                queryParams += `&country=${encodeURIComponent(countryFilter)}`;
            }
            if (tempFilter !== 'all') {
                queryParams += `&temp=${tempFilter}`;
            }
            if (missingFilter !== 'all') {
                queryParams += `&missing=${missingFilter}`;
            }
            
            // Add multi-select filters
            const selectedBrands = Array.from(brandFilter.selectedOptions)
                .map(opt => opt.value)
                .filter(val => val !== '');
            if (selectedBrands.length > 0) {
                queryParams += `&brands=${encodeURIComponent(selectedBrands.join(','))}`;
            }
            
            const selectedBundles = Array.from(bundleFilter.selectedOptions)
                .map(opt => opt.value)
                .filter(val => val !== '');
            if (selectedBundles.length > 0) {
                queryParams += `&bundles=${encodeURIComponent(selectedBundles.join(','))}`;
            }
            
            const selectedPpgs = Array.from(ppgFilter.selectedOptions)
                .map(opt => opt.value)
                .filter(val => val !== '');
            if (selectedPpgs.length > 0) {
                queryParams += `&ppgs=${encodeURIComponent(selectedPpgs.join(','))}`;
            }
        }
        
        const response = await fetch(`${API_BASE}/items?${queryParams}`);
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

// Load available countries/marketplaces from vendor mapping
async function loadCountries() {
    try {
        const response = await fetch(`${API_BASE}/countries`);
        const countries = await response.json();
        
        const countryFilter = document.getElementById('countryFilter');
        // Clear existing options except "All Countries"
        while (countryFilter.options.length > 1) {
            countryFilter.remove(1);
        }
        
        // Add new options with marketplace names
        countries.forEach(country => {
            const option = document.createElement('option');
            option.value = country.marketplace;
            option.textContent = country.marketplace;
            option.dataset.countryCode = country.country_code;
            countryFilter.appendChild(option);
        });
    } catch (error) {
        console.error('Error loading countries:', error);
    }
}

// Load filter options from variations_master
async function loadVariationFilters() {
    try {
        // Get currently selected values
        const brandFilter = document.getElementById('brandFilter');
        const bundleFilter = document.getElementById('bundleFilter');
        const ppgFilter = document.getElementById('ppgFilter');
        
        const selectedBrands = Array.from(brandFilter.selectedOptions).map(opt => opt.value);
        const selectedBundles = Array.from(bundleFilter.selectedOptions).map(opt => opt.value);
        const selectedPpgs = Array.from(ppgFilter.selectedOptions).map(opt => opt.value);
        
        // Build query params for cross-filtering
        let queryParams = [];
        if (selectedBrands.length > 0) {
            queryParams.push(`brands=${encodeURIComponent(selectedBrands.join(','))}`);
        }
        if (selectedBundles.length > 0) {
            queryParams.push(`bundles=${encodeURIComponent(selectedBundles.join(','))}`);
        }
        if (selectedPpgs.length > 0) {
            queryParams.push(`ppgs=${encodeURIComponent(selectedPpgs.join(','))}`);
        }
        
        const queryString = queryParams.length > 0 ? `?${queryParams.join('&')}` : '';
        const response = await fetch(`${API_BASE}/variation-filters${queryString}`);
        const data = await response.json();
        
        // Update Brand filter
        populateFilterOptions('brandFilter', data.brands, selectedBrands, 'All Brands');
        
        // Update Bundle filter
        populateFilterOptions('bundleFilter', data.bundles, selectedBundles, 'All Bundles');
        
        // Update PPG filter
        populateFilterOptions('ppgFilter', data.ppgs, selectedPpgs, 'All PPGs');
        
    } catch (error) {
        console.error('Error loading variation filters:', error);
    }
}

function populateFilterOptions(filterId, options, selectedValues, defaultLabel) {
    const filterElement = document.getElementById(filterId);
    
    // Save scroll position
    const scrollTop = filterElement.parentElement.scrollTop;
    
    // Clear and rebuild
    filterElement.innerHTML = `<option value="">${defaultLabel}</option>`;
    options.forEach(option => {
        const opt = document.createElement('option');
        opt.value = option;
        opt.textContent = option;
        opt.selected = selectedValues.includes(option);
        filterElement.appendChild(opt);
    });
    
    // Restore scroll
    filterElement.parentElement.scrollTop = scrollTop;
}

function setupFilterSearch() {
    const filterIds = ['brandFilter', 'bundleFilter', 'ppgFilter'];
    
    filterIds.forEach(filterId => {
        const filterElement = document.getElementById(filterId);
        const allOptions = Array.from(filterElement.options).map(opt => ({
            value: opt.value,
            text: opt.textContent
        }));
        const defaultLabel = allOptions[0].text;
        
        // Add search input above the filter
        const searchInput = document.createElement('input');
        searchInput.type = 'text';
        searchInput.placeholder = `Search ${defaultLabel.replace('All ', '')}...`;
        searchInput.className = 'filter-search';
        filterElement.parentElement.insertBefore(searchInput, filterElement);
        
        searchInput.addEventListener('input', (e) => {
            const searchTerm = e.target.value.toLowerCase();
            filterSelectOptions(filterId, allOptions, searchTerm, defaultLabel);
        });
    });
}

function filterSelectOptions(filterId, allOptions, searchTerm, defaultLabel) {
    const filterElement = document.getElementById(filterId);
    const selectedValues = Array.from(filterElement.selectedOptions).map(opt => opt.value);
    
    // Filter and rebuild options
    filterElement.innerHTML = `<option value="">${defaultLabel}</option>`;
    allOptions.slice(1).forEach(option => {
        if (option.text.toLowerCase().includes(searchTerm)) {
            const opt = document.createElement('option');
            opt.value = option.value;
            opt.textContent = option.text;
            opt.selected = selectedValues.includes(option.value);
            filterElement.appendChild(opt);
        }
    });
}

// Switch between views
function switchView(view) {
    currentView = view;
    
    console.log('[View] ========== Switching to:', view, '==========');
    
    // Update active tab
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('active');
        if (btn.dataset.view === view) {
            btn.classList.add('active');
        }
    });
    
    // Get all view elements
    const productsView = document.getElementById('productsView');
    const pricingApprovalsView = document.getElementById('pricingApprovalsView');
    const customerAdminView = document.getElementById('customerAdminView');
    const databaseView = document.getElementById('databaseView');
    
    console.log('[View] Elements found:', {
        productsView: !!productsView,
        pricingApprovalsView: !!pricingApprovalsView,
        customerAdminView: !!customerAdminView,
        databaseView: !!databaseView
    });
    
    // Remove active from all
    [productsView, pricingApprovalsView, customerAdminView, databaseView].forEach(v => {
        if (v) {
            v.classList.remove('active');
            v.style.display = 'none';
        }
    });
    
    // Add active to current view
    if (view === 'products' && productsView) {
        productsView.classList.add('active');
        productsView.style.display = 'block';
    } else if (view === 'pricing-approvals' && pricingApprovalsView) {
        pricingApprovalsView.classList.add('active');
        pricingApprovalsView.style.display = 'block';
    } else if (view === 'customer-admin' && customerAdminView) {
        customerAdminView.classList.add('active');
        customerAdminView.style.display = 'block';
        customerAdminView.style.visibility = 'visible';
        customerAdminView.style.opacity = '1';
        customerAdminView.style.position = 'relative';
        customerAdminView.style.zIndex = '1';
        
        console.log('[View] ✓ Customer Admin now active');
        console.log('[View] ✓ Display:', customerAdminView.style.display);
        console.log('[View] ✓ Has active class:', customerAdminView.classList.contains('active'));
        console.log('[View] ✓ Computed style:', window.getComputedStyle(customerAdminView).display);
        console.log('[View] ✓ offsetHeight:', customerAdminView.offsetHeight);
        console.log('[View] ✓ clientHeight:', customerAdminView.clientHeight);
        console.log('[View] ✓ scrollHeight:', customerAdminView.scrollHeight);
        console.log('[View] ✓ Parent:', customerAdminView.parentElement?.id);
        console.log('[View] ✓ Parent display:', window.getComputedStyle(customerAdminView.parentElement).display);
        console.log('[View] ✓ Parent height:', customerAdminView.parentElement?.offsetHeight);
        
        // Force reflow
        customerAdminView.offsetHeight;
        
        console.log('[View] ✓ After reflow - Visible:', customerAdminView.offsetHeight > 0);
    } else if (view === 'database' && databaseView) {
        databaseView.classList.add('active');
        databaseView.style.display = 'block';
    }
    
    // Load data based on view
    if (view === 'pricing-approvals') {
        loadPricingApprovalsView();
    } else if (view === 'customer-admin') {
        console.log('[View] About to load customer admin data...');
        setTimeout(() => loadVendorMappingFromMain(), 100); // Small delay to ensure DOM is ready
    } else if (view === 'products' && items.length > 0) {
        renderItems(items);
    } else if (view === 'database') {
        loadDatabaseTable(currentTable);
    }
}

// Load database table
async function loadDatabaseTable(tableName) {
    try {
        currentTable = tableName;
        const response = await fetch(`${API_BASE}/database/${tableName}`);
        const data = await response.json();
        renderDatabaseTable(data);
    } catch (error) {
        console.error('Error loading database table:', error);
        showError('Failed to load table');
    }
}

function renderDatabaseTable(data) {
    const container = document.getElementById('databaseTableContainer');
    
    if (!data.data || data.data.length === 0) {
        container.innerHTML = '<p>No data available</p>';
        return;
    }
    
    // Get column names from first row
    const columns = Object.keys(data.data[0]);
    
    let html = '<table><thead><tr>';
    columns.forEach(col => {
        html += `<th>${col}</th>`;
    });
    html += '</tr></thead><tbody>';
    
    data.data.forEach(row => {
        html += '<tr>';
        columns.forEach(col => {
            const value = row[col] === null ? '-' : row[col];
            html += `<td>${value}</td>`;
        });
        html += '</tr>';
    });
    
    html += '</tbody></table>';
    container.innerHTML = html;
}

// Render items in the main view
function renderItems(itemsToRender) {
    const itemsTableBody = document.getElementById('itemsTableBody');
    if (!itemsTableBody) return;
    
    if (!itemsToRender || itemsToRender.length === 0) {
        itemsTableBody.innerHTML = '<tr><td colspan="10" style="text-align: center; padding: 40px;">No products found. Click "Add New Item" to create one.</td></tr>';
        return;
    }
    
    let html = '';
    itemsToRender.forEach(item => {
        // Check if it's a temp ASIN (no PIM data yet)
        const isTempAsin = item.is_temp_asin === 1;
        const displayName = item.display_name || item.name || item.asin;
        const displayBrand = item.display_brand || item.brand || '-';
        const skus = item.skus ? (Array.isArray(item.skus) ? item.skus.join(', ') : item.skus) : '-';
        
        // Create circular stage indicators
        const s1Circle = renderTableStageCircle(1, item.stage_1_idea_considered, item.asin);
        const s2Circle = renderTableStageCircle(2, item.stage_2_product_finalized, item.asin, item.stage_2_newly_finalized);
        
        // Stage 2.5: Pricing - show submission and approval status
        let pricingCircle = '';
        let pricingClickable = '';
        
        if (!item.stage_2_product_finalized) {
            // Stage 2 not done yet - gray circle
            pricingCircle = `<div style="width: 40px; height: 40px; border-radius: 50%; background: #e5e7eb; display: flex; align-items: center; justify-content: center; color: #9ca3af; font-weight: 600; font-size: 11px; margin: 0 auto;"></div>`;
        } else if (item.stage_3b_pricing_approved) {
            // Approved - green checkmark
            pricingCircle = `<div style="width: 40px; height: 40px; border-radius: 50%; background: var(--success-color); display: flex; align-items: center; justify-content: center; color: white; font-weight: 600; font-size: 18px; margin: 0 auto;">✓</div>`;
        } else if (item.stage_3a_pricing_submitted) {
            // Submitted but not approved yet - yellow pending (clickable to view/edit)
            pricingClickable = currentUser.role !== 'viewer' ? ` style="cursor: pointer;" onclick="openPricingSubmissionModal(${item.id})" title="Click to view or resubmit pricing"` : '';
            pricingCircle = `<div${pricingClickable} style="width: 40px; height: 40px; border-radius: 50%; background: var(--warning-color); display: flex; align-items: center; justify-content: center; color: white; font-weight: 600; font-size: 18px; margin: 0 auto; position: relative; ${currentUser.role !== 'viewer' ? 'cursor: pointer; transition: transform 0.2s;' : ''}" ${currentUser.role !== 'viewer' ? 'onmouseover="this.style.transform=\'scale(1.1)\'" onmouseout="this.style.transform=\'scale(1)\'"' : ''}>
                <span>⏳</span>
            </div>
            <div style="text-align: center; margin-top: 5px; font-size: 11px; color: var(--warning-color); font-weight: 600;">Pending</div>`;
        } else {
            // Stage 2 done but pricing not submitted yet - clickable dollar sign
            pricingClickable = currentUser.role !== 'viewer' ? ` onclick="openPricingSubmissionModal(${item.id})" title="Click to submit pricing"` : '';
            pricingCircle = `<div${pricingClickable} style="width: 40px; height: 40px; border-radius: 50%; background: #e5e7eb; display: flex; align-items: center; justify-content: center; color: #6b7280; font-weight: 600; font-size: 18px; margin: 0 auto; ${currentUser.role !== 'viewer' ? 'cursor: pointer; transition: all 0.2s;' : ''}" ${currentUser.role !== 'viewer' ? 'onmouseover="this.style.background=\'var(--warning-color)\'; this.style.color=\'white\'; this.style.transform=\'scale(1.1)\';" onmouseout="this.style.background=\'#e5e7eb\'; this.style.color=\'#6b7280\'; this.style.transform=\'scale(1)\';"' : ''}>💰</div>
            <div style="text-align: center; margin-top: 5px; font-size: 11px; color: #6b7280;">Required</div>`;
        }
        
        // Stage 3: VC Listed - show country coverage
        const vcCountryCount = item.vc_country_count || 0;
        const vcTotalCountries = item.vc_total_countries || 11;
        const vcPercentage = vcTotalCountries > 0 ? Math.round((vcCountryCount / vcTotalCountries) * 100) : 0;
        const s3Circle = renderTableStageCircleWithPercentage(3, item.stage_4_product_listed, item.asin, vcCountryCount, vcTotalCountries, vcPercentage);
        
        // Stage 4: QPI - show file coverage
        const qpiFileCount = item.qpi_file_count || 0;
        const qpiTotalFiles = item.qpi_total_files || 5;
        const qpiPercentage = qpiTotalFiles > 0 ? Math.round((qpiFileCount / qpiTotalFiles) * 100) : 0;
        const s4Circle = renderTableStageCircleWithPercentage(4, item.stage_5_product_ordered, item.asin, qpiFileCount, qpiTotalFiles, qpiPercentage);
        
        // Stage 5: Online - show country coverage
        const onlineCountryCount = item.online_country_count || 0;
        const onlineTotalCountries = item.online_total_countries || 9;
        const onlinePercentage = onlineTotalCountries > 0 ? Math.round((onlineCountryCount / onlineTotalCountries) * 100) : 0;
        const s5Circle = renderTableStageCircleWithPercentage(5, item.stage_6_product_online, item.asin, onlineCountryCount, onlineTotalCountries, onlinePercentage);
        
        html += `
            <tr>
                <td>
                    ${isTempAsin ? '<span class="temp-badge" title="Temporary ASIN">TEMP</span> ' : ''}
                    ${escapeHtml(displayName)}
                    ${isTempAsin ? `<button class="btn-icon" onclick="editProductName('${item.asin}', '${escapeHtml(displayName)}')" title="Edit Name">✏️</button>` : ''}
                </td>
                <td>${escapeHtml(displayBrand)}</td>
                <td><strong>${escapeHtml(item.asin)}</strong></td>
                <td><small>${escapeHtml(skus)}</small></td>
                <td style="text-align: center;">${s1Circle}</td>
                <td style="text-align: center;">${s2Circle}</td>
                <td style="text-align: center;">${pricingCircle}</td>
                <td style="text-align: center;">${s3Circle}</td>
                <td style="text-align: center;">${s4Circle}</td>
                <td style="text-align: center;">${s5Circle}</td>
                <td>
                    <button class="btn btn-secondary btn-sm" onclick="openFlowModal(${item.id})">View</button>
                    <button class="btn btn-danger btn-sm" onclick="deleteItem(${item.id})">Delete</button>
                </td>
            </tr>
        `;
    });
    
    itemsTableBody.innerHTML = html;
}

function renderTableStageCircle(stageNumber, completed, asin, isNew = false) {
    const color = completed ? '#4caf50' : '#ddd';
    const textColor = completed ? '#fff' : '#999';
    const newBadge = isNew ? '<span style="color: #ff5722; font-size: 10px; font-weight: bold;">NEW</span>' : '';
    
    return `
        <div onclick="openWorkflowModal('${asin}', ${stageNumber})" style="cursor: pointer; display: inline-block;">
            <div style="width: 32px; height: 32px; border-radius: 50%; background-color: ${color}; display: flex; align-items: center; justify-content: center; font-weight: bold; font-size: 14px; color: ${textColor}; margin: 0 auto;">
                ${stageNumber}
            </div>
            ${newBadge}
        </div>
    `;
}

function renderTableStageCircleWithPercentage(stageNumber, completed, asin, count, total, percentage) {
    // Determine color based on percentage
    let fillColor = '#ddd';
    if (percentage >= 75) fillColor = '#4caf50';
    else if (percentage >= 50) fillColor = '#ff9800';
    else if (percentage >= 25) fillColor = '#ff5722';
    else if (percentage > 0) fillColor = '#f44336';
    
    // Calculate pie chart angle (percentage to degrees)
    const degrees = (percentage / 100) * 360;
    
    // Create conic gradient for pie chart effect
    const pieGradient = percentage > 0 
        ? `conic-gradient(${fillColor} 0deg ${degrees}deg, #e0e0e0 ${degrees}deg 360deg)`
        : '#e0e0e0';
    
    return `
        <div onclick="openWorkflowModal('${asin}', ${stageNumber})" style="cursor: pointer; display: inline-block;" title="${count} of ${total} (${percentage}%)">
            <div style="width: 40px; height: 40px; border-radius: 50%; background: ${pieGradient}; display: flex; align-items: center; justify-content: center; font-weight: bold; font-size: 11px; color: #333; margin: 0 auto; position: relative; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                <div style="background: white; border-radius: 50%; width: 28px; height: 28px; display: flex; align-items: center; justify-content: center;">
                    ${percentage}%
                </div>
            </div>
            <div style="font-size: 9px; margin-top: 2px; color: #666;">
                ${count}/${total}
            </div>
        </div>
    `;
}

function getCurrentStage(item) {
    if (item.stage_6_product_online) return 6;
    if (item.stage_5_product_ordered) return 5;
    if (item.stage_4_product_listed) return 4;
    if (item.stage_3b_pricing_approved) return '3b';
    if (item.stage_3a_pricing_submitted) return '3a';
    if (item.stage_2_product_finalized) return 2;
    if (item.stage_1_idea_considered) return 1;
    return 0;
}

function renderStageDisplay(number, label, completed, isOptional = false, itemId = null, isNewlyFinalized = false, isPricingStage = false) {
    const completedClass = completed ? 'completed' : '';
    const optionalClass = isOptional ? 'optional' : '';
    const newBadge = isNewlyFinalized ? '<span class="new-badge">NEW</span>' : '';
    const clickHandler = itemId ? `onclick="openWorkflowModal('${itemId}', ${number})"` : '';
    
    return `
        <div class="stage ${completedClass} ${optionalClass}" ${clickHandler} style="cursor: ${itemId ? 'pointer' : 'default'}">
            <div class="stage-number">${number}</div>
            <div class="stage-label">${label} ${newBadge}</div>
            ${completed ? '<div class="stage-check-icon">✓</div>' : ''}
        </div>
    `;
}

// Render SKUs view
function renderSkus(skusToRender) {
    const skusList = document.getElementById('skusList');
    if (!skusList) return;
    
    if (!skusToRender || skusToRender.length === 0) {
        skusList.innerHTML = '<div class="empty-state">No SKUs found</div>';
        return;
    }
    
    // Group SKUs by ASIN
    const skusByAsin = {};
    skusToRender.forEach(sku => {
        if (!skusByAsin[sku.asin]) {
            skusByAsin[sku.asin] = [];
        }
        skusByAsin[sku.asin].push(sku);
    });
    
    // Check for inconsistencies
    const inconsistencies = checkForInconsistencies(skusToRender);
    
    let html = '';
    Object.keys(skusByAsin).forEach(asin => {
        const skus = skusByAsin[asin];
        const firstSku = skus[0];
        const hasInconsistency = inconsistencies[asin];
        
        const currentStage = getCurrentStageForSku(firstSku);
        
        html += `
            <div class="item-card ${hasInconsistency ? 'inconsistent' : ''}">
                ${hasInconsistency ? '<div class="inconsistency-badge">⚠️ Inconsistent Progress</div>' : ''}
                <div class="item-header">
                    <div class="item-title-section">
                        <h3 class="item-title">${escapeHtml(firstSku.product_name || asin)}</h3>
                        <div class="item-meta">
                            <span><strong>ASIN:</strong> ${asin}</span>
                            <span><strong>SKUs (${skus.length}):</strong> ${skus.map(s => s.sku).join(', ')}</span>
                        </div>
                    </div>
                </div>
                
                <div class="item-stages">
                    ${renderStageDisplay(1, 'Idea', firstSku.stage_1_idea_considered, false)}
                    ${renderStageDisplay(2, 'PIM Done', firstSku.stage_2_product_finalized, false)}
                    ${renderStageDisplay('3a', 'Pricing Sub', firstSku.stage_3a_pricing_submitted, true)}
                    ${renderStageDisplay('3b', 'Pricing OK', firstSku.stage_3b_pricing_approved, true)}
                    ${renderStageDisplay(4, 'VC Listed', firstSku.stage_4_product_listed, false)}
                    ${renderStageDisplay(5, 'Ordered', firstSku.stage_5_product_ordered, false)}
                    ${renderStageDisplay(6, 'Online', firstSku.stage_6_product_online, false)}
                </div>
            </div>
        `;
    });
    
    skusList.innerHTML = html;
}

function checkForInconsistencies(skus) {
    const inconsistencies = {};
    
    // Group by ASIN and check if all SKUs have same stage values
    const skusByAsin = {};
    skus.forEach(sku => {
        if (!skusByAsin[sku.asin]) {
            skusByAsin[sku.asin] = [];
        }
        skusByAsin[sku.asin].push(sku);
    });
    
    // More logic here if needed
    
    return inconsistencies;
}

function getCurrentStageForSku(sku) {
    if (sku.stage_6_product_online) return 6;
    if (sku.stage_5_product_ordered) return 5;
    if (sku.stage_4_product_listed) return 4;
    if (sku.stage_3b_pricing_approved) return '3b';
    if (sku.stage_3a_pricing_submitted) return '3a';
    if (sku.stage_2_product_finalized) return 2;
    if (sku.stage_1_idea_considered) return 1;
    return 0;
}

// Filter items based on search and filters
async function filterItems() {
    await loadItems(1, true); // true = apply filters
}

// Open modal to add or edit item
function openItemModal(itemId = null) {
    const modal = document.getElementById('itemModal');
    const form = document.getElementById('itemForm');
    const modalTitle = document.getElementById('modalTitle');
    
    if (itemId) {
        modalTitle.textContent = 'Edit Product';
        // Load item data
        const item = items.find(i => i.id === itemId);
        if (item) {
            document.getElementById('itemId').value = item.id;
            document.getElementById('sku').value = item.sku || '';
            document.getElementById('asin').value = item.asin || '';
            document.getElementById('productName').value = item.name || '';
        }
    } else {
        modalTitle.textContent = 'Add New Product';
        form.reset();
        document.getElementById('itemId').value = '';
    }
    
    modal.style.display = 'block';
}

// Open pricing modal for a product
async function openPricingModal(itemId) {
    const modal = document.getElementById('pricingModal');
    const item = items.find(i => i.id === itemId);
    
    if (!item) {
        showError('Product not found');
        return;
    }
    
    document.getElementById('pricingItemId').value = itemId;
    document.getElementById('pricingAsin').textContent = item.asin;
    
    // Fetch existing pricing data and VC status
    const response = await fetch(`${API_BASE}/items/${item.asin}`);
    const data = await response.json();
    
    renderPricingTable(data.pricing || [], data.vc_countries || []);
    
    modal.style.display = 'block';
}

function renderPricingTable(pricingData, vcStatusData) {
    const tbody = document.querySelector('#existingPricing tbody');
    tbody.innerHTML = '';
    
    const countries = {
        'US': { name: 'United States', currency: 'USD' },
        'CA': { name: 'Canada', currency: 'CAD' },
        'UK': { name: 'United Kingdom', currency: 'GBP' },
        'DE': { name: 'Germany', currency: 'EUR' },
        'FR': { name: 'France', currency: 'EUR' },
        'IT': { name: 'Italy', currency: 'EUR' },
        'ES': { name: 'Spain', currency: 'EUR' },
        'JP': { name: 'Japan', currency: 'JPY' },
        'AU': { name: 'Australia', currency: 'AUD' }
    };
    
    Object.keys(countries).forEach(code => {
        const pricing = pricingData.find(p => p.country_code === code);
        const vcStatus = vcStatusData.find(v => v.country_code === code);
        const isInVc = !!vcStatus;
        
        if (isInVc) {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>${countries[code].name}</td>
                <td>${pricing ? pricing.cost_price + ' ' + pricing.currency : '-'}</td>
                <td>${pricing ? pricing.retail_price + ' ' + pricing.currency : '-'}</td>
                <td>${pricing ? pricing.status : '-'}</td>
                <td>
                    ${pricing && pricing.status === 'pending' ? `
                        <button class="btn btn-success btn-sm" onclick="approvePricing(${pricing.product_id}, '${code}')">Approve</button>
                        <button class="btn btn-danger btn-sm" onclick="rejectPricing(${pricing.product_id}, '${code}')">Reject</button>
                    ` : '-'}
                </td>
            `;
            tbody.appendChild(row);
        }
    });
}

// Open flow tracking modal
async function openFlowModal(itemId) {
    // Redirect to the new product details modal
    await openProductDetailsModal(itemId);
}

async function openProductDetailsModal(itemId) {
    const modal = document.getElementById('productDetailsModal');
    const item = items.find(i => i.id === itemId);
    
    if (!item) {
        showError('Product not found');
        return;
    }
    
    // Populate product information
    document.getElementById('detailAsin').textContent = item.asin || 'N/A';
    document.getElementById('detailSku').textContent = item.skus ? item.skus.join(', ') : 'N/A';
    document.getElementById('detailBrand').textContent = item.display_brand || item.brand || 'N/A';
    document.getElementById('detailName').textContent = item.display_name || item.name || 'N/A';
    document.getElementById('detailItemNumber').textContent = item.stage_1_item_number || 'N/A';
    document.getElementById('detailCountries').textContent = item.stage_1_country || 'N/A';
    document.getElementById('detailDescription').textContent = item.stage_1_description || item.product_description || 'N/A';
    document.getElementById('detailSeasonLaunch').textContent = item.stage_1_season_launch || 'N/A';
    document.getElementById('detailIsTempAsin').textContent = item.is_temp_asin ? 'Yes' : 'No';
    document.getElementById('detailCreatedAt').textContent = item.created_at || 'N/A';
    
    // Fetch country-specific data
    await loadCountryStatusData(item.asin);
    
    modal.style.display = 'block';
}

async function loadCountryStatusData(asin) {
    const tableBody = document.getElementById('countryStatusTableBody');
    tableBody.innerHTML = '<tr><td colspan="7" style="text-align: center;">Loading...</td></tr>';
    
    try {
        // Fetch all country data for this ASIN and get all marketplaces
        const [vcData, qpiData, onlineData, marketplaces] = await Promise.all([
            fetch(`${API_BASE}/database/asin_country_status?limit=10000`).then(r => r.json()),
            fetch(`${API_BASE}/database/qpi_file_tracking?limit=10000`).then(r => r.json()),
            fetch(`${API_BASE}/database/asin_online_status?limit=10000`).then(r => r.json()),
            fetch(`${API_BASE}/marketplaces`).then(r => r.json())
        ]);
        
        // Filter data for this ASIN
        const vcRecords = vcData.rows.filter(r => r.asin === asin);
        const qpiRecords = qpiData.rows.filter(r => r.asin === asin);
        const onlineRecords = onlineData.rows.filter(r => r.asin === asin);
        
        // Build country rows for ALL marketplaces
        const rows = [];
        for (const marketplace of marketplaces) {
            // Check if product is in VC for this marketplace
            const vcRecord = vcRecords.find(r => r.country_code === marketplace);
            
            // Check if product is online in this marketplace
            const onlineRecord = onlineRecords.find(r => r.country === marketplace);
            
            // QPI is per-ASIN, not per-country, so just check if any QPI records exist
            const hasQpi = qpiRecords.length > 0;
            
            rows.push(`
                <tr>
                    <td><strong>${marketplace}</strong></td>
                    <td style="text-align: center; color: ${vcRecord ? 'var(--success)' : 'var(--text-secondary)'};">${vcRecord ? '✓' : '✗'}</td>
                    <td>${vcRecord ? (vcRecord.vc_status || 'N/A') : '-'}</td>
                    <td style="text-align: center; color: ${hasQpi ? 'var(--success)' : 'var(--text-secondary)'};">${hasQpi ? '✓' : '✗'}</td>
                    <td style="text-align: center; color: ${onlineRecord ? 'var(--success)' : 'var(--text-secondary)'};">${onlineRecord ? '✓' : '✗'}</td>
                    <td>${onlineRecord && onlineRecord.last_buybox_price ? '$' + (onlineRecord.last_buybox_price / 100).toFixed(2) : '-'}</td>
                    <td>${onlineRecord ? (onlineRecord.last_seen_online || 'N/A') : '-'}</td>
                </tr>
            `);
        }
        
        if (rows.length === 0) {
            tableBody.innerHTML = '<tr><td colspan="7" style="text-align: center; color: var(--text-secondary);">No marketplaces configured</td></tr>';
        } else {
            tableBody.innerHTML = rows.join('');
        }
        
    } catch (error) {
        console.error('Error loading country status:', error);
        tableBody.innerHTML = '<tr><td colspan="7" style="text-align: center; color: var(--danger);">Error loading data</td></tr>';
    }
}

function closeProductDetailsModal() {
    document.getElementById('productDetailsModal').style.display = 'none';
}

// ============ PRICING APPROVAL WORKFLOW ============

async function openPricingSubmissionModal(productId) {
    const item = items.find(i => i.id === productId);
    if (!item) {
        showError('Product not found');
        return;
    }
    
    // Check if user has permission
    if (currentUser.role === 'viewer') {
        showError('You do not have permission to submit pricing');
        return;
    }
    
    // Populate modal
    document.getElementById('pricingProductId').value = item.id;
    document.getElementById('pricingAsin').value = item.asin;
    document.getElementById('pricingProductName').textContent = item.display_name || item.name || item.asin;
    
    // Reset form
    document.getElementById('pricingSubmissionForm').reset();
    document.getElementById('pricingCompanyMargin').value = '';
    
    // Load FX rates and render country table
    await loadFxRatesForPricing();
    
    // Show modal
    document.getElementById('pricingSubmissionModal').style.display = 'block';
}

// Global function for calculating multi-country pricing (called from HTML oninput)
let fxRates = [];

async function loadFxRatesForPricing() {
    const token = localStorage.getItem('token');
    try {
        const response = await fetch(`${API_BASE}/fx-rates`, {
            headers: {
                'Authorization': `Bearer ${token}`
            }
        });
        fxRates = await response.json();
        renderCountryPricingTable();
    } catch (error) {
        console.error('Error loading FX rates:', error);
    }
}

function renderCountryPricingTable() {
    const tbody = document.getElementById('countryPricingTableBody');
    const productCost = parseFloat(document.getElementById('pricingProductCost').value) || 0;
    
    if (fxRates.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" style="text-align: center; padding: 20px;">No countries available</td></tr>';
        return;
    }
    
    tbody.innerHTML = fxRates.map(rate => `
        <tr>
            <td><strong>${rate.country}</strong></td>
            <td>${rate.currency}</td>
            <td>${parseFloat(rate.rate_to_usd).toFixed(4)}</td>
            <td>
                <input 
                    type="number" 
                    step="0.01" 
                    min="0"
                    data-country="${rate.country}"
                    data-type="sell"
                    data-currency="${rate.currency}"
                    data-fx-rate="${rate.rate_to_usd}"
                    oninput="calculateCountryMargins(this)"
                    placeholder="0.00"
                    style="width: 100px; padding: 6px; border: 1px solid var(--border-color); border-radius: 4px;"
                    required
                />
            </td>
            <td>
                <span data-company-margin="${rate.country}" style="font-weight: 600;">-</span>
            </td>
            <td>
                <input 
                    type="number" 
                    step="0.01" 
                    min="0"
                    data-country="${rate.country}"
                    data-type="retail"
                    data-currency="${rate.currency}"
                    data-fx-rate="${rate.rate_to_usd}"
                    oninput="calculateCountryMargins(this)"
                    placeholder="0.00"
                    style="width: 120px; padding: 6px; border: 1px solid var(--border-color); border-radius: 4px;"
                    required
                />
            </td>
            <td>
                <span data-usd-price="${rate.country}">-</span>
            </td>
            <td>
                <span data-customer-margin="${rate.country}" style="font-weight: 600;">-</span>
            </td>
        </tr>
    `).join('');
}

function calculateCountryMargins(input) {
    const productCost = parseFloat(document.getElementById('pricingProductCost').value) || 0;
    const country = input.dataset.country;
    const fxRate = parseFloat(input.dataset.fxRate) || 1;
    
    // Get both sell and retail inputs for this country
    const sellInput = document.querySelector(`input[data-country="${country}"][data-type="sell"]`);
    const retailInput = document.querySelector(`input[data-country="${country}"][data-type="retail"]`);
    
    const sellPriceUSD = parseFloat(sellInput?.value) || 0;
    const retailPriceLocal = parseFloat(retailInput?.value) || 0;
    
    // Calculate Company Margin (if sell price entered)
    if (sellPriceUSD > 0 && productCost > 0) {
        const companyMargin = ((sellPriceUSD - productCost) / sellPriceUSD * 100).toFixed(2);
        const companyMarginSpan = document.querySelector(`span[data-company-margin="${country}"]`);
        
        if (companyMarginSpan) {
            companyMarginSpan.textContent = `${companyMargin}%`;
            
            // Color code based on margin
            if (companyMargin < 20) {
                companyMarginSpan.style.color = '#c40000';
            } else if (companyMargin < 35) {
                companyMarginSpan.style.color = '#f69931';
            } else {
                companyMarginSpan.style.color = '#067d62';
            }
        }
    }
    
    // Calculate Customer Margin (if both prices entered)
    if (sellPriceUSD > 0 && retailPriceLocal > 0) {
        // Convert retail price to USD
        const retailPriceUSD = retailPriceLocal / fxRate;
        
        // Calculate customer margin
        const customerMargin = ((retailPriceUSD - sellPriceUSD) / retailPriceUSD * 100).toFixed(2);
        
        // Update display
        const usdSpan = document.querySelector(`span[data-usd-price="${country}"]`);
        const customerMarginSpan = document.querySelector(`span[data-customer-margin="${country}"]`);
        
        if (usdSpan) {
            usdSpan.textContent = `$${retailPriceUSD.toFixed(2)}`;
        }
        
        if (customerMarginSpan) {
            customerMarginSpan.textContent = `${customerMargin}%`;
            
            // Color code based on margin
            if (customerMargin < 25) {
                customerMarginSpan.style.color = '#c40000';
            } else if (customerMargin < 40) {
                customerMarginSpan.style.color = '#f69931';
            } else {
                customerMarginSpan.style.color = '#067d62';
            }
        }
    }
}

function closePricingSubmissionModal() {
    document.getElementById('pricingSubmissionModal').style.display = 'none';
}

async function handlePricingSubmissionSubmit(e) {
    e.preventDefault();
    
    const productCost = parseFloat(document.getElementById('pricingProductCost').value);
    
    // Validate product cost
    if (isNaN(productCost) || productCost <= 0) {
        showError('Please enter a valid Product Cost');
        return;
    }
    
    // Collect country pricing data
    const countries = [];
    const countriesProcessed = new Set();
    
    // Get all sell price inputs
    const sellInputs = document.querySelectorAll('input[data-type="sell"]');
    
    sellInputs.forEach(sellInput => {
        const country = sellInput.dataset.country;
        const sellPriceUSD = parseFloat(sellInput.value);
        
        if (sellPriceUSD && sellPriceUSD > 0) {
            const retailInput = document.querySelector(`input[data-country="${country}"][data-type="retail"]`);
            const retailPriceLocal = parseFloat(retailInput?.value);
            
            if (retailPriceLocal && retailPriceLocal > 0) {
                const fxRate = parseFloat(sellInput.dataset.fxRate);
                const retailPriceUSD = retailPriceLocal / fxRate;
                const companyMargin = ((sellPriceUSD - productCost) / sellPriceUSD * 100).toFixed(2);
                const customerMargin = ((retailPriceUSD - sellPriceUSD) / retailPriceUSD * 100).toFixed(2);
                
                countries.push({
                    country: country,
                    currency: sellInput.dataset.currency,
                    fx_rate: fxRate,
                    sell_price_usd: sellPriceUSD,
                    company_margin: companyMargin,
                    retail_price_local: retailPriceLocal,
                    retail_price_usd: retailPriceUSD,
                    customer_margin: customerMargin
                });
                
                countriesProcessed.add(country);
            }
        }
    });
    
    if (countries.length === 0) {
        showError('Please enter both sell price and retail price for at least one country');
        return;
    }
    
    const token = localStorage.getItem('token');
    const avgSellPrice = (countries.reduce((sum, c) => sum + parseFloat(c.sell_price_usd), 0) / countries.length).toFixed(2);
    
    const data = {
        product_id: parseInt(document.getElementById('pricingProductId').value),
        asin: document.getElementById('pricingAsin').value,
        product_cost: productCost,
        sell_price: avgSellPrice, // Average sell price for summary
        countries: countries,
        notes: document.getElementById('pricingNotes').value
    };
    
    try {
        const response = await fetch(`${API_BASE}/pricing/submit`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`
            },
            body: JSON.stringify(data)
        });
        
        const result = await response.json();
        
        if (response.ok) {
            closePricingSubmissionModal();
            loadItems(); // Refresh to show updated stage
            showSuccess('Pricing submitted for approval successfully!');
        } else {
            showError(result.error || 'Failed to submit pricing');
        }
    } catch (error) {
        showError('Error submitting pricing: ' + error.message);
    }
}

// Open approvals modal (for approvers)
async function openApprovalsModal() {
    const modal = document.getElementById('approvalsModal');
    const container = document.getElementById('approvalsList');
    
    container.innerHTML = '<p style="text-align: center; padding: 40px;">Loading approvals...</p>';
    modal.style.display = 'block';
    
    const token = localStorage.getItem('token');
    
    try {
        const response = await fetch(`${API_BASE}/pricing/submissions?status=pending`, {
            headers: {
                'Authorization': `Bearer ${token}`
            }
        });
        
        const submissions = await response.json();
        
        if (submissions.length === 0) {
            container.innerHTML = '<p style="text-align: center; padding: 40px; color: var(--text-secondary);">No pending approvals</p>';
            return;
        }
        
        container.innerHTML = submissions.map(sub => `
            <div class="approval-card" style="background: var(--card-bg); padding: 20px; border-radius: 8px; margin-bottom: 15px; border-left: 4px solid var(--warning);">
                <div style="display: flex; justify-content: space-between; align-items: start;">
                    <div>
                        <h3 style="margin: 0 0 10px 0;">${sub.product_name || sub.asin}</h3>
                        <p style="color: var(--text-secondary); margin: 0;">ASIN: ${sub.asin}</p>
                        <p style="color: var(--text-secondary); margin: 5px 0 0 0; font-size: 13px;">
                            Submitted by: ${sub.submitted_by_full_name || sub.submitted_by_name} on ${new Date(sub.submitted_at).toLocaleString()}
                        </p>
                    </div>
                    <div style="text-align: right;">
                        <span style="background: var(--warning); color: white; padding: 4px 12px; border-radius: 12px; font-size: 12px; font-weight: 600;">
                            PENDING
                        </span>
                    </div>
                </div>
                
                <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 15px; margin: 20px 0; padding: 15px; background: var(--bg-secondary); border-radius: 6px;">
                    <div>
                        <small style="color: var(--text-secondary);">Product Cost</small>
                        <div style="font-size: 18px; font-weight: 600;">${sub.currency} $${parseFloat(sub.product_cost).toFixed(2)}</div>
                    </div>
                    <div>
                        <small style="color: var(--text-secondary);">Sell Price</small>
                        <div style="font-size: 18px; font-weight: 600;">${sub.currency} $${parseFloat(sub.sell_price).toFixed(2)}</div>
                    </div>
                    <div>
                        <small style="color: var(--text-secondary);">Company Margin</small>
                        <div style="font-size: 18px; font-weight: 600; color: var(--success);">${parseFloat(sub.company_margin).toFixed(2)}%</div>
                    </div>
                    <div>
                        <small style="color: var(--text-secondary);">Retail Price</small>
                        <div style="font-size: 18px; font-weight: 600;">${sub.currency} $${parseFloat(sub.retail_price).toFixed(2)}</div>
                    </div>
                    <div>
                        <small style="color: var(--text-secondary);">Customer Margin</small>
                        <div style="font-size: 18px; font-weight: 600; color: var(--success);">${parseFloat(sub.customer_margin).toFixed(2)}%</div>
                    </div>
                </div>
                
                ${sub.notes ? `<div style="padding: 10px; background: var(--bg-primary); border-radius: 6px; margin-bottom: 15px;"><strong>Notes:</strong> ${sub.notes}</div>` : ''}
                
                <div style="display: flex; gap: 10px; margin-top: 15px;">
                    <input type="text" id="reviewNotes_${sub.id}" placeholder="Add review notes (optional)" style="flex: 1;">
                    <button class="btn btn-success" onclick="reviewPricing(${sub.id}, 'approve')">✓ Approve</button>
                    <button class="btn btn-danger" onclick="reviewPricing(${sub.id}, 'reject')">✗ Reject</button>
                </div>
            </div>
        `).join('');
        
    } catch (error) {
        container.innerHTML = '<p style="text-align: center; padding: 40px; color: var(--danger);">Error loading approvals</p>';
    }
}

async function reviewPricing(submissionId, action) {
    const notes = document.getElementById(`reviewNotes_${submissionId}`)?.value || '';
    const token = localStorage.getItem('token');
    
    try {
        const response = await fetch(`${API_BASE}/pricing/${submissionId}/review`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`
            },
            body: JSON.stringify({ action, notes })
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showSuccess(`Pricing ${action === 'approve' ? 'approved' : 'rejected'} successfully!`);
            openApprovalsModal(); // Refresh the list
            loadItems(); // Refresh products to show updated stages
        } else {
            showError(result.error || 'Failed to review pricing');
        }
    } catch (error) {
        showError('Error: ' + error.message);
    }
}

function closeApprovalsModal() {
    document.getElementById('approvalsModal').style.display = 'none';
}

// Load pricing approvals view
async function loadPricingApprovalsView() {
    const container = document.getElementById('pricingApprovalsContainer');
    
    container.innerHTML = '<p style="text-align: center; padding: 40px;">Loading pricing submissions...</p>';
    
    const token = localStorage.getItem('token');
    
    try {
        const response = await fetch(`${API_BASE}/pricing/submissions`, {
            headers: {
                'Authorization': `Bearer ${token}`
            }
        });
        
        if (!response.ok) {
            throw new Error('Failed to load pricing submissions');
        }
        
        const submissions = await response.json();
        
        if (submissions.length === 0) {
            container.innerHTML = '<p style="text-align: center; padding: 40px; color: var(--text-secondary);">No pricing submissions found</p>';
            return;
        }
        
        // Render as table
        let html = `
            <div style="overflow-x: auto;">
                <table class="products-table">
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Product</th>
                            <th>ASIN</th>
                            <th>Cost</th>
                            <th>Sell Price</th>
                            <th>Company Margin</th>
                            <th>Retail Price</th>
                            <th>Customer Margin</th>
                            <th>Submitted By</th>
                            <th>Submitted At</th>
                            <th>Status</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody>
        `;
        
        submissions.forEach(sub => {
            const statusColor = sub.status === 'approved' ? 'var(--success-color)' : 
                               sub.status === 'rejected' ? 'var(--danger-color)' : 
                               'var(--warning-color)';
            const statusIcon = sub.status === 'approved' ? '✓' : 
                              sub.status === 'rejected' ? '✗' : '⏳';
            
            html += `
                <tr>
                    <td>${sub.id}</td>
                    <td>${sub.product_name || sub.asin}</td>
                    <td>${sub.asin}</td>
                    <td>${sub.currency} $${parseFloat(sub.product_cost).toFixed(2)}</td>
                    <td>${sub.currency} $${parseFloat(sub.sell_price).toFixed(2)}</td>
                    <td style="color: ${parseFloat(sub.company_margin) < 20 ? 'var(--danger-color)' : parseFloat(sub.company_margin) < 35 ? 'var(--warning-color)' : 'var(--success-color)'}; font-weight: 600;">
                        ${parseFloat(sub.company_margin).toFixed(2)}%
                    </td>
                    <td>${sub.currency} $${parseFloat(sub.retail_price).toFixed(2)}</td>
                    <td style="color: ${parseFloat(sub.customer_margin) < 25 ? 'var(--danger-color)' : parseFloat(sub.customer_margin) < 40 ? 'var(--warning-color)' : 'var(--success-color)'}; font-weight: 600;">
                        ${parseFloat(sub.customer_margin).toFixed(2)}%
                    </td>
                    <td>${sub.submitted_by_full_name || sub.submitted_by_name}</td>
                    <td>${new Date(sub.submitted_at).toLocaleString()}</td>
                    <td>
                        <span style="background: ${statusColor}; color: white; padding: 4px 12px; border-radius: 12px; font-size: 12px; font-weight: 600;">
                            ${statusIcon} ${sub.status.toUpperCase()}
                        </span>
                    </td>
                    <td>
                        ${sub.status === 'pending' && (currentUser.role === 'approver' || currentUser.role === 'admin') ? 
                            `<button class="btn btn-success btn-sm" onclick="quickApproveReject(${sub.id}, 'approve')">✓ Approve</button>
                             <button class="btn btn-danger btn-sm" onclick="quickApproveReject(${sub.id}, 'reject')">✗ Reject</button>` : 
                            sub.reviewed_at ? 
                                `<small style="color: var(--text-secondary);">Reviewed by ${sub.reviewed_by_full_name || sub.reviewed_by_name}<br>${new Date(sub.reviewed_at).toLocaleString()}</small>` : 
                                '<span style="color: var(--text-secondary);">-</span>'}
                    </td>
                </tr>
            `;
        });
        
        html += `
                    </tbody>
                </table>
            </div>
        `;
        
        container.innerHTML = html;
        
    } catch (error) {
        container.innerHTML = `<p style="text-align: center; padding: 40px; color: var(--danger-color);">Error loading pricing submissions: ${error.message}</p>`;
        console.error('Error loading pricing submissions:', error);
        showError('Failed to load pricing submissions: ' + error.message);
    }
}

// Quick approve/reject from table
async function quickApproveReject(submissionId, action) {
    const notes = prompt(`${action === 'approve' ? 'Approve' : 'Reject'} this pricing submission. Add notes (optional):`);
    if (notes === null) return; // User cancelled
    
    await reviewPricing(submissionId, action, notes);
    loadPricingApprovalsView(); // Refresh the view
}

// ============ SIDEBAR NAVIGATION ============

function initSidebar() {
    const sidebar = document.getElementById('sidebar');
    const sidebarOverlay = document.getElementById('sidebarOverlay');
    const sidebarToggle = document.getElementById('sidebarToggle');
    const sidebarClose = document.getElementById('sidebarClose');
    
    // Open sidebar
    sidebarToggle.addEventListener('click', () => {
        sidebar.classList.add('active');
        sidebarOverlay.classList.add('active');
    });
    
    // Close sidebar
    const closeSidebar = () => {
        sidebar.classList.remove('active');
        sidebarOverlay.classList.remove('active');
    };
    
    sidebarClose.addEventListener('click', closeSidebar);
    sidebarOverlay.addEventListener('click', closeSidebar);
    
    // Submenu toggle
    document.querySelectorAll('.sidebar-toggle').forEach(toggle => {
        toggle.addEventListener('click', () => {
            const targetId = toggle.dataset.target;
            const submenu = document.getElementById(targetId);
            
            toggle.classList.toggle('active');
            submenu.classList.toggle('active');
        });
    });
    
    // Wire up sidebar buttons to existing functions
    document.getElementById('sidebarExportBtn').addEventListener('click', () => {
        exportToCSV();
        closeSidebar();
    });
    
    document.getElementById('sidebarSyncPimBtn').addEventListener('click', () => {
        syncPimData();
        closeSidebar();
    });
    
    document.getElementById('sidebarSyncVcBtn').addEventListener('click', () => {
        syncVcData();
        closeSidebar();
    });
    
    document.getElementById('sidebarSyncQpiBtn').addEventListener('click', () => {
        syncQpiData();
        closeSidebar();
    });
    
    document.getElementById('sidebarImportQpiBtn').addEventListener('click', () => {
        importFromQpi();
        closeSidebar();
    });
    
    document.getElementById('sidebarSyncVariationsBtn').addEventListener('click', () => {
        syncVariations();
        closeSidebar();
    });
    
    document.getElementById('sidebarSyncOnlineBtn').addEventListener('click', () => {
        syncOnlineStatus();
        closeSidebar();
    });
    
    // Customer Admin button
    const customerAdminBtn = document.getElementById('sidebarCustomerAdminBtn');
    if (customerAdminBtn) {
        customerAdminBtn.addEventListener('click', () => {
            switchView('customer-admin');
            closeSidebar();
        });
    }
}

// Handle item form submission
async function handleItemSubmit(e) {
    e.preventDefault();
    
    const itemId = document.getElementById('itemId').value;
    const sku = document.getElementById('sku').value;
    const asin = document.getElementById('asin').value || '';  // Optional
    const name = document.getElementById('name').value || '';  // Optional
    
    // Get selected countries
    const countrySelect = document.getElementById('country');
    const selectedCountries = Array.from(countrySelect.selectedOptions).map(option => option.value);
    
    // Get other fields
    const itemNumber = document.getElementById('itemNumber').value || '';
    const brand = document.getElementById('brand').value || '';
    const description = document.getElementById('description').value || '';
    const seasonLaunch = document.getElementById('seasonLaunch').value || '';
    
    // Build item data
    const itemData = {
        sku,
        asin,
        name,
        brand,
        stage_1_country: selectedCountries.join(','),  // Store as comma-separated
        stage_1_item_number: itemNumber,
        stage_1_description: description,
        stage_1_season_launch: seasonLaunch,
        stage_1_brand: brand,
        stage_1_idea_considered: 1,  // Mark as Stage 1
        is_temp_asin: asin ? 0 : 1   // If no ASIN, mark as temp
    };
    
    try {
        let response;
        if (itemId) {
            // Update existing item
            response = await fetch(`${API_BASE}/items/${itemId}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(itemData)
            });
        } else {
            // Create new item
            response = await fetch(`${API_BASE}/items`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(itemData)
            });
        }
        
        if (response.ok) {
            document.getElementById('itemModal').style.display = 'none';
            document.getElementById('itemForm').reset();
            loadItems();
            showSuccess(itemId ? 'Product updated successfully' : 'Product created successfully');
        } else {
            const error = await response.json();
            throw new Error(error.error || 'Failed to save product');
        }
    } catch (error) {
        console.error('Error saving item:', error);
        showError(error.message);
    }
}

// Handle pricing form submission
async function handlePricingSubmit(e) {
    e.preventDefault();
    
    const itemId = document.getElementById('pricingItemId').value;
    const countryCode = document.getElementById('countryCode').value;
    const costPrice = document.getElementById('costPrice').value;
    const retailPrice = document.getElementById('retailPrice').value;
    const currency = document.getElementById('currency').value;
    
    const pricingData = {
        country_code: countryCode,
        cost_price: parseFloat(costPrice),
        retail_price: parseFloat(retailPrice),
        currency: currency
    };
    
    try {
        const response = await fetch(`${API_BASE}/items/${itemId}/pricing`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(pricingData)
        });
        
        if (response.ok) {
            document.getElementById('pricingForm').reset();
            openPricingModal(itemId); // Refresh the pricing table
            showSuccess('Pricing submitted for approval');
        } else {
            throw new Error('Failed to submit pricing');
        }
    } catch (error) {
        console.error('Error submitting pricing:', error);
        showError('Failed to submit pricing');
    }
}

// Handle stage change
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
        console.error('Error updating product name:', error);
        showError('Failed to update product name');
    }
}

async function deleteItem(itemId) {
    if (!confirm('Are you sure you want to delete this product?')) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/items/${itemId}`, {
            method: 'DELETE'
        });
        
        if (response.ok) {
            loadItems();
            showSuccess('Product deleted');
        } else {
            throw new Error('Failed to delete product');
        }
    } catch (error) {
        console.error('Error deleting item:', error);
        showError('Failed to delete product');
    }
}

// Pricing approval/rejection
async function approvePricing(itemId, countryCode) {
    try {
        const response = await fetch(`${API_BASE}/items/${itemId}/pricing/${countryCode}/approve`, {
            method: 'POST'
        });
        if (response.ok) {
            openPricingModal(itemId);
            showSuccess('Pricing approved');
        }
    } catch (error) {
        console.error('Error approving pricing:', error);
        showError('Failed to approve pricing');
    }
}

async function rejectPricing(itemId, countryCode) {
    const reason = prompt('Enter rejection reason:');
    if (!reason) return;
    
    try {
        const response = await fetch(`${API_BASE}/items/${itemId}/pricing/${countryCode}/reject`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ reason })
        });
        if (response.ok) {
            openPricingModal(itemId);
            showSuccess('Pricing rejected');
        }
    } catch (error) {
        console.error('Error rejecting pricing:', error);
        showError('Failed to reject pricing');
    }
}

// Utility functions
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Success/Error messages
function showSuccess(message) {
    alert(message); // Replace with better toast notification
}

function showError(message) {
    alert('Error: ' + message); // Replace with better toast notification
}

async function importQpiData() {
    const fileInput = document.createElement('input');
    fileInput.type = 'file';
    fileInput.accept = '.xlsx,.csv';
    
    fileInput.onchange = async (e) => {
        const file = e.target.files[0];
        if (!file) return;
        
        const formData = new FormData();
        formData.append('file', file);
        
        try {
            const response = await fetch(`${API_BASE}/import-qpi`, {
                method: 'POST',
                body: formData
            });
            
            if (response.ok) {
                loadItems();
                showSuccess('QPI data imported successfully');
            } else {
                throw new Error('Failed to import QPI data');
            }
        } catch (error) {
            console.error('Error importing QPI:', error);
            showError('Failed to import QPI data');
        }
    };
    
    fileInput.click();
}

async function syncQpiData() {
    try {
        const response = await fetch(`${API_BASE}/sync-qpi`, {
            method: 'POST'
        });
        
        if (response.ok) {
            const result = await response.json();
            loadItems();
            showSuccess(`QPI sync complete: ${result.message}`);
        } else {
            throw new Error('Failed to sync QPI data');
        }
    } catch (error) {
        console.error('Error syncing QPI:', error);
        showError('Failed to sync QPI data');
    }
}

async function syncVcData() {
    try {
        const response = await fetch(`${API_BASE}/sync-vc`, {
            method: 'POST'
        });
        
        if (response.ok) {
            const result = await response.json();
            loadItems();
            showSuccess(`VC sync complete: ${result.message}`);
        } else {
            throw new Error('Failed to sync VC data');
        }
    } catch (error) {
        console.error('Error syncing VC:', error);
        showError('Failed to sync VC data');
    }
}

async function syncVariationsData() {
    try {
        const response = await fetch(`${API_BASE}/sync-variations`, {
            method: 'POST'
        });
        
        if (response.ok) {
            const result = await response.json();
            loadVariationFilters(); // Reload filter options
            loadItems(); // Refresh items to show new variation data
            showSuccess(`Variations sync complete: ${result.message}`);
        } else {
            throw new Error('Failed to sync variations data');
        }
    } catch (error) {
        console.error('Error syncing variations:', error);
        showError('Failed to sync variations data');
    }
}

async function syncOnlineStatus() {
    try {
        const response = await fetch(`${API_BASE}/sync-online`, {
            method: 'POST'
        });
        
        if (response.ok) {
            const result = await response.json();
            loadItems();
            showSuccess(`Online status sync complete: ${result.message}`);
        } else {
            throw new Error('Failed to sync online status');
        }
    } catch (error) {
        console.error('Error syncing online status:', error);
        showError('Failed to sync online status');
    }
}

async function syncPimData() {
    try {
        const response = await fetch(`${API_BASE}/sync-pim`, {
            method: 'POST'
        });
        
        if (response.ok) {
            const result = await response.json();
            loadItems();
            showSuccess(`PIM sync complete: ${result.message}`);
        } else {
            throw new Error('Failed to sync PIM data');
        }
    } catch (error) {
        console.error('Error syncing PIM:', error);
        showError('Failed to sync PIM data');
    }
}

function openExportModal() {
    // Simple CSV export for now
    let csv = 'ASIN,Name,SKU,Stage\n';
    
    items.forEach(item => {
        const stage = getCurrentStage(item);
        csv += `"${item.asin}","${item.name || ''}","${item.sku || ''}","${stage}"\n`;
    });
    
    // Download CSV
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = 'products_export.csv';
    link.click();
    
    showSuccess('Export complete');
}

// Pagination Controls
function updatePaginationControls(view, currentPage, totalPages) {
    if (view === 'products') {
        const pageInfo = document.getElementById('productsPageInfo');
        const prevBtn = document.getElementById('productsPrevBtn');
        const nextBtn = document.getElementById('productsNextBtn');
        
        if (pageInfo) pageInfo.textContent = `Page ${currentPage} of ${totalPages}`;
        if (prevBtn) prevBtn.disabled = currentPage === 1;
        if (nextBtn) nextBtn.disabled = currentPage === totalPages;
    } else if (view === 'skus') {
        const pageInfo = document.getElementById('skusPageInfo');
        const prevBtn = document.getElementById('skusPrevBtn');
        const nextBtn = document.getElementById('skusNextBtn');
        
        if (pageInfo) pageInfo.textContent = `Page ${currentPage} of ${totalPages}`;
        if (prevBtn) prevBtn.disabled = currentPage === 1;
        if (nextBtn) nextBtn.disabled = currentPage === totalPages;
    }
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
        3: 'Stage 3: VC Listed',
        4: 'Stage 4: Ordered (QPI)',
        5: 'Stage 5: Online'
    };
    
    title.textContent = `${stageNames[stageNumber]} - ${asin}`;
    
    // If it's Stage 3 (VC Listed), show country status
    if (stageNumber === 3) {
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
    } else if (stageNumber === 4) {
        // Stage 4: QPI Status - show which QPI source files contain this ASIN
        content.innerHTML = '<p style="text-align: center; color: var(--text-secondary);">Loading QPI status...</p>';
        modal.style.display = 'block';
        
        try {
            // Fetch the product details first to get SKUs
            const productResponse = await fetch(`${API_BASE}/items/${asin}`);
            const productData = await productResponse.json();
            
            // Fetch QPI file status
            const qpiResponse = await fetch(`${API_BASE}/qpi-files/${asin}`);
            const qpiData = await qpiResponse.json();
            
            console.log('QPI Data:', qpiData); // Debug log
            
            const inQPI = productData.stage_5_product_ordered === 1;
            const skus = productData.skus || [];
            const filesFound = qpiData.files.filter(f => f.found).length;
            const totalFiles = qpiData.total_source_files;
            
            console.log(`Files found: ${filesFound}/${totalFiles}`, qpiData.files); // Debug log
            
            // Map source file names to regions
            const regionMap = {
                'S26 QPI CA.xlsx': 'Canada',
                'S26 QPI EMG.xlsx': 'Emerging Markets',
                'S26 QPI EU.xlsx': 'Europe',
                'S26 QPI JP003.xlsx': 'Japan',
                'S26 QPI US.xlsx': 'United States'
            };
            
            content.innerHTML = `
                <div style="margin-bottom: 20px;">
                    <h3>QPI Status by Source File</h3>
                    <p>ASIN: <strong>${asin}</strong></p>
                    <p>SKUs: <strong>${skus.join(', ')}</strong></p>
                    <p>In QPI: <strong style="color: ${inQPI ? 'var(--success-color)' : 'var(--danger-color)'}">
                        ${inQPI ? `✓ YES - Found in ${filesFound}/${totalFiles} source files` : '✗ NO - Not in any QPI source files'}
                    </strong></p>
                </div>
                
                <div class="country-status-table-wrapper">
                    <table class="country-status-table">
                        <thead>
                            <tr>
                                <th>Source File (Region)</th>
                                <th>Status</th>
                                <th>SKU</th>
                                <th>Last Seen</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${qpiData.files.map(file => {
                                const region = regionMap[file.source_file] || file.source_file;
                                return `
                                    <tr class="${file.found ? 'listed' : 'not-listed'}">
                                        <td>
                                            <strong>${escapeHtml(file.source_file)}</strong>
                                            <br><small style="color: var(--text-secondary);">${region}</small>
                                        </td>
                                        <td>
                                            ${file.found 
                                                ? '<span class="status-badge listed">✓ Found</span>' 
                                                : '<span class="status-badge not-listed">✗ Missing</span>'}
                                        </td>
                                        <td>${escapeHtml(file.sku || '-')}</td>
                                        <td><small>${file.last_seen ? new Date(file.last_seen).toLocaleString() : '-'}</small></td>
                                    </tr>
                                `;
                            }).join('')}
                        </tbody>
                    </table>
                </div>
                
                <div style="margin-top: 20px;">
                    <p style="font-size: 14px; color: var(--text-secondary);">
                        <strong>Note:</strong> Shows all QPI source files. 
                        This ASIN appears in <strong>${filesFound} out of ${totalFiles}</strong> source files.
                        ${filesFound < totalFiles ? ' Missing from some regions - may need to be added to additional QPIs.' : ' Present in all source files!'}
                    </p>
                </div>
                
                <div style="margin-top: 20px; text-align: right;">
                    <button class="btn btn-primary" onclick="document.getElementById('workflowModal').style.display='none'">Close</button>
                </div>
            `;
        } catch (error) {
            console.error('Error loading QPI status:', error);
            content.innerHTML = `
                <p style="color: var(--danger-color);">Error loading QPI status</p>
                <div style="margin-top: 20px;">
                    <button class="btn btn-primary" onclick="document.getElementById('workflowModal').style.display='none'">Close</button>
                </div>
            `;
        }
    } else if (stageNumber === 5) {
        // Stage 5: Online Status - show which countries ASIN is online in
        content.innerHTML = '<p style="text-align: center; color: var(--text-secondary);">Loading online status...</p>';
        modal.style.display = 'block';
        
        try {
            // Fetch online status
            const response = await fetch(`${API_BASE}/online-status/${asin}`);
            const data = await response.json();
            
            const countriesOnline = data.countries_online;
            const totalCountries = data.total_countries;
            
            content.innerHTML = `
                <div style="margin-bottom: 20px;">
                    <h3>Online Status by Country</h3>
                    <p>ASIN: <strong>${asin}</strong></p>
                    <p>Online Status: <strong style="color: ${countriesOnline > 0 ? 'var(--success-color)' : 'var(--danger-color)'}">
                        ${countriesOnline > 0 ? `✓ ONLINE - Found in ${countriesOnline}/${totalCountries} countries` : '✗ NOT ONLINE - Not found in any country'}
                    </strong></p>
                </div>
                
                <div class="country-status-table-wrapper">
                    <table class="country-status-table">
                        <thead>
                            <tr>
                                <th>Country</th>
                                <th>Status</th>
                                <th>First Seen Online</th>
                                <th>Last Seen Online</th>
                                <th>Last Buybox Price</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${data.countries.map(country => {
                                return `
                                    <tr class="${country.online ? 'listed' : 'not-listed'}">
                                        <td>
                                            <strong>${escapeHtml(country.country)}</strong>
                                            <br><small style="color: var(--text-secondary);">${escapeHtml(country.country_code)}</small>
                                        </td>
                                        <td>
                                            ${country.online 
                                                ? '<span class="status-badge listed">✓ Online</span>' 
                                                : '<span class="status-badge not-listed">✗ Not Online</span>'}
                                        </td>
                                        <td><small>${country.first_seen ? escapeHtml(country.first_seen) : '-'}</small></td>
                                        <td><small>${country.last_seen ? escapeHtml(country.last_seen) : '-'}</small></td>
                                        <td><strong>${country.last_price ? '$' + parseFloat(country.last_price).toFixed(2) : '-'}</strong></td>
                                    </tr>
                                `;
                            }).join('')}
                        </tbody>
                    </table>
                </div>
                
                <div style="margin-top: 20px;">
                    <p style="font-size: 14px; color: var(--text-secondary);">
                        <strong>Note:</strong> Shows all Amazon marketplaces tracked. 
                        This ASIN is online in <strong>${countriesOnline} out of ${totalCountries}</strong> countries.
                        ${countriesOnline < totalCountries ? ' May launch in additional countries soon.' : ' Live in all tracked countries!'}
                    </p>
                </div>
                
                <div style="margin-top: 20px; text-align: right;">
                    <button class="btn btn-primary" onclick="document.getElementById('workflowModal').style.display='none'">Close</button>
                </div>
            `;
        } catch (error) {
            console.error('Error loading online status:', error);
            content.innerHTML = `
                <p style="color: var(--danger-color);">Error loading online status: ${error.message}</p>
                <div style="margin-top: 20px;">
                    <button class="btn btn-primary" onclick="document.getElementById('workflowModal').style.display='none'">Close</button>
                </div>
            `;
        }
    } else {
        // For other stages, show basic info
        content.innerHTML = '<p style="text-align: center; color: var(--text-secondary);">Loading details...</p>';
        modal.style.display = 'block';
        
        try {
            // Fetch product details
            const response = await fetch(`${API_BASE}/items/${asin}`);
            const product = await response.json();
            
            if (stageNumber === 1) {
                // Stage 1: Ideation - show planned countries and other details
                const countries = product.stage_1_country ? product.stage_1_country.split(',') : [];
                const brand = product.stage_1_brand || '-';
                const description = product.stage_1_description || '-';
                const firstOrderDate = product.stage_1_season_launch || '-';
                const itemNumber = product.stage_1_item_number || '-';
                
                const countryNames = {
                    'US': 'United States',
                    'CA': 'Canada',
                    'MX': 'Mexico',
                    'UK': 'United Kingdom',
                    'DE': 'Germany',
                    'FR': 'France',
                    'IT': 'Italy',
                    'ES': 'Spain',
                    'JP': 'Japan',
                    'AU': 'Australia'
                };
                
                content.innerHTML = `
                    <div style="margin-bottom: 20px;">
                        <h3>Ideation Details</h3>
                        <p>ASIN: <strong>${escapeHtml(asin)}</strong></p>
                    </div>
                    
                    <div style="background: var(--background); padding: 15px; border-radius: 8px; margin-bottom: 15px;">
                        <h4 style="margin-top: 0; color: var(--primary-color);">📍 Planned Launch Countries</h4>
                        ${countries.length > 0 ? `
                            <div style="display: flex; flex-wrap: wrap; gap: 8px; margin-top: 10px;">
                                ${countries.map(code => `
                                    <span style="background: var(--primary-color); color: white; padding: 6px 12px; border-radius: 20px; font-size: 13px; font-weight: 500;">
                                        ${escapeHtml(countryNames[code] || code)}
                                    </span>
                                `).join('')}
                            </div>
                        ` : '<p style="color: var(--text-secondary);">No countries specified</p>'}
                    </div>
                    
                    <div style="background: var(--background); padding: 15px; border-radius: 8px; margin-bottom: 15px;">
                        <table style="width: 100%; border-collapse: collapse;">
                            <tr>
                                <td style="padding: 8px 0; font-weight: 600; color: var(--text-secondary); width: 150px;">Brand:</td>
                                <td style="padding: 8px 0;">${escapeHtml(brand)}</td>
                            </tr>
                            <tr>
                                <td style="padding: 8px 0; font-weight: 600; color: var(--text-secondary);">Item Number:</td>
                                <td style="padding: 8px 0;">${escapeHtml(itemNumber)}</td>
                            </tr>
                            <tr>
                                <td style="padding: 8px 0; font-weight: 600; color: var(--text-secondary);">First Order Date:</td>
                                <td style="padding: 8px 0;">${escapeHtml(firstOrderDate)}</td>
                            </tr>
                            <tr>
                                <td style="padding: 8px 0; font-weight: 600; color: var(--text-secondary); vertical-align: top;">Description:</td>
                                <td style="padding: 8px 0;">${escapeHtml(description)}</td>
                            </tr>
                        </table>
                    </div>
                    
                    <div style="margin-top: 20px; text-align: right;">
                        <button class="btn btn-primary" onclick="document.getElementById('workflowModal').style.display='none'">Close</button>
                    </div>
                `;
            } else {
                // Other stages - show basic info
                content.innerHTML = `
                    <p>Workflow details for ASIN: <strong>${asin}</strong></p>
                    <p>Stage: <strong>${stageNames[stageNumber]}</strong></p>
                    <div style="margin-top: 20px; text-align: right;">
                        <button class="btn btn-primary" onclick="document.getElementById('workflowModal').style.display='none'">Close</button>
                    </div>
                `;
            }
        } catch (error) {
            console.error('Error loading stage details:', error);
            content.innerHTML = `
                <p style="color: var(--danger-color);">Error loading details</p>
                <div style="margin-top: 20px; text-align: right;">
                    <button class="btn btn-primary" onclick="document.getElementById('workflowModal').style.display='none'">Close</button>
                </div>
            `;
        }
    }
}

document.getElementById('workflowModalClose').addEventListener('click', () => {
    document.getElementById('workflowModal').style.display = 'none';
});

// Add close handlers for pricing modals
document.querySelectorAll('#pricingSubmissionModal .close, #approvalsModal .close').forEach(closeBtn => {
    closeBtn.addEventListener('click', (e) => {
        e.target.closest('.modal').style.display = 'none';
    });
});

// Close modals when clicking outside
window.addEventListener('click', (e) => {
    if (e.target.classList.contains('modal')) {
        e.target.style.display = 'none';
    }
});

// ============================================
// REPORTS FUNCTIONS
// ============================================

async function generateReport(reportType) {
    let endpoint = '';
    let containerId = '';
    let reportTitle = '';
    
    switch(reportType) {
        case 'temp-asins':
            endpoint = `${API_BASE}/reports/temp-asins`;
            containerId = 'tempAsinsReport';
            reportTitle = 'Temporary ASINs Not in PIM';
            break;
        case 'pim-not-vc':
            endpoint = `${API_BASE}/reports/pim-not-vc`;
            containerId = 'pimNotVcReport';
            reportTitle = 'PIM SKUs Not in VC';
            break;
        case 'vc-not-qpi':
            endpoint = `${API_BASE}/reports/vc-not-qpi`;
            containerId = 'vcNotQpiReport';
            reportTitle = 'VC Listed Products Not in QPI';
            break;
        default:
            showError('Unknown report type');
            return;
    }
    
    const container = document.getElementById(containerId);
    container.style.display = 'block';
    container.innerHTML = '<p style="text-align: center; padding: 20px; color: var(--text-secondary);">Loading report...</p>';
    
    try {
        const response = await fetch(endpoint);
        const result = await response.json();
        
        if (result.data && result.data.length > 0) {
            let tableHTML = `
                <div class="report-summary">
                    📊 Found ${result.total} ${result.total === 1 ? 'record' : 'records'}
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>ASIN</th>
                            <th>Name</th>
                            <th>SKUs</th>
                            <th>Item Number</th>
                            <th>Brand</th>
                            <th>Countries</th>
                            <th>Created</th>
                        </tr>
                    </thead>
                    <tbody>
            `;
            
            result.data.forEach(row => {
                const skus = Array.isArray(row.skus) ? row.skus.join(', ') : (row.skus || '-');
                const itemNumber = row.stage_1_item_number || '-';
                const brand = row.stage_1_brand || '-';
                const countries = row.stage_1_country || '-';
                const name = row.name || row.asin;
                const createdDate = new Date(row.created_at).toLocaleDateString();
                
                tableHTML += `
                    <tr>
                        <td><strong>${escapeHtml(row.asin)}</strong></td>
                        <td>${escapeHtml(name)}</td>
                        <td>${escapeHtml(skus)}</td>
                        <td>${escapeHtml(itemNumber)}</td>
                        <td>${escapeHtml(brand)}</td>
                        <td>${escapeHtml(countries)}</td>
                        <td><small>${createdDate}</small></td>
                    </tr>
                `;
            });
            
            tableHTML += `
                    </tbody>
                </table>
                <div style="margin-top: 15px; text-align: center;">
                    <button class="btn btn-secondary" onclick="exportReportExcel('${reportType}', '${reportTitle}')">
                        📥 Export to Excel
                    </button>
                </div>
            `;
            
            container.innerHTML = tableHTML;
        } else {
            container.innerHTML = `
                <div class="report-empty">
                    <div class="report-empty-icon">✅</div>
                    <h3>No Issues Found</h3>
                    <p>All records are in good standing for this report.</p>
                </div>
            `;
        }
    } catch (error) {
        console.error('Error generating report:', error);
        container.innerHTML = `
            <div class="report-empty">
                <div class="report-empty-icon">❌</div>
                <h3>Error Loading Report</h3>
                <p>Failed to generate report. Please try again.</p>
            </div>
        `;
        showError('Failed to generate report');
    }
}

async function exportReportExcel(reportType, reportTitle) {
    try {
        const url = `${API_BASE}/reports/${reportType}/export`;
        
        // Create a temporary link to download the file
        const link = document.createElement('a');
        link.href = url;
        link.download = `${reportTitle.replace(/\s+/g, '_')}_${new Date().toISOString().split('T')[0]}.xlsx`;
        
        // Trigger download
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        
        showSuccess('Report exported to Excel successfully');
    } catch (error) {
        console.error('Error exporting report:', error);
        showError('Failed to export report');
    }
}

// ============ VENDOR MAPPING / CUSTOMER ADMIN (Main App) ============

async function loadVendorMappingFromMain() {
    const token = localStorage.getItem('token');
    
    console.log('[Customer Admin] Loading vendor mapping...');
    
    try {
        const response = await fetch(`${API_BASE}/vendor-mapping`, {
            headers: {
                'Authorization': `Bearer ${token}`
            }
        });
        
        if (!response.ok) {
            throw new Error('Failed to load vendor mapping');
        }
        
        vendorMappingData = await response.json();
        console.log('[Customer Admin] Loaded', vendorMappingData.length, 'records');
        
        renderVendorMappingFromMain(vendorMappingData);
        updateMappingStatsFromMain(vendorMappingData);
        
        console.log('[Customer Admin] Rendering complete');
    } catch (error) {
        console.error('[Customer Admin] Error loading vendor mapping:', error);
        showError('Failed to load vendor mapping: ' + error.message);
        
        // Show error in the table
        const tbody = document.getElementById('mainVendorMappingTableBody');
        if (tbody) {
            tbody.innerHTML = `<tr><td colspan="8" style="text-align: center; padding: 40px; color: var(--danger-color);">Error: ${error.message}</td></tr>`;
        }
    }
}

function renderVendorMappingFromMain(data) {
    console.log('[Customer Admin] Rendering', data?.length || 0, 'rows');
    
    const tbody = document.getElementById('mainVendorMappingTableBody');
    
    if (!tbody) {
        console.error('[Customer Admin] mainVendorMappingTableBody element not found!');
        return;
    }
    
    console.log('[Customer Admin] Table body element found');
    
    if (!data || data.length === 0) {
        console.warn('[Customer Admin] No data to display');
        tbody.innerHTML = '<tr><td colspan="9" style="text-align: center; padding: 40px; color: var(--text-secondary); border: 1px solid #e5e7eb;">No mappings found</td></tr>';
        return;
    }
    
    tbody.innerHTML = data.map(row => `
        <tr data-id="${row.id}">
            <td style="padding: 12px; border: 1px solid #e5e7eb;">${row.customer || '-'}</td>
            <td style="padding: 12px; border: 1px solid #e5e7eb;">${row.country || '-'}</td>
            <td style="padding: 12px; border: 1px solid #e5e7eb;"><strong>${row.keepa_marketplace || '-'}</strong></td>
            <td style="padding: 12px; border: 1px solid #e5e7eb;" data-field="customer_code"><code>${row.customer_code || '-'}</code></td>
            <td style="padding: 12px; border: 1px solid #e5e7eb;">${row.vendor_code || '-'}</td>
            <td style="padding: 12px; border: 1px solid #e5e7eb;" data-field="qpi_source_file"><small>${row.qpi_source_file || '-'}</small></td>
            <td style="padding: 12px; border: 1px solid #e5e7eb;"><small>${row.vc_file || '-'}</small></td>
            <td style="padding: 12px; border: 1px solid #e5e7eb;" data-field="language">${row.language || '-'}</td>
            <td style="padding: 12px; border: 1px solid #e5e7eb;" data-field="currency">${row.currency || '-'}</td>
            <td style="padding: 12px; border: 1px solid #e5e7eb; text-align: center;">
                <button onclick="editVendorMappingRow(${row.id})" class="btn-sm" style="padding: 4px 8px; font-size: 12px;">✏️ Edit</button>
            </td>
        </tr>
    `).join('');
    
    console.log('[Customer Admin] Rendered', tbody.children.length, 'rows in table');
}

function updateMappingStatsFromMain(data) {
    const statsElement = document.getElementById('mainMappingStatsContent');
    
    if (!statsElement) {
        console.error('mainMappingStatsContent element not found');
        return;
    }
    
    if (!data || data.length === 0) {
        statsElement.innerHTML = 'No data available';
        return;
    }
    
    const stats = {
        totalRecords: data.length,
        uniqueCountries: new Set(data.map(r => r.country)).size,
        uniqueMarketplaces: new Set(data.map(r => r.keepa_marketplace)).size,
        uniqueVendorCodes: new Set(data.map(r => r.vendor_code)).size,
        uniqueQPIs: new Set(data.map(r => r.qpi_source_file).filter(Boolean)).size,
        uniqueCurrencies: new Set(data.map(r => r.currency).filter(Boolean)).size
    };
    
    statsElement.innerHTML = `
        ${stats.totalRecords} records | 
        ${stats.uniqueCountries} countries | 
        ${stats.uniqueMarketplaces} marketplaces | 
        ${stats.uniqueVendorCodes} vendor codes | 
        ${stats.uniqueQPIs} QPI files | 
        ${stats.uniqueCurrencies} currencies
    `;
}

async function syncVendorMappingFromMain() {
    const token = localStorage.getItem('token');
    
    if (!confirm('This will re-sync the vendor mapping from the Excel file. Continue?')) {
        return;
    }
    
    try {
        showInfo('Syncing vendor mapping...');
        
        const response = await fetch(`${API_BASE}/sync/vendor-mapping`, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${token}`
            }
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showSuccess(result.message || 'Vendor mapping synced successfully!');
            loadVendorMappingFromMain(); // Reload data
        } else {
            showError('Error: ' + (result.error || 'Failed to sync vendor mapping'));
        }
    } catch (error) {
        showError('Error syncing vendor mapping: ' + error.message);
    }
}

function setupCustomerAdminFilters() {
    // No filters needed anymore
}
