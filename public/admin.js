const API_BASE = '/api';
let currentUser = null;
let users = [];
let customerGroups = [];
let customers = [];
let fxRatesData = [];
const changedFxRates = new Set();

// Check authentication on page load
document.addEventListener('DOMContentLoaded', () => {
    checkAuth();
    loadUsers();
    loadCustomerGroups();
    loadCustomers();
    loadFxRates();
    
    // Setup tab switching
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const tab = btn.dataset.tab;
            switchTab(tab);
        });
    });
    
    // Setup form handlers
    document.getElementById('userForm').addEventListener('submit', handleUserSubmit);
    document.getElementById('customerGroupForm').addEventListener('submit', handleCustomerGroupSubmit);
    document.getElementById('customerForm').addEventListener('submit', handleCustomerSubmit);
    
    // Close modals on outside click
    window.addEventListener('click', (e) => {
        if (e.target.classList.contains('modal')) {
            e.target.style.display = 'none';
        }
    });
});

function switchTab(tab) {
    // Update tab buttons
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tab === tab);
    });
    
    // Show/hide content
    document.querySelectorAll('.admin-tab-content').forEach(content => {
        content.classList.remove('active');
    });
    
    const tabMap = {
        'users': 'usersTab',
        'customer-groups': 'customerGroupsTab',
        'customers': 'customersTab',
        'fx-rates': 'fxRatesTab'
    };
    
    const tabElement = document.getElementById(tabMap[tab]);
    if (tabElement) {
        tabElement.classList.add('active');
    }
}

async function checkAuth() {
    const token = localStorage.getItem('token');
    const user = JSON.parse(localStorage.getItem('user') || '{}');
    
    if (!token || !user.role) {
        window.location.href = '/login.html';
        return;
    }
    
    if (user.role !== 'admin') {
        alert('Access denied. Admin privileges required.');
        window.location.href = '/';
        return;
    }
    
    currentUser = user;
    document.getElementById('currentUser').textContent = user.full_name || user.username;
}

function getAuthHeaders() {
    return {
        'Authorization': `Bearer ${localStorage.getItem('token')}`,
        'Content-Type': 'application/json'
    };
}

// ============ USER MANAGEMENT ============

async function loadUsers() {
    const tableBody = document.getElementById('usersTableBody');
    
    try {
        const response = await fetch(`${API_BASE}/users`, {
            headers: getAuthHeaders()
        });
        
        if (response.status === 401) {
            localStorage.clear();
            window.location.href = '/login.html';
            return;
        }
        
        users = await response.json();
        renderUsers();
    } catch (error) {
        tableBody.innerHTML = '<tr><td colspan="7" style="text-align: center; color: #dc2626;">Error loading users</td></tr>';
    }
}

function renderUsers() {
    const tableBody = document.getElementById('usersTableBody');
    
    if (users.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="7" style="text-align: center;">No users found</td></tr>';
        return;
    }
    
    tableBody.innerHTML = users.map(user => `
        <tr>
            <td><strong>${user.username}</strong></td>
            <td>${user.full_name || '-'}</td>
            <td>${user.email}</td>
            <td><span class="role-badge role-${user.role}">${user.role}</span></td>
            <td class="${user.is_active ? 'status-active' : 'status-inactive'}">
                ${user.is_active ? '✓ Active' : '✗ Inactive'}
            </td>
            <td>${user.last_login ? new Date(user.last_login).toLocaleString() : 'Never'}</td>
            <td class="action-buttons">
                <button class="btn btn-secondary btn-sm" onclick="editUser(${user.id})">Edit</button>
                ${user.id !== currentUser.id ? `<button class="btn btn-danger btn-sm" onclick="deleteUser(${user.id}, '${user.username}')">Delete</button>` : ''}
            </td>
        </tr>
    `).join('');
}

function openAddUserModal() {
    document.getElementById('userModalTitle').textContent = 'Add New User';
    document.getElementById('userForm').reset();
    document.getElementById('userId').value = '';
    document.getElementById('passwordHint').textContent = '*';
    document.getElementById('userPassword').required = true;
    document.getElementById('activeGroup').style.display = 'none';
    document.getElementById('userModal').style.display = 'block';
}

function editUser(userId) {
    const user = users.find(u => u.id === userId);
    if (!user) return;
    
    document.getElementById('userModalTitle').textContent = 'Edit User';
    document.getElementById('userId').value = user.id;
    document.getElementById('userUsername').value = user.username;
    document.getElementById('userEmail').value = user.email;
    document.getElementById('userFullName').value = user.full_name || '';
    document.getElementById('userRole').value = user.role;
    document.getElementById('userPassword').value = '';
    document.getElementById('userPassword').required = false;
    document.getElementById('passwordHint').textContent = '(leave blank to keep current)';
    document.getElementById('userActive').checked = user.is_active;
    document.getElementById('activeGroup').style.display = 'block';
    document.getElementById('userModal').style.display = 'block';
}

function closeUserModal() {
    document.getElementById('userModal').style.display = 'none';
    document.getElementById('userForm').reset();
}

async function handleUserSubmit(e) {
    e.preventDefault();
    
    const userId = document.getElementById('userId').value;
    const username = document.getElementById('userUsername').value;
    const email = document.getElementById('userEmail').value;
    const full_name = document.getElementById('userFullName').value;
    const role = document.getElementById('userRole').value;
    const password = document.getElementById('userPassword').value;
    const is_active = document.getElementById('userActive').checked;
    
    const userData = { username, email, full_name, role };
    if (password) userData.password = password;
    if (userId) userData.is_active = is_active;
    
    try {
        const url = userId ? `${API_BASE}/users/${userId}` : `${API_BASE}/users`;
        const method = userId ? 'PUT' : 'POST';
        
        const response = await fetch(url, {
            method,
            headers: getAuthHeaders(),
            body: JSON.stringify(userData)
        });
        
        const data = await response.json();
        
        if (response.ok) {
            closeUserModal();
            loadUsers();
            showSuccess(userId ? 'User updated successfully' : 'User created successfully');
        } else {
            showError(data.error || 'Operation failed');
        }
    } catch (error) {
        showError('Error: ' + error.message);
    }
}

async function deleteUser(userId, username) {
    if (!confirm(`Are you sure you want to delete user "${username}"?`)) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/users/${userId}`, {
            method: 'DELETE',
            headers: getAuthHeaders()
        });
        
        if (response.ok) {
            loadUsers();
            showSuccess('User deleted successfully');
        } else {
            const data = await response.json();
            showError(data.error || 'Delete failed');
        }
    } catch (error) {
        showError('Error: ' + error.message);
    }
}

// ============ CUSTOMER GROUPS ============

async function loadCustomerGroups() {
    try {
        const response = await fetch(`${API_BASE}/customer-groups`, {
            headers: getAuthHeaders()
        });
        
        if (!response.ok) throw new Error('Failed to load customer groups');
        
        customerGroups = await response.json();
        renderCustomerGroups();
        populateCustomerGroupSelect();
    } catch (error) {
        console.error('Error loading customer groups:', error);
    }
}

function renderCustomerGroups() {
    const tbody = document.getElementById('customerGroupsTableBody');
    
    if (customerGroups.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" style="text-align: center;">No customer groups found</td></tr>';
        return;
    }
    
    tbody.innerHTML = customerGroups.map(g => `
        <tr>
            <td><strong>${g.name}</strong></td>
            <td>${g.customer_count}</td>
            <td>${g.product_count}</td>
            <td>${formatDate(g.created_at)}</td>
            <td class="action-buttons">
                <button class="btn btn-secondary btn-sm" onclick="editCustomerGroup(${g.id}, '${escapeHtml(g.name)}')">Edit</button>
                <button class="btn btn-danger btn-sm" onclick="deleteCustomerGroup(${g.id}, '${escapeHtml(g.name)}')">Delete</button>
            </td>
        </tr>
    `).join('');
}

function populateCustomerGroupSelect() {
    const select = document.getElementById('customerGroupSelect');
    select.innerHTML = '<option value="">Select Customer Group</option>';
    customerGroups.forEach(g => {
        select.innerHTML += `<option value="${g.id}">${g.name}</option>`;
    });
}

function openAddCustomerGroupModal() {
    document.getElementById('customerGroupModalTitle').textContent = 'Add Customer Group';
    document.getElementById('customerGroupForm').reset();
    document.getElementById('customerGroupId').value = '';
    document.getElementById('customerGroupModal').style.display = 'block';
}

function editCustomerGroup(id, name) {
    document.getElementById('customerGroupModalTitle').textContent = 'Edit Customer Group';
    document.getElementById('customerGroupId').value = id;
    document.getElementById('customerGroupName').value = name;
    document.getElementById('customerGroupModal').style.display = 'block';
}

function closeCustomerGroupModal() {
    document.getElementById('customerGroupModal').style.display = 'none';
}

async function handleCustomerGroupSubmit(e) {
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
            const data = await response.json();
            throw new Error(data.error || 'Operation failed');
        }
        
        closeCustomerGroupModal();
        loadCustomerGroups();
        showSuccess(id ? 'Customer group updated' : 'Customer group created');
    } catch (error) {
        showError(error.message);
    }
}

async function deleteCustomerGroup(id, name) {
    if (!confirm(`Are you sure you want to delete customer group "${name}"? This will delete all associated customers and products.`)) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/customer-groups/${id}`, {
            method: 'DELETE',
            headers: getAuthHeaders()
        });
        
        if (response.ok) {
            loadCustomerGroups();
            loadCustomers();
            showSuccess('Customer group deleted');
        } else {
            const data = await response.json();
            showError(data.error || 'Delete failed');
        }
    } catch (error) {
        showError(error.message);
    }
}

// ============ CUSTOMERS ============

async function loadCustomers() {
    try {
        const response = await fetch(`${API_BASE}/customers`, {
            headers: getAuthHeaders()
        });
        
        if (!response.ok) throw new Error('Failed to load customers');
        
        customers = await response.json();
        renderCustomers();
    } catch (error) {
        console.error('Error loading customers:', error);
    }
}

function renderCustomers() {
    const tbody = document.getElementById('customersTableBody');
    
    if (customers.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" style="text-align: center;">No customers found</td></tr>';
        return;
    }
    
    tbody.innerHTML = customers.map(c => `
        <tr>
            <td><strong>${c.name}</strong></td>
            <td>${c.customer_group_name}</td>
            <td>${c.product_count}</td>
            <td>${formatDate(c.created_at)}</td>
            <td class="action-buttons">
                <button class="btn btn-secondary btn-sm" onclick="editCustomer(${c.id}, '${escapeHtml(c.name)}', ${c.customer_group_id})">Edit</button>
                <button class="btn btn-danger btn-sm" onclick="deleteCustomer(${c.id}, '${escapeHtml(c.name)}')">Delete</button>
            </td>
        </tr>
    `).join('');
}

function openAddCustomerModal() {
    document.getElementById('customerModalTitle').textContent = 'Add Customer';
    document.getElementById('customerForm').reset();
    document.getElementById('customerId').value = '';
    document.getElementById('customerModal').style.display = 'block';
}

function editCustomer(id, name, customerGroupId) {
    document.getElementById('customerModalTitle').textContent = 'Edit Customer';
    document.getElementById('customerId').value = id;
    document.getElementById('customerName').value = name;
    document.getElementById('customerGroupSelect').value = customerGroupId;
    document.getElementById('customerModal').style.display = 'block';
}

function closeCustomerModal() {
    document.getElementById('customerModal').style.display = 'none';
}

async function handleCustomerSubmit(e) {
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
            const data = await response.json();
            throw new Error(data.error || 'Operation failed');
        }
        
        closeCustomerModal();
        loadCustomers();
        showSuccess(id ? 'Customer updated' : 'Customer created');
    } catch (error) {
        showError(error.message);
    }
}

async function deleteCustomer(id, name) {
    if (!confirm(`Are you sure you want to delete customer "${name}"? This will delete all associated products.`)) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/customers/${id}`, {
            method: 'DELETE',
            headers: getAuthHeaders()
        });
        
        if (response.ok) {
            loadCustomers();
            showSuccess('Customer deleted');
        } else {
            const data = await response.json();
            showError(data.error || 'Delete failed');
        }
    } catch (error) {
        showError(error.message);
    }
}

// ============ FX RATES ============

async function loadFxRates() {
    try {
        const response = await fetch(`${API_BASE}/fx-rates`, {
            headers: getAuthHeaders()
        });
        
        if (!response.ok) throw new Error('Failed to load FX rates');
        
        fxRatesData = await response.json();
        renderFxRates();
    } catch (error) {
        console.error('Error loading FX rates:', error);
    }
}

function renderFxRates() {
    const tbody = document.getElementById('fxRatesTableBody');
    
    if (fxRatesData.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" style="text-align: center; padding: 40px;">No FX rates found</td></tr>';
        return;
    }
    
    tbody.innerHTML = fxRatesData.map(rate => `
        <tr>
            <td><strong>${rate.country}</strong></td>
            <td>${rate.currency}</td>
            <td>
                <input 
                    type="number" 
                    step="0.000001" 
                    value="${rate.rate_to_usd}" 
                    data-rate-id="${rate.id}"
                    style="width: 150px; padding: 8px; border: 1px solid var(--border-color); border-radius: 4px;"
                    onchange="markFxRateChanged(${rate.id}, this.value)"
                />
            </td>
            <td>${rate.updated_at ? new Date(rate.updated_at).toLocaleString() : 'Never'}</td>
        </tr>
    `).join('');
}

function markFxRateChanged(rateId, newValue) {
    const rate = fxRatesData.find(r => r.id === rateId);
    if (rate && parseFloat(newValue) !== parseFloat(rate.rate_to_usd)) {
        changedFxRates.add(rateId);
    } else {
        changedFxRates.delete(rateId);
    }
}

async function saveFxRates() {
    if (changedFxRates.size === 0) {
        showInfo('No changes to save');
        return;
    }
    
    const ratesToUpdate = [];
    
    changedFxRates.forEach(rateId => {
        const input = document.querySelector(`input[data-rate-id="${rateId}"]`);
        const newRate = parseFloat(input.value);
        
        if (newRate > 0) {
            ratesToUpdate.push({ id: rateId, rate_to_usd: newRate });
        }
    });
    
    try {
        const response = await fetch(`${API_BASE}/fx-rates/bulk-update`, {
            method: 'POST',
            headers: getAuthHeaders(),
            body: JSON.stringify({ rates: ratesToUpdate })
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showSuccess(result.message || 'FX rates updated successfully!');
            changedFxRates.clear();
            loadFxRates();
        } else {
            showError('Error: ' + (result.error || 'Failed to update FX rates'));
        }
    } catch (error) {
        showError('Error updating FX rates: ' + error.message);
    }
}

// ============ UTILITIES ============

function logout() {
    fetch(`${API_BASE}/auth/logout`, { method: 'POST' });
    localStorage.clear();
    window.location.href = '/login.html';
}

function formatDate(dateStr) {
    if (!dateStr) return '-';
    return new Date(dateStr).toLocaleDateString();
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

function showSuccess(message) {
    showToast(message, 'success');
}

function showError(message) {
    showToast(message, 'error');
}

function showInfo(message) {
    showToast(message, 'info');
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

// Expose functions globally for onclick handlers
window.openAddUserModal = openAddUserModal;
window.editUser = editUser;
window.closeUserModal = closeUserModal;
window.deleteUser = deleteUser;
window.openAddCustomerGroupModal = openAddCustomerGroupModal;
window.editCustomerGroup = editCustomerGroup;
window.closeCustomerGroupModal = closeCustomerGroupModal;
window.deleteCustomerGroup = deleteCustomerGroup;
window.openAddCustomerModal = openAddCustomerModal;
window.editCustomer = editCustomer;
window.closeCustomerModal = closeCustomerModal;
window.deleteCustomer = deleteCustomer;
window.saveFxRates = saveFxRates;
window.markFxRateChanged = markFxRateChanged;
window.logout = logout;
