const API_BASE = '/api';
let currentUser = null;
let users = [];

// Check authentication on page load
document.addEventListener('DOMContentLoaded', () => {
    checkAuth();
    loadUsers();
    loadFxRates();
    
    // Setup tab switching
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const tab = btn.dataset.tab;
            switchTab(tab);
        });
    });
    
    // Setup modal close handlers
    document.querySelector('.close').addEventListener('click', closeUserModal);
    document.getElementById('userForm').addEventListener('submit', handleUserSubmit);
    
    window.addEventListener('click', (e) => {
        if (e.target.classList.contains('modal')) {
            closeUserModal();
        }
    });
});

function switchTab(tab) {
    // Update tab buttons
    document.querySelectorAll('.tab-btn').forEach(btn => {
        if (btn.dataset.tab === tab) {
            btn.classList.add('active');
        } else {
            btn.classList.remove('active');
        }
    });
    
    // Show/hide content
    document.getElementById('usersTab').style.display = tab === 'users' ? 'block' : 'none';
    document.getElementById('fxRatesTab').style.display = tab === 'fx-rates' ? 'block' : 'none';
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

async function loadUsers() {
    const token = localStorage.getItem('token');
    const tableBody = document.getElementById('usersTableBody');
    
    try {
        const response = await fetch(`${API_BASE}/users`, {
            headers: {
                'Authorization': `Bearer ${token}`
            }
        });
        
        if (response.status === 401) {
            localStorage.clear();
            window.location.href = '/login.html';
            return;
        }
        
        users = await response.json();
        renderUsers();
    } catch (error) {
        tableBody.innerHTML = '<tr><td colspan="7" style="text-align: center; color: var(--danger);">Error loading users</td></tr>';
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
    document.getElementById('modalTitle').textContent = 'Add New User';
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
    
    document.getElementById('modalTitle').textContent = 'Edit User';
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
    
    const token = localStorage.getItem('token');
    
    try {
        const url = userId ? `${API_BASE}/users/${userId}` : `${API_BASE}/users`;
        const method = userId ? 'PUT' : 'POST';
        
        const response = await fetch(url, {
            method,
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`
            },
            body: JSON.stringify(userData)
        });
        
        const data = await response.json();
        
        if (response.ok) {
            closeUserModal();
            loadUsers();
            showSuccess(userId ? 'User updated successfully' : 'User created successfully');
        } else {
            alert(data.error || 'Operation failed');
        }
    } catch (error) {
        alert('Error: ' + error.message);
    }
}

async function deleteUser(userId, username) {
    if (!confirm(`Are you sure you want to delete user "${username}"?`)) {
        return;
    }
    
    const token = localStorage.getItem('token');
    
    try {
        const response = await fetch(`${API_BASE}/users/${userId}`, {
            method: 'DELETE',
            headers: {
                'Authorization': `Bearer ${token}`
            }
        });
        
        if (response.ok) {
            loadUsers();
            showSuccess('User deleted successfully');
        } else {
            const data = await response.json();
            alert(data.error || 'Delete failed');
        }
    } catch (error) {
        alert('Error: ' + error.message);
    }
}

function logout() {
    fetch(`${API_BASE}/auth/logout`, { method: 'POST' });
    localStorage.clear();
    window.location.href = '/login.html';
}

// ============ FX RATES MANAGEMENT ============

let fxRatesData = [];

async function loadFxRates() {
    const token = localStorage.getItem('token');
    
    try {
        const response = await fetch(`${API_BASE}/fx-rates`, {
            headers: {
                'Authorization': `Bearer ${token}`
            }
        });
        
        if (!response.ok) {
            throw new Error('Failed to load FX rates');
        }
        
        fxRatesData = await response.json();
        renderFxRates();
    } catch (error) {
        console.error('Error loading FX rates:', error);
        showMessage('Error loading FX rates: ' + error.message, 'error');
    }
}

function renderFxRates() {
    const tbody = document.getElementById('fxRatesTableBody');
    
    if (fxRatesData.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" style="text-align: center; padding: 40px; color: var(--text-secondary);">No FX rates found</td></tr>';
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
            <td>${rate.updated_by || '-'}</td>
        </tr>
    `).join('');
}

const changedFxRates = new Set();

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
        showMessage('No changes to save', 'info');
        return;
    }
    
    const token = localStorage.getItem('token');
    const ratesToUpdate = [];
    
    changedFxRates.forEach(rateId => {
        const input = document.querySelector(`input[data-rate-id="${rateId}"]`);
        const newRate = parseFloat(input.value);
        
        if (newRate > 0) {
            ratesToUpdate.push({
                id: rateId,
                rate_to_usd: newRate
            });
        }
    });
    
    try {
        const response = await fetch(`${API_BASE}/fx-rates/bulk-update`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`
            },
            body: JSON.stringify({ rates: ratesToUpdate })
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showMessage(result.message || 'FX rates updated successfully!', 'success');
            changedFxRates.clear();
            loadFxRates(); // Reload to show updated timestamps
        } else {
            showMessage('Error: ' + (result.error || 'Failed to update FX rates'), 'error');
        }
    } catch (error) {
        showMessage('Error updating FX rates: ' + error.message, 'error');
    }
}

function showSuccess(message) {
    // Simple success notification
    const notification = document.createElement('div');
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        background: #10b981;
        color: white;
        padding: 15px 20px;
        border-radius: 8px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        z-index: 10000;
    `;
    notification.textContent = message;
    document.body.appendChild(notification);
    
    setTimeout(() => {
        notification.remove();
    }, 3000);
}

