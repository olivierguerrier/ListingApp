const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');
const xlsx = require('xlsx');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const session = require('express-session');
const cookieParser = require('cookie-parser');
const multer = require('multer');

const app = express();
const PORT = process.env.PORT || 7777;
const JWT_SECRET = process.env.JWT_SECRET || 'your-secret-key-change-in-production';

// Configure multer for file uploads
const upload = multer({ 
  storage: multer.memoryStorage(),
  limits: { fileSize: 50 * 1024 * 1024 } // 50MB limit
});

// Middleware
app.use(cors());
app.use(bodyParser.json());
app.use(cookieParser());
app.use(session({
  secret: JWT_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: { secure: false, maxAge: 24 * 60 * 60 * 1000 } // 24 hours
}));
app.use(express.static('public'));

// Database connection
const db = new sqlite3.Database('./database.db', (err) => {
  if (err) {
    console.error('Error opening database:', err.message);
  } else {
    console.log('Connected to SQLite database');
    initializeDatabase();
  }
});

// Create default admin user if no users exist
function createDefaultAdmin() {
  db.get('SELECT COUNT(*) as count FROM users', [], async (err, row) => {
    if (err) {
      console.error('Error checking users:', err.message);
      return;
    }
    
    if (row.count === 0) {
      const defaultPassword = 'admin123';
      const passwordHash = await bcrypt.hash(defaultPassword, 10);
      
      db.run(`
        INSERT INTO users (username, email, password_hash, full_name, role)
        VALUES (?, ?, ?, ?, ?)
      `, ['admin', 'admin@listingapp.com', passwordHash, 'System Administrator', 'admin'], (err) => {
        if (err) {
          console.error('Error creating default admin:', err.message);
        } else {
          console.log('[SETUP] Default admin created - username: admin, password: admin123');
          console.log('[SECURITY] Please change the default admin password immediately!');
        }
      });
    }
  });
}

// Authentication middleware
function authenticateToken(req, res, next) {
  const token = req.session.token || req.headers['authorization']?.split(' ')[1];
  
  if (!token) {
    return res.status(401).json({ error: 'Authentication required' });
  }
  
  jwt.verify(token, JWT_SECRET, (err, user) => {
    if (err) {
      return res.status(403).json({ error: 'Invalid or expired token' });
    }
    req.user = user;
    next();
  });
}

// Role-based access control middleware
function requireRole(...allowedRoles) {
  return (req, res, next) => {
    if (!req.user) {
      return res.status(401).json({ error: 'Authentication required' });
    }
    
    if (!allowedRoles.includes(req.user.role)) {
      return res.status(403).json({ error: 'Insufficient permissions' });
    }
    
    next();
  };
}

// Initialize database tables
function initializeDatabase() {
  db.serialize(() => {
    // Customer Groups table
    db.run(`CREATE TABLE IF NOT EXISTS customer_groups (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT UNIQUE NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`, (err) => {
      if (err) console.error('Error creating customer_groups:', err.message);
      else console.log('[SETUP] customer_groups table ready');
    });

    // Customers table
    db.run(`CREATE TABLE IF NOT EXISTS customers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      customer_group_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (customer_group_id) REFERENCES customer_groups(id) ON DELETE CASCADE,
      UNIQUE(customer_group_id, name)
    )`, (err) => {
      if (err) console.error('Error creating customers:', err.message);
      else console.log('[SETUP] customers table ready');
    });

    // Products table - Customer Number is the primary identifier
    db.run(`CREATE TABLE IF NOT EXISTS products (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      customer_id INTEGER NOT NULL,
      customer_number TEXT NOT NULL,
      item_number TEXT,
      description TEXT,
      fcl_lcl TEXT CHECK(fcl_lcl IN ('FCL', 'LCL', 'Both')),
      status TEXT CHECK(status IN ('Existing', 'New', 'NCF')),
      sell_price DECIMAL(10, 2),
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
      UNIQUE(customer_id, customer_number)
    )`, (err) => {
      if (err) console.error('Error creating products:', err.message);
      else console.log('[SETUP] products table ready');
    });

    // Create indexes for products
    db.run(`CREATE INDEX IF NOT EXISTS idx_products_customer_id ON products(customer_id)`);
    db.run(`CREATE INDEX IF NOT EXISTS idx_products_customer_number ON products(customer_number)`);
    db.run(`CREATE INDEX IF NOT EXISTS idx_products_item_number ON products(item_number)`);
    db.run(`CREATE INDEX IF NOT EXISTS idx_products_status ON products(status)`);
    db.run(`CREATE INDEX IF NOT EXISTS idx_customers_group_id ON customers(customer_group_id)`);

    // Item Numbers table - Stores PIM Extract data per Item Number (SKU)
    db.run(`CREATE TABLE IF NOT EXISTS item_numbers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      item_number TEXT UNIQUE NOT NULL,
      series TEXT,
      product_taxonomy_category TEXT,
      legal_name TEXT,
      upc_number TEXT,
      brand_product_line TEXT,
      age_grade TEXT,
      product_description_internal TEXT,
      item_spec_sheet_status TEXT,
      product_development_status TEXT,
      item_spec_data_last_updated TEXT,
      case_pack TEXT,
      package_length_cm REAL,
      package_width_cm REAL,
      package_height_cm REAL,
      package_weight_kg REAL,
      product_number TEXT,
      last_synced DATETIME DEFAULT CURRENT_TIMESTAMP,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`, (err) => {
      if (err) console.error('Error creating item_numbers:', err.message);
      else console.log('[SETUP] item_numbers table ready');
    });

    // Users table for authentication
    db.run(`CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT UNIQUE NOT NULL,
      email TEXT UNIQUE NOT NULL,
      password_hash TEXT NOT NULL,
      full_name TEXT,
      role TEXT NOT NULL CHECK(role IN ('viewer', 'approver', 'salesperson', 'admin')) DEFAULT 'viewer',
      is_active BOOLEAN DEFAULT 1,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      last_login DATETIME
    )`, (err) => {
      if (err) {
        console.error('Error creating users table:', err.message);
      } else {
        console.log('[SETUP] users table ready');
        createDefaultAdmin();
      }
    });

    // Notifications table
    db.run(`CREATE TABLE IF NOT EXISTS notifications (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      type TEXT NOT NULL,
      title TEXT NOT NULL,
      message TEXT NOT NULL,
      link TEXT,
      is_read BOOLEAN DEFAULT 0,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    )`, (err) => {
      if (err) console.error('Error creating notifications table:', err.message);
      else console.log('[SETUP] notifications table ready');
    });

    // FX rates table
    db.run(`CREATE TABLE IF NOT EXISTS fx_rates (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      country TEXT NOT NULL UNIQUE,
      currency TEXT NOT NULL,
      rate_to_usd DECIMAL(10,6) NOT NULL,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_by INTEGER,
      FOREIGN KEY (updated_by) REFERENCES users(id)
    )`, (err) => {
      if (err) {
        console.error('Error creating fx_rates table:', err.message);
      } else {
        console.log('[SETUP] fx_rates table ready');
        // Insert default FX rates
        db.run(`INSERT OR IGNORE INTO fx_rates (country, currency, rate_to_usd) VALUES 
          ('Canada', 'CAD', 1.35),
          ('United States', 'USD', 1.00),
          ('Mexico', 'MXN', 20.00),
          ('United Kingdom', 'GBP', 0.79),
          ('Germany', 'EUR', 0.92),
          ('France', 'EUR', 0.92),
          ('Italy', 'EUR', 0.92),
          ('Spain', 'EUR', 0.92),
          ('Netherlands', 'EUR', 0.92),
          ('Poland', 'PLN', 4.00),
          ('Sweden', 'SEK', 10.50),
          ('Japan', 'JPY', 149.00),
          ('Australia', 'AUD', 1.52),
          ('Singapore', 'SGD', 1.34)
        `);
      }
    });

    console.log('Database tables initialized');
  });
}

// ============= API ENDPOINTS =============

// ============ AUTHENTICATION ============

// Login
app.post('/api/auth/login', async (req, res) => {
  const { username, password } = req.body;
  
  if (!username || !password) {
    return res.status(400).json({ error: 'Username and password required' });
  }
  
  db.get('SELECT * FROM users WHERE username = ? AND is_active = 1', [username], async (err, user) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    
    if (!user) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }
    
    const validPassword = await bcrypt.compare(password, user.password_hash);
    if (!validPassword) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }
    
    // Update last login
    db.run('UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = ?', [user.id]);
    
    // Generate token
    const token = jwt.sign(
      { id: user.id, username: user.username, role: user.role, full_name: user.full_name },
      JWT_SECRET,
      { expiresIn: '24h' }
    );
    
    req.session.token = token;
    
    res.json({
      token,
      user: {
        id: user.id,
        username: user.username,
        email: user.email,
        full_name: user.full_name,
        role: user.role
      }
    });
  });
});

// Logout
app.post('/api/auth/logout', (req, res) => {
  req.session.destroy();
  res.json({ message: 'Logged out successfully' });
});

// Check auth status
app.get('/api/auth/me', authenticateToken, (req, res) => {
  res.json({ user: req.user });
});

// ============ USER MANAGEMENT ============

// Get all users (admin only)
app.get('/api/users', authenticateToken, requireRole('admin'), (req, res) => {
  db.all('SELECT id, username, email, full_name, role, is_active, created_at, last_login FROM users ORDER BY username', [], (err, rows) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    res.json(rows);
  });
});

// Create user (admin only)
app.post('/api/users', authenticateToken, requireRole('admin'), async (req, res) => {
  const { username, email, password, full_name, role } = req.body;
  
  if (!username || !email || !password || !role) {
    return res.status(400).json({ error: 'Missing required fields' });
  }
  
  const passwordHash = await bcrypt.hash(password, 10);
  
  db.run(`
    INSERT INTO users (username, email, password_hash, full_name, role)
    VALUES (?, ?, ?, ?, ?)
  `, [username, email, passwordHash, full_name, role], function(err) {
    if (err) {
      if (err.message.includes('UNIQUE')) {
        return res.status(400).json({ error: 'Username or email already exists' });
      }
      return res.status(500).json({ error: err.message });
    }
    
    res.json({ id: this.lastID, message: 'User created successfully' });
  });
});

// Update user (admin only)
app.put('/api/users/:id', authenticateToken, requireRole('admin'), async (req, res) => {
  const { id } = req.params;
  const { username, email, password, full_name, role, is_active } = req.body;
  
  let query = 'UPDATE users SET username = ?, email = ?, full_name = ?, role = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP';
  let params = [username, email, full_name, role, is_active ? 1 : 0];
  
  if (password) {
    const passwordHash = await bcrypt.hash(password, 10);
    query += ', password_hash = ?';
    params.push(passwordHash);
  }
  
  query += ' WHERE id = ?';
  params.push(id);
  
  db.run(query, params, function(err) {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    if (this.changes === 0) {
      return res.status(404).json({ error: 'User not found' });
    }
    res.json({ message: 'User updated successfully' });
  });
});

// Delete user (admin only)
app.delete('/api/users/:id', authenticateToken, requireRole('admin'), (req, res) => {
  const { id } = req.params;
  
  // Prevent self-deletion
  if (parseInt(id) === req.user.id) {
    return res.status(400).json({ error: 'Cannot delete your own account' });
  }
  
  db.run('DELETE FROM users WHERE id = ?', [id], function(err) {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    if (this.changes === 0) {
      return res.status(404).json({ error: 'User not found' });
    }
    res.json({ message: 'User deleted successfully' });
  });
});

// ============ CUSTOMER GROUPS ============

// Get all customer groups
app.get('/api/customer-groups', authenticateToken, (req, res) => {
  db.all(`
    SELECT cg.*, 
           COUNT(DISTINCT c.id) as customer_count,
           COUNT(DISTINCT p.id) as product_count
    FROM customer_groups cg
    LEFT JOIN customers c ON c.customer_group_id = cg.id
    LEFT JOIN products p ON p.customer_id = c.id
    GROUP BY cg.id
    ORDER BY cg.name
  `, [], (err, rows) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    res.json(rows);
  });
});

// Get single customer group
app.get('/api/customer-groups/:id', authenticateToken, (req, res) => {
  db.get(`
    SELECT cg.*, 
           COUNT(DISTINCT c.id) as customer_count,
           COUNT(DISTINCT p.id) as product_count
    FROM customer_groups cg
    LEFT JOIN customers c ON c.customer_group_id = cg.id
    LEFT JOIN products p ON p.customer_id = c.id
    WHERE cg.id = ?
    GROUP BY cg.id
  `, [req.params.id], (err, row) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    if (!row) {
      return res.status(404).json({ error: 'Customer group not found' });
    }
    res.json(row);
  });
});

// Create customer group
app.post('/api/customer-groups', authenticateToken, requireRole('salesperson', 'admin'), (req, res) => {
  const { name } = req.body;
  
  if (!name) {
    return res.status(400).json({ error: 'Name is required' });
  }
  
  db.run('INSERT INTO customer_groups (name) VALUES (?)', [name], function(err) {
    if (err) {
      if (err.message.includes('UNIQUE')) {
        return res.status(400).json({ error: 'Customer group already exists' });
      }
      return res.status(500).json({ error: err.message });
    }
    res.json({ id: this.lastID, message: 'Customer group created successfully' });
  });
});

// Update customer group
app.put('/api/customer-groups/:id', authenticateToken, requireRole('salesperson', 'admin'), (req, res) => {
  const { id } = req.params;
  const { name } = req.body;
  
  if (!name) {
    return res.status(400).json({ error: 'Name is required' });
  }
  
  db.run('UPDATE customer_groups SET name = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?', 
    [name, id], function(err) {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    if (this.changes === 0) {
      return res.status(404).json({ error: 'Customer group not found' });
    }
    res.json({ message: 'Customer group updated successfully' });
  });
});

// Delete customer group
app.delete('/api/customer-groups/:id', authenticateToken, requireRole('admin'), (req, res) => {
  const { id } = req.params;
  
  db.run('DELETE FROM customer_groups WHERE id = ?', [id], function(err) {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    if (this.changes === 0) {
      return res.status(404).json({ error: 'Customer group not found' });
    }
    res.json({ message: 'Customer group deleted successfully' });
  });
});

// ============ CUSTOMERS ============

// Get all customers
app.get('/api/customers', authenticateToken, (req, res) => {
  const { customer_group_id } = req.query;
  
  let query = `
    SELECT c.*, 
           cg.name as customer_group_name,
           COUNT(DISTINCT p.id) as product_count
    FROM customers c
    JOIN customer_groups cg ON c.customer_group_id = cg.id
    LEFT JOIN products p ON p.customer_id = c.id
  `;
  
  let params = [];
  if (customer_group_id) {
    query += ' WHERE c.customer_group_id = ?';
    params.push(customer_group_id);
  }
  
  query += ' GROUP BY c.id ORDER BY cg.name, c.name';
  
  db.all(query, params, (err, rows) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    res.json(rows);
  });
});

// Get single customer
app.get('/api/customers/:id', authenticateToken, (req, res) => {
  db.get(`
    SELECT c.*, 
           cg.name as customer_group_name,
           COUNT(DISTINCT p.id) as product_count
    FROM customers c
    JOIN customer_groups cg ON c.customer_group_id = cg.id
    LEFT JOIN products p ON p.customer_id = c.id
    WHERE c.id = ?
    GROUP BY c.id
  `, [req.params.id], (err, row) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    if (!row) {
      return res.status(404).json({ error: 'Customer not found' });
    }
    res.json(row);
  });
});

// Create customer
app.post('/api/customers', authenticateToken, requireRole('salesperson', 'admin'), (req, res) => {
  const { name, customer_group_id } = req.body;
  
  if (!name || !customer_group_id) {
    return res.status(400).json({ error: 'Name and customer_group_id are required' });
  }
  
  db.run('INSERT INTO customers (name, customer_group_id) VALUES (?, ?)', 
    [name, customer_group_id], function(err) {
    if (err) {
      if (err.message.includes('UNIQUE')) {
        return res.status(400).json({ error: 'Customer already exists in this group' });
      }
      return res.status(500).json({ error: err.message });
    }
    res.json({ id: this.lastID, message: 'Customer created successfully' });
  });
});

// Update customer
app.put('/api/customers/:id', authenticateToken, requireRole('salesperson', 'admin'), (req, res) => {
  const { id } = req.params;
  const { name, customer_group_id } = req.body;
  
  if (!name || !customer_group_id) {
    return res.status(400).json({ error: 'Name and customer_group_id are required' });
  }
  
  db.run('UPDATE customers SET name = ?, customer_group_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?', 
    [name, customer_group_id, id], function(err) {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    if (this.changes === 0) {
      return res.status(404).json({ error: 'Customer not found' });
    }
    res.json({ message: 'Customer updated successfully' });
  });
});

// Delete customer
app.delete('/api/customers/:id', authenticateToken, requireRole('admin'), (req, res) => {
  const { id } = req.params;
  
  db.run('DELETE FROM customers WHERE id = ?', [id], function(err) {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    if (this.changes === 0) {
      return res.status(404).json({ error: 'Customer not found' });
    }
    res.json({ message: 'Customer deleted successfully' });
  });
});

// ============ PRODUCTS ============

// Get all products with pagination and filters
app.get('/api/products', authenticateToken, (req, res) => {
  const limit = parseInt(req.query.limit) || 50;
  const offset = parseInt(req.query.offset) || 0;
  const { customer_group_id, customer_id, status, search, stage } = req.query;
  
  let whereConditions = [];
  let params = [];
  
  if (customer_group_id) {
    whereConditions.push('cg.id = ?');
    params.push(customer_group_id);
  }
  
  if (customer_id) {
    whereConditions.push('c.id = ?');
    params.push(customer_id);
  }
  
  if (status) {
    whereConditions.push('p.status = ?');
    params.push(status);
  }
  
  if (search) {
    whereConditions.push('(p.customer_number LIKE ? OR p.item_number LIKE ? OR p.description LIKE ?)');
    const searchPattern = `%${search}%`;
    params.push(searchPattern, searchPattern, searchPattern);
  }
  
  // Stage filter
  if (stage) {
    switch(stage) {
      case '1':
        whereConditions.push('p.stage_1_ideation = 1');
        break;
      case '2':
        whereConditions.push('i.item_number IS NOT NULL');
        break;
      case '2.5':
        whereConditions.push('(p.sell_price IS NOT NULL OR p.stage_2_5_pricing = 1)');
        break;
      case '3':
        whereConditions.push("p.status IN ('Existing', 'New')");
        break;
      case '4':
        whereConditions.push('p.stage_4_listed_with_customer = 1');
        break;
      case '5':
        whereConditions.push('p.stage_5_available = 1');
        break;
      case '6':
        whereConditions.push("p.status = 'NCF'");
        break;
    }
  }
  
  const whereClause = whereConditions.length > 0 ? 'WHERE ' + whereConditions.join(' AND ') : '';
  
  const countQuery = `
    SELECT COUNT(*) as count
    FROM products p
    JOIN customers c ON p.customer_id = c.id
    JOIN customer_groups cg ON c.customer_group_id = cg.id
    ${whereClause}
  `;
  
  const dataQuery = `
    SELECT p.*,
           c.name as customer_name,
           cg.name as customer_group_name,
           cg.id as customer_group_id,
           i.legal_name as pim_legal_name,
           i.brand_product_line as pim_brand,
           i.series as pim_series,
           i.product_taxonomy_category as pim_taxonomy,
           i.age_grade as pim_age_grade,
           i.product_development_status as pim_dev_status,
           i.item_spec_sheet_status as pim_spec_status,
           -- Computed stages
           COALESCE(p.stage_1_ideation, 0) as stage_1_ideation,
           CASE WHEN i.item_number IS NOT NULL THEN 1 ELSE 0 END as stage_2_pim,
           CASE WHEN p.sell_price IS NOT NULL OR p.stage_2_5_pricing = 1 THEN 1 ELSE 0 END as stage_2_5_pricing,
           CASE WHEN p.status IN ('Existing', 'New') THEN 1 ELSE 0 END as stage_3_qpi,
           COALESCE(p.stage_4_listed_with_customer, 0) as stage_4_listed_with_customer,
           COALESCE(p.stage_5_available, 0) as stage_5_available,
           CASE WHEN p.status = 'NCF' THEN 1 ELSE 0 END as stage_6_eol
    FROM products p
    JOIN customers c ON p.customer_id = c.id
    JOIN customer_groups cg ON c.customer_group_id = cg.id
    LEFT JOIN item_numbers i ON p.item_number = i.item_number
    ${whereClause}
    ORDER BY cg.name, c.name, p.customer_number
    LIMIT ? OFFSET ?
  `;
  
  const countParams = [...params];
  params.push(limit, offset);
  
  db.get(countQuery, countParams, (countErr, countRow) => {
    if (countErr) {
      return res.status(500).json({ error: countErr.message });
    }
    
    db.all(dataQuery, params, (err, rows) => {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      
      res.json({
        data: rows,
        pagination: {
          total: countRow.count,
          limit: limit,
          offset: offset,
          page: Math.floor(offset / limit) + 1,
          total_pages: Math.ceil(countRow.count / limit)
        }
      });
    });
  });
});

// Get single product
app.get('/api/products/:id', authenticateToken, (req, res) => {
  db.get(`
    SELECT p.*,
           c.name as customer_name,
           cg.name as customer_group_name,
           cg.id as customer_group_id,
           i.legal_name as pim_legal_name,
           i.brand_product_line as pim_brand,
           i.series as pim_series,
           i.product_taxonomy_category as pim_taxonomy,
           i.age_grade as pim_age_grade,
           i.upc_number as pim_upc,
           i.product_development_status as pim_dev_status,
           i.item_spec_sheet_status as pim_spec_status,
           i.package_length_cm as pim_length,
           i.package_width_cm as pim_width,
           i.package_height_cm as pim_height,
           i.package_weight_kg as pim_weight,
           -- Computed stages
           COALESCE(p.stage_1_ideation, 0) as stage_1_ideation,
           CASE WHEN i.item_number IS NOT NULL THEN 1 ELSE 0 END as stage_2_pim,
           CASE WHEN p.sell_price IS NOT NULL OR p.stage_2_5_pricing = 1 THEN 1 ELSE 0 END as stage_2_5_pricing,
           CASE WHEN p.status IN ('Existing', 'New') THEN 1 ELSE 0 END as stage_3_qpi,
           COALESCE(p.stage_4_listed_with_customer, 0) as stage_4_listed_with_customer,
           COALESCE(p.stage_5_available, 0) as stage_5_available,
           CASE WHEN p.status = 'NCF' THEN 1 ELSE 0 END as stage_6_eol
    FROM products p
    JOIN customers c ON p.customer_id = c.id
    JOIN customer_groups cg ON c.customer_group_id = cg.id
    LEFT JOIN item_numbers i ON p.item_number = i.item_number
    WHERE p.id = ?
  `, [req.params.id], (err, row) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    if (!row) {
      return res.status(404).json({ error: 'Product not found' });
    }
    res.json(row);
  });
});

// Create product
app.post('/api/products', authenticateToken, requireRole('salesperson', 'admin'), (req, res) => {
  const { customer_id, customer_number, item_number, description, fcl_lcl, status, sell_price } = req.body;
  
  if (!customer_id || !customer_number) {
    return res.status(400).json({ error: 'customer_id and customer_number are required' });
  }
  
  db.run(`
    INSERT INTO products (customer_id, customer_number, item_number, description, fcl_lcl, status, sell_price)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `, [customer_id, customer_number, item_number, description, fcl_lcl, status, sell_price], function(err) {
    if (err) {
      if (err.message.includes('UNIQUE')) {
        return res.status(400).json({ error: 'Product with this customer number already exists for this customer' });
      }
      return res.status(500).json({ error: err.message });
    }
    res.json({ id: this.lastID, message: 'Product created successfully' });
  });
});

// Update product
app.put('/api/products/:id', authenticateToken, requireRole('salesperson', 'admin'), (req, res) => {
  const { id } = req.params;
  const { customer_number, item_number, description, fcl_lcl, status, sell_price } = req.body;
  
  db.run(`
    UPDATE products 
    SET customer_number = ?, item_number = ?, description = ?, fcl_lcl = ?, status = ?, sell_price = ?, updated_at = CURRENT_TIMESTAMP
    WHERE id = ?
  `, [customer_number, item_number, description, fcl_lcl, status, sell_price, id], function(err) {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    if (this.changes === 0) {
      return res.status(404).json({ error: 'Product not found' });
    }
    res.json({ message: 'Product updated successfully' });
  });
});

// Delete product
app.delete('/api/products/:id', authenticateToken, requireRole('admin'), (req, res) => {
  const { id } = req.params;
  
  db.run('DELETE FROM products WHERE id = ?', [id], function(err) {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    if (this.changes === 0) {
      return res.status(404).json({ error: 'Product not found' });
    }
    res.json({ message: 'Product deleted successfully' });
  });
});

// Update product stage (manual stages only: 1, 2.5, 4, 5)
app.put('/api/products/:id/stage', authenticateToken, requireRole('salesperson', 'admin'), (req, res) => {
  const { id } = req.params;
  const { stage, value } = req.body;
  
  // Only allow manual stages
  const allowedStages = ['stage_1_ideation', 'stage_2_5_pricing', 'stage_4_listed_with_customer', 'stage_5_available'];
  
  if (!allowedStages.includes(stage)) {
    return res.status(400).json({ error: 'Invalid stage. Only stages 1, 2.5, 4, and 5 can be manually updated.' });
  }
  
  db.run(`UPDATE products SET ${stage} = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
    [value ? 1 : 0, id], function(err) {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    if (this.changes === 0) {
      return res.status(404).json({ error: 'Product not found' });
    }
    res.json({ message: 'Stage updated successfully' });
  });
});

// ============ PRODUCT UPLOAD ============

// Upload products from Excel/CSV - BATCH PROCESSING for speed
app.post('/api/products/upload', authenticateToken, requireRole('salesperson', 'admin'), upload.single('file'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'No file uploaded' });
  }
  
  try {
    // Parse the uploaded file
    const workbook = xlsx.read(req.file.buffer, { type: 'buffer' });
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const data = xlsx.utils.sheet_to_json(worksheet);
    
    if (data.length === 0) {
      return res.status(400).json({ error: 'File is empty or has no valid data' });
    }
    
    console.log(`[UPLOAD] Processing ${data.length} rows from uploaded file`);
    console.log('[UPLOAD] Sample row:', JSON.stringify(data[0]));
    
    // Track statistics
    let customerGroupsCreated = 0;
    let customersCreated = 0;
    let productsCreated = 0;
    let productsUpdated = 0;
    let errors = [];
    
    // Phase 1: Parse all rows and collect unique customer groups and customers
    const customerGroupMap = new Map(); // name -> id
    const customerMap = new Map(); // "groupId|name" -> id
    const parsedRows = [];
    
    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      const customerGroupName = (row['Customer Group'] || row['CustomerGroup'] || row['customer_group'] || '').toString().trim();
      const customerName = (row['Customer'] || row['Customer_QPI'] || row['customer'] || '').toString().trim();
      const customerNumber = (row['Customer Number'] || row['CustomerNumber'] || row['customer_number'] || '').toString().trim();
      const itemNumber = (row['Item Number'] || row['ItemNumber'] || row['item_number'] || '').toString().trim();
      const description = (row['Description'] || row['description'] || '').toString().trim();
      const fclLcl = (row['FCL/LCL'] || row['FCLLCL'] || row['fcl_lcl'] || '').toString().trim();
      const status = (row['Status'] || row['status'] || '').toString().trim();
      let sellPrice = row['SellPrice'] || row['Sell Price'] || row['sell_price'] || null;
      
      // Clean up sell price
      if (sellPrice && typeof sellPrice === 'string') {
        sellPrice = parseFloat(sellPrice.replace(/[$,]/g, ''));
      }
      if (sellPrice && typeof sellPrice === 'number' && !isNaN(sellPrice)) {
        sellPrice = sellPrice;
      } else {
        sellPrice = null;
      }
      
      // Validate required fields
      if (!customerGroupName || !customerName || !customerNumber) {
        errors.push(`Row ${i + 2}: Missing required fields`);
        continue;
      }
      
      // Normalize FCL/LCL
      let normalizedFclLcl = null;
      if (fclLcl) {
        const upper = fclLcl.toUpperCase();
        if (upper === 'FCL') normalizedFclLcl = 'FCL';
        else if (upper === 'LCL') normalizedFclLcl = 'LCL';
        else if (upper === 'BOTH') normalizedFclLcl = 'Both';
      }
      
      // Normalize Status
      let normalizedStatus = null;
      if (status) {
        const upper = status.toUpperCase();
        if (upper === 'EXISTING') normalizedStatus = 'Existing';
        else if (upper === 'NEW') normalizedStatus = 'New';
        else if (upper === 'NCF') normalizedStatus = 'NCF';
      }
      
      // Track unique customer groups and customers
      if (!customerGroupMap.has(customerGroupName)) {
        customerGroupMap.set(customerGroupName, null);
      }
      
      const customerKey = `${customerGroupName}|${customerName}`;
      if (!customerMap.has(customerKey)) {
        customerMap.set(customerKey, null);
      }
      
      parsedRows.push({
        customerGroupName,
        customerName,
        customerNumber,
        itemNumber,
        description,
        fclLcl: normalizedFclLcl,
        status: normalizedStatus,
        sellPrice
      });
    }
    
    console.log(`[UPLOAD] Parsed ${parsedRows.length} valid rows, ${customerGroupMap.size} customer groups, ${customerMap.size} customers`);
    
    // Phase 2: Use transactions for batch insert
    db.serialize(() => {
      db.run('BEGIN TRANSACTION');
      
      // Insert customer groups
      const cgNames = Array.from(customerGroupMap.keys());
      const cgStmt = db.prepare('INSERT OR IGNORE INTO customer_groups (name) VALUES (?)');
      cgNames.forEach(name => cgStmt.run(name));
      cgStmt.finalize();
      
      // Get all customer group IDs
      db.all('SELECT id, name FROM customer_groups', [], (err, cgRows) => {
        if (err) {
          db.run('ROLLBACK');
          return res.status(500).json({ error: 'Failed to get customer groups: ' + err.message });
        }
        
        cgRows.forEach(row => customerGroupMap.set(row.name, row.id));
        customerGroupsCreated = cgNames.length;
        
        // Insert customers
        const cStmt = db.prepare('INSERT OR IGNORE INTO customers (customer_group_id, name) VALUES (?, ?)');
        customerMap.forEach((_, key) => {
          const [groupName, custName] = key.split('|');
          const groupId = customerGroupMap.get(groupName);
          if (groupId) {
            cStmt.run(groupId, custName);
          }
        });
        cStmt.finalize();
        
        // Get all customer IDs
        db.all('SELECT c.id, c.name, c.customer_group_id, cg.name as group_name FROM customers c JOIN customer_groups cg ON c.customer_group_id = cg.id', [], (err, cRows) => {
          if (err) {
            db.run('ROLLBACK');
            return res.status(500).json({ error: 'Failed to get customers: ' + err.message });
          }
          
          cRows.forEach(row => {
            const key = `${row.group_name}|${row.name}`;
            customerMap.set(key, row.id);
          });
          customersCreated = customerMap.size;
          
          // Batch insert/update products
          const insertStmt = db.prepare(`
            INSERT INTO products (customer_id, customer_number, item_number, description, fcl_lcl, status, sell_price)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(customer_id, customer_number) DO UPDATE SET
              item_number = excluded.item_number,
              description = excluded.description,
              fcl_lcl = excluded.fcl_lcl,
              status = excluded.status,
              sell_price = excluded.sell_price,
              updated_at = CURRENT_TIMESTAMP
          `);
          
          parsedRows.forEach(row => {
            const customerKey = `${row.customerGroupName}|${row.customerName}`;
            const customerId = customerMap.get(customerKey);
            if (customerId) {
              insertStmt.run(customerId, row.customerNumber, row.itemNumber, row.description, row.fclLcl, row.status, row.sellPrice);
              productsCreated++;
            }
          });
          
          insertStmt.finalize((err) => {
            if (err) {
              db.run('ROLLBACK');
              return res.status(500).json({ error: 'Failed to insert products: ' + err.message });
            }
            
            db.run('COMMIT', (err) => {
              if (err) {
                return res.status(500).json({ error: 'Failed to commit: ' + err.message });
              }
              
              console.log(`[UPLOAD] Complete: ${customerGroupsCreated} groups, ${customersCreated} customers, ${productsCreated} products`);
              
              res.json({
                message: 'Upload complete',
                summary: {
                  total_rows: data.length,
                  customer_groups_created: customerGroupsCreated,
                  customers_created: customersCreated,
                  products_created: productsCreated,
                  products_updated: productsUpdated,
                  errors: errors.length
                },
                errors: errors.slice(0, 50)
              });
            });
          });
        });
      });
    });
    
  } catch (error) {
    console.error('[UPLOAD] Error parsing file:', error);
    res.status(500).json({ error: 'Failed to parse file: ' + error.message });
  }
});

// ============ ITEM NUMBERS (PIM DATA) ============

// Get all item numbers with pagination
app.get('/api/item-numbers', authenticateToken, (req, res) => {
  const limit = parseInt(req.query.limit) || 50;
  const offset = parseInt(req.query.offset) || 0;
  const { search, series, taxonomy, brand, dev_status, spec_status } = req.query;
  
  let whereConditions = [];
  let params = [];
  
  if (search) {
    whereConditions.push('(item_number LIKE ? OR legal_name LIKE ? OR product_description_internal LIKE ?)');
    const searchPattern = `%${search}%`;
    params.push(searchPattern, searchPattern, searchPattern);
  }
  
  if (series) {
    whereConditions.push('series = ?');
    params.push(series);
  }
  
  if (taxonomy) {
    whereConditions.push('product_taxonomy_category = ?');
    params.push(taxonomy);
  }
  
  if (brand) {
    whereConditions.push('brand_product_line = ?');
    params.push(brand);
  }
  
  if (dev_status) {
    whereConditions.push('product_development_status = ?');
    params.push(dev_status);
  }
  
  if (spec_status) {
    whereConditions.push('item_spec_sheet_status = ?');
    params.push(spec_status);
  }
  
  const whereClause = whereConditions.length > 0 ? 'WHERE ' + whereConditions.join(' AND ') : '';
  
  const countParams = [...params];
  params.push(limit, offset);
  
  db.get(`SELECT COUNT(*) as count FROM item_numbers ${whereClause}`, countParams, (countErr, countRow) => {
    if (countErr) {
      return res.status(500).json({ error: countErr.message });
    }
    
    db.all(`SELECT * FROM item_numbers ${whereClause} ORDER BY item_number LIMIT ? OFFSET ?`, params, (err, rows) => {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      
      res.json({
        data: rows,
        pagination: {
          total: countRow.count,
          limit: limit,
          offset: offset,
          page: Math.floor(offset / limit) + 1,
          total_pages: Math.ceil(countRow.count / limit)
        }
      });
    });
  });
});

// Get unique filter values for item_numbers
app.get('/api/item-numbers/filters', authenticateToken, (req, res) => {
  const queries = {
    series: 'SELECT DISTINCT series FROM item_numbers WHERE series IS NOT NULL AND series != "" ORDER BY series',
    taxonomies: 'SELECT DISTINCT product_taxonomy_category FROM item_numbers WHERE product_taxonomy_category IS NOT NULL AND product_taxonomy_category != "" ORDER BY product_taxonomy_category',
    brands: 'SELECT DISTINCT brand_product_line FROM item_numbers WHERE brand_product_line IS NOT NULL AND brand_product_line != "" ORDER BY brand_product_line',
    dev_statuses: 'SELECT DISTINCT product_development_status FROM item_numbers WHERE product_development_status IS NOT NULL AND product_development_status != "" ORDER BY product_development_status',
    spec_statuses: 'SELECT DISTINCT item_spec_sheet_status FROM item_numbers WHERE item_spec_sheet_status IS NOT NULL AND item_spec_sheet_status != "" ORDER BY item_spec_sheet_status',
    age_grades: 'SELECT DISTINCT age_grade FROM item_numbers WHERE age_grade IS NOT NULL AND age_grade != "" ORDER BY age_grade'
  };
  
  const results = {};
  const keys = Object.keys(queries);
  let completed = 0;
  
  keys.forEach(key => {
    db.all(queries[key], [], (err, rows) => {
      if (err) {
        results[key] = [];
      } else {
        results[key] = rows.map(r => Object.values(r)[0]);
      }
      completed++;
      if (completed === keys.length) {
        res.json(results);
      }
    });
  });
});

// Sync PIM data from Excel file
app.post('/api/sync/pim', authenticateToken, requireRole('salesperson', 'admin'), (req, res) => {
  const pimPath = 'A:\\Code\\InputFiles\\PIM Extract.xlsx';
  
  if (!fs.existsSync(pimPath)) {
    return res.status(404).json({ error: 'PIM Extract file not found' });
  }
  
  try {
    console.log('[SYNC] Reading PIM Extract...');
    const workbook = xlsx.readFile(pimPath);
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const data = xlsx.utils.sheet_to_json(worksheet);
    
    console.log(`[SYNC] Processing ${data.length} PIM records...`);
    let updated = 0;
    let created = 0;
    let processed = 0;
    
    if (data.length === 0) {
      return res.json({ success: true, message: 'No data to process' });
    }
    
    const processRow = (row) => {
      return new Promise((resolve) => {
        const itemNumber = row['Item Number'];
        if (!itemNumber) {
          resolve();
          return;
        }
        
        const updateData = {
          series: row['Series'] || null,
          product_taxonomy_category: row['Product Taxonomy Category'] || null,
          legal_name: row['Legal Name'] || null,
          upc_number: row['UPC Number'] || null,
          brand_product_line: row['Brand (Product Line)'] || null,
          age_grade: row['Age Grade'] || null,
          product_description_internal: row['Product Description (internal)'] || null,
          item_spec_sheet_status: row['Item Spec Sheet Status'] || null,
          product_development_status: row['Product Development Status'] || null,
          item_spec_data_last_updated: row['Item Spec Data Last Updated'] || null,
          case_pack: row['Case Pack'] || null,
          package_length_cm: row['Single Package Size - Length (cm)'] || null,
          package_width_cm: row['Single Package Size - Width (cm)'] || null,
          package_height_cm: row['Single Package Size - Height (cm)'] || null,
          package_weight_kg: row['Single Package Size - Weight (kg)'] || null,
          product_number: row['Product Number'] || null
        };
        
        // Check if exists
        db.get('SELECT id FROM item_numbers WHERE item_number = ?', [itemNumber], (err, existing) => {
          if (err) {
            resolve();
            return;
          }
          
          if (existing) {
            // Update
            db.run(`
              UPDATE item_numbers 
              SET series = ?, product_taxonomy_category = ?, legal_name = ?, upc_number = ?,
                  brand_product_line = ?, age_grade = ?, product_description_internal = ?,
                  item_spec_sheet_status = ?, product_development_status = ?, item_spec_data_last_updated = ?,
                  case_pack = ?, package_length_cm = ?, package_width_cm = ?, package_height_cm = ?,
                  package_weight_kg = ?, product_number = ?, last_synced = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
              WHERE item_number = ?
            `, [
              updateData.series, updateData.product_taxonomy_category, updateData.legal_name, updateData.upc_number,
              updateData.brand_product_line, updateData.age_grade, updateData.product_description_internal,
              updateData.item_spec_sheet_status, updateData.product_development_status, updateData.item_spec_data_last_updated,
              updateData.case_pack, updateData.package_length_cm, updateData.package_width_cm, updateData.package_height_cm,
              updateData.package_weight_kg, updateData.product_number, itemNumber
            ], function(err) {
              if (!err && this.changes > 0) updated++;
              resolve();
            });
          } else {
            // Insert
            db.run(`
              INSERT INTO item_numbers (item_number, series, product_taxonomy_category, legal_name, upc_number,
                  brand_product_line, age_grade, product_description_internal, item_spec_sheet_status,
                  product_development_status, item_spec_data_last_updated, case_pack, package_length_cm,
                  package_width_cm, package_height_cm, package_weight_kg, product_number)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `, [
              itemNumber, updateData.series, updateData.product_taxonomy_category, updateData.legal_name, updateData.upc_number,
              updateData.brand_product_line, updateData.age_grade, updateData.product_description_internal,
              updateData.item_spec_sheet_status, updateData.product_development_status, updateData.item_spec_data_last_updated,
              updateData.case_pack, updateData.package_length_cm, updateData.package_width_cm, updateData.package_height_cm,
              updateData.package_weight_kg, updateData.product_number
            ], function(err) {
              if (!err && this.changes > 0) created++;
              resolve();
            });
          }
        });
      });
    };
    
    const processAll = async () => {
      for (const row of data) {
        await processRow(row);
        processed++;
      }
      
      console.log(`[SYNC] PIM Complete: ${created} created, ${updated} updated`);
      res.json({
        success: true,
        message: `PIM sync complete: ${created} created, ${updated} updated`,
        created,
        updated,
        total: data.length
      });
    };
    
    processAll().catch(err => {
      console.error('[SYNC] PIM Error:', err);
      res.status(500).json({ error: err.message });
    });
    
  } catch (error) {
    console.error('[SYNC] PIM Error:', error);
    res.status(500).json({ error: error.message });
  }
});

// ============ DATABASE EXPLORER ============

app.get('/api/database/:table', authenticateToken, (req, res) => {
  const tableName = req.params.table;
  
  const allowedTables = [
    'customer_groups',
    'customers',
    'products',
    'item_numbers',
    'users',
    'notifications',
    'fx_rates'
  ];
  
  if (!allowedTables.includes(tableName)) {
    return res.status(400).json({ error: 'Invalid table name' });
  }
  
  const limit = parseInt(req.query.limit) || 100;
  const offset = parseInt(req.query.offset) || 0;
  
  db.all(`SELECT * FROM ${tableName} LIMIT ? OFFSET ?`, [limit, offset], (err, rows) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    
    db.get(`SELECT COUNT(*) as count FROM ${tableName}`, (countErr, countRow) => {
      if (countErr) {
        return res.status(500).json({ error: countErr.message });
      }
      
      db.all(`PRAGMA table_info(${tableName})`, (schemaErr, columns) => {
        if (schemaErr) {
          return res.status(500).json({ error: schemaErr.message });
        }
        
        res.json({
          table: tableName,
          columns: columns,
          rows: rows,
          total: countRow.count,
          limit: limit,
          offset: offset
        });
      });
    });
  });
});

// ============ NOTIFICATIONS ============

app.get('/api/notifications', authenticateToken, (req, res) => {
  const { unread_only } = req.query;
  
  let query = 'SELECT * FROM notifications WHERE user_id = ?';
  if (unread_only === 'true') {
    query += ' AND is_read = 0';
  }
  query += ' ORDER BY created_at DESC LIMIT 50';
  
  db.all(query, [req.user.id], (err, rows) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    res.json(rows);
  });
});

app.put('/api/notifications/:id/read', authenticateToken, (req, res) => {
  db.run('UPDATE notifications SET is_read = 1 WHERE id = ? AND user_id = ?', 
    [req.params.id, req.user.id], 
    function(err) {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      res.json({ message: 'Notification marked as read' });
    }
  );
});

app.put('/api/notifications/read-all', authenticateToken, (req, res) => {
  db.run('UPDATE notifications SET is_read = 1 WHERE user_id = ?', 
    [req.user.id], 
    function(err) {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      res.json({ message: 'All notifications marked as read', count: this.changes });
    }
  );
});

// ============ FX RATES ============

app.get('/api/fx-rates', authenticateToken, (req, res) => {
  db.all('SELECT * FROM fx_rates ORDER BY country', [], (err, rows) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    res.json(rows);
  });
});

app.put('/api/fx-rates/:id', authenticateToken, requireRole('admin'), (req, res) => {
  const { id } = req.params;
  const { rate_to_usd } = req.body;
  
  if (!rate_to_usd || rate_to_usd <= 0) {
    return res.status(400).json({ error: 'Invalid FX rate' });
  }
  
  db.run(`
    UPDATE fx_rates 
    SET rate_to_usd = ?, 
        updated_at = CURRENT_TIMESTAMP,
        updated_by = ?
    WHERE id = ?
  `, [rate_to_usd, req.user.id, id], function(err) {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    if (this.changes === 0) {
      return res.status(404).json({ error: 'FX rate not found' });
    }
    res.json({ message: 'FX rate updated successfully' });
  });
});

app.post('/api/fx-rates/bulk-update', authenticateToken, requireRole('admin'), (req, res) => {
  const { rates } = req.body;
  
  if (!Array.isArray(rates) || rates.length === 0) {
    return res.status(400).json({ error: 'Invalid rates data' });
  }
  
  db.serialize(() => {
    db.run('BEGIN TRANSACTION');
    
    const stmt = db.prepare(`
      UPDATE fx_rates 
      SET rate_to_usd = ?, 
          updated_at = CURRENT_TIMESTAMP,
          updated_by = ?
      WHERE id = ?
    `);
    
    rates.forEach(rate => {
      stmt.run([rate.rate_to_usd, req.user.id, rate.id]);
    });
    
    stmt.finalize((err) => {
      if (err) {
        db.run('ROLLBACK');
        return res.status(500).json({ error: err.message });
      }
      
      db.run('COMMIT', (err) => {
        if (err) {
          return res.status(500).json({ error: err.message });
        }
        res.json({ message: `${rates.length} FX rates updated successfully` });
      });
    });
  });
});

// ============ STATS & DASHBOARD ============

app.get('/api/stats', authenticateToken, (req, res) => {
  const stats = {};
  
  const queries = [
    { key: 'total_customer_groups', sql: 'SELECT COUNT(*) as count FROM customer_groups' },
    { key: 'total_customers', sql: 'SELECT COUNT(*) as count FROM customers' },
    { key: 'total_products', sql: 'SELECT COUNT(*) as count FROM products' },
    { key: 'total_item_numbers', sql: 'SELECT COUNT(*) as count FROM item_numbers' },
    { key: 'products_existing', sql: "SELECT COUNT(*) as count FROM products WHERE status = 'Existing'" },
    { key: 'products_new', sql: "SELECT COUNT(*) as count FROM products WHERE status = 'New'" },
    { key: 'products_ncf', sql: "SELECT COUNT(*) as count FROM products WHERE status = 'NCF'" }
  ];
  
  let completed = 0;
  
  queries.forEach(q => {
    db.get(q.sql, [], (err, row) => {
      stats[q.key] = err ? 0 : row.count;
      completed++;
      if (completed === queries.length) {
        res.json(stats);
      }
    });
  });
});

// Catch-all route for SPA
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start server
app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\n[SHUTDOWN] Closing database...');
  db.close((err) => {
    if (err) {
      console.error(err.message);
    }
    console.log('Database connection closed');
    process.exit(0);
  });
});
