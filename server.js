const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');
const csv = require('csv-parser');
const duckdb = require('duckdb');
const xlsx = require('xlsx');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const session = require('express-session');
const cookieParser = require('cookie-parser');
const SyncScheduler = require('./syncScheduler');

const app = express();
const PORT = process.env.PORT || 7777;
const JWT_SECRET = process.env.JWT_SECRET || 'your-secret-key-change-in-production';

let syncScheduler; // Global sync scheduler instance

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
      const defaultPassword = 'admin123'; // Change this in production!
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

// Hardcoded Amazon vendor mapping data
function insertDefaultVendorMapping() {
  const amazonMappings = [
    // United States
    { customer: 'Amazon', country: 'United States', keepa_marketplace: 'United States', customer_code: 'C002', vendor_code: null, qpi_source_file: 'US_QPI.parquet', vc_file: 'US_VC.parquet', language: 'English', currency: 'USD', domain: 1 },
    // Canada
    { customer: 'Amazon', country: 'Canada', keepa_marketplace: 'Canada', customer_code: 'C055', vendor_code: null, qpi_source_file: 'CA_QPI.parquet', vc_file: 'CA_VC.parquet', language: 'English', currency: 'CAD', domain: 7 },
    // Mexico
    { customer: 'Amazon', country: 'Mexico', keepa_marketplace: 'Mexico', customer_code: 'C059', vendor_code: null, qpi_source_file: 'MX_QPI.parquet', vc_file: 'MX_VC.parquet', language: 'Spanish', currency: 'MXN', domain: 8 },
    // United Kingdom
    { customer: 'Amazon', country: 'United Kingdom', keepa_marketplace: 'United Kingdom', customer_code: 'C003', vendor_code: null, qpi_source_file: 'UK_QPI.parquet', vc_file: 'UK_VC.parquet', language: 'English', currency: 'GBP', domain: 3 },
    // Germany
    { customer: 'Amazon', country: 'Germany', keepa_marketplace: 'Germany', customer_code: 'C004', vendor_code: null, qpi_source_file: 'DE_QPI.parquet', vc_file: 'DE_VC.parquet', language: 'German', currency: 'EUR', domain: 4 },
    // France
    { customer: 'Amazon', country: 'France', keepa_marketplace: 'France', customer_code: 'C005', vendor_code: null, qpi_source_file: 'FR_QPI.parquet', vc_file: 'FR_VC.parquet', language: 'French', currency: 'EUR', domain: 5 },
    // Italy
    { customer: 'Amazon', country: 'Italy', keepa_marketplace: 'Italy', customer_code: 'C035', vendor_code: null, qpi_source_file: 'IT_QPI.parquet', vc_file: 'IT_VC.parquet', language: 'Italian', currency: 'EUR', domain: 35 },
    // Spain
    { customer: 'Amazon', country: 'Spain', keepa_marketplace: 'Spain', customer_code: 'C044', vendor_code: null, qpi_source_file: 'ES_QPI.parquet', vc_file: 'ES_VC.parquet', language: 'Spanish', currency: 'EUR', domain: 44 },
    // Japan
    { customer: 'Amazon', country: 'Japan', keepa_marketplace: 'Japan', customer_code: 'C006', vendor_code: null, qpi_source_file: 'JP_QPI.parquet', vc_file: 'JP_VC.parquet', language: 'Japanese', currency: 'JPY', domain: 6 },
    // Australia
    { customer: 'Amazon', country: 'Australia', keepa_marketplace: 'Australia', customer_code: 'C071', vendor_code: null, qpi_source_file: 'AU_QPI.parquet', vc_file: 'AU_VC.parquet', language: 'English', currency: 'AUD', domain: 71 },
    // Singapore
    { customer: 'Amazon', country: 'Singapore', keepa_marketplace: 'Singapore', customer_code: 'C052', vendor_code: null, qpi_source_file: 'SG_QPI.parquet', vc_file: 'SG_VC.parquet', language: 'English', currency: 'SGD', domain: 52 },
    // United Arab Emirates
    { customer: 'Amazon', country: 'United Arab Emirates', keepa_marketplace: 'United Arab Emirates', customer_code: 'C062', vendor_code: null, qpi_source_file: 'AE_QPI.parquet', vc_file: 'AE_VC.parquet', language: 'Arabic', currency: 'AED', domain: 62 },
    // India
    { customer: 'Amazon', country: 'India', keepa_marketplace: 'India', customer_code: 'C031', vendor_code: null, qpi_source_file: 'IN_QPI.parquet', vc_file: 'IN_VC.parquet', language: 'English', currency: 'INR', domain: 31 },
    // Brazil
    { customer: 'Amazon', country: 'Brazil', keepa_marketplace: 'Brazil', customer_code: 'C029', vendor_code: null, qpi_source_file: 'BR_QPI.parquet', vc_file: 'BR_VC.parquet', language: 'Portuguese', currency: 'BRL', domain: 29 }
  ];

  const insertStmt = db.prepare(`
    INSERT OR IGNORE INTO vendor_mapping 
    (customer, country, keepa_marketplace, customer_code, vendor_code, qpi_source_file, vc_file, language, currency, domain)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  db.serialize(() => {
    db.run('BEGIN TRANSACTION');
    amazonMappings.forEach(mapping => {
      insertStmt.run(
        mapping.customer,
        mapping.country,
        mapping.keepa_marketplace,
        mapping.customer_code,
        mapping.vendor_code,
        mapping.qpi_source_file,
        mapping.vc_file,
        mapping.language,
        mapping.currency,
        mapping.domain
      );
    });
    insertStmt.finalize();
    db.run('COMMIT', (err) => {
      if (err) {
        console.error('[SETUP] Error inserting default mappings:', err);
      } else {
        console.log('[SETUP] ✓ Inserted', amazonMappings.length, 'Amazon marketplace mappings');
      }
    });
  });
}

// Initialize database tables
function initializeDatabase() {
  db.serialize(() => {
    // Products table - ASIN is the master ID
    db.run(`CREATE TABLE IF NOT EXISTS products (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      asin TEXT UNIQUE NOT NULL,
      name TEXT,
      is_temp_asin BOOLEAN DEFAULT 0,
      primary_item_number TEXT,
      brand TEXT,
      age_grade TEXT,
      product_description TEXT,
      legal_name TEXT,
      upc_number TEXT,
      pim_spec_status TEXT,
      product_dev_status TEXT,
      package_length_cm REAL,
      package_width_cm REAL,
      package_height_cm REAL,
      package_weight_kg REAL,
      stage_1_idea_considered BOOLEAN DEFAULT 0,
      stage_1_brand TEXT,
      stage_1_description TEXT,
      stage_1_season_launch TEXT,
      stage_1_country TEXT,
      stage_1_item_number TEXT,
      stage_2_product_finalized BOOLEAN DEFAULT 0,
      stage_2_newly_finalized BOOLEAN DEFAULT 0,
      stage_3a_pricing_submitted BOOLEAN DEFAULT 0,
      stage_3b_pricing_approved BOOLEAN DEFAULT 0,
      stage_4_product_listed BOOLEAN DEFAULT 0,
      stage_5_product_ordered BOOLEAN DEFAULT 0,
      stage_6_product_online BOOLEAN DEFAULT 0,
      stage_7_end_of_life BOOLEAN DEFAULT 0,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);
    
    // Add stage_7_end_of_life column if it doesn't exist (for existing databases)
    db.run(`ALTER TABLE products ADD COLUMN stage_7_end_of_life BOOLEAN DEFAULT 0`, (err) => {
      if (err && !err.message.includes('duplicate column')) {
        console.error('Error adding stage_7_end_of_life column:', err.message);
      }
    });
    // Product SKUs table - Links multiple SKUs to one ASIN
    db.run(`CREATE TABLE IF NOT EXISTS product_skus (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      product_id INTEGER NOT NULL,
      sku TEXT UNIQUE NOT NULL,
      is_primary BOOLEAN DEFAULT 0,
      source TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
    )`);
    
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
    )`);
    
    // Add series and product_taxonomy_category columns if they don't exist (for existing databases)
    db.run(`ALTER TABLE item_numbers ADD COLUMN series TEXT`, (err) => {
      if (err && !err.message.includes('duplicate column')) {
        console.error('Error adding series column:', err.message);
      }
    });
    db.run(`ALTER TABLE item_numbers ADD COLUMN product_taxonomy_category TEXT`, (err) => {
      if (err && !err.message.includes('duplicate column')) {
        console.error('Error adding product_taxonomy_category column:', err.message);
      }
    });
    // Temp ASIN counter table
    db.run(`CREATE TABLE IF NOT EXISTS temp_asin_counter (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      counter INTEGER DEFAULT 1
    )`);
    
    db.run(`INSERT OR IGNORE INTO temp_asin_counter (id, counter) VALUES (1, 1)`);
    
    // Add primary_item_number column to products if it doesn't exist (for existing databases)
    db.run(`ALTER TABLE products ADD COLUMN primary_item_number TEXT`, (err) => {
      if (err && !err.message.includes('duplicate column')) {
        console.error('Error adding primary_item_number column:', err.message);
      }
    });

    // Legacy items table - will migrate data from this
    db.run(`CREATE TABLE IF NOT EXISTS items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sku TEXT UNIQUE NOT NULL,
      asin TEXT,
      name TEXT NOT NULL,
      dimensions TEXT,
      case_pack INTEGER,
      sioc_status TEXT,
      vendor_central_setup BOOLEAN DEFAULT 0,
      btr_submission BOOLEAN DEFAULT 0,
      btr_optional BOOLEAN DEFAULT 0,
      order_received BOOLEAN DEFAULT 0,
      order_shipped BOOLEAN DEFAULT 0,
      online_available BOOLEAN DEFAULT 0,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    // Migration: Add asin column if it doesn't exist
    db.run(`SELECT sql FROM sqlite_master WHERE name='items'`, (err, row) => {
      if (!err) {
        db.run(`PRAGMA table_info(items)`, [], (err, rows) => {
          if (!err) {
            db.all(`PRAGMA table_info(items)`, [], (err, columns) => {
              if (!err) {
                const hasAsin = columns.some(col => col.name === 'asin');
                if (!hasAsin) {
                  console.log('Adding asin column to items table...');
                  db.run(`ALTER TABLE items ADD COLUMN asin TEXT`, (err) => {
                    if (err) {
                      console.error('Error adding asin column:', err.message);
                    } else {
                      console.log('ASIN column added successfully');
                    }
                  });
                }
                
                // Migration: Add PIM-related columns
                const pimColumns = [
                  { name: 'legal_name', type: 'TEXT' },
                  { name: 'upc_number', type: 'TEXT' },
                  { name: 'brand', type: 'TEXT' },
                  { name: 'age_grade', type: 'TEXT' },
                  { name: 'product_description', type: 'TEXT' },
                  { name: 'pim_spec_status', type: 'TEXT' },
                  { name: 'product_dev_status', type: 'TEXT' },
                  { name: 'package_length_cm', type: 'REAL' },
                  { name: 'package_width_cm', type: 'REAL' },
                  { name: 'package_height_cm', type: 'REAL' },
                  { name: 'package_weight_kg', type: 'REAL' }
                ];
                
                pimColumns.forEach(col => {
                  const hasColumn = columns.some(c => c.name === col.name);
                  if (!hasColumn) {
                    db.run(`ALTER TABLE items ADD COLUMN ${col.name} ${col.type}`, (err) => {
                      if (err && !err.message.includes('duplicate column')) {
                        console.error(`Error adding ${col.name} column:`, err.message);
                      }
                    });
                  }
                });
                
                // Migration: Add new flow stage columns
                const flowColumns = [
                  { name: 'stage_1_idea_considered', type: 'BOOLEAN DEFAULT 0' },
                  { name: 'stage_1_brand', type: 'TEXT' },
                  { name: 'stage_1_description', type: 'TEXT' },
                  { name: 'stage_1_season_launch', type: 'TEXT' },
                  { name: 'stage_1_country', type: 'TEXT' },
                  { name: 'stage_1_item_number', type: 'TEXT' },
                  { name: 'stage_2_product_finalized', type: 'BOOLEAN DEFAULT 0' },
                  { name: 'stage_2_newly_finalized', type: 'BOOLEAN DEFAULT 0' },
                  { name: 'stage_3a_pricing_submitted', type: 'BOOLEAN DEFAULT 0' },
                  { name: 'stage_3b_pricing_approved', type: 'BOOLEAN DEFAULT 0' },
                  { name: 'stage_4_product_listed', type: 'BOOLEAN DEFAULT 0' },
                  { name: 'stage_5_product_ordered', type: 'BOOLEAN DEFAULT 0' },
                  { name: 'stage_6_product_online', type: 'BOOLEAN DEFAULT 0' }
                ];
                
                flowColumns.forEach(col => {
                  const hasColumn = columns.some(c => c.name === col.name);
                  if (!hasColumn) {
                    db.run(`ALTER TABLE items ADD COLUMN ${col.name} ${col.type}`, (err) => {
                      if (err && !err.message.includes('duplicate column')) {
                        console.error(`Error adding ${col.name} column:`, err.message);
                      }
                    });
                  }
                });
                
                // Also migrate products table
                db.all('PRAGMA table_info(products)', (err, prodColumns) => {
                  if (!err && prodColumns) {
                    flowColumns.forEach(col => {
                      const hasColumn = prodColumns.some(c => c.name === col.name);
                      if (!hasColumn) {
                        db.run(`ALTER TABLE products ADD COLUMN ${col.name} ${col.type}`, (err) => {
                          if (err && !err.message.includes('duplicate column')) {
                            console.error(`Error adding ${col.name} to products:`, err.message);
                          }
                        });
                      }
                    });
                  }
                });
              }
            });
          }
        });
      }
    });

    // Country pricing table - references products (ASIN)
    db.run(`CREATE TABLE IF NOT EXISTS product_country_pricing (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      product_id INTEGER NOT NULL,
      country_code TEXT NOT NULL,
      retail_price DECIMAL(10, 2),
      sell_price DECIMAL(10, 2),
      currency TEXT NOT NULL,
      approval_status TEXT DEFAULT 'pending',
      approved_at DATETIME,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
      UNIQUE(product_id, country_code)
    )`);
    
    // Legacy item_country_pricing for migration
    db.run(`CREATE TABLE IF NOT EXISTS item_country_pricing (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      item_id INTEGER NOT NULL,
      country_code TEXT NOT NULL,
      retail_price DECIMAL(10, 2),
      sell_price DECIMAL(10, 2),
      currency TEXT NOT NULL,
      approval_status TEXT DEFAULT 'pending',
      approved_at DATETIME,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (item_id) REFERENCES items(id) ON DELETE CASCADE,
      UNIQUE(item_id, country_code)
    )`);

    // Flow stage history table - references products
    db.run(`CREATE TABLE IF NOT EXISTS flow_stage_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      product_id INTEGER NOT NULL,
      stage_name TEXT NOT NULL,
      completed BOOLEAN DEFAULT 0,
      completed_at DATETIME,
      notes TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
    )`);

    // ASIN Country Status table - tracks VC listing status per country
    db.run(`CREATE TABLE IF NOT EXISTS asin_country_status (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      asin TEXT NOT NULL,
      sku TEXT,
      country_code TEXT NOT NULL,
      vc_status TEXT,
      last_synced DATETIME DEFAULT CURRENT_TIMESTAMP,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(asin, country_code)
    )`);

    // QPI Country Status table - tracks which SKUs/ASINs are in QPI per country
    db.run(`CREATE TABLE IF NOT EXISTS qpi_country_status (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      asin TEXT NOT NULL,
      sku TEXT,
      country_code TEXT NOT NULL,
      in_qpi BOOLEAN DEFAULT 1,
      last_synced DATETIME DEFAULT CURRENT_TIMESTAMP,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(asin, country_code)
    )`);
    
    // QPI file tracking table - check if migration needed first
    db.all('PRAGMA table_info(qpi_file_tracking)', (err, columns) => {
      if (err || !columns || columns.length === 0) {
        // Table doesn't exist, create new one
        db.run(`CREATE TABLE IF NOT EXISTS qpi_file_tracking (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          asin TEXT NOT NULL,
          sku TEXT,
          source_file TEXT NOT NULL,
          status TEXT,
          qpi_sync_date DATE,
          last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
          UNIQUE(asin, source_file)
        )`, (createErr) => {
          if (createErr) {
            console.error('Error creating qpi_file_tracking table:', createErr.message);
          } else {
            console.log('[SETUP] qpi_file_tracking table ready');
            // Add status column if it doesn't exist (for existing databases)
            db.run(`ALTER TABLE qpi_file_tracking ADD COLUMN status TEXT`, (err) => {
              if (err && !err.message.includes('duplicate column')) {
                console.error('Error adding status column:', err.message);
              }
            });
          }
          callback();
        });
      } else {
        // Table exists, check if migration needed
        const hasSourceFile = columns.some(c => c.name === 'source_file');
        const hasOldFileNameColumn = columns.some(c => c.name === 'qpi_file_name');
        
        // If table has old structure, drop and recreate
        if (hasOldFileNameColumn && !hasSourceFile) {
          console.log('[MIGRATION] Updating qpi_file_tracking table structure...');
          db.serialize(() => {
            db.run('DROP TABLE qpi_file_tracking', (dropErr) => {
              if (dropErr) {
                console.error('Error dropping old qpi_file_tracking table:', dropErr.message);
                return;
              }
              
              db.run(`CREATE TABLE qpi_file_tracking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asin TEXT NOT NULL,
                sku TEXT,
                source_file TEXT NOT NULL,
                qpi_sync_date DATE,
                last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(asin, source_file)
              )`, (createErr) => {
                if (createErr) {
                  console.error('Error creating new qpi_file_tracking table:', createErr.message);
                } else {
                  console.log('[MIGRATION] qpi_file_tracking table updated successfully. Please re-sync QPI data.');
                }
              });
            });
          });
        } else if (hasSourceFile) {
          console.log('[SETUP] qpi_file_tracking table ready');
        }
      }
    });
    
    // Variations Master data table
    db.run(`CREATE TABLE IF NOT EXISTS variations_master (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      asin TEXT UNIQUE NOT NULL,
      brand TEXT,
      title TEXT,
      bundle TEXT,
      ppg TEXT,
      synced_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`, (err) => {
      if (err) {
        console.error('Error creating variations_master table:', err.message);
      } else {
        console.log('[SETUP] variations_master table ready');
      }
    });
    
    // Vendor code mapping table
    db.run(`CREATE TABLE IF NOT EXISTS vendor_mapping (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      customer TEXT DEFAULT 'Amazon',
      country TEXT NOT NULL,
      keepa_marketplace TEXT NOT NULL,
      customer_code TEXT NOT NULL,
      vendor_code TEXT,
      qpi_source_file TEXT,
      vc_file TEXT,
      language TEXT,
      currency TEXT,
      domain INTEGER,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(customer_code, vendor_code)
    )`, (err) => {
      if (err) {
        console.error('Error creating vendor_mapping table:', err.message);
      } else {
        console.log('[SETUP] vendor_mapping table ready');
        // Insert hardcoded Amazon mapping data if table is empty
        db.get('SELECT COUNT(*) as count FROM vendor_mapping', [], (err, row) => {
          if (err) {
            console.error('Error checking vendor_mapping:', err);
          } else if (row.count === 0) {
            console.log('[SETUP] Inserting hardcoded Amazon vendor mapping...');
            insertDefaultVendorMapping();
          }
        });
      }
    });
    
    // Online status tracking table
    db.run(`CREATE TABLE IF NOT EXISTS asin_online_status (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      asin TEXT NOT NULL,
      country TEXT NOT NULL,
      first_seen_online DATE,
      last_seen_online DATE,
      last_buybox_price DECIMAL,
      last_synced DATETIME,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(asin, country)
    )`, (err) => {
      if (err) {
        console.error('Error creating asin_online_status table:', err.message);
      } else {
        console.log('[SETUP] asin_online_status table ready');
      }
    });
    
    // Keepa file tracking table
    db.run(`CREATE TABLE IF NOT EXISTS keepa_file_tracking (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      filename TEXT UNIQUE NOT NULL,
      processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      asins_found INTEGER,
      asins_online INTEGER
    )`, (err) => {
      if (err) {
        console.error('Error creating keepa_file_tracking table:', err.message);
      } else {
        console.log('[SETUP] keepa_file_tracking table ready');
      }
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
    
    // Pricing submissions table
    db.run(`CREATE TABLE IF NOT EXISTS pricing_submissions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      product_id INTEGER NOT NULL,
      asin TEXT NOT NULL,
      product_cost DECIMAL(10,2) NOT NULL,
      sell_price DECIMAL(10,2) NOT NULL,
      company_margin DECIMAL(5,2) NOT NULL,
      retail_price DECIMAL(10,2) NOT NULL,
      customer_margin DECIMAL(5,2) NOT NULL,
      currency TEXT DEFAULT 'USD',
      notes TEXT,
      status TEXT CHECK(status IN ('pending', 'approved', 'rejected')) DEFAULT 'pending',
      submitted_by INTEGER NOT NULL,
      submitted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      reviewed_by INTEGER,
      reviewed_at DATETIME,
      review_notes TEXT,
      FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
      FOREIGN KEY (submitted_by) REFERENCES users(id),
      FOREIGN KEY (reviewed_by) REFERENCES users(id)
    )`, (err) => {
      if (err) {
        console.error('Error creating pricing_submissions table:', err.message);
      } else {
        console.log('[SETUP] pricing_submissions table ready');
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
      if (err) {
        console.error('Error creating notifications table:', err.message);
      } else {
        console.log('[SETUP] notifications table ready');
      }
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
    
    // Pricing submissions by country table
    db.run(`CREATE TABLE IF NOT EXISTS pricing_submissions_country (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      pricing_submission_id INTEGER NOT NULL,
      country TEXT NOT NULL,
      sell_price_usd DECIMAL(10,2) NOT NULL,
      retail_price_local DECIMAL(10,2) NOT NULL,
      retail_price_usd DECIMAL(10,2) NOT NULL,
      customer_margin DECIMAL(5,2) NOT NULL,
      fx_rate DECIMAL(10,6) NOT NULL,
      FOREIGN KEY (pricing_submission_id) REFERENCES pricing_submissions(id) ON DELETE CASCADE
    )`, (err) => {
      if (err) {
        console.error('Error creating pricing_submissions_country table:', err.message);
      } else {
        console.log('[SETUP] pricing_submissions_country table ready');
        // Add sell_price_usd column if it doesn't exist (for existing databases)
        db.run(`ALTER TABLE pricing_submissions_country ADD COLUMN sell_price_usd DECIMAL(10,2)`, (err) => {
          if (err && !err.message.includes('duplicate column')) {
            console.error('Note: Could not add sell_price_usd column:', err.message);
          }
        });
      }
    });

    console.log('Database tables initialized');
    
    // Initialize and start sync scheduler
    initializeSyncScheduler();
  });
}

// Helper function to generate temp ASIN
function generateTempAsin(callback) {
  db.get('SELECT counter FROM temp_asin_counter WHERE id = 1', (err, row) => {
    if (err) {
      callback(err, null);
      return;
    }
    const counter = row ? row.counter : 1;
    const tempAsin = `TEMP${String(counter).padStart(6, '0')}`;
    
    db.run('UPDATE temp_asin_counter SET counter = counter + 1 WHERE id = 1', (err) => {
      callback(err, tempAsin);
    });
  });
}

// Helper function to find or create product by ASIN
function findOrCreateProduct(asin, name, callback) {
  if (!asin) {
    // Generate temp ASIN
    generateTempAsin((err, tempAsin) => {
      if (err) {
        callback(err, null);
        return;
      }
      
      db.run(
        'INSERT INTO products (asin, name, is_temp_asin) VALUES (?, ?, 1)',
        [tempAsin, name || tempAsin],
        function(err) {
          if (err) {
            callback(err, null);
          } else {
            callback(null, { id: this.lastID, asin: tempAsin, is_temp_asin: true });
          }
        }
      );
    });
  } else {
    // Check if product exists with this ASIN
    db.get('SELECT * FROM products WHERE asin = ?', [asin], (err, product) => {
      if (err) {
        callback(err, null);
      } else if (product) {
        callback(null, product);
      } else {
        // Create new product
        db.run(
          'INSERT INTO products (asin, name, is_temp_asin) VALUES (?, ?, 0)',
          [asin, name || asin],
          function(err) {
            if (err) {
              callback(err, null);
            } else {
              callback(null, { id: this.lastID, asin: asin, is_temp_asin: false });
            }
          }
        );
      }
    });
  }
}

// Helper function to add SKU to product
function addSkuToProduct(productId, sku, isPrimary, source, callback) {
  db.run(
    'INSERT OR IGNORE INTO product_skus (product_id, sku, is_primary, source) VALUES (?, ?, ?, ?)',
    [productId, sku, isPrimary ? 1 : 0, source],
    function(err) {
      callback(err, this.changes);
    }
  );
}

// Initialize sync scheduler
function initializeSyncScheduler() {
  syncScheduler = new SyncScheduler(db);
  
  // Skip initial sync on startup - user can manually trigger syncs via UI
  // This prevents startup delays when processing large data files
  console.log('\n[STARTUP] Sync scheduler initialized. Use UI buttons to sync data.\n');
  
  // Start daily sync schedule (runs at 2 AM)
  syncScheduler.startDailySync();
}

// ============= API ENDPOINTS =============

// Get all items with their pricing status
// API Routes

// Get all products (grouped by ASIN with their SKUs) - with pagination
app.get('/api/products', (req, res) => {
  const limit = parseInt(req.query.limit) || 50;
  const offset = parseInt(req.query.offset) || 0;
  
  const countQuery = `SELECT COUNT(*) as count FROM products`;
  
  const query = `
    SELECT 
      p.*,
      GROUP_CONCAT(DISTINCT ps.sku) as skus,
      GROUP_CONCAT(
        DISTINCT json_object(
          'sku', ps.sku,
          'is_primary', ps.is_primary,
          'source', ps.source
        )
      ) as sku_details,
      (SELECT COUNT(DISTINCT country_code) FROM asin_country_status WHERE asin = p.asin) as vc_country_count,
      (SELECT COUNT(DISTINCT country_code) FROM asin_country_status) as vc_total_countries,
      (SELECT COUNT(DISTINCT source_file) FROM qpi_file_tracking WHERE asin = p.asin) as qpi_file_count,
      (SELECT COUNT(DISTINCT source_file) FROM qpi_file_tracking) as qpi_total_files,
      (SELECT COUNT(DISTINCT country) FROM asin_online_status WHERE asin = p.asin) as online_country_count,
      9 as online_total_countries,
      vm.brand as vm_brand,
      vm.title as vm_title,
      vm.bundle as vm_bundle,
      vm.ppg as vm_ppg,
      COALESCE(vm.title, p.legal_name, p.name, p.asin) as display_name,
      COALESCE(vm.brand, p.brand) as display_brand
    FROM products p
    LEFT JOIN product_skus ps ON p.id = ps.product_id
    LEFT JOIN variations_master vm ON p.asin = vm.asin
    GROUP BY p.id
    ORDER BY p.created_at DESC
    LIMIT ? OFFSET ?
  `;

  db.get(countQuery, [], (countErr, countRow) => {
    if (countErr) {
      res.status(500).json({ error: countErr.message });
      return;
    }
    
    db.all(query, [limit, offset], (err, rows) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
      
      // Parse SKU details JSON
      const products = rows.map(row => ({
        ...row,
        skus: row.skus ? row.skus.split(',') : [],
        sku_details: row.sku_details ? JSON.parse(`[${row.sku_details}]`) : []
      }));
      
      res.json({
        data: products,
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

// Get all SKUs with their ASIN relationships - with pagination
app.get('/api/skus', (req, res) => {
  const limit = parseInt(req.query.limit) || 50;
  const offset = parseInt(req.query.offset) || 0;
  
  const countQuery = `SELECT COUNT(*) as count FROM product_skus`;
  
  const query = `
    SELECT 
      ps.id,
      ps.sku,
      ps.is_primary,
      ps.source,
      ps.created_at,
      p.asin,
      p.name as product_name,
      p.is_temp_asin,
      p.stage_1_country,
      p.stage_2_product_finalized,
      p.stage_3a_pricing_submitted,
      p.stage_3b_pricing_approved,
      p.stage_4_product_listed,
      p.stage_5_product_ordered,
      p.stage_6_product_online
    FROM product_skus ps
    LEFT JOIN products p ON ps.product_id = p.id
    ORDER BY ps.sku ASC
    LIMIT ? OFFSET ?
  `;

  db.get(countQuery, [], (countErr, countRow) => {
    if (countErr) {
      res.status(500).json({ error: countErr.message });
      return;
    }
    
    db.all(query, [limit, offset], (err, rows) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
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

// Get ASIN country status - all ASINs across all countries (with pagination)
app.get('/api/asin-status', (req, res) => {
  const limit = parseInt(req.query.limit) || 50;
  const offset = parseInt(req.query.offset) || 0;
  const country = req.query.country; // Optional filter by country
  
  let query = `
    SELECT 
      asin,
      sku,
      country_code,
      vc_status,
      last_synced,
      created_at,
      updated_at
    FROM asin_country_status
  `;
  
  let countQuery = `SELECT COUNT(*) as count FROM asin_country_status`;
  let params = [];
  let countParams = [];
  
  if (country) {
    query += ` WHERE country_code = ?`;
    countQuery += ` WHERE country_code = ?`;
    params.push(country);
    countParams.push(country);
  }
  
  query += ` ORDER BY asin, country_code LIMIT ? OFFSET ?`;
  params.push(limit, offset);

  db.get(countQuery, countParams, (countErr, countRow) => {
    if (countErr) {
      res.status(500).json({ error: countErr.message });
      return;
    }
    
    db.all(query, params, (err, rows) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
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

// Get ASIN country status by specific ASIN
app.get('/api/asin-status/:asin', (req, res) => {
  const asin = req.params.asin;
  
  const query = `
    SELECT 
      asin,
      sku,
      country_code,
      vc_status,
      last_synced,
      created_at,
      updated_at
    FROM asin_country_status
    WHERE asin = ?
    ORDER BY country_code
  `;

  db.all(query, [asin], (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    
    if (rows.length === 0) {
      res.status(404).json({ error: 'ASIN not found' });
      return;
    }
    
    res.json({
      asin: asin,
      countries: rows
    });
  });
});

// Get ASIN status summary - shows which ASINs are in which countries (with pagination)
app.get('/api/asin-status/summary/all', (req, res) => {
  const limit = parseInt(req.query.limit) || 50;
  const offset = parseInt(req.query.offset) || 0;
  
  const countQuery = `
    SELECT COUNT(DISTINCT asin) as count
    FROM asin_country_status
  `;
  
  const query = `
    SELECT 
      asin,
      GROUP_CONCAT(country_code || ':' || COALESCE(vc_status, 'unknown')) as country_status,
      COUNT(DISTINCT country_code) as total_countries,
      SUM(CASE WHEN vc_status IS NOT NULL AND vc_status != '' THEN 1 ELSE 0 END) as countries_with_status,
      MAX(last_synced) as last_synced
    FROM asin_country_status
    GROUP BY asin
    ORDER BY asin
    LIMIT ? OFFSET ?
  `;

  db.get(countQuery, [], (countErr, countRow) => {
    if (countErr) {
      res.status(500).json({ error: countErr.message });
      return;
    }
    
    db.all(query, [limit, offset], (err, rows) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
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

// Get ASINs missing in specific country (with pagination)
app.get('/api/asin-status/missing/:country', (req, res) => {
  const country = req.params.country;
  const limit = parseInt(req.query.limit) || 50;
  const offset = parseInt(req.query.offset) || 0;
  
  const countQuery = `
    SELECT COUNT(DISTINCT p.asin) as count
    FROM products p
    WHERE p.asin NOT IN (
      SELECT asin 
      FROM asin_country_status 
      WHERE country_code = ?
    )
    AND p.is_temp_asin = 0
  `;
  
  const query = `
    SELECT DISTINCT p.asin, p.name
    FROM products p
    WHERE p.asin NOT IN (
      SELECT asin 
      FROM asin_country_status 
      WHERE country_code = ?
    )
    AND p.is_temp_asin = 0
    ORDER BY p.asin
    LIMIT ? OFFSET ?
  `;

  db.get(countQuery, [country], (countErr, countRow) => {
    if (countErr) {
      res.status(500).json({ error: countErr.message });
      return;
    }
    
    db.all(query, [country, limit, offset], (err, rows) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
      
      res.json({
        country: country,
        missing_asins: rows,
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

// Database explorer endpoint
app.get('/api/database/:table', (req, res) => {
  const tableName = req.params.table;
  
  // Whitelist allowed tables for security
  const allowedTables = [
    'products',
    'product_skus',
    'product_country_pricing',
    'flow_stage_history',
    'temp_asin_counter',
    'items',
    'item_country_pricing',
    'asin_country_status',
    'qpi_file_tracking',
    'variations_master',
    'asin_online_status',
    'keepa_file_tracking'
  ];
  
  if (!allowedTables.includes(tableName)) {
    res.status(400).json({ error: 'Invalid table name' });
    return;
  }
  
  const limit = parseInt(req.query.limit) || 100;
  const offset = parseInt(req.query.offset) || 0;
  
  // Get table data
  db.all(`SELECT * FROM ${tableName} LIMIT ? OFFSET ?`, [limit, offset], (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    
    // Get total count
    db.get(`SELECT COUNT(*) as count FROM ${tableName}`, (countErr, countRow) => {
      if (countErr) {
        res.status(500).json({ error: countErr.message });
        return;
      }
      
      // Get column info
      db.all(`PRAGMA table_info(${tableName})`, (schemaErr, columns) => {
        if (schemaErr) {
          res.status(500).json({ error: schemaErr.message });
          return;
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

// Legacy endpoint for backward compatibility - with pagination and filters
app.get('/api/items', (req, res) => {
  const limit = parseInt(req.query.limit) || 50;
  const offset = parseInt(req.query.offset) || 0;
  
  // Get filter parameters
  const search = req.query.search || '';
  const stage = req.query.stage || '';
  const country = req.query.country || '';
  const temp = req.query.temp || 'all';
  const missing = req.query.missing || 'all';
  const brands = req.query.brands ? req.query.brands.split(',') : [];
  const bundles = req.query.bundles ? req.query.bundles.split(',') : [];
  const ppgs = req.query.ppgs ? req.query.ppgs.split(',') : [];
  
  // Helper function to build queries and execute
  const buildAndExecuteQuery = (marketplaceName, marketplaceCountryCodes) => {
    // Build WHERE clauses
    let whereConditions = [];
    let queryParams = [];
    let countParams = [];
    
    // Search filter
    if (search) {
      whereConditions.push('(p.name LIKE ? OR p.asin LIKE ? OR ps.sku LIKE ?)');
      const searchPattern = `%${search}%`;
      queryParams.push(searchPattern, searchPattern, searchPattern);
      countParams.push(searchPattern, searchPattern, searchPattern);
    }
    
    // Stage filter
    if (stage && stage !== 'all') {
      // Map stage_5 to stage_5_product_ordered column name
      const stageMap = {
        'stage_1': 'stage_1_idea_considered',
        'stage_2': 'stage_2_product_finalized',
        'stage_3a': 'stage_3a_pricing_submitted',
        'stage_3b': 'stage_3b_pricing_approved',
        'stage_4': 'stage_4_product_listed',
        'stage_5': 'stage_5_product_ordered',
        'stage_6': 'stage_6_product_online',
        'stage_7': 'stage_7_end_of_life'
      };
      
      const stageColumn = stageMap[stage];
      if (stageColumn) {
        whereConditions.push(`p.${stageColumn} = 1`);
      }
    }
    
    // Marketplace filter
    let joinClause = '';
    if (marketplaceName) {
      // Use marketplace name to filter asin_country_status.country_code
      joinClause = 'INNER JOIN asin_country_status acs ON p.asin = acs.asin';
      whereConditions.push(`acs.country_code = ?`);
      queryParams.push(marketplaceName);
      countParams.push(marketplaceName);
    }
    
    // Temp ASIN filter
    if (temp === 'temp') {
      whereConditions.push('p.is_temp_asin = 1');
    } else if (temp === 'permanent') {
      whereConditions.push('(p.is_temp_asin = 0 OR p.is_temp_asin IS NULL)');
    }
    
    // Missing filter - check if ASIN exists in variations_master
    if (missing === 'missing') {
      whereConditions.push('vm.asin IS NULL');
    } else if (missing === 'present') {
      whereConditions.push('vm.asin IS NOT NULL');
    }
    
    // Variation filters
    if (brands.length > 0) {
      const placeholders = brands.map(() => '?').join(',');
      whereConditions.push(`vm.brand IN (${placeholders})`);
      queryParams.push(...brands);
      countParams.push(...brands);
    }
    
    if (bundles.length > 0) {
      const placeholders = bundles.map(() => '?').join(',');
      whereConditions.push(`vm.bundle IN (${placeholders})`);
      queryParams.push(...bundles);
      countParams.push(...bundles);
    }
    
    if (ppgs.length > 0) {
      const placeholders = ppgs.map(() => '?').join(',');
      whereConditions.push(`vm.ppg IN (${placeholders})`);
      queryParams.push(...ppgs);
      countParams.push(...ppgs);
    }
    
    const whereClause = whereConditions.length > 0 ? 'WHERE ' + whereConditions.join(' AND ') : '';
    
    const countQuery = `
      SELECT COUNT(DISTINCT p.id) as count 
      FROM products p
      LEFT JOIN product_skus ps ON p.id = ps.product_id
      LEFT JOIN variations_master vm ON p.asin = vm.asin
      ${joinClause}
      ${whereClause}
    `;
    
    // Build marketplace-filtered subqueries
    let vcCountSubquery, vcTotalSubquery, qpiCountSubquery, qpiTotalSubquery, onlineCountSubquery, onlineTotalSubquery, eolCountSubquery, eolTotalSubquery;
    
    if (marketplaceName && marketplaceCountryCodes.length > 0) {
      // Filter by marketplace
      // For VC count: check if ASIN is in asin_country_status with marketplace name
      vcCountSubquery = `(SELECT CASE WHEN EXISTS(SELECT 1 FROM asin_country_status WHERE asin = p.asin AND country_code = ?) THEN 1 ELSE 0 END)`;
      vcTotalSubquery = `1`;  // Only one marketplace selected
      
      // For QPI: count how many QPI files for this marketplace contain this ASIN
      const codePlaceholders = marketplaceCountryCodes.map(() => '?').join(',');
      qpiCountSubquery = `(SELECT COUNT(DISTINCT qpi_source_file) FROM vendor_mapping WHERE customer_code IN (${codePlaceholders}) AND qpi_source_file IN (SELECT DISTINCT source_file FROM qpi_file_tracking WHERE asin = p.asin))`;
      qpiTotalSubquery = `(SELECT COUNT(DISTINCT qpi_source_file) FROM vendor_mapping WHERE customer_code IN (${codePlaceholders}) AND qpi_source_file IS NOT NULL AND qpi_source_file != '')`;
      
      // For Online: check if ASIN is online in this marketplace
      onlineCountSubquery = `(SELECT CASE WHEN EXISTS(SELECT 1 FROM asin_online_status WHERE asin = p.asin AND country = ?) THEN 1 ELSE 0 END)`;
      onlineTotalSubquery = `1`;  // Only one marketplace selected
      
      // For EOL: count NCF status for this marketplace's QPI files
      eolCountSubquery = `(SELECT COUNT(DISTINCT qpi_source_file) FROM vendor_mapping WHERE customer_code IN (${codePlaceholders}) AND qpi_source_file IN (SELECT DISTINCT source_file FROM qpi_file_tracking WHERE asin = p.asin AND status = 'NCF'))`;
      eolTotalSubquery = `(SELECT COUNT(DISTINCT qpi_source_file) FROM vendor_mapping WHERE customer_code IN (${codePlaceholders}) AND qpi_source_file IN (SELECT DISTINCT source_file FROM qpi_file_tracking WHERE asin = p.asin))`;
      
      // Add params for subqueries: marketplace name, country codes (3x for QPI+EOL), marketplace name again
      console.log('[DEBUG] Marketplace filter:', { marketplaceName, marketplaceCountryCodes });
      console.log('[DEBUG] Online count subquery:', onlineCountSubquery);
      queryParams.push(marketplaceName, ...marketplaceCountryCodes, ...marketplaceCountryCodes, ...marketplaceCountryCodes, ...marketplaceCountryCodes, marketplaceName);
    } else {
      // No marketplace filter - use all countries that exist in the data
      vcCountSubquery = `(SELECT COUNT(DISTINCT country_code) FROM asin_country_status WHERE asin = p.asin)`;
      vcTotalSubquery = `(SELECT COUNT(DISTINCT keepa_marketplace) FROM vendor_mapping)`;
      qpiCountSubquery = `(SELECT COUNT(DISTINCT source_file) FROM qpi_file_tracking WHERE asin = p.asin)`;
      qpiTotalSubquery = `(SELECT COUNT(DISTINCT qpi_source_file) FROM vendor_mapping WHERE qpi_source_file IS NOT NULL AND qpi_source_file != '')`;
      onlineCountSubquery = `(SELECT COUNT(DISTINCT country) FROM asin_online_status WHERE asin = p.asin)`;
      onlineTotalSubquery = `(SELECT COUNT(DISTINCT country) FROM asin_online_status)`;  // Count actual countries in data, not all marketplaces
      eolCountSubquery = `(SELECT COUNT(DISTINCT qft.source_file) FROM qpi_file_tracking qft WHERE qft.asin = p.asin AND qft.status = 'NCF')`;
      eolTotalSubquery = `(SELECT COUNT(DISTINCT source_file) FROM qpi_file_tracking WHERE asin = p.asin)`;
    }
    
    const query = `
      SELECT 
        p.*,
        GROUP_CONCAT(DISTINCT ps.sku) as skus,
        GROUP_CONCAT(
          DISTINCT json_object(
            'sku', ps.sku,
            'is_primary', ps.is_primary,
            'source', ps.source
          )
        ) as sku_details,
        ${vcCountSubquery} as vc_country_count,
        ${vcTotalSubquery} as vc_total_countries,
        ${qpiCountSubquery} as qpi_file_count,
        ${qpiTotalSubquery} as qpi_total_files,
        ${onlineCountSubquery} as online_country_count,
        ${onlineTotalSubquery} as online_total_countries,
        ${eolCountSubquery} as eol_country_count,
        ${eolTotalSubquery} as eol_total_countries,
        vm.brand as vm_brand,
        vm.title as vm_title,
        vm.bundle as vm_bundle,
        vm.ppg as vm_ppg,
        COALESCE(vm.title, p.legal_name, p.name, p.asin) as display_name,
        COALESCE(vm.brand, p.brand) as display_brand
      FROM products p
      LEFT JOIN product_skus ps ON p.id = ps.product_id
      LEFT JOIN variations_master vm ON p.asin = vm.asin
      ${joinClause}
      ${whereClause}
      GROUP BY p.id
      ORDER BY p.created_at DESC
      LIMIT ? OFFSET ?
    `;
    
    queryParams.push(limit, offset);

    db.get(countQuery, countParams, (countErr, countRow) => {
      if (countErr) {
        res.status(500).json({ error: countErr.message });
        return;
      }
      
      db.all(query, queryParams, (err, rows) => {
        if (err) {
          res.status(500).json({ error: err.message });
          return;
        }
        
        // Debug: Log first row if Canada filter
        if (country === 'Canada' && rows.length > 0) {
          console.log('[DEBUG] First Canada result:', {
            asin: rows[0].asin,
            online_country_count: rows[0].online_country_count,
            online_total_countries: rows[0].online_total_countries
          });
        }
        
        // Parse SKU details and convert to old format
        const items = rows.map(row => ({
          id: row.id,
          sku: row.skus ? row.skus.split(',')[0] : '',  // First SKU as primary
          asin: row.asin,
          name: row.name,
          ...row,
          skus: row.skus ? row.skus.split(',') : [],
          sku_details: row.sku_details ? JSON.parse(`[${row.sku_details}]`) : []
        }));
        
        res.json({
          data: items,
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
  };
  
  // If marketplace is selected, get country codes for that marketplace
  if (country && country !== 'all') {
    db.all('SELECT DISTINCT customer_code FROM vendor_mapping WHERE keepa_marketplace = ?', [country], (err, rows) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
      const countryCodes = rows.map(row => row.customer_code);
      buildAndExecuteQuery(country, countryCodes);
    });
  } else {
    // No marketplace filter
    buildAndExecuteQuery(null, []);
  }
});

// Get list of marketplaces for filter dropdown
app.get('/api/marketplaces', (req, res) => {
  db.all(`
    SELECT DISTINCT keepa_marketplace
    FROM vendor_mapping
    ORDER BY keepa_marketplace
  `, [], (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    res.json(rows.map(row => row.keepa_marketplace));
  });
});

// Get all vendor mapping data (Sales Person, Approver, Admin)
app.get('/api/vendor-mapping', authenticateToken, requireRole('sales person', 'approver', 'admin'), (req, res) => {
  db.all(`
    SELECT 
      id,
      customer,
      country,
      keepa_marketplace,
      customer_code,
      vendor_code,
      qpi_source_file,
      vc_file,
      language,
      currency
    FROM vendor_mapping
    ORDER BY customer, keepa_marketplace, country
  `, [], (err, rows) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    res.json(rows);
  });
});

// Get available QPI source files
app.get('/api/qpi-files', authenticateToken, requireRole('sales person', 'approver', 'admin'), (req, res) => {
  const qpiFile = 'A:\\ProcessOutput\\QPI_Validation\\QPI_validation_full.csv';
  
  try {
    if (!fs.existsSync(qpiFile)) {
      console.error('QPI validation file not found:', qpiFile);
      return res.json([]);
    }
    
    // Read the CSV file
    const csvData = fs.readFileSync(qpiFile, 'utf8');
    const lines = csvData.split('\n');
    
    if (lines.length === 0) {
      return res.json([]);
    }
    
    // Parse header to find "Source File" column index
    const header = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
    const sourceFileIndex = header.findIndex(h => h.toLowerCase() === 'source file');
    
    if (sourceFileIndex === -1) {
      console.error('Source File column not found in QPI validation file');
      return res.json([]);
    }
    
    // Extract unique source files
    const sourceFiles = new Set();
    for (let i = 1; i < lines.length; i++) {
      if (!lines[i].trim()) continue;
      
      const cols = lines[i].split(',');
      if (cols.length > sourceFileIndex) {
        const sourceFile = cols[sourceFileIndex].trim().replace(/"/g, '');
        if (sourceFile && sourceFile !== '') {
          sourceFiles.add(sourceFile);
        }
      }
    }
    
    // Convert to sorted array
    const result = Array.from(sourceFiles).sort();
    console.log(`[QPI Files] Found ${result.length} unique source files`);
    
    res.json(result);
  } catch (error) {
    console.error('Error reading QPI validation file:', error);
    res.status(500).json({ error: error.message });
  }
});

// Update vendor mapping record
app.put('/api/vendor-mapping/:id', authenticateToken, requireRole('sales person', 'admin'), (req, res) => {
  const { id } = req.params;
  const { country, keepa_marketplace, customer_code, vendor_code, qpi_source_file, language, currency } = req.body;

  // First, get the original country for this record
  db.get('SELECT country FROM vendor_mapping WHERE id = ?', [id], (err, row) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    if (!row) {
      return res.status(404).json({ error: 'Vendor mapping not found' });
    }

    const originalCountry = row.country;

    // Update all records with the same original country
    db.run(`
      UPDATE vendor_mapping 
      SET 
        country = ?,
        keepa_marketplace = ?,
        customer_code = ?,
        qpi_source_file = ?,
        language = ?,
        currency = ?,
        updated_at = CURRENT_TIMESTAMP
      WHERE country = ?
    `, [country, keepa_marketplace, customer_code, qpi_source_file, language, currency, originalCountry], function(err) {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      
      console.log(`[Vendor Mapping] Updated ${this.changes} records for country: ${originalCountry} -> ${country}`);
      res.json({ 
        message: `Vendor mapping updated for all ${this.changes} records`, 
        changes: this.changes,
        country: country
      });
    });
  });
});

// Get list of marketplaces from vendor mapping (legacy)
app.get('/api/countries', (req, res) => {
  db.all(`
    SELECT DISTINCT keepa_marketplace as marketplace, customer_code as country_code
    FROM vendor_mapping
    ORDER BY keepa_marketplace
  `, [], (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    res.json(rows);
  });
});

// Get single item with full details
app.get('/api/items/:id', (req, res) => {
  const identifier = req.params.id;
  
  // Check if it's an ASIN or numeric ID
  const isNumeric = /^\d+$/.test(identifier);
  const whereClause = isNumeric ? 'p.id = ?' : 'p.asin = ?';

  const query = `
    SELECT p.*, 
           GROUP_CONCAT(DISTINCT ps.sku) as skus,
           GROUP_CONCAT(
             DISTINCT json_object(
               'sku', ps.sku,
               'is_primary', ps.is_primary,
               'source', ps.source
             )
           ) as sku_details
    FROM products p
    LEFT JOIN product_skus ps ON p.id = ps.product_id
    WHERE ${whereClause}
    GROUP BY p.id
  `;

  db.get(query, [identifier], (err, product) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    if (!product) {
      res.status(404).json({ error: 'Product not found' });
      return;
    }

    // Parse SKU details
    product.skus = product.skus ? product.skus.split(',') : [];
    product.sku_details = product.sku_details ? JSON.parse(`[${product.sku_details}]`) : [];

    // Get pricing data
    db.all(
      'SELECT * FROM product_country_pricing WHERE product_id = ?',
      [product.id],
      (err, pricing) => {
        if (err) {
          res.status(500).json({ error: err.message });
          return;
        }

        res.json({
          ...product,
          pricing: pricing
        });
      }
    );
  });
});

// Create new item
// Create new product
app.post('/api/items', (req, res) => {
  const { 
    sku, 
    asin, 
    name, 
    brand,
    stage_1_country,
    stage_1_item_number,
    stage_1_description,
    stage_1_season_launch,
    stage_1_brand,
    stage_1_idea_considered,
    is_temp_asin
  } = req.body;

  if (!sku) {
    res.status(400).json({ error: 'SKU is required' });
    return;
  }

  // Use findOrCreateProduct helper with additional fields
  findOrCreateProduct(asin, name, (err, product) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }

    // Update product with additional stage 1 fields
    const updateQuery = `
      UPDATE products 
      SET brand = COALESCE(?, brand),
          stage_1_country = COALESCE(?, stage_1_country),
          stage_1_item_number = COALESCE(?, stage_1_item_number),
          stage_1_description = COALESCE(?, stage_1_description),
          stage_1_season_launch = COALESCE(?, stage_1_season_launch),
          stage_1_brand = COALESCE(?, stage_1_brand),
          stage_1_idea_considered = COALESCE(?, stage_1_idea_considered),
          is_temp_asin = COALESCE(?, is_temp_asin),
          updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
    `;

    db.run(updateQuery, [
      brand,
      stage_1_country,
      stage_1_item_number,
      stage_1_description,
      stage_1_season_launch,
      stage_1_brand,
      stage_1_idea_considered,
      is_temp_asin,
      product.id
    ], (updateErr) => {
      if (updateErr) {
        console.error('Error updating product fields:', updateErr.message);
      }

      // Add SKU to product
      addSkuToProduct(product.id, sku, true, 'manual', (err, changes) => {
        if (err) {
          res.status(500).json({ error: err.message });
          return;
        }
        
        res.json({ 
          id: product.id, 
          asin: product.asin,
          message: 'Product created successfully',
          is_temp_asin: is_temp_asin || product.is_temp_asin
        });
      });
    });
  });
});

// Update product
app.put('/api/items/:id', (req, res) => {
  const identifier = req.params.id;
  const { asin, name } = req.body;

  // Check if it's an ASIN or numeric ID
  const isNumeric = /^\d+$/.test(identifier);
  const whereClause = isNumeric ? 'id = ?' : 'asin = ?';

  const query = `
    UPDATE products 
    SET asin = COALESCE(?, asin),
        name = COALESCE(?, name),
        updated_at = CURRENT_TIMESTAMP
    WHERE ${whereClause}
  `;

  db.run(query, [asin, name, identifier], function(err) {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    res.json({ message: 'Product updated successfully' });
  });
});

// Delete item
// Delete product
app.delete('/api/items/:id', (req, res) => {
  const identifier = req.params.id;
  const isNumeric = /^\d+$/.test(identifier);
  const whereClause = isNumeric ? 'id = ?' : 'asin = ?';

  db.run(`DELETE FROM products WHERE ${whereClause}`, [identifier], function(err) {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    res.json({ message: 'Product deleted successfully' });
  });
});

// Add or update country pricing
app.post('/api/items/:id/pricing', (req, res) => {
  const identifier = req.params.id;
  const { country_code, retail_price, sell_price, currency } = req.body;

  if (!country_code || !currency) {
    res.status(400).json({ error: 'Country code and currency are required' });
    return;
  }

  // Get product ID
  const isNumeric = /^\d+$/.test(identifier);
  const selectQuery = isNumeric ? 'SELECT id FROM products WHERE id = ?' : 'SELECT id FROM products WHERE asin = ?';
  
  db.get(selectQuery, [identifier], (err, product) => {
    if (err || !product) {
      res.status(404).json({ error: 'Product not found' });
      return;
    }

    const productId = product.id;

    const query = `
      INSERT INTO product_country_pricing (product_id, country_code, retail_price, sell_price, currency)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(product_id, country_code) 
      DO UPDATE SET 
        retail_price = excluded.retail_price,
        sell_price = excluded.sell_price,
        currency = excluded.currency,
        updated_at = CURRENT_TIMESTAMP
    `;

    db.run(query, [productId, country_code, retail_price, sell_price, currency], function(err) {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
      
      // Mark Stage 3a (Pricing Submitted) for this product
      db.run(
        'UPDATE products SET stage_3a_pricing_submitted = 1 WHERE id = ?',
        [productId],
        (err) => {
          if (err) {
            console.error('Error updating stage 3a:', err.message);
          }
        }
      );
      
      res.json({ message: 'Pricing updated successfully' });
    });
  });
});

// Approve pricing for a country
app.put('/api/items/:id/pricing/:country/approve', (req, res) => {
  const { id, country } = req.params;

  // Get product ID
  const isNumeric = /^\d+$/.test(id);
  const selectQuery = isNumeric ? 'SELECT id FROM products WHERE id = ?' : 'SELECT id FROM products WHERE asin = ?';
  
  db.get(selectQuery, [id], (err, product) => {
    if (err || !product) {
      res.status(404).json({ error: 'Product not found' });
      return;
    }

    const productId = product.id;

    const query = `
      UPDATE product_country_pricing 
      SET approval_status = 'approved', 
          approved_at = CURRENT_TIMESTAMP,
          updated_at = CURRENT_TIMESTAMP
      WHERE product_id = ? AND country_code = ?
    `;

    db.run(query, [productId, country], function(err) {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
      
      // Check if all pricings for this product are approved
      db.get(
        'SELECT COUNT(*) as total, SUM(CASE WHEN approval_status = "approved" THEN 1 ELSE 0 END) as approved FROM product_country_pricing WHERE product_id = ?',
        [productId],
        (err, row) => {
          if (!err && row.total > 0 && row.total === row.approved) {
            // All pricings approved, mark Stage 3b
            db.run('UPDATE products SET stage_3b_pricing_approved = 1 WHERE id = ?', [productId]);
          }
        }
      );
      
      res.json({ message: 'Pricing approved successfully' });
    });
  });
});

// Reject pricing for a country
app.put('/api/items/:id/pricing/:country/reject', (req, res) => {
  const { id, country } = req.params;

  // Get product ID
  const isNumeric = /^\d+$/.test(id);
  const selectQuery = isNumeric ? 'SELECT id FROM products WHERE id = ?' : 'SELECT id FROM products WHERE asin = ?';
  
  db.get(selectQuery, [id], (err, product) => {
    if (err || !product) {
      res.status(404).json({ error: 'Product not found' });
      return;
    }

    const productId = product.id;

    const query = `
      UPDATE product_country_pricing 
      SET approval_status = 'rejected',
          updated_at = CURRENT_TIMESTAMP
      WHERE product_id = ? AND country_code = ?
    `;

    db.run(query, [productId, country], function(err) {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
      res.json({ message: 'Pricing rejected' });
    });
  });
});

// Get VC listing status by country for a product
app.get('/api/items/:id/vc-status', (req, res) => {
  const identifier = req.params.id;
  
  // Get all SKUs for this product
  const isNumeric = /^\d+$/.test(identifier);
  const query = isNumeric 
    ? 'SELECT ps.sku FROM product_skus ps WHERE ps.product_id = ?'
    : 'SELECT ps.sku FROM product_skus ps JOIN products p ON ps.product_id = p.id WHERE p.asin = ?';
  
  db.all(query, [identifier], (err, skus) => {
    if (err || !skus || skus.length === 0) {
      res.json({ vc_status: [] });
      return;
    }
    
    const vcDir = 'A:\\ProcessOutput\\VC_Extracts\\Comparison_Extracts';
    
    try {
      const files = fs.readdirSync(vcDir)
        .filter(f => f.startsWith('vc_extracts_') && f.endsWith('.parquet'))
        .sort()
        .reverse();
      
      if (files.length === 0) {
        res.json({ vc_status: [] });
        return;
      }
      
      const latestFile = path.join(vcDir, files[0]).replace(/\\/g, '/');
      const duckDb = new duckdb.Database(':memory:');
      
      // Query for all SKUs
      const skuList = skus.map(s => `'${s.sku}'`).join(',');
      
      duckDb.all(`
        SELECT 
          sku,
          country,
          summaries_0_asin as asin,
          summaries_0_status_0 as status
        FROM '${latestFile}'
        WHERE sku IN (${skuList})
      `, [], (err, rows) => {
        duckDb.close();
        
        if (err) {
          console.error('Error querying VC status:', err.message);
          res.status(500).json({ error: 'Error querying VC status' });
          return;
        }
        
        res.json({ vc_status: rows || [] });
      });
      
    } catch (error) {
      console.error('Error accessing VC extract:', error.message);
      res.status(500).json({ error: 'Error accessing VC data' });
    }
  });
});

// Update Stage 1 (Idea Considered)
app.put('/api/items/:id/stage1', (req, res) => {
  const identifier = req.params.id;
  const { brand, description, season_launch, country, item_number } = req.body;

  // Get product ID
  const isNumeric = /^\d+$/.test(identifier);
  const whereClause = isNumeric ? 'id = ?' : 'asin = ?';

  const query = `
    UPDATE products 
    SET stage_1_idea_considered = 1,
        stage_1_brand = ?,
        stage_1_description = ?,
        stage_1_season_launch = ?,
        stage_1_country = ?,
        updated_at = CURRENT_TIMESTAMP
    WHERE ${whereClause}
  `;

  db.run(query, [brand, description, season_launch, country, identifier], function(err) {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    
    // If item_number provided, add it as a SKU
    if (item_number) {
      db.get(`SELECT id FROM products WHERE ${whereClause}`, [identifier], (err, product) => {
        if (product) {
          addSkuToProduct(product.id, item_number, false, 'manual', () => {});
        }
      });
    }
    
    res.json({ message: 'Stage 1 updated successfully' });
  });
});

// Update flow stage
app.put('/api/items/:id/stage', (req, res) => {
  const identifier = req.params.id;
  const { stage, completed } = req.body;

  if (!stage) {
    res.status(400).json({ error: 'Stage name is required' });
    return;
  }

  // Get product ID
  const isNumeric = /^\d+$/.test(identifier);
  const whereClause = isNumeric ? 'id = ?' : 'asin = ?';

  const stageColumn = stage.toLowerCase().replace(/ /g, '_');
  const query = `UPDATE products SET ${stageColumn} = ?, updated_at = CURRENT_TIMESTAMP WHERE ${whereClause}`;

  db.run(query, [completed ? 1 : 0, identifier], function(err) {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }

    // Log in history
    if (completed) {
      db.get(`SELECT id FROM products WHERE ${whereClause}`, [identifier], (err, product) => {
        if (product) {
          db.run(
            'INSERT INTO flow_stage_history (product_id, stage_name, completed, completed_at) VALUES (?, ?, 1, CURRENT_TIMESTAMP)',
            [product.id, stage]
          );
        }
      });
    }

    res.json({ message: 'Stage updated successfully' });
  });
});

// Get flow stage history for a product
app.get('/api/items/:id/history', (req, res) => {
  const identifier = req.params.id;

  // Get product ID
  const isNumeric = /^\d+$/.test(identifier);
  const query = isNumeric 
    ? 'SELECT * FROM flow_stage_history WHERE product_id = ? ORDER BY completed_at DESC'
    : 'SELECT fsh.* FROM flow_stage_history fsh JOIN products p ON fsh.product_id = p.id WHERE p.asin = ? ORDER BY fsh.completed_at DESC';

  db.all(query, [identifier], (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    res.json(rows);
  });
});

// Import items from QPI CSV
app.post('/api/import/qpi', (req, res) => {
  const qpiPath = 'A:\\ProcessOutput\\QPI_Validation\\QPI_validation_full.csv';

  if (!fs.existsSync(qpiPath)) {
    res.status(404).json({ error: 'QPI CSV file not found' });
    return;
  }

  const results = [];
  let imported = 0;
  let skipped = 0;
  let errors = 0;

  fs.createReadStream(qpiPath)
    .pipe(csv())
    .on('data', (data) => {
      const sku = data['Item no'];
      const asin = data['ASIN'];
      const name = data['Description'];
      const sioc_status = data['SIOC'] ? data['SIOC'].trim() : null;

      if (sku && name) {
        results.push({ sku, asin, name, sioc_status });
      }
    })
    .on('end', () => {
      // Insert items into database
      const insertStmt = db.prepare(`
        INSERT OR IGNORE INTO items (sku, asin, name, sioc_status, order_received)
        VALUES (?, ?, ?, ?, 1)
      `);

      results.forEach(item => {
        insertStmt.run([item.sku, item.asin, item.name, item.sioc_status], function(err) {
          if (err) {
            errors++;
          } else if (this.changes > 0) {
            imported++;
          } else {
            skipped++;
          }
        });
      });

      insertStmt.finalize(() => {
        res.json({
          message: 'QPI import completed',
          total: results.length,
          imported: imported,
          skipped: skipped,
          errors: errors
        });
      });
    })
    .on('error', (err) => {
      res.status(500).json({ error: 'Error reading CSV file: ' + err.message });
    });
});

// Sync order received status from QPI CSV
app.post('/api/sync/qpi', (req, res) => {
  const qpiPath = 'A:\\ProcessOutput\\QPI_Validation\\QPI_validation_full.csv';

  if (!fs.existsSync(qpiPath)) {
    res.status(404).json({ error: 'QPI CSV file not found' });
    return;
  }

  const qpiData = [];

  fs.createReadStream(qpiPath)
    .pipe(csv())
    .on('data', (data) => {
      const sku = data['Item no'];
      const asin = data['ASIN'];
      const sourceFile = data['Source File'];
      const status = data['Status']; // Get the Status column
      if (sku && asin && sourceFile) {
        qpiData.push({ sku, asin, sourceFile, status });
      }
    })
    .on('end', () => {
      console.log(`Found ${qpiData.length} items in QPI`);
      
      // Collect unique SKUs, ASINs, and source files
      const qpiSkus = new Set();
      const qpiAsins = new Set();
      const skuAsinMap = new Map();
      const sourceFiles = new Set();
      const ncfAsins = new Set(); // Track ASINs with NCF status
      
      qpiData.forEach(item => {
        qpiSkus.add(item.sku);
        if (item.asin) {
          qpiAsins.add(item.asin);
          skuAsinMap.set(item.sku, item.asin);
          // Check if status is NCF
          if (item.status && item.status.toUpperCase() === 'NCF') {
            ncfAsins.add(item.asin);
          }
        }
        if (item.sourceFile) {
          sourceFiles.add(item.sourceFile);
        }
      });
      
      console.log(`Collected ${qpiSkus.size} SKUs, ${qpiAsins.size} ASINs, ${sourceFiles.size} source files`);
      console.log(`Found ${ncfAsins.size} ASINs with NCF (End of Life) status`);
      console.log(`Source files found: ${Array.from(sourceFiles).join(', ')}`);
      console.log(`Sample items with source: ${qpiData.slice(0, 3).map(i => `${i.sku}:${i.sourceFile || 'NULL'}`).join(', ')}`);
      
      let itemsUpdated = 0;
      let productsUpdated = 0;
      let errors = 0;
      
      // Batch update items table
      if (qpiSkus.size > 0) {
        const skuList = Array.from(qpiSkus);
        const placeholders = skuList.map(() => '?').join(',');
        
        db.run(
          `UPDATE items 
           SET order_received = 1, 
               stage_5_product_ordered = 1, 
               updated_at = CURRENT_TIMESTAMP 
           WHERE sku IN (${placeholders})`,
          skuList,
          function(err) {
            if (err) {
              console.error('Error updating items:', err.message);
              errors++;
            } else {
              itemsUpdated = this.changes;
              console.log(`Updated ${itemsUpdated} items in legacy table`);
            }
          }
        );
      }
      
      // Batch update ASINs in items table if they're missing
      const stmt = db.prepare(`UPDATE items SET asin = ? WHERE sku = ? AND (asin IS NULL OR asin = "")`);
      skuAsinMap.forEach((asin, sku) => {
        stmt.run([asin, sku], function(err) {
          if (err) errors++;
        });
      });
      stmt.finalize();
      
      // Batch update products table - mark stage_5_product_ordered for all ASINs in QPI
      if (qpiAsins.size > 0) {
        const asinList = Array.from(qpiAsins);
        
        // Collect SKUs per ASIN
        const asinToSkus = new Map();
        qpiData.forEach(item => {
          if (item.asin && item.sku) {
            if (!asinToSkus.has(item.asin)) {
              asinToSkus.set(item.asin, new Set());
            }
            asinToSkus.get(item.asin).add(item.sku);
          }
        });
        
        console.log(`[QPI] Found SKUs for ${asinToSkus.size} ASINs`);
        
        // First, create products for ASINs that don't exist yet using a more efficient approach
        console.log(`[QPI] Creating products for ASINs not in products table...`);
        
        // Use INSERT OR IGNORE with all values at once
        db.serialize(() => {
          db.run('BEGIN TRANSACTION');
          
          const insertStmt = db.prepare(`
            INSERT OR IGNORE INTO products (asin, name, stage_5_product_ordered, stage_7_end_of_life, created_at, updated_at)
            VALUES (?, ?, 1, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          `);
          
          let newProductsInserted = 0;
          asinList.forEach(asin => {
            const isNCF = ncfAsins.has(asin) ? 1 : 0;
            insertStmt.run([asin, `Product ${asin}`, isNCF], function(err) {
              if (err) {
                console.error(`Error inserting product ${asin}:`, err.message);
              } else if (this.changes > 0) {
                newProductsInserted++;
                if (isNCF) {
                  console.log(`✓ Created ${asin} and marked as End of Life (NCF)`);
                }
              }
            });
          });
          
          insertStmt.finalize();
          
          db.run('COMMIT', (err) => {
            if (err) {
              console.error('Error committing product inserts:', err.message);
            } else {
              // Count how many we created by checking total
              db.get('SELECT COUNT(*) as count FROM products', [], (err, row) => {
                if (!err && row) {
                  console.log(`[QPI] Products table now has ${row.count} total products`);
                }
              });
            }
          });
          
          // Update products with SKU information and EOL status
          console.log(`[QPI] Updating products with SKU data and EOL status...`);
          
          db.run('BEGIN TRANSACTION', (err) => {
            if (err) {
              console.error('Error starting product update transaction:', err.message);
              return;
            }
            
            const updateStmt = db.prepare(`
              UPDATE products 
              SET stage_5_product_ordered = 1,
                  stage_7_end_of_life = ?,
                  updated_at = CURRENT_TIMESTAMP
              WHERE asin = ?
            `);
            
            let skusUpdated = 0;
            let eolMarked = 0;
            
            asinToSkus.forEach((skus, asin) => {
              const isNCF = ncfAsins.has(asin) ? 1 : 0;
              updateStmt.run([isNCF, asin], function(err) {
                if (err) {
                  console.error(`Error updating product ${asin}:`, err.message);
                } else if (this.changes > 0) {
                  skusUpdated++;
                  if (isNCF) {
                    eolMarked++;
                    console.log(`✓ Marked ${asin} as End of Life (NCF status)`);
                  }
                }
              });
            });
            
            updateStmt.finalize((err) => {
              if (err) {
                console.error('Error finalizing product updates:', err.message);
                db.run('ROLLBACK');
                return;
              }
              
              db.run('COMMIT', (err) => {
                if (err) {
                  console.error('Error committing product updates:', err.message);
                } else {
                  console.log(`[QPI] Updated ${skusUpdated} products with stage_5`);
                  console.log(`[QPI] Marked ${eolMarked} products as End of Life (NCF)`);
                }
              });
            });
          });
          
          // Insert SKUs into product_skus table
          console.log(`[QPI] Inserting SKUs into product_skus table...`);
          
          // Delete old and insert new in a transaction
          db.run('BEGIN TRANSACTION', (err) => {
            if (err) {
              console.error('Error starting SKU transaction:', err.message);
              return;
            }
            
            // Clear old SKUs
            const asinPlaceholders = asinList.map(() => '?').join(',');
            db.run(`DELETE FROM product_skus WHERE product_id IN (SELECT id FROM products WHERE asin IN (${asinPlaceholders}))`, asinList, (err) => {
              if (err) {
                console.error('Error clearing old SKUs:', err.message);
                db.run('ROLLBACK');
                return;
              }
              
              console.log(`[QPI] Cleared old SKU mappings`);
              
              // Insert new SKU mappings
              const skuInsertStmt = db.prepare(`
                INSERT INTO product_skus (product_id, sku, created_at)
                SELECT p.id, ?, CURRENT_TIMESTAMP
                FROM products p
                WHERE p.asin = ?
              `);
              
              let skusInserted = 0;
              let insertErrors = 0;
              
              asinToSkus.forEach((skus, asin) => {
                skus.forEach(sku => {
                  skuInsertStmt.run([sku, asin], function(err) {
                    if (err) {
                      insertErrors++;
                      if (insertErrors <= 3) {  // Only log first 3 errors
                        console.error(`Error inserting SKU ${sku} for ${asin}:`, err.message);
                      }
                    } else if (this.changes > 0) {
                      skusInserted++;
                    }
                  });
                });
              });
              
              skuInsertStmt.finalize((err) => {
                if (err) {
                  console.error('Error finalizing SKU insert:', err.message);
                  db.run('ROLLBACK');
                } else {
                  db.run('COMMIT', (err) => {
                    if (err) {
                      console.error('Error committing SKU transaction:', err.message);
                    } else {
                      console.log(`[QPI] Committed ${skusInserted} SKU mappings (${insertErrors} errors)`);
                    }
                  });
                }
              });
            });
          });
        });
        
        // Track which source files contain each ASIN
        const syncDate = new Date().toISOString().split('T')[0];
        
        // First, clear old tracking data for this sync
        db.run(`DELETE FROM qpi_file_tracking WHERE qpi_sync_date < date('now', '-30 days')`, (err) => {
          if (err) console.error('Error clearing old QPI tracking:', err.message);
        });
        
        // Insert/update file tracking for each ASIN-source file combination
        const trackStmt = db.prepare(`
          INSERT INTO qpi_file_tracking (asin, sku, source_file, status, qpi_sync_date, last_seen, created_at)
          VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          ON CONFLICT(asin, source_file) 
          DO UPDATE SET 
            sku = excluded.sku,
            status = excluded.status,
            qpi_sync_date = excluded.qpi_sync_date,
            last_seen = CURRENT_TIMESTAMP
        `);
        
        let trackingInserts = 0;
        let trackingErrors = 0;
        qpiData.forEach(item => {
          if (item.asin && item.sourceFile) {
            trackStmt.run([item.asin, item.sku, item.sourceFile, item.status, syncDate], function(err) {
              if (err) {
                trackingErrors++;
                console.error('Error tracking QPI source file:', err.message);
              } else {
                trackingInserts++;
              }
            });
          }
        });
        trackStmt.finalize(() => {
          console.log(`Tracked ${trackingInserts} ASIN-source combinations across ${sourceFiles.size} source files`);
          console.log(`Tracking errors: ${trackingErrors}`);
        });
      }
      
      // Give it a moment to finish all updates
      setTimeout(() => {
        res.json({
          message: 'QPI sync completed',
          total_in_qpi: qpiData.length,
          unique_skus: qpiSkus.size,
          unique_asins: qpiAsins.size,
          items_updated: itemsUpdated,
          products_updated: productsUpdated,
          errors: errors
        });
      }, 1000);
    })
    .on('error', (err) => {
      res.status(500).json({ error: 'Error reading CSV file: ' + err.message });
    });
});

// Get unique filter values for variations master with cross-filtering
app.get('/api/variations/filters', (req, res) => {
  const selectedBrands = req.query.brands ? req.query.brands.split(',').filter(b => b) : [];
  const selectedBundles = req.query.bundles ? req.query.bundles.split(',').filter(b => b) : [];
  const selectedPpgs = req.query.ppgs ? req.query.ppgs.split(',').filter(p => p) : [];
  
  const results = {};
  let completed = 0;
  
  // Get brands (filtered by bundles and ppgs if selected)
  let brandConditions = [];
  let brandParams = [];
  if (selectedBundles.length > 0) {
    const placeholders = selectedBundles.map(() => '?').join(',');
    brandConditions.push(`bundle IN (${placeholders})`);
    brandParams.push(...selectedBundles);
  }
  if (selectedPpgs.length > 0) {
    const placeholders = selectedPpgs.map(() => '?').join(',');
    brandConditions.push(`ppg IN (${placeholders})`);
    brandParams.push(...selectedPpgs);
  }
  brandConditions.push('brand IS NOT NULL');
  brandConditions.push('brand != ""');
  const brandWhere = 'WHERE ' + brandConditions.join(' AND ');
  const brandQuery = `SELECT DISTINCT brand FROM variations_master ${brandWhere} ORDER BY brand`;
  
  // Get bundles (filtered by brands and ppgs if selected)
  let bundleConditions = [];
  let bundleParams = [];
  if (selectedBrands.length > 0) {
    const placeholders = selectedBrands.map(() => '?').join(',');
    bundleConditions.push(`brand IN (${placeholders})`);
    bundleParams.push(...selectedBrands);
  }
  if (selectedPpgs.length > 0) {
    const placeholders = selectedPpgs.map(() => '?').join(',');
    bundleConditions.push(`ppg IN (${placeholders})`);
    bundleParams.push(...selectedPpgs);
  }
  bundleConditions.push('bundle IS NOT NULL');
  bundleConditions.push('bundle != ""');
  const bundleWhere = 'WHERE ' + bundleConditions.join(' AND ');
  const bundleQuery = `SELECT DISTINCT bundle FROM variations_master ${bundleWhere} ORDER BY bundle`;
  
  // Get ppgs (filtered by brands and bundles if selected)
  let ppgConditions = [];
  let ppgParams = [];
  if (selectedBrands.length > 0) {
    const placeholders = selectedBrands.map(() => '?').join(',');
    ppgConditions.push(`brand IN (${placeholders})`);
    ppgParams.push(...selectedBrands);
  }
  if (selectedBundles.length > 0) {
    const placeholders = selectedBundles.map(() => '?').join(',');
    ppgConditions.push(`bundle IN (${placeholders})`);
    ppgParams.push(...selectedBundles);
  }
  ppgConditions.push('ppg IS NOT NULL');
  ppgConditions.push('ppg != ""');
  const ppgWhere = 'WHERE ' + ppgConditions.join(' AND ');
  const ppgQuery = `SELECT DISTINCT ppg FROM variations_master ${ppgWhere} ORDER BY ppg`;
  
  // Execute queries
  db.all(brandQuery, brandParams, (err, rows) => {
    if (err) {
      console.error('Error fetching brands:', err.message);
      results.brands = [];
    } else {
      results.brands = rows.map(r => r.brand);
    }
    completed++;
    if (completed === 3) res.json(results);
  });
  
  db.all(bundleQuery, bundleParams, (err, rows) => {
    if (err) {
      console.error('Error fetching bundles:', err.message);
      results.bundles = [];
    } else {
      results.bundles = rows.map(r => r.bundle);
    }
    completed++;
    if (completed === 3) res.json(results);
  });
  
  db.all(ppgQuery, ppgParams, (err, rows) => {
    if (err) {
      console.error('Error fetching ppgs:', err.message);
      results.ppgs = [];
    } else {
      results.ppgs = rows.map(r => r.ppg);
    }
    completed++;
    if (completed === 3) res.json(results);
  });
});

// Get unique filter values for variations master (deprecated - use above with params)
app.get('/api/variations/filters-old', (req, res) => {
  const queries = {
    brands: 'SELECT DISTINCT brand FROM variations_master WHERE brand IS NOT NULL AND brand != "" ORDER BY brand',
    bundles: 'SELECT DISTINCT bundle FROM variations_master WHERE bundle IS NOT NULL AND bundle != "" ORDER BY bundle',
    ppgs: 'SELECT DISTINCT ppg FROM variations_master WHERE ppg IS NOT NULL AND ppg != "" ORDER BY ppg'
  };
  
  const results = {};
  let completed = 0;
  
  Object.keys(queries).forEach(key => {
    db.all(queries[key], [], (err, rows) => {
      if (err) {
        console.error(`Error fetching ${key}:`, err.message);
        results[key] = [];
      } else {
        const column = key === 'brands' ? 'brand' : (key === 'bundles' ? 'bundle' : 'ppg');
        results[key] = rows.map(r => r[column]);
      }
      
      completed++;
      if (completed === Object.keys(queries).length) {
        res.json(results);
      }
    });
  });
});

// Sync Variations Master data
app.post('/api/sync/variations', (req, res) => {
  const variationsPath = 'A:\\Code\\InputFiles\\Variations_Master.csv';
  
  if (!fs.existsSync(variationsPath)) {
    res.status(404).json({ error: 'Variations Master CSV file not found' });
    return;
  }
  
  console.log('[VARIATIONS SYNC] Starting sync from:', variationsPath);
  const variationsData = [];
  let rowCount = 0;
  
  const stream = fs.createReadStream(variationsPath)
    .pipe(csv())
    .on('data', (data) => {
      rowCount++;
      // Handle BOM in first column name
      const keys = Object.keys(data);
      const asinKey = keys.find(k => k.includes('ASIN')) || 'ASIN';
      
      const asin = data[asinKey];
      const brand = data['brand'];
      const title = data['title'];
      const bundle = data['Bundle_Name'];
      const ppg = data['PPG_Grouping'];
      
      if (asin) {
        variationsData.push({ asin, brand, title, bundle, ppg });
      }
      
      // Log first few rows for debugging
      if (rowCount <= 3) {
        console.log(`[VARIATIONS SYNC] Row ${rowCount}:`, { asin, brand: brand?.substring(0, 20), bundle: bundle?.substring(0, 20) });
      }
    })
    .on('end', () => {
      console.log(`[VARIATIONS SYNC] Stream ended. Total rows processed: ${rowCount}, Valid items: ${variationsData.length}`);
      
      if (variationsData.length === 0) {
        res.json({
          message: 'Variations Master sync completed - no data found',
          total: 0,
          imported: 0,
          updated: 0,
          errors: 0
        });
        return;
      }
      
      let completed = 0;
      let imported = 0;
      let updated = 0;
      let errors = 0;
      
      const stmt = db.prepare(`
        INSERT INTO variations_master (asin, brand, title, bundle, ppg, synced_at, updated_at)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(asin) 
        DO UPDATE SET 
          brand = excluded.brand,
          title = excluded.title,
          bundle = excluded.bundle,
          ppg = excluded.ppg,
          synced_at = CURRENT_TIMESTAMP,
          updated_at = CURRENT_TIMESTAMP
      `);
      
      variationsData.forEach((item, index) => {
        stmt.run([item.asin, item.brand, item.title, item.bundle, item.ppg], function(err) {
          completed++;
          if (err) {
            errors++;
            console.error('Error importing variation:', err.message);
          } else {
            if (this.lastID) {
              imported++;
            } else {
              updated++;
            }
          }
          
          // Send response only after all rows are processed
          if (completed === variationsData.length) {
            stmt.finalize(() => {
              console.log(`[VARIATIONS SYNC] Complete - Imported: ${imported}, Updated: ${updated}, Errors: ${errors}`);
              res.json({
                message: 'Variations Master sync completed',
                total: variationsData.length,
                imported: imported,
                updated: updated,
                errors: errors
              });
            });
          }
        });
      });
    })
    .on('error', (err) => {
      console.error('[VARIATIONS SYNC] Error reading CSV:', err.message);
      res.status(500).json({ error: 'Error reading Variations Master CSV: ' + err.message });
    });
});

// Sync Vendor Code Mapping from Excel file
app.post('/api/sync/vendor-mapping', (req, res) => {
  const XLSX = require('xlsx');
  const mappingFile = 'A:\\Code\\InputFiles\\Mapping\\VendorCode_Mapping.xlsx';
  
  if (!fs.existsSync(mappingFile)) {
    res.status(404).json({ error: 'Vendor mapping file not found' });
    return;
  }
  
  try {
    console.log('[VENDOR MAPPING] Reading Excel file...');
    const workbook = XLSX.readFile(mappingFile);
    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];
    const data = XLSX.utils.sheet_to_json(sheet);
    
    console.log(`[VENDOR MAPPING] Found ${data.length} mapping records`);
    
    let imported = 0;
    let updated = 0;
    let errors = 0;
    
    db.run('DELETE FROM vendor_mapping', (err) => {
      if (err) {
        console.error('[VENDOR MAPPING] Error clearing table:', err.message);
      }
      
      const stmt = db.prepare(`
        INSERT INTO vendor_mapping (country, marketplace, country_code, vendor_code, qpi_file, domain)
        VALUES (?, ?, ?, ?, ?, ?)
      `);
      
      data.forEach(row => {
        const country = row['Country'] || row['Country_1'] || '';
        const marketplace = row['Marketplace'] || row['Country_1'] || '';
        const countryCode = row['Country code'] || '';
        const vendorCode = row['Sub Vendor code'] || '';
        const qpiFile = row['QPI'] || row['QPI_1'] || '';
        const domain = row['Domain'] || null;
        
        if (country && marketplace && countryCode) {
          stmt.run([country, marketplace, countryCode, vendorCode, qpiFile, domain], (err) => {
            if (err) {
              errors++;
              console.error(`[VENDOR MAPPING] Error inserting:`, err.message);
            } else {
              imported++;
            }
          });
        }
      });
      
      stmt.finalize(() => {
        console.log(`[VENDOR MAPPING] Complete - Imported: ${imported}, Errors: ${errors}`);
        res.json({
          message: 'Vendor mapping sync completed',
          total: data.length,
          imported: imported,
          errors: errors
        });
      });
    });
    
  } catch (error) {
    console.error('[VENDOR MAPPING] Error:', error.message);
    res.status(500).json({ error: 'Error syncing vendor mapping: ' + error.message });
  }
});

// Sync Online Status from Keepa parquet files
app.post('/api/sync/online', (req, res) => {
  const keepaDir = 'A:\\Keepa_PBI\\Output\\Battat_Keepa_Extract';
  
  // Domain mapping (Keepa numeric codes to country names)
  const domainMap = {
    1: 'United States',
    2: 'United Kingdom',
    3: 'Germany',
    4: 'France',
    5: 'Japan',
    6: 'Canada',
    8: 'Italy',
    9: 'Spain',
    11: 'Mexico'
  };
  
  try {
    // Find all Keepa extract files
    const files = fs.readdirSync(keepaDir)
      .filter(f => f.startsWith('KeepaFiltered_') && f.endsWith('.parquet'))
      .sort(); // Process chronologically
    
    if (files.length === 0) {
      res.status(404).json({ error: 'No Keepa extract files found' });
      return;
    }

    console.log(`[KEEPA SYNC] Found ${files.length} Keepa files to process`);

    let filesProcessed = 0;
    let filesSkipped = 0;
    let totalAsinsFound = 0;
    let totalAsinsOnline = 0;
    
    // Get list of already processed files
    db.all('SELECT filename FROM keepa_file_tracking', [], (err, processedFiles) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
      
      const processedFilenames = new Set(processedFiles.map(f => f.filename));
      const filesToProcess = files.filter(f => !processedFilenames.has(f));
      
      if (filesToProcess.length === 0) {
        res.json({
          message: 'All Keepa files already processed',
          total_files: files.length,
          files_processed: 0,
          files_skipped: files.length,
          asins_found: 0,
          asins_online: 0
        });
        return;
      }
      
      console.log(`[KEEPA SYNC] Processing ${filesToProcess.length} new files (${files.length - filesToProcess.length} already processed)`);
      
      // Get ASINs already online in all countries (to skip querying)
      db.all(`
        SELECT DISTINCT asin 
        FROM asin_online_status 
        GROUP BY asin
        HAVING COUNT(DISTINCT country) >= ?
      `, [Object.keys(domainMap).length], (err, fullyOnlineAsins) => {
        if (err) {
          console.error('Error fetching fully online ASINs:', err.message);
        }
        
        const skipAsins = new Set((fullyOnlineAsins || []).map(r => r.asin));
        console.log(`[KEEPA SYNC] Skipping ${skipAsins.size} ASINs already online in all countries`);
        
        // Process each file sequentially - function defined here to access skipAsins via closure
        function processFiles(filesToProcess, index) {
        if (index >= filesToProcess.length) {
          // All done - now update stage_6_product_online for all ASINs that are online
          db.run(`
            UPDATE products 
            SET stage_6_product_online = 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE asin IN (
              SELECT DISTINCT asin FROM asin_online_status
            )
          `, function(err) {
            const productsMarkedOnline = err ? 0 : this.changes;
            
            if (err) {
              console.error('[KEEPA SYNC] Error updating stage_6_product_online:', err.message);
            } else {
              console.log(`[KEEPA SYNC] Updated stage_6_product_online for ${productsMarkedOnline} products`);
            }
            
            res.json({
              message: 'Keepa sync completed',
              total_files: files.length,
              files_processed: filesProcessed,
              files_skipped: filesSkipped,
              asins_found: totalAsinsFound,
              asins_online: totalAsinsOnline,
              products_marked_online: productsMarkedOnline
            });
          });
          return;
        }
        
        const filename = filesToProcess[index];
        const filePath = path.join(keepaDir, filename).replace(/\\/g, '/');
        
        console.log(`[KEEPA SYNC] Processing file ${index + 1}/${filesToProcess.length}: ${filename}`);
        
        const duckDb = new duckdb.Database(':memory:');
        
        const query = `
          SELECT 
            CAST(ASIN AS VARCHAR) as asin,
            CAST(domain AS BIGINT) as domain,
            CAST(stats_buyBoxPrice AS BIGINT) as buybox_price,
            CAST(timestamp AS VARCHAR) as timestamp
          FROM read_parquet('${filePath}')
          WHERE ASIN IS NOT NULL 
            AND ASIN != ''
            AND stats_buyBoxPrice > 0
        `;

        duckDb.all(query, (err, rows) => {
          duckDb.close();
          
          if (err) {
            console.error(`Error reading ${filename}:`, err.message);
            filesSkipped++;
            processFiles(filesToProcess, index + 1);
            return;
          }
          
          console.log(`[KEEPA SYNC] Found ${rows.length} online ASINs in ${filename}`);
          
          let fileAsinsFound = new Set();
          let fileAsinsOnline = 0;
          
          // Process all rows in one transaction (simpler and more reliable)
          const date = new Date().toISOString().split('T')[0];
          
          db.serialize(() => {
            db.run('BEGIN TRANSACTION', (err) => {
              if (err) {
                console.error('[KEEPA SYNC] Error starting transaction:', err.message);
                filesSkipped++;
                processFiles(filesToProcess, index + 1);
                return;
              }
              
              const stmt = db.prepare(`
                INSERT INTO asin_online_status (asin, country, first_seen_online, last_seen_online, last_buybox_price, last_synced)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(asin, country)
                DO UPDATE SET
                  last_seen_online = excluded.last_seen_online,
                  last_buybox_price = excluded.last_buybox_price,
                  last_synced = CURRENT_TIMESTAMP
              `);
              
              let inserted = 0;
              for (const row of rows) {
                const asin = row.asin;
                const country = domainMap[row.domain] || `Unknown_${row.domain}`;
                const price = row.buybox_price;
                
                // Skip if this ASIN is already online everywhere
                if (skipAsins.has(asin)) {
                  continue;
                }
                
                fileAsinsFound.add(asin);
                stmt.run([asin, country, date, date, price]);
                inserted++;
                
                // Log progress every 1000 records
                if (inserted % 1000 === 0) {
                  console.log(`[KEEPA SYNC] Inserted ${inserted}/${rows.length} records...`);
                }
              }
              
              stmt.finalize((err) => {
                if (err) {
                  console.error('[KEEPA SYNC] Error finalizing statement:', err.message);
                  db.run('ROLLBACK');
                  filesSkipped++;
                  processFiles(filesToProcess, index + 1);
                  return;
                }
                
                db.run('COMMIT', (err) => {
                  if (err) {
                    console.error('[KEEPA SYNC] Error committing transaction:', err.message);
                    filesSkipped++;
                    processFiles(filesToProcess, index + 1);
                    return;
                  }
                  
                  fileAsinsOnline = fileAsinsFound.size;
                  totalAsinsFound += fileAsinsOnline;
                  
                  // Mark file as processed
                  db.run(
                    'INSERT INTO keepa_file_tracking (filename, asins_found, asins_online) VALUES (?, ?, ?)',
                    [filename, rows.length, fileAsinsOnline],
                    (err) => {
                      if (err) {
                        console.error('Error tracking file:', err.message);
                      }
                      
                      filesProcessed++;
                      console.log(`[KEEPA SYNC] Completed ${filename}: ${fileAsinsOnline} unique ASINs online`);
                      
                      // Process next file
                      processFiles(filesToProcess, index + 1);
                    }
                  );
                });
              });
            });
          });
        });
      }
      
      // Start processing files
      processFiles(filesToProcess, 0);
      });
    });
    
  } catch (error) {
    res.status(500).json({ error: 'Error processing Keepa files: ' + error.message });
  }
});

// Sync Vendor Central status from parquet extract
app.post('/api/sync/vc', (req, res) => {
  const vcDir = 'A:\\ProcessOutput\\VC_Extracts\\Comparison_Extracts';
  
  try {
    // Find most recent VC extract file
    const files = fs.readdirSync(vcDir)
      .filter(f => f.startsWith('vc_extracts_') && f.endsWith('.parquet'))
      .sort()
      .reverse();
    
    if (files.length === 0) {
      res.status(404).json({ error: 'No VC extract files found' });
      return;
    }

    const latestFile = path.join(vcDir, files[0]).replace(/\\/g, '/');
    console.log('Reading VC extract:', latestFile);

    const duckDb = new duckdb.Database(':memory:');
    
    const query = `
      SELECT DISTINCT 
        sku,
        summaries_0_asin as asin,
        summaries_0_status_0 as status,
        country,
        vendor_code
      FROM '${latestFile}'
      WHERE sku IS NOT NULL AND sku != ''
    `;

    duckDb.all(query, (err, rows) => {
      if (err) {
        res.status(500).json({ error: 'Error reading parquet file: ' + err.message });
        duckDb.close();
        return;
      }

      console.log(`Found ${rows.length} SKU-Country combinations in VC extract`);

      let updated = 0;
      let asinStatusUpdated = 0;
      let productsUpdated = 0;
      let errors = 0;

      // Collect unique ASINs for products table update
      const asinsInVC = new Set();
      const asinCountryData = [];
      
      rows.forEach(row => {
        if (row.asin) {
          asinsInVC.add(row.asin);
          if (row.country) {
            asinCountryData.push({
              asin: row.asin,
              sku: row.sku,
              country: row.country,
              status: row.status
            });
          }
        }
      });

      // Batch update products table - mark stage_4_product_listed for all ASINs in VC
      if (asinsInVC.size > 0) {
        const asinList = Array.from(asinsInVC);
        const placeholders = asinList.map(() => '?').join(',');
        
        db.run(
          `UPDATE products 
           SET stage_4_product_listed = 1,
               updated_at = CURRENT_TIMESTAMP
           WHERE asin IN (${placeholders})`,
          asinList,
          function(err) {
            if (err) {
              console.error('Error updating products stage_4:', err.message);
              errors++;
            } else {
              productsUpdated = this.changes;
              console.log(`Updated stage_4_product_listed for ${productsUpdated} products`);
            }
          }
        );
      }

      // Update vendor_mapping table with real vendor codes from VC extract
      const vendorCodesByCountry = new Map();
      rows.forEach(row => {
        if (row.vendor_code && row.country) {
          if (!vendorCodesByCountry.has(row.country)) {
            vendorCodesByCountry.set(row.country, new Set());
          }
          vendorCodesByCountry.get(row.country).add(row.vendor_code);
        }
      });

      console.log(`Found vendor codes for ${vendorCodesByCountry.size} countries`);
      
      // Update vendor_mapping with discovered vendor codes
      // First, get the base mapping info for each country
      db.all(
        `SELECT DISTINCT country, keepa_marketplace, customer_code, qpi_source_file, vc_file, language, currency, domain
         FROM vendor_mapping`,
        [],
        (err, baseRows) => {
          if (err) {
            console.error('Error fetching base vendor_mapping:', err.message);
            return;
          }

          const countryBaseMap = new Map();
          baseRows.forEach(row => {
            countryBaseMap.set(row.country, row);
          });

          // Now insert new rows for each vendor code
          vendorCodesByCountry.forEach((vendorCodes, country) => {
            const vendorCodeList = Array.from(vendorCodes).join(', ');
            console.log(`  ${country}: ${vendorCodeList}`);
            
            const baseInfo = countryBaseMap.get(country);
            if (!baseInfo) {
              console.log(`  ⚠ No base mapping found for ${country}, skipping`);
              return;
            }

            // Delete existing vendor codes for this country
            db.run(
              `DELETE FROM vendor_mapping WHERE country = ?`,
              [country],
              function(err) {
                if (err) {
                  console.error(`Error deleting old mappings for ${country}:`, err.message);
                  return;
                }

                // Insert new rows for each vendor code
                const stmt = db.prepare(`
                  INSERT INTO vendor_mapping 
                    (customer, country, keepa_marketplace, customer_code, vendor_code, qpi_source_file, vc_file, language, currency, domain)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                `);

                let inserted = 0;
                vendorCodes.forEach(vendorCode => {
                  stmt.run(
                    [
                      'Amazon',
                      country,
                      baseInfo.keepa_marketplace,
                      baseInfo.customer_code,
                      vendorCode,
                      baseInfo.qpi_source_file,
                      baseInfo.vc_file,
                      baseInfo.language,
                      baseInfo.currency,
                      baseInfo.domain
                    ],
                    function(err) {
                      if (err) {
                        console.error(`Error inserting vendor_code ${vendorCode} for ${country}:`, err.message);
                      } else {
                        inserted++;
                      }
                    }
                  );
                });

                stmt.finalize(() => {
                  console.log(`  ✓ Inserted ${inserted} vendor codes for ${country}`);
                });
              }
            );
          });
        }
      );

      // Batch insert/update ASIN country status
      if (asinCountryData.length > 0) {
        const stmt = db.prepare(`
          INSERT INTO asin_country_status (asin, sku, country_code, vc_status, last_synced, updated_at)
          VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          ON CONFLICT(asin, country_code) 
          DO UPDATE SET 
            sku = excluded.sku,
            vc_status = excluded.vc_status,
            last_synced = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        `);

        asinCountryData.forEach(data => {
          stmt.run([data.asin, data.sku, data.country, data.status], function(err) {
            if (err) {
              errors++;
            } else {
              asinStatusUpdated++;
            }
          });
        });

        stmt.finalize();
      }

      // Update legacy items table in batch
      const skuUpdates = new Map();
      rows.forEach(row => {
        if (row.sku && row.asin) {
          skuUpdates.set(row.sku, { asin: row.asin, status: row.status });
        }
      });

      if (skuUpdates.size > 0) {
        const stmt = db.prepare(`
          UPDATE items 
          SET vendor_central_setup = ?, 
              asin = COALESCE(NULLIF(asin, ''), ?),
              stage_4_product_listed = 1,
              updated_at = CURRENT_TIMESTAMP
          WHERE sku = ?
        `);

        skuUpdates.forEach((data, sku) => {
          const vcSetup = data.status ? 1 : 0;
          stmt.run([vcSetup, data.asin, sku], function(err) {
            if (err) {
              errors++;
            } else if (this.changes > 0) {
              updated++;
            }
          });
        });

        stmt.finalize();
      }

      // Give it a moment to finish all updates
      setTimeout(() => {
        duckDb.close();
        res.json({
          message: 'VC sync completed',
          file: files[0],
          total_in_vc: rows.length,
          unique_asins: asinsInVC.size,
          products_updated: productsUpdated,
          items_updated: updated,
          asin_status_updated: asinStatusUpdated,
          errors: errors
        });
      }, 2000);
    });

  } catch (error) {
    res.status(500).json({ error: 'Error accessing VC extract: ' + error.message });
  }
});

// Sync PIM Extract data
app.post('/api/sync/pim', (req, res) => {
  const pimPath = 'A:\\Code\\InputFiles\\PIM Extract.xlsx';

  if (!fs.existsSync(pimPath)) {
    res.status(404).json({ error: 'PIM Extract file not found' });
    return;
  }

  try {
    console.log('Reading PIM Extract...');
    const workbook = xlsx.readFile(pimPath);
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const data = xlsx.utils.sheet_to_json(worksheet);

    let itemsInserted = 0;
    let itemsUpdated = 0;
    let productsUpdated = 0;
    let errors = 0;

    console.log(`Found ${data.length} rows in PIM Extract`);

    // Step 1: Insert/Update item_numbers table
    db.serialize(() => {
      db.run('BEGIN TRANSACTION');
      
      const itemStmt = db.prepare(`
        INSERT INTO item_numbers (
          item_number, series, product_taxonomy_category, legal_name, upc_number, brand_product_line, age_grade,
          product_description_internal, item_spec_sheet_status, product_development_status,
          item_spec_data_last_updated, case_pack, package_length_cm, package_width_cm,
          package_height_cm, package_weight_kg, product_number, last_synced, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(item_number)
        DO UPDATE SET
          series = excluded.series,
          product_taxonomy_category = excluded.product_taxonomy_category,
          legal_name = excluded.legal_name,
          upc_number = excluded.upc_number,
          brand_product_line = excluded.brand_product_line,
          age_grade = excluded.age_grade,
          product_description_internal = excluded.product_description_internal,
          item_spec_sheet_status = excluded.item_spec_sheet_status,
          product_development_status = excluded.product_development_status,
          item_spec_data_last_updated = excluded.item_spec_data_last_updated,
          case_pack = excluded.case_pack,
          package_length_cm = excluded.package_length_cm,
          package_width_cm = excluded.package_width_cm,
          package_height_cm = excluded.package_height_cm,
          package_weight_kg = excluded.package_weight_kg,
          product_number = excluded.product_number,
          last_synced = CURRENT_TIMESTAMP,
          updated_at = CURRENT_TIMESTAMP
      `);

      data.forEach((row) => {
        const itemNumber = row['Item Number'];
        
        if (!itemNumber) {
          return;
        }

        itemStmt.run([
          itemNumber,
          row['Series'] || null,
          row['Product Taxonomy (Category)'] || null,
          row['Legal Name'] || null,
          row['UPC Number'] || null,
          row['Brand (Product Line) '] || null,  // Note: trailing space in Excel column name
          row['Age Grade'] || null,
          row['Product Description (internal)'] || null,
          row['Item Spec Sheet Status'] || null,
          row['Product Development Status'] || null,
          row['Item Spec Data Last Updated'] || null,
          row['Case Pack'] || null,
          row['Single Package Size - Length (cm)'] || null,
          row['Single Package Size - Width (cm)'] || null,
          row['Single Package Size - Height (cm)'] || null,
          row['Single Package Size - Weight (kg)'] || null,
          row['Product Number'] || null
        ], function(err) {
          if (err) {
            errors++;
            console.error(`Error upserting item ${itemNumber}:`, err.message);
          } else {
            if (this.changes > 0) {
              itemsInserted++;
            } else {
              itemsUpdated++;
            }
          }
        });
      });

      itemStmt.finalize(() => {
        db.run('COMMIT', (err) => {
          if (err) {
            console.error('Error committing item_numbers:', err.message);
            db.run('ROLLBACK');
            return;
          }
          
          console.log(`✓ Inserted/Updated ${itemsInserted + itemsUpdated} items in item_numbers table`);
          
          // Step 2: Set primary_item_number for each product based on most frequent item number
          console.log('Setting primary_item_number for products...');
          
          db.all(`
            SELECT 
              p.id as product_id,
              p.asin,
              ps.sku,
              COUNT(*) as sku_count
            FROM products p
            INNER JOIN product_skus ps ON p.id = ps.product_id
            WHERE ps.sku IN (SELECT item_number FROM item_numbers)
            GROUP BY p.id, ps.sku
            ORDER BY p.id, sku_count DESC
          `, [], (err, rows) => {
            if (err) {
              console.error('Error finding SKUs for products:', err.message);
              res.status(500).json({ error: 'Error setting primary item numbers' });
              return;
            }
            
            // Group by product_id and pick the first (most frequent) SKU
            const productPrimaryItems = new Map();
            rows.forEach(row => {
              if (!productPrimaryItems.has(row.product_id)) {
                productPrimaryItems.set(row.product_id, row.sku);
              }
            });
            
            console.log(`Found ${productPrimaryItems.size} products with item numbers`);
            
            // Update products with primary_item_number and data from item_numbers
            db.serialize(() => {
              db.run('BEGIN TRANSACTION');
              
              const updateStmt = db.prepare(`
                UPDATE products
                SET primary_item_number = ?,
                    brand = (SELECT brand_product_line FROM item_numbers WHERE item_number = ?),
                    product_description = (SELECT product_description_internal FROM item_numbers WHERE item_number = ?),
                    legal_name = (SELECT legal_name FROM item_numbers WHERE item_number = ?),
                    age_grade = (SELECT age_grade FROM item_numbers WHERE item_number = ?),
                    pim_spec_status = (SELECT item_spec_sheet_status FROM item_numbers WHERE item_number = ?),
                    product_dev_status = (SELECT product_development_status FROM item_numbers WHERE item_number = ?),
                    package_length_cm = (SELECT package_length_cm FROM item_numbers WHERE item_number = ?),
                    package_width_cm = (SELECT package_width_cm FROM item_numbers WHERE item_number = ?),
                    package_height_cm = (SELECT package_height_cm FROM item_numbers WHERE item_number = ?),
                    package_weight_kg = (SELECT package_weight_kg FROM item_numbers WHERE item_number = ?),
                    stage_2_product_finalized = (
                      SELECT CASE 
                        WHEN product_development_status = 'Finalized' THEN 1 
                        ELSE 0 
                      END 
                      FROM item_numbers WHERE item_number = ?
                    ),
                    stage_7_end_of_life = (
                      SELECT CASE 
                        WHEN product_development_status = 'NCF' THEN 1 
                        ELSE 0 
                      END 
                      FROM item_numbers WHERE item_number = ?
                    ),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
              `);
              
              productPrimaryItems.forEach((itemNumber, productId) => {
                updateStmt.run([
                  itemNumber,  // primary_item_number
                  itemNumber, itemNumber, itemNumber, itemNumber, itemNumber, itemNumber,
                  itemNumber, itemNumber, itemNumber, itemNumber, itemNumber, itemNumber,
                  productId
                ], function(err) {
                  if (err) {
                    errors++;
                    console.error(`Error updating product ${productId}:`, err.message);
                  } else if (this.changes > 0) {
                    productsUpdated++;
                  }
                });
              });
              
              updateStmt.finalize(() => {
                db.run('COMMIT', (err) => {
                  if (err) {
                    console.error('Error committing product updates:', err.message);
                    db.run('ROLLBACK');
                  } else {
                    console.log(`✓ Updated ${productsUpdated} products with PIM data`);
                  }
                  
                  res.json({
                    message: 'PIM sync completed',
                    total_in_pim: data.length,
                    items_inserted_updated: itemsInserted + itemsUpdated,
                    products_updated: productsUpdated,
                    errors: errors
                  });
                });
              });
            });
          });
        });
      });
    });

  } catch (error) {
    res.status(500).json({ error: 'Error reading PIM Extract: ' + error.message });
  }
});

// Get filter options for item numbers
app.get('/api/item-numbers/filters', (req, res) => {
  db.all(`
    SELECT DISTINCT item_number FROM item_numbers WHERE item_number IS NOT NULL ORDER BY item_number
  `, [], (err, itemNumbers) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    
    db.all(`
      SELECT DISTINCT product_number FROM item_numbers WHERE product_number IS NOT NULL ORDER BY product_number
    `, [], (err, productNumbers) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
      
      db.all(`
        SELECT DISTINCT product_description_internal FROM item_numbers WHERE product_description_internal IS NOT NULL ORDER BY product_description_internal
      `, [], (err, descriptions) => {
        if (err) {
          res.status(500).json({ error: err.message });
          return;
        }
        
        db.all(`
          SELECT DISTINCT brand_product_line FROM item_numbers WHERE brand_product_line IS NOT NULL ORDER BY brand_product_line
        `, [], (err, brands) => {
          if (err) {
            res.status(500).json({ error: err.message });
            return;
          }
          
          db.all(`
            SELECT DISTINCT series FROM item_numbers WHERE series IS NOT NULL ORDER BY series
          `, [], (err, series) => {
            if (err) {
              res.status(500).json({ error: err.message });
              return;
            }
            
            db.all(`
              SELECT DISTINCT product_taxonomy_category FROM item_numbers WHERE product_taxonomy_category IS NOT NULL ORDER BY product_taxonomy_category
            `, [], (err, taxonomies) => {
              if (err) {
                res.status(500).json({ error: err.message });
                return;
              }
              
              db.all(`
                SELECT DISTINCT legal_name FROM item_numbers WHERE legal_name IS NOT NULL ORDER BY legal_name
              `, [], (err, legalNames) => {
                if (err) {
                  res.status(500).json({ error: err.message });
                  return;
                }
                
                db.all(`
                  SELECT DISTINCT age_grade FROM item_numbers WHERE age_grade IS NOT NULL ORDER BY age_grade
                `, [], (err, ageGrades) => {
                  if (err) {
                    res.status(500).json({ error: err.message });
                    return;
                  }
                  
                  db.all(`
                    SELECT DISTINCT item_spec_sheet_status FROM item_numbers WHERE item_spec_sheet_status IS NOT NULL ORDER BY item_spec_sheet_status
                  `, [], (err, statuses) => {
                    if (err) {
                      res.status(500).json({ error: err.message });
                      return;
                    }
                    
                    db.all(`
                      SELECT DISTINCT product_development_status FROM item_numbers WHERE product_development_status IS NOT NULL ORDER BY product_development_status
                    `, [], (err, devStatuses) => {
                      if (err) {
                        res.status(500).json({ error: err.message });
                        return;
                      }
                      
                      db.all(`
                        SELECT DISTINCT upc_number FROM item_numbers WHERE upc_number IS NOT NULL ORDER BY upc_number
                      `, [], (err, upcs) => {
                        if (err) {
                          res.status(500).json({ error: err.message });
                          return;
                        }
                        
                        res.json({
                          itemNumbers: itemNumbers.map(r => r.item_number),
                          productNumbers: productNumbers.map(r => r.product_number),
                          descriptions: descriptions.map(r => r.product_description_internal),
                          brands: brands.map(r => r.brand_product_line),
                          series: series.map(r => r.series),
                          taxonomies: taxonomies.map(r => r.product_taxonomy_category),
                          legalNames: legalNames.map(r => r.legal_name),
                          ageGrades: ageGrades.map(r => r.age_grade),
                          statuses: statuses.map(r => r.item_spec_sheet_status),
                          devStatuses: devStatuses.map(r => r.product_development_status),
                          upcs: upcs.map(r => r.upc_number)
                        });
                      });
                    });
                  });
                });
              });
            });
          });
        });
      });
    });
  });
});

// Get all item numbers (Items tab)
app.get('/api/item-numbers', (req, res) => {
  const page = parseInt(req.query.page) || 1;
  const limit = parseInt(req.query.limit) || 50;
  const offset = (page - 1) * limit;
  const search = req.query.search || '';
  const series = req.query.series || '';
  const taxonomy = req.query.taxonomy || '';
  const brand = req.query.brand || '';
  const itemNumber = req.query.itemNumber || '';
  const productNumber = req.query.productNumber || '';
  const description = req.query.description || '';
  const legalName = req.query.legalName || '';
  const ageGrade = req.query.ageGrade || '';
  const status = req.query.status || '';
  const devStatus = req.query.devStatus || '';
  const upc = req.query.upc || '';
  
  let whereConditions = [];
  let params = [];
  
  if (search) {
    whereConditions.push('(item_number LIKE ? OR legal_name LIKE ? OR brand_product_line LIKE ?)');
    const searchPattern = `%${search}%`;
    params.push(searchPattern, searchPattern, searchPattern);
  }
  
  if (itemNumber && itemNumber !== 'all') {
    const itemNumberList = itemNumber.split(',');
    const itemNumberPlaceholders = itemNumberList.map(() => '?').join(',');
    whereConditions.push(`item_number IN (${itemNumberPlaceholders})`);
    params.push(...itemNumberList);
  }
  
  if (productNumber && productNumber !== 'all') {
    const productNumberList = productNumber.split(',');
    const productNumberPlaceholders = productNumberList.map(() => '?').join(',');
    whereConditions.push(`product_number IN (${productNumberPlaceholders})`);
    params.push(...productNumberList);
  }
  
  if (description && description !== 'all') {
    const descriptionList = description.split(',');
    const descriptionPlaceholders = descriptionList.map(() => '?').join(',');
    whereConditions.push(`product_description_internal IN (${descriptionPlaceholders})`);
    params.push(...descriptionList);
  }
  
  if (series && series !== 'all') {
    const seriesList = series.split(',');
    const seriesPlaceholders = seriesList.map(() => '?').join(',');
    whereConditions.push(`series IN (${seriesPlaceholders})`);
    params.push(...seriesList);
  }
  
  if (taxonomy && taxonomy !== 'all') {
    const taxonomyList = taxonomy.split(',');
    const taxonomyPlaceholders = taxonomyList.map(() => '?').join(',');
    whereConditions.push(`product_taxonomy_category IN (${taxonomyPlaceholders})`);
    params.push(...taxonomyList);
  }
  
  if (brand && brand !== 'all') {
    const brandList = brand.split(',');
    const brandPlaceholders = brandList.map(() => '?').join(',');
    whereConditions.push(`brand_product_line IN (${brandPlaceholders})`);
    params.push(...brandList);
  }
  
  if (legalName && legalName !== 'all') {
    const legalNameList = legalName.split(',');
    const legalNamePlaceholders = legalNameList.map(() => '?').join(',');
    whereConditions.push(`legal_name IN (${legalNamePlaceholders})`);
    params.push(...legalNameList);
  }
  
  if (ageGrade && ageGrade !== 'all') {
    const ageGradeList = ageGrade.split(',');
    const ageGradePlaceholders = ageGradeList.map(() => '?').join(',');
    whereConditions.push(`age_grade IN (${ageGradePlaceholders})`);
    params.push(...ageGradeList);
  }
  
  if (status && status !== 'all') {
    const statusList = status.split(',');
    const statusPlaceholders = statusList.map(() => '?').join(',');
    whereConditions.push(`item_spec_sheet_status IN (${statusPlaceholders})`);
    params.push(...statusList);
  }
  
  if (devStatus && devStatus !== 'all') {
    const devStatusList = devStatus.split(',');
    const devStatusPlaceholders = devStatusList.map(() => '?').join(',');
    whereConditions.push(`product_development_status IN (${devStatusPlaceholders})`);
    params.push(...devStatusList);
  }
  
  if (upc && upc !== 'all') {
    const upcList = upc.split(',');
    const upcPlaceholders = upcList.map(() => '?').join(',');
    whereConditions.push(`upc_number IN (${upcPlaceholders})`);
    params.push(...upcList);
  }
  
  const whereClause = whereConditions.length > 0 ? 'WHERE ' + whereConditions.join(' AND ') : '';
  
  // Get total count
  db.get(`SELECT COUNT(*) as total FROM item_numbers ${whereClause}`, params, (err, countResult) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    
    // Get paginated data
    const paginatedParams = [...params, limit, offset];
    db.all(`
      SELECT 
        i.*,
        p.stage_1_idea_considered,
        p.stage_2_product_finalized,
        p.stage_3a_pricing_submitted,
        p.stage_3b_pricing_approved,
        p.stage_4_product_listed,
        p.stage_5_product_ordered,
        p.stage_6_product_online,
        p.stage_7_end_of_life,
        p.asin
      FROM item_numbers i
      LEFT JOIN products p ON i.item_number = p.primary_item_number
      ${whereClause}
      ORDER BY i.item_number
      LIMIT ? OFFSET ?
    `, paginatedParams, (err, rows) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
      
      res.json({
        items: rows,
        total: countResult.total,
        page: page,
        limit: limit,
        totalPages: Math.ceil(countResult.total / limit)
      });
    });
  });
});

// Get unique series for filter
app.get('/api/item-numbers/series', (req, res) => {
  db.all(`
    SELECT DISTINCT series 
    FROM item_numbers 
    WHERE series IS NOT NULL 
    ORDER BY series
  `, [], (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    res.json(rows.map(r => r.series));
  });
});

// Get unique brands for filter
app.get('/api/item-numbers/brands', (req, res) => {
  db.all(`
    SELECT DISTINCT brand_product_line 
    FROM item_numbers 
    WHERE brand_product_line IS NOT NULL 
    ORDER BY brand_product_line
  `, [], (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    res.json(rows.map(r => r.brand_product_line));
  });
});

// Get unique taxonomies for filter
app.get('/api/item-numbers/taxonomies', (req, res) => {
  db.all(`
    SELECT DISTINCT product_taxonomy_category 
    FROM item_numbers 
    WHERE product_taxonomy_category IS NOT NULL 
    ORDER BY product_taxonomy_category
  `, [], (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    res.json(rows.map(r => r.product_taxonomy_category));
  });
});

// Get item numbers for a product
app.get('/api/products/:id/item-numbers', (req, res) => {
  const productId = req.params.id;
  
  db.all(`
    SELECT 
      i.*,
      ps.is_primary,
      CASE WHEN p.primary_item_number = i.item_number THEN 1 ELSE 0 END as is_primary_for_product
    FROM item_numbers i
    INNER JOIN product_skus ps ON i.item_number = ps.sku
    INNER JOIN products p ON ps.product_id = p.id
    WHERE p.id = ?
    ORDER BY is_primary_for_product DESC, i.item_number
  `, [productId], (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    res.json(rows);
  });
});

// Export item list (SKUs and ASINs)
app.get('/api/export/items', (req, res) => {
  const format = req.query.format || 'json'; // json, csv, or txt
  const fields = req.query.fields || 'sku,asin'; // Which fields to export
  
  const query = `
    SELECT 
      sku,
      asin,
      name,
      vendor_central_setup,
      order_received,
      online_available
    FROM items
    ORDER BY sku
  `;
  
  db.all(query, [], (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }

    const fieldList = fields.split(',').map(f => f.trim());

    if (format === 'csv') {
      // CSV format
      const headers = fieldList.join(',');
      const csvRows = rows.map(row => {
        return fieldList.map(field => {
          const value = row[field] || '';
          // Escape commas and quotes in CSV
          return `"${String(value).replace(/"/g, '""')}"`;
        }).join(',');
      });
      
      const csvContent = [headers, ...csvRows].join('\n');
      
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', 'attachment; filename="items_export.csv"');
      res.send(csvContent);
      
    } else if (format === 'txt') {
      // Plain text format (one per line)
      const txtRows = rows.map(row => {
        return fieldList.map(field => row[field] || '').join('\t');
      });
      
      const txtContent = txtRows.join('\n');
      
      res.setHeader('Content-Type', 'text/plain');
      res.setHeader('Content-Disposition', 'attachment; filename="items_export.txt"');
      res.send(txtContent);
      
    } else {
      // JSON format (default)
      const jsonData = rows.map(row => {
        const obj = {};
        fieldList.forEach(field => {
          if (row.hasOwnProperty(field)) {
            obj[field] = row[field];
          }
        });
        return obj;
      });
      
      res.json({
        total: jsonData.length,
        items: jsonData
      });
    }
  });
});

// ============================================
// REPORTS ENDPOINTS
// ============================================

// Report 1: Temporary ASINs not in PIM (no item_number)
app.get('/api/reports/temp-asins', (req, res) => {
  const query = `
    SELECT 
      p.asin,
      p.name,
      p.is_temp_asin,
      p.stage_1_item_number,
      GROUP_CONCAT(DISTINCT ps.sku) as skus,
      p.stage_1_brand,
      p.stage_1_country,
      p.created_at
    FROM products p
    LEFT JOIN product_skus ps ON p.id = ps.product_id
    WHERE p.is_temp_asin = 1 
      AND (p.stage_1_item_number IS NULL OR p.stage_1_item_number = '')
    GROUP BY p.id
    ORDER BY p.created_at DESC
  `;
  
  db.all(query, [], (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    
    const results = rows.map(row => ({
      ...row,
      skus: row.skus ? row.skus.split(',') : []
    }));
    
    res.json({
      total: results.length,
      data: results
    });
  });
});

// Report 2: PIM SKUs not in VC (has item_number but not stage_4_product_listed)
app.get('/api/reports/pim-not-vc', (req, res) => {
  const query = `
    SELECT 
      p.asin,
      p.name,
      p.stage_1_item_number,
      GROUP_CONCAT(DISTINCT ps.sku) as skus,
      p.stage_1_brand,
      p.stage_1_country,
      p.stage_2_product_finalized,
      p.stage_4_product_listed,
      p.created_at
    FROM products p
    LEFT JOIN product_skus ps ON p.id = ps.product_id
    WHERE p.stage_1_item_number IS NOT NULL 
      AND p.stage_1_item_number != ''
      AND (p.stage_4_product_listed = 0 OR p.stage_4_product_listed IS NULL)
    GROUP BY p.id
    ORDER BY p.created_at DESC
  `;
  
  db.all(query, [], (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    
    const results = rows.map(row => ({
      ...row,
      skus: row.skus ? row.skus.split(',') : []
    }));
    
    res.json({
      total: results.length,
      data: results
    });
  });
});

// Report 3: VC SKUs not in QPI (stage_4_product_listed but not stage_5_product_ordered)
app.get('/api/reports/vc-not-qpi', (req, res) => {
  const query = `
    SELECT 
      p.asin,
      p.name,
      p.stage_1_item_number,
      GROUP_CONCAT(DISTINCT ps.sku) as skus,
      p.stage_1_brand,
      p.stage_1_country,
      p.stage_4_product_listed,
      p.stage_5_product_ordered,
      p.created_at
    FROM products p
    LEFT JOIN product_skus ps ON p.id = ps.product_id
    WHERE p.stage_4_product_listed = 1
      AND (p.stage_5_product_ordered = 0 OR p.stage_5_product_ordered IS NULL)
    GROUP BY p.id
    ORDER BY p.created_at DESC
  `;
  
  db.all(query, [], (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    
    const results = rows.map(row => ({
      ...row,
      skus: row.skus ? row.skus.split(',') : []
    }));
    
    res.json({
      total: results.length,
      data: results
    });
  });
});

// Get online status for a specific ASIN
app.get('/api/online-status/:asin', (req, res) => {
  const asin = req.params.asin;
  
  const domainMap = {
    'United States': 'US',
    'United Kingdom': 'UK',
    'Germany': 'DE',
    'France': 'FR',
    'Japan': 'JP',
    'Canada': 'CA',
    'Italy': 'IT',
    'Spain': 'ES',
    'Mexico': 'MX'
  };
  
  const allCountries = Object.keys(domainMap);
  
  db.all(
    `SELECT country, first_seen_online, last_seen_online, last_buybox_price, last_synced 
     FROM asin_online_status 
     WHERE asin = ? 
     ORDER BY country`,
    [asin],
    (err, rows) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
      
      const onlineCountriesMap = new Map();
      rows.forEach(row => {
        onlineCountriesMap.set(row.country, {
          online: true,
          first_seen: row.first_seen_online,
          last_seen: row.last_seen_online,
          last_price: row.last_buybox_price,
          last_synced: row.last_synced
        });
      });
      
      const countriesStatus = allCountries.map(country => {
        const status = onlineCountriesMap.get(country);
        return {
          country: country,
          country_code: domainMap[country],
          online: status ? true : false,
          first_seen: status ? status.first_seen : null,
          last_seen: status ? status.last_seen : null,
          last_price: status ? status.last_price : null,
          last_synced: status ? status.last_synced : null
        };
      });
      
      const countriesOnline = rows.length;
      const totalCountries = allCountries.length;
      
      res.json({
        asin: asin,
        total_countries: totalCountries,
        countries_online: countriesOnline,
        countries: countriesStatus
      });
    }
  );
});

// Get QPI file status for a specific ASIN
app.get('/api/qpi-files/:asin', (req, res) => {
  const asin = req.params.asin;
  
  // Get all unique source files from qpi_file_tracking
  const allSourceFilesQuery = `
    SELECT DISTINCT source_file 
    FROM qpi_file_tracking 
    ORDER BY source_file
  `;
  
  db.all(allSourceFilesQuery, [], (err, allFiles) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    
    // Get files where this ASIN appears
    const asinFilesQuery = `
      SELECT 
        source_file,
        sku,
        qpi_sync_date,
        last_seen
      FROM qpi_file_tracking
      WHERE asin = ?
      ORDER BY source_file
    `;
    
    db.all(asinFilesQuery, [asin], (err, asinFiles) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
      
      // Create a map of files where ASIN was found
      const foundInFiles = new Map(asinFiles.map(r => [r.source_file, r]));
      
      // Get all unique source files (in case table is empty, use defaults)
      const knownSourceFiles = allFiles.length > 0 
        ? allFiles.map(f => f.source_file)
        : ['S26 QPI CA.xlsx', 'S26 QPI EMG.xlsx', 'S26 QPI EU.xlsx', 'S26 QPI JP003.xlsx', 'S26 QPI US.xlsx'];
      
      // Build response showing all source files
      const fileStatus = knownSourceFiles.map(fileName => ({
        source_file: fileName,
        found: foundInFiles.has(fileName),
        sku: foundInFiles.get(fileName)?.sku || null,
        last_seen: foundInFiles.get(fileName)?.last_seen || null,
        sync_date: foundInFiles.get(fileName)?.qpi_sync_date || null
      }));
      
      res.json({
        asin: asin,
        files: fileStatus,
        total_source_files: knownSourceFiles.length,
        files_found_in: asinFiles.length
      });
    });
  });
});

// Export Reports to Excel
app.get('/api/reports/:reportType/export', (req, res) => {
  const reportType = req.params.reportType;
  let query = '';
  let filename = '';
  
  if (reportType === 'temp-asins') {
    filename = 'Temporary_ASINs_Not_in_PIM.xlsx';
    query = `
      SELECT 
        p.asin AS ASIN,
        p.name AS Name,
        GROUP_CONCAT(DISTINCT ps.sku) as SKUs,
        p.stage_1_item_number AS 'Item Number',
        p.stage_1_brand AS Brand,
        p.stage_1_country AS Countries,
        datetime(p.created_at) AS Created
      FROM products p
      LEFT JOIN product_skus ps ON p.id = ps.product_id
      WHERE p.is_temp_asin = 1 
        AND (p.stage_1_item_number IS NULL OR p.stage_1_item_number = '')
      GROUP BY p.id
      ORDER BY p.created_at DESC
    `;
  } else if (reportType === 'pim-not-vc') {
    filename = 'PIM_SKUs_Not_in_VC.xlsx';
    query = `
      SELECT 
        p.asin AS ASIN,
        p.name AS Name,
        GROUP_CONCAT(DISTINCT ps.sku) as SKUs,
        p.stage_1_item_number AS 'Item Number',
        p.stage_1_brand AS Brand,
        p.stage_1_country AS Countries,
        CASE WHEN p.stage_2_product_finalized = 1 THEN 'Yes' ELSE 'No' END AS 'PIM Finalized',
        datetime(p.created_at) AS Created
      FROM products p
      LEFT JOIN product_skus ps ON p.id = ps.product_id
      WHERE p.stage_1_item_number IS NOT NULL 
        AND p.stage_1_item_number != ''
        AND (p.stage_4_product_listed = 0 OR p.stage_4_product_listed IS NULL)
      GROUP BY p.id
      ORDER BY p.created_at DESC
    `;
  } else if (reportType === 'vc-not-qpi') {
    filename = 'VC_Listed_Not_in_QPI.xlsx';
    query = `
      SELECT 
        p.asin AS ASIN,
        p.name AS Name,
        GROUP_CONCAT(DISTINCT ps.sku) as SKUs,
        p.stage_1_item_number AS 'Item Number',
        p.stage_1_brand AS Brand,
        p.stage_1_country AS Countries,
        datetime(p.created_at) AS Created
      FROM products p
      LEFT JOIN product_skus ps ON p.id = ps.product_id
      WHERE p.stage_4_product_listed = 1
        AND (p.stage_5_product_ordered = 0 OR p.stage_5_product_ordered IS NULL)
      GROUP BY p.id
      ORDER BY p.created_at DESC
    `;
  } else {
    res.status(400).json({ error: 'Invalid report type' });
    return;
  }
  
  db.all(query, [], (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    
    if (rows.length === 0) {
      res.status(404).json({ error: 'No data found for this report' });
      return;
    }
    
    // Create Excel workbook
    const XLSX = require('xlsx');
    const worksheet = XLSX.utils.json_to_sheet(rows);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, 'Report');
    
    // Auto-size columns
    const colWidths = [];
    const headers = Object.keys(rows[0]);
    headers.forEach((header, idx) => {
      const maxLength = Math.max(
        header.length,
        ...rows.map(row => String(row[header] || '').length)
      );
      colWidths.push({ wch: Math.min(maxLength + 2, 50) });
    });
    worksheet['!cols'] = colWidths;
    
    // Generate Excel file buffer
    const buffer = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
    
    // Send file
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.send(buffer);
  });
});

// Serve frontend
// ============ AUTHENTICATION & USER MANAGEMENT ROUTES ============

// Login endpoint
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
    
    // Create JWT token
    const token = jwt.sign(
      { 
        id: user.id, 
        username: user.username, 
        role: user.role,
        email: user.email,
        full_name: user.full_name
      },
      JWT_SECRET,
      { expiresIn: '24h' }
    );
    
    req.session.token = token;
    req.session.user = {
      id: user.id,
      username: user.username,
      role: user.role,
      email: user.email,
      full_name: user.full_name
    };
    
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

// Logout endpoint
app.post('/api/auth/logout', (req, res) => {
  req.session.destroy();
  res.json({ message: 'Logged out successfully' });
});

// Get current user
app.get('/api/auth/me', authenticateToken, (req, res) => {
  db.get('SELECT id, username, email, full_name, role, last_login FROM users WHERE id = ?', 
    [req.user.id], 
    (err, user) => {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      if (!user) {
        return res.status(404).json({ error: 'User not found' });
      }
      res.json(user);
    }
  );
});

// Get all users (Admin only)
app.get('/api/users', authenticateToken, requireRole('admin'), (req, res) => {
  db.all('SELECT id, username, email, full_name, role, is_active, created_at, last_login FROM users ORDER BY created_at DESC', 
    [], 
    (err, users) => {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      res.json(users);
    }
  );
});

// Create user (Admin only)
app.post('/api/users', authenticateToken, requireRole('admin'), async (req, res) => {
  const { username, email, password, full_name, role } = req.body;
  
  if (!username || !email || !password || !role) {
    return res.status(400).json({ error: 'Username, email, password, and role are required' });
  }
  
  const validRoles = ['viewer', 'approver', 'salesperson', 'admin'];
  if (!validRoles.includes(role)) {
    return res.status(400).json({ error: 'Invalid role' });
  }
  
  try {
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
      
      res.json({
        id: this.lastID,
        username,
        email,
        full_name,
        role,
        message: 'User created successfully'
      });
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Update user (Admin only)
app.put('/api/users/:id', authenticateToken, requireRole('admin'), async (req, res) => {
  const { id } = req.params;
  const { email, full_name, role, is_active, password } = req.body;
  
  const updates = [];
  const params = [];
  
  if (email !== undefined) {
    updates.push('email = ?');
    params.push(email);
  }
  if (full_name !== undefined) {
    updates.push('full_name = ?');
    params.push(full_name);
  }
  if (role !== undefined) {
    const validRoles = ['viewer', 'approver', 'salesperson', 'admin'];
    if (!validRoles.includes(role)) {
      return res.status(400).json({ error: 'Invalid role' });
    }
    updates.push('role = ?');
    params.push(role);
  }
  if (is_active !== undefined) {
    updates.push('is_active = ?');
    params.push(is_active ? 1 : 0);
  }
  if (password) {
    const passwordHash = await bcrypt.hash(password, 10);
    updates.push('password_hash = ?');
    params.push(passwordHash);
  }
  
  if (updates.length === 0) {
    return res.status(400).json({ error: 'No fields to update' });
  }
  
  updates.push('updated_at = CURRENT_TIMESTAMP');
  params.push(id);
  
  db.run(`UPDATE users SET ${updates.join(', ')} WHERE id = ?`, params, function(err) {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    if (this.changes === 0) {
      return res.status(404).json({ error: 'User not found' });
    }
    res.json({ message: 'User updated successfully' });
  });
});

// Delete user (Admin only)
app.delete('/api/users/:id', authenticateToken, requireRole('admin'), (req, res) => {
  const { id } = req.params;
  
  // Prevent deleting own account
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

// ============ PRICING APPROVAL WORKFLOW ============

// Submit pricing for approval
app.post('/api/pricing/submit', authenticateToken, requireRole('salesperson', 'admin'), (req, res) => {
  const { product_id, asin, product_cost, sell_price, countries, notes } = req.body;
  
  if (!product_id || !asin || !product_cost || !sell_price) {
    return res.status(400).json({ error: 'Product cost and sell price are required' });
  }
  
  if (!countries || !Array.isArray(countries) || countries.length === 0) {
    return res.status(400).json({ error: 'At least one country pricing is required' });
  }
  
  // Calculate company margin (always in USD)
  const company_margin = ((sell_price - product_cost) / sell_price * 100).toFixed(2);
  
  // Calculate average customer margin across all countries
  const avgCustomerMargin = (countries.reduce((sum, c) => sum + parseFloat(c.customer_margin), 0) / countries.length).toFixed(2);
  
  db.serialize(() => {
    db.run('BEGIN TRANSACTION');
    
    // Insert main pricing submission
    db.run(`
      INSERT INTO pricing_submissions 
      (product_id, asin, product_cost, sell_price, company_margin, retail_price, customer_margin, currency, notes, submitted_by)
      VALUES (?, ?, ?, ?, ?, ?, ?, 'USD', ?, ?)
    `, [product_id, asin, product_cost, sell_price, company_margin, 0, avgCustomerMargin, notes, req.user.id], 
    function(err) {
      if (err) {
        db.run('ROLLBACK');
        return res.status(500).json({ error: err.message });
      }
      
      const submissionId = this.lastID;
      
      // Insert country-specific pricing
      const stmt = db.prepare(`
        INSERT INTO pricing_submissions_country 
        (pricing_submission_id, country, sell_price_usd, retail_price_local, retail_price_usd, customer_margin, fx_rate)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `);
      
      countries.forEach(country => {
        stmt.run([
          submissionId,
          country.country,
          country.sell_price_usd,
          country.retail_price_local,
          country.retail_price_usd,
          country.customer_margin,
          country.fx_rate
        ]);
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
          
          // Create notifications for all approvers
          db.all('SELECT id FROM users WHERE role = "approver" AND is_active = 1', [], (err, approvers) => {
            if (!err && approvers.length > 0) {
              const stmt = db.prepare(`
                INSERT INTO notifications (user_id, type, title, message, link)
                VALUES (?, 'pricing_approval', ?, ?, ?)
              `);
              
              approvers.forEach(approver => {
                stmt.run([
                  approver.id,
                  'Pricing Approval Required',
                  `New multi-country pricing submission for ASIN ${asin} requires your approval`,
                  `/pricing-approvals`
                ]);
              });
              
              stmt.finalize();
            }
          });
          
          // Update product stage
          db.run(`
            UPDATE products 
            SET stage_3a_pricing_submitted = 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
          `, [product_id]);
          
          res.json({
            id: submissionId,
            message: 'Multi-country pricing submitted for approval',
            company_margin,
            avg_customer_margin: avgCustomerMargin,
            countries_count: countries.length
          });
        });
      });
    });
  });
});

// Get pricing submissions (for approvers or own submissions)
app.get('/api/pricing/submissions', authenticateToken, (req, res) => {
  const { status, product_id } = req.query;
  
  let query = `
    SELECT 
      ps.*,
      p.asin as product_asin,
      p.name as product_name,
      submitter.username as submitted_by_name,
      submitter.full_name as submitted_by_full_name,
      reviewer.username as reviewed_by_name,
      reviewer.full_name as reviewed_by_full_name
    FROM pricing_submissions ps
    LEFT JOIN products p ON ps.product_id = p.id
    LEFT JOIN users submitter ON ps.submitted_by = submitter.id
    LEFT JOIN users reviewer ON ps.reviewed_by = reviewer.id
    WHERE 1=1
  `;
  
  const params = [];
  
  // Approvers see all pending, others see only their own
  if (req.user.role !== 'approver' && req.user.role !== 'admin') {
    query += ' AND ps.submitted_by = ?';
    params.push(req.user.id);
  }
  
  if (status) {
    query += ' AND ps.status = ?';
    params.push(status);
  }
  
  if (product_id) {
    query += ' AND ps.product_id = ?';
    params.push(product_id);
  }
  
  query += ' ORDER BY ps.submitted_at DESC';
  
  db.all(query, params, (err, rows) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    res.json(rows);
  });
});

// Approve or reject pricing
app.post('/api/pricing/:id/review', authenticateToken, requireRole('approver', 'admin'), (req, res) => {
  const { id } = req.params;
  const { action, notes } = req.body; // action: 'approve' or 'reject'
  
  if (!action || !['approve', 'reject'].includes(action)) {
    return res.status(400).json({ error: 'Invalid action' });
  }
  
  const status = action === 'approve' ? 'approved' : 'rejected';
  
  // Get submission details first
  db.get('SELECT * FROM pricing_submissions WHERE id = ?', [id], (err, submission) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    if (!submission) {
      return res.status(404).json({ error: 'Submission not found' });
    }
    
    // Update submission
    db.run(`
      UPDATE pricing_submissions 
      SET status = ?,
          reviewed_by = ?,
          reviewed_at = CURRENT_TIMESTAMP,
          review_notes = ?
      WHERE id = ?
    `, [status, req.user.id, notes, id], function(err) {
      if (err) {
        return res.status(500).json({ error: err.message });
      }
      
      // If approved, update product stage
      if (status === 'approved') {
        db.run(`
          UPDATE products 
          SET stage_3b_pricing_approved = 1,
              updated_at = CURRENT_TIMESTAMP
          WHERE id = ?
        `, [submission.product_id]);
      }
      
      // Notify submitter
      db.run(`
        INSERT INTO notifications (user_id, type, title, message, link)
        VALUES (?, 'pricing_review', ?, ?, ?)
      `, [
        submission.submitted_by,
        `Pricing ${status === 'approved' ? 'Approved' : 'Rejected'}`,
        `Your pricing submission for ASIN ${submission.asin} has been ${status}`,
        `/pricing-submissions`
      ]);
      
      res.json({ 
        message: `Pricing ${status} successfully`,
        status
      });
    });
  });
});

// Get notifications for current user
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

// Mark notification as read
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

// Mark all notifications as read
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

// ============ FX RATES MANAGEMENT ============

// Get all FX rates
app.get('/api/fx-rates', authenticateToken, (req, res) => {
  db.all('SELECT * FROM fx_rates ORDER BY country', [], (err, rows) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    res.json(rows);
  });
});

// Update FX rate (Admin only)
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

// Bulk update FX rates (Admin only)
app.post('/api/fx-rates/bulk-update', authenticateToken, requireRole('admin'), (req, res) => {
  const { rates } = req.body; // Array of { id, rate_to_usd }
  
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

app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start server
app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\n[SHUTDOWN] Stopping sync scheduler...');
  if (syncScheduler) {
    syncScheduler.stop();
  }
  
  db.close((err) => {
    if (err) {
      console.error(err.message);
    }
    console.log('Database connection closed');
    process.exit(0);
  });
});

