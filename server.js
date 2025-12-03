const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');
const csv = require('csv-parser');
const duckdb = require('duckdb');
const xlsx = require('xlsx');
const SyncScheduler = require('./syncScheduler');

const app = express();
const PORT = process.env.PORT || 7777;

let syncScheduler; // Global sync scheduler instance

// Middleware
app.use(cors());
app.use(bodyParser.json());
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

// Initialize database tables
function initializeDatabase() {
  db.serialize(() => {
    // Products table - ASIN is the master ID
    db.run(`CREATE TABLE IF NOT EXISTS products (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      asin TEXT UNIQUE NOT NULL,
      name TEXT,
      is_temp_asin BOOLEAN DEFAULT 0,
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
      stage_2_product_finalized BOOLEAN DEFAULT 0,
      stage_2_newly_finalized BOOLEAN DEFAULT 0,
      stage_3a_pricing_submitted BOOLEAN DEFAULT 0,
      stage_3b_pricing_approved BOOLEAN DEFAULT 0,
      stage_4_product_listed BOOLEAN DEFAULT 0,
      stage_5_product_ordered BOOLEAN DEFAULT 0,
      stage_6_product_online BOOLEAN DEFAULT 0,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

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

    // Temp ASIN counter table
    db.run(`CREATE TABLE IF NOT EXISTS temp_asin_counter (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      counter INTEGER DEFAULT 1
    )`);
    
    db.run(`INSERT OR IGNORE INTO temp_asin_counter (id, counter) VALUES (1, 1)`);

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
      ) as sku_details
    FROM products p
    LEFT JOIN product_skus ps ON p.id = ps.product_id
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
    'asin_country_status'
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

// Legacy endpoint for backward compatibility - with pagination
app.get('/api/items', (req, res) => {
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
      ) as sku_details
    FROM products p
    LEFT JOIN product_skus ps ON p.id = ps.product_id
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
  const { sku, asin, name } = req.body;

  if (!sku) {
    res.status(400).json({ error: 'SKU is required' });
    return;
  }

  // Use findOrCreateProduct helper
  findOrCreateProduct(asin, name, (err, product) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
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
        is_temp_asin: product.is_temp_asin
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
      if (sku && asin) {
        qpiData.push({ sku, asin });
      }
    })
    .on('end', () => {
      console.log(`Found ${qpiData.length} items in QPI`);
      
      // Collect unique SKUs and ASINs
      const qpiSkus = new Set();
      const qpiAsins = new Set();
      const skuAsinMap = new Map();
      
      qpiData.forEach(item => {
        qpiSkus.add(item.sku);
        if (item.asin) {
          qpiAsins.add(item.asin);
          skuAsinMap.set(item.sku, item.asin);
        }
      });
      
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
        const placeholders = asinList.map(() => '?').join(',');
        
        db.run(
          `UPDATE products 
           SET stage_5_product_ordered = 1,
               updated_at = CURRENT_TIMESTAMP
           WHERE asin IN (${placeholders})`,
          asinList,
          function(err) {
            if (err) {
              console.error('Error updating products stage_5:', err.message);
              errors++;
            } else {
              productsUpdated = this.changes;
              console.log(`Updated stage_5_product_ordered for ${productsUpdated} products`);
            }
          }
        );
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
        country
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

    let updated = 0;
    let notFound = 0;
    let errors = 0;

    data.forEach((row) => {
      const itemNumber = row['Item Number'];
      
      if (!itemNumber) {
        return;
      }

      const updateData = {
        legal_name: row['Legal Name'] || null,
        upc_number: row['UPC Number'] || null,
        brand: row['Brand (Product Line)'] || null,
        age_grade: row['Age Grade'] || null,
        product_description: row['Product Description (internal)'] || null,
        pim_spec_status: row['Item Spec Sheet Status'] || null,
        product_dev_status: row['Product Development Status'] || null,
        case_pack: row['Case Pack'] || null,
        package_length_cm: row['Single Package Size - Length (cm)'] || null,
        package_width_cm: row['Single Package Size - Width (cm)'] || null,
        package_height_cm: row['Single Package Size - Height (cm)'] || null,
        package_weight_kg: row['Single Package Size - Weight (kg)'] || null
      };

      // Update item with name from Legal Name if available
      const nameToUse = updateData.legal_name || itemNumber;
      
      // Check if Stage 2 should be marked (Product Finalized)
      const stage2Finalized = (updateData.product_dev_status && 
                               updateData.product_dev_status.toLowerCase() === 'finalized') ? 1 : 0;

      db.run(
        `UPDATE items 
         SET name = ?,
             legal_name = ?,
             upc_number = ?,
             brand = ?,
             age_grade = ?,
             product_description = ?,
             pim_spec_status = ?,
             product_dev_status = ?,
             case_pack = ?,
             package_length_cm = ?,
             package_width_cm = ?,
             package_height_cm = ?,
             package_weight_kg = ?,
             stage_2_product_finalized = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE sku = ?`,
        [
          nameToUse,
          updateData.legal_name,
          updateData.upc_number,
          updateData.brand,
          updateData.age_grade,
          updateData.product_description,
          updateData.pim_spec_status,
          updateData.product_dev_status,
          updateData.case_pack,
          updateData.package_length_cm,
          updateData.package_width_cm,
          updateData.package_height_cm,
          updateData.package_weight_kg,
          stage2Finalized,
          itemNumber
        ],
        function(err) {
          if (err) {
            errors++;
            console.error(`Error updating ${itemNumber}:`, err.message);
          } else if (this.changes > 0) {
            updated++;
          } else {
            notFound++;
          }
        }
      );
    });

    // Give it a moment to finish all updates
    setTimeout(() => {
      res.json({
        message: 'PIM sync completed',
        total_in_pim: data.length,
        updated: updated,
        not_found: notFound,
        errors: errors
      });
    }, 1000);

  } catch (error) {
    res.status(500).json({ error: 'Error reading PIM Extract: ' + error.message });
  }
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

// Serve frontend
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

