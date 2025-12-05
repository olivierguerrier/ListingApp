/**
 * Migration Script: Migrate to Customer-based Product Structure
 * 
 * This script:
 * 1. Drops old product-related tables (products, product_skus, product_country_pricing, etc.)
 * 2. Creates new customer_groups table
 * 3. Creates new customers table
 * 4. Creates new products table with customer-based structure
 * 5. Keeps item_numbers table intact
 * 
 * Run with: node migrate_to_customer_structure.js
 */

const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const dbPath = path.join(__dirname, 'database.db');
const db = new sqlite3.Database(dbPath);

console.log('='.repeat(60));
console.log('MIGRATION: Customer-based Product Structure');
console.log('='.repeat(60));
console.log('');

db.serialize(() => {
  console.log('[1/6] Dropping old product-related tables...');
  
  // Drop tables that reference products first (foreign keys)
  const tablesToDrop = [
    'product_country_pricing',
    'flow_stage_history',
    'product_skus',
    'pricing_submissions_country',
    'pricing_submissions',
    'qpi_file_tracking',
    'qpi_country_status',
    'asin_country_status',
    'asin_online_status',
    'products'
  ];
  
  tablesToDrop.forEach(table => {
    db.run(`DROP TABLE IF EXISTS ${table}`, (err) => {
      if (err) {
        console.error(`  Error dropping ${table}:`, err.message);
      } else {
        console.log(`  ✓ Dropped ${table}`);
      }
    });
  });

  console.log('\n[2/6] Creating customer_groups table...');
  db.run(`
    CREATE TABLE IF NOT EXISTS customer_groups (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT UNIQUE NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `, (err) => {
    if (err) {
      console.error('  Error creating customer_groups:', err.message);
    } else {
      console.log('  ✓ Created customer_groups table');
    }
  });

  console.log('\n[3/6] Creating customers table...');
  db.run(`
    CREATE TABLE IF NOT EXISTS customers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      customer_group_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (customer_group_id) REFERENCES customer_groups(id) ON DELETE CASCADE,
      UNIQUE(customer_group_id, name)
    )
  `, (err) => {
    if (err) {
      console.error('  Error creating customers:', err.message);
    } else {
      console.log('  ✓ Created customers table');
    }
  });

  console.log('\n[4/6] Creating new products table...');
  db.run(`
    CREATE TABLE IF NOT EXISTS products (
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
    )
  `, (err) => {
    if (err) {
      console.error('  Error creating products:', err.message);
    } else {
      console.log('  ✓ Created products table');
    }
  });

  console.log('\n[5/6] Creating indexes for better query performance...');
  
  db.run(`CREATE INDEX IF NOT EXISTS idx_products_customer_id ON products(customer_id)`, (err) => {
    if (err) console.error('  Error creating index:', err.message);
    else console.log('  ✓ Created idx_products_customer_id');
  });
  
  db.run(`CREATE INDEX IF NOT EXISTS idx_products_customer_number ON products(customer_number)`, (err) => {
    if (err) console.error('  Error creating index:', err.message);
    else console.log('  ✓ Created idx_products_customer_number');
  });
  
  db.run(`CREATE INDEX IF NOT EXISTS idx_products_item_number ON products(item_number)`, (err) => {
    if (err) console.error('  Error creating index:', err.message);
    else console.log('  ✓ Created idx_products_item_number');
  });
  
  db.run(`CREATE INDEX IF NOT EXISTS idx_products_status ON products(status)`, (err) => {
    if (err) console.error('  Error creating index:', err.message);
    else console.log('  ✓ Created idx_products_status');
  });
  
  db.run(`CREATE INDEX IF NOT EXISTS idx_customers_group_id ON customers(customer_group_id)`, (err) => {
    if (err) console.error('  Error creating index:', err.message);
    else console.log('  ✓ Created idx_customers_group_id');
  });

  console.log('\n[6/6] Verifying item_numbers table is intact...');
  db.get(`SELECT COUNT(*) as count FROM item_numbers`, [], (err, row) => {
    if (err) {
      console.error('  Error checking item_numbers:', err.message);
    } else {
      console.log(`  ✓ item_numbers table has ${row.count} records`);
    }
  });

  // Close database after all operations
  setTimeout(() => {
    db.close((err) => {
      if (err) {
        console.error('Error closing database:', err.message);
      } else {
        console.log('\n' + '='.repeat(60));
        console.log('MIGRATION COMPLETE');
        console.log('='.repeat(60));
        console.log('\nNext steps:');
        console.log('1. Restart the server');
        console.log('2. Upload products via the new upload feature');
        console.log('3. Manage customers via Admin Portal');
      }
    });
  }, 1000);
});

