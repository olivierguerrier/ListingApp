const sqlite3 = require('sqlite3').verbose();
const SyncScheduler = require('./syncScheduler');

console.log('============================================================');
console.log('DATABASE REBUILD SCRIPT');
console.log('============================================================\n');

// Connect to database (will be created if doesn't exist)
const db = new sqlite3.Database('./amazon_vendor_central.db', (err) => {
  if (err) {
    console.error('Error connecting to database:', err);
    process.exit(1);
  }
  console.log('✓ Connected to database\n');
});

// Initialize database schema
function initializeDatabase() {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      // Products table (ASIN is primary key)
      db.run(`
        CREATE TABLE IF NOT EXISTS products (
          asin TEXT PRIMARY KEY,
          name TEXT,
          legal_name TEXT,
          upc_number TEXT,
          brand TEXT,
          age_grade TEXT,
          product_description TEXT,
          pim_spec_status TEXT,
          product_dev_status TEXT,
          package_length_cm REAL,
          package_width_cm REAL,
          package_height_cm REAL,
          package_weight_kg REAL,
          stage_1_idea_considered INTEGER DEFAULT 0,
          stage_1_brand TEXT,
          stage_1_description TEXT,
          stage_1_season_launch TEXT,
          stage_1_country TEXT,
          stage_1_item_number TEXT,
          stage_2_product_finalized INTEGER DEFAULT 0,
          stage_2_newly_finalized INTEGER DEFAULT 0,
          stage_3a_pricing_submitted INTEGER DEFAULT 0,
          stage_3b_pricing_approved INTEGER DEFAULT 0,
          stage_4_product_listed INTEGER DEFAULT 0,
          stage_5_product_ordered INTEGER DEFAULT 0,
          stage_6_product_online INTEGER DEFAULT 0,
          is_temp_asin INTEGER DEFAULT 0,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
      `);

      // Product SKUs (many-to-one with products)
      db.run(`
        CREATE TABLE IF NOT EXISTS product_skus (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          product_id TEXT NOT NULL,
          sku TEXT NOT NULL,
          is_primary INTEGER DEFAULT 0,
          source TEXT,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
          FOREIGN KEY (product_id) REFERENCES products(asin),
          UNIQUE(product_id, sku)
        )
      `);

      // Product country pricing
      db.run(`
        CREATE TABLE IF NOT EXISTS product_country_pricing (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          product_id TEXT NOT NULL,
          country TEXT NOT NULL,
          sell_price REAL,
          retail_price REAL,
          currency TEXT,
          approval_status TEXT DEFAULT 'pending',
          submitted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
          approved_at DATETIME,
          FOREIGN KEY (product_id) REFERENCES products(asin),
          UNIQUE(product_id, country)
        )
      `);

      // Flow stage history
      db.run(`
        CREATE TABLE IF NOT EXISTS product_flow_history (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          product_id TEXT NOT NULL,
          stage_number INTEGER NOT NULL,
          stage_name TEXT NOT NULL,
          completed INTEGER DEFAULT 0,
          timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
          notes TEXT,
          FOREIGN KEY (product_id) REFERENCES products(asin)
        )
      `);

      // Temp ASIN counter
      db.run(`
        CREATE TABLE IF NOT EXISTS temp_asin_counter (
          id INTEGER PRIMARY KEY,
          count INTEGER DEFAULT 0
        )
      `, (err) => {
        if (err) {
          reject(err);
        } else {
          console.log('✓ Database schema initialized\n');
          resolve();
        }
      });
    });
  });
}

// Run the full sync
async function runFullSync() {
  console.log('============================================================');
  console.log('STARTING FULL DATA SYNC');
  console.log('============================================================\n');
  
  const syncScheduler = new SyncScheduler(db);
  
  try {
    await syncScheduler.syncAll();
    console.log('\n============================================================');
    console.log('✓ FULL SYNC COMPLETE');
    console.log('============================================================\n');
  } catch (error) {
    console.error('\n✗ SYNC FAILED:', error);
    throw error;
  }
}

// Get database statistics
function getStatistics() {
  return new Promise((resolve, reject) => {
    console.log('============================================================');
    console.log('DATABASE STATISTICS');
    console.log('============================================================\n');
    
    db.get('SELECT COUNT(*) as count FROM products', (err, row) => {
      if (err) return reject(err);
      console.log(`Products: ${row.count}`);
      
      db.get('SELECT COUNT(*) as count FROM product_skus', (err, row) => {
        if (err) return reject(err);
        console.log(`SKUs: ${row.count}`);
        
        db.get('SELECT COUNT(*) as count FROM products WHERE stage_4_product_listed = 1', (err, row) => {
          if (err) return reject(err);
          console.log(`Products Listed (Stage 4): ${row.count}`);
          
          db.get('SELECT COUNT(*) as count FROM products WHERE stage_5_product_ordered = 1', (err, row) => {
            if (err) return reject(err);
            console.log(`Products Ordered (Stage 5): ${row.count}`);
            
            db.get('SELECT COUNT(*) as count FROM products WHERE is_temp_asin = 1', (err, row) => {
              if (err) return reject(err);
              console.log(`Products with Temp ASIN: ${row.count}\n`);
              resolve();
            });
          });
        });
      });
    });
  });
}

// Main execution
(async () => {
  try {
    await initializeDatabase();
    await runFullSync();
    await getStatistics();
    
    console.log('✓ Database rebuild complete!\n');
    process.exit(0);
  } catch (error) {
    console.error('✗ Error during rebuild:', error);
    process.exit(1);
  }
})();

