const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('./amazon_vendor_central.db');

console.log('Creating item_numbers table...\n');

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
  if (err) {
    console.error('Error creating table:', err.message);
  } else {
    console.log('✓ item_numbers table created');
  }
  
  // Check if table was created
  db.all(`SELECT name FROM sqlite_master WHERE type='table' AND name='item_numbers'`, [], (err, rows) => {
    if (err) {
      console.error('Error checking table:', err.message);
    } else if (rows.length > 0) {
      console.log('✓ Table exists');
      
      // Count records
      db.get(`SELECT COUNT(*) as count FROM item_numbers`, [], (err, row) => {
        if (err) {
          console.error('Error counting:', err.message);
        } else {
          console.log(`  Records: ${row.count}`);
        }
        db.close();
      });
    } else {
      console.log('✗ Table does not exist');
      db.close();
    }
  });
});

