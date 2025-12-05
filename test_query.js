const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('./amazon_vendor_central.db');

console.log('Checking database structure...\n');

// First check what tables exist
db.all(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`, [], (err, tables) => {
  if (err) {
    console.error('Error listing tables:', err.message);
    db.close();
    return;
  }
  
  console.log('Tables in database:');
  tables.forEach(t => console.log('  -', t.name));
  
  // Now check if products table exists and what columns it has
  db.all(`PRAGMA table_info(products)`, [], (err, columns) => {
    if (err) {
      console.error('\nError getting products table info:', err.message);
      db.close();
      return;
    }
    
    console.log('\nColumns in products table:');
    columns.forEach(col => console.log(`  - ${col.name} (${col.type})`));
    
    // Now run the actual query
    db.all(`
      SELECT 
        id,
        asin,
        name,
        brand,
        primary_item_number,
        (SELECT GROUP_CONCAT(DISTINCT sku) FROM product_skus WHERE product_id = products.id) as skus
      FROM products
      ORDER BY created_at DESC
      LIMIT 5
    `, [], (err, rows) => {
      if (err) {
        console.error('\nError querying products:', err.message);
        db.close();
        return;
      }
      
      console.log(`\n\nFound ${rows.length} products`);
      console.log('\nSample data:');
      rows.forEach((row, i) => {
        console.log(`\n--- Product ${i + 1} ---`);
        console.log('ID:', row.id);
        console.log('ASIN:', row.asin);
        console.log('Name:', row.name);
        console.log('Brand:', row.brand);
        console.log('Primary Item #:', row.primary_item_number);
        console.log('SKUs:', row.skus);
      });
      
      db.close();
    });
  });
});

