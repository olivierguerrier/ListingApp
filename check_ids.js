const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('./amazon_vendor_central.db');

db.all(`
  SELECT id, asin, primary_item_number, created_at
  FROM products
  WHERE id IN (14959, 3029, 4)
  ORDER BY id
`, [], (err, rows) => {
  if (err) {
    console.error('Error:', err.message);
  } else {
    console.log('Checking specific products:');
    rows.forEach(r => {
      console.log(`  ID ${r.id}: ${r.asin} - primary: ${r.primary_item_number} - created: ${r.created_at}`);
    });
    
    // Now check what ORDER BY created_at DESC gives us
    db.all(`
      SELECT id, asin, primary_item_number, created_at
      FROM products
      ORDER BY created_at DESC
      LIMIT 3
      OFFSET 3
    `, [], (err2, rows2) => {
      console.log('\nProducts at offset 3:');
      rows2.forEach(r => {
        console.log(`  ID ${r.id}: ${r.asin} - primary: ${r.primary_item_number} - created: ${r.created_at}`);
      });
      db.close();
    });
  }
});

