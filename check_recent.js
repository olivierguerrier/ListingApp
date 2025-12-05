const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('./amazon_vendor_central.db');

db.all(`
  SELECT id, asin, primary_item_number, created_at
  FROM products
  ORDER BY created_at DESC
  LIMIT 5
`, [], (err, rows) => {
  if (err) {
    console.error('Error:', err.message);
  } else {
    console.log('Most recent products:');
    rows.forEach(r => {
      console.log(`  ID ${r.id}: ${r.asin} - primary: ${r.primary_item_number} - created: ${r.created_at}`);
    });
  }
  db.close();
});

