const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('./amazon_vendor_central.db');

db.get(`
  SELECT 
    p.*,
    p.primary_item_number as primary_item_number,
    GROUP_CONCAT(DISTINCT ps.sku) as skus
  FROM products p
  LEFT JOIN product_skus ps ON p.id = ps.product_id
  WHERE p.asin = 'B079C6W6W3'
  GROUP BY p.id
`, [], (err, row) => {
  if (err) {
    console.error('Error:', err.message);
  } else if (row) {
    console.log('DB Result:');
    console.log('  ID:', row.id);
    console.log('  ASIN:', row.asin);
    console.log('  primary_item_number:', row.primary_item_number);
    console.log('  skus:', row.skus);
  } else {
    console.log('Not found');
  }
  db.close();
});

