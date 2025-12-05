const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('./amazon_vendor_central.db');

db.get(`
  SELECT 
    p.*,
    GROUP_CONCAT(DISTINCT ps.sku) as skus
  FROM products p
  LEFT JOIN product_skus ps ON p.id = ps.product_id
  WHERE p.asin = 'B01F98ABAQ'
  GROUP BY p.id
`, [], (err, row) => {
  if (err) {
    console.error('Error:', err.message);
  } else if (row) {
    console.log('Product data:');
    console.log('  ASIN:', row.asin);
    console.log('  Name:', row.name);
    console.log('  Brand:', row.brand);
    console.log('  Primary Item #:', row.primary_item_number);
    console.log('  SKUs:', row.skus);
    console.log('  ID:', row.id);
  } else {
    console.log('Product not found');
  }
  db.close();
});

