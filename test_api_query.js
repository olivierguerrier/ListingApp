const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('./amazon_vendor_central.db');

const query = `
  SELECT 
    p.id,
    p.asin,
    p.primary_item_number,
    p.brand,
    GROUP_CONCAT(DISTINCT ps.sku) as skus
  FROM products p
  LEFT JOIN product_skus ps ON p.id = ps.product_id
  WHERE p.id = 4
  GROUP BY p.id
`;

db.get(query, [], (err, row) => {
  if (err) {
    console.error('Error:', err.message);
  } else {
    console.log('Result from query:', row);
  }
  db.close();
});

