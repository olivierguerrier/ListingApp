const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('./amazon_vendor_central.db');

db.get(`SELECT id, asin, primary_item_number FROM products WHERE id = 4`, [], (err, row) => {
  console.log('Product ID 4:', row);
  
  db.get(`SELECT id, asin, primary_item_number FROM products ORDER BY created_at DESC LIMIT 1 OFFSET 3740`, [], (err2, row2) => {
    console.log('Product at offset 3740:', row2);
    db.close();
  });
});

