const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('./amazon_vendor_central.db');

db.get(`SELECT id, asin, primary_item_number FROM products WHERE asin = 'B0FHJ8XN71'`, [], (err, row) => {
  if (err) {
    console.error('Error:', err.message);
  } else if (row) {
    console.log('Product:', row);
  } else {
    console.log('Not found');
  }
  db.close();
});

