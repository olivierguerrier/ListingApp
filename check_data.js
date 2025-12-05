const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('./amazon_vendor_central.db');

console.log('Checking data availability...\n');

// Check item_numbers table
db.all(`SELECT COUNT(*) as count FROM item_numbers`, [], (err, rows) => {
  if (err) {
    console.log('item_numbers table does not exist or error:', err.message);
  } else {
    console.log(`item_numbers table: ${rows[0].count} records`);
    
    // Show sample
    db.all(`SELECT item_number, brand_product_line, product_description_internal FROM item_numbers LIMIT 3`, [], (err, samples) => {
      if (!err && samples.length > 0) {
        console.log('Sample item_numbers:');
        samples.forEach(s => console.log(`  - ${s.item_number}: ${s.brand_product_line}`));
      }
    });
  }
});

// Check product_skus table
db.all(`SELECT COUNT(*) as count FROM product_skus`, [], (err, rows) => {
  if (err) {
    console.log('\nproduct_skus table error:', err.message);
  } else {
    console.log(`\nproduct_skus table: ${rows[0].count} records`);
    
    // Show sample with product info
    db.all(`
      SELECT ps.id, ps.sku, ps.product_id, p.asin, p.name
      FROM product_skus ps
      LEFT JOIN products p ON ps.product_id = p.id
      LIMIT 5
    `, [], (err, samples) => {
      if (!err && samples.length > 0) {
        console.log('Sample product_skus:');
        samples.forEach(s => console.log(`  - SKU: ${s.sku}, Product ID: ${s.product_id}, ASIN: ${s.asin || 'NULL'}`));
      }
    });
  }
});

// Check if there's a relationship issue
setTimeout(() => {
  db.all(`
    SELECT 
      p.id as product_id,
      p.asin,
      p.name,
      COUNT(ps.id) as sku_count,
      GROUP_CONCAT(ps.sku) as skus
    FROM products p
    LEFT JOIN product_skus ps ON p.id = ps.product_id
    GROUP BY p.id
    LIMIT 5
  `, [], (err, rows) => {
    if (!err) {
      console.log('\n\nProduct-SKU relationships:');
      rows.forEach(r => {
        console.log(`  Product ${r.product_id} (${r.asin}): ${r.sku_count} SKUs = ${r.skus || 'NONE'}`);
      });
    }
    db.close();
  });
}, 1000);

