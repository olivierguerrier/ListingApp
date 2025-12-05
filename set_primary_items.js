const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('./amazon_vendor_central.db');

console.log('Setting primary_item_number for all products...\n');

// Get all products with their SKUs
db.all(`
  SELECT 
    p.id,
    p.asin,
    GROUP_CONCAT(ps.sku) as skus
  FROM products p
  LEFT JOIN product_skus ps ON p.id = ps.product_id
  GROUP BY p.id
  HAVING skus IS NOT NULL
`, [], (err, products) => {
  if (err) {
    console.error('Error:', err.message);
    db.close();
    return;
  }
  
  console.log(`Found ${products.length} products with SKUs`);
  
  // For each product, pick the first SKU as primary (or most frequent if multiple)
  const updateStmt = db.prepare(`UPDATE products SET primary_item_number = ? WHERE id = ?`);
  
  let updated = 0;
  products.forEach(product => {
    const skuList = product.skus.split(',');
    
    // Count frequencies
    const skuCounts = {};
    skuList.forEach(sku => {
      skuCounts[sku] = (skuCounts[sku] || 0) + 1;
    });
    
    // Find most frequent
    let primary = skuList[0];
    let maxCount = 0;
    for (const [sku, count] of Object.entries(skuCounts)) {
      if (count > maxCount) {
        maxCount = count;
        primary = sku;
      }
    }
    
    updateStmt.run([primary, product.id], function(err) {
      if (err) {
        console.error(`Error updating ${product.asin}:`, err.message);
      } else if (this.changes > 0) {
        updated++;
        if (updated <= 5) {
          console.log(`  ✓ Set ${product.asin} primary_item_number = ${primary}`);
        }
      }
    });
  });
  
  updateStmt.finalize(() => {
    console.log(`\n✓ Updated ${updated} products with primary_item_number`);
    db.close();
  });
});

