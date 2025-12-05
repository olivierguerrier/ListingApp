const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('./amazon_vendor_central.db');

console.log('Fixing product_skus foreign key references...\n');

db.serialize(() => {
  // Get all unique product_id values from product_skus
  db.all(`SELECT DISTINCT product_id FROM product_skus`, [], (err, rows) => {
    if (err) {
      console.error('Error:', err.message);
      db.close();
      return;
    }
    
    console.log(`Found ${rows.length} unique product_id values in product_skus`);
    
    // For each product_id (which should be an ASIN), find the numeric ID from products table
    const updateStmt = db.prepare(`
      UPDATE product_skus
      SET product_id = (SELECT id FROM products WHERE asin = ?)
      WHERE product_id = ?
    `);
    
    let updated = 0;
    let errors = 0;
    
    db.run('BEGIN TRANSACTION');
    
    rows.forEach((row, index) => {
      const oldProductId = row.product_id;
      
      // Check if this looks like an ASIN or if it needs updating
      db.get('SELECT id, asin FROM products WHERE asin = ? OR id = ?', [oldProductId, oldProductId], (err, product) => {
        if (err) {
          console.error(`Error finding product for ${oldProductId}:`, err.message);
          errors++;
        } else if (product) {
          updateStmt.run([product.asin, oldProductId], function(err) {
            if (err) {
              console.error(`Error updating SKUs for ${oldProductId}:`, err.message);
              errors++;
            } else if (this.changes > 0) {
              updated += this.changes;
              if (index < 5) {
                console.log(`  Updated ${this.changes} SKUs: ${oldProductId} (ASIN) → ${product.id} (ID)`);
              }
            }
          });
        }
        
        // When done with all, commit
        if (index === rows.length - 1) {
          setTimeout(() => {
            updateStmt.finalize(() => {
              db.run('COMMIT', (err) => {
                if (err) {
                  console.error('Error committing:', err.message);
                  db.run('ROLLBACK');
                } else {
                  console.log(`\n✓ Updated ${updated} product_skus records`);
                  console.log(`Errors: ${errors}`);
                }
                db.close();
              });
            });
          }, 1000);
        }
      });
    });
  });
});

