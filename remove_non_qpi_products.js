const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('./database.db');

console.log('Finding and removing products not in QPI data...\n');

// Step 1: Get all ASINs that have QPI data (via qpi_file_tracking table)
db.all(`SELECT DISTINCT asin FROM qpi_file_tracking WHERE asin IS NOT NULL`, [], (err, qpiRows) => {
  if (err) {
    console.error('Error getting QPI ASINs:', err.message);
    db.close();
    return;
  }
  
  const qpiAsins = new Set(qpiRows.map(r => r.asin));
  console.log(`Found ${qpiAsins.size} unique ASINs in QPI data`);
  
  // Step 2: Get all products
  db.all(`SELECT id, asin FROM products`, [], (err, allProducts) => {
    if (err) {
      console.error('Error getting all products:', err.message);
      db.close();
      return;
    }
    
    console.log(`Total products in database: ${allProducts.length}`);
    
    // Step 3: Find products NOT in QPI
    const productsToDelete = allProducts.filter(p => !qpiAsins.has(p.asin));
    console.log(`Products to delete (not in QPI): ${productsToDelete.length}\n`);
    
    if (productsToDelete.length === 0) {
      console.log('✓ All products are in QPI data. Nothing to delete.');
      db.close();
      return;
    }
    
    // Show sample of products to be deleted
    console.log('Sample products to delete:');
    productsToDelete.slice(0, 10).forEach(p => {
      console.log(`  - ${p.asin} (ID: ${p.id})`);
    });
    
    if (productsToDelete.length > 10) {
      console.log(`  ... and ${productsToDelete.length - 10} more\n`);
    }
    
    // Step 4: Delete products and their related data
    console.log('Deleting products and related data...');
    
    db.serialize(() => {
      db.run('BEGIN TRANSACTION');
      
      const productIds = productsToDelete.map(p => p.id);
      const placeholders = productIds.map(() => '?').join(',');
      
      // Delete related data first
      db.run(`DELETE FROM product_skus WHERE product_id IN (${placeholders})`, productIds, function(err) {
        if (err) {
          console.error('Error deleting SKUs:', err.message);
        } else {
          console.log(`  Deleted ${this.changes} SKU records`);
        }
      });
      
      db.run(`DELETE FROM product_country_pricing WHERE product_id IN (${placeholders})`, productIds, function(err) {
        if (err) {
          console.error('Error deleting pricing:', err.message);
        } else {
          console.log(`  Deleted ${this.changes} pricing records`);
        }
      });
      
      db.run(`DELETE FROM product_flow_history WHERE product_id IN (${placeholders})`, productIds, function(err) {
        if (err) {
          console.error('Error deleting flow history:', err.message);
        } else {
          console.log(`  Deleted ${this.changes} flow history records`);
        }
      });
      
      // Delete the products themselves
      db.run(`DELETE FROM products WHERE id IN (${placeholders})`, productIds, function(err) {
        if (err) {
          console.error('Error deleting products:', err.message);
          db.run('ROLLBACK');
        } else {
          console.log(`  Deleted ${this.changes} products`);
          
          db.run('COMMIT', (commitErr) => {
            if (commitErr) {
              console.error('Error committing:', commitErr.message);
              db.run('ROLLBACK');
            } else {
              console.log('\n✓ Successfully removed all products not in QPI data');
              
              // Verify final count
              db.get('SELECT COUNT(*) as count FROM products', [], (err, row) => {
                if (!err) {
                  console.log(`\nRemaining products: ${row.count}`);
                  console.log(`Expected (QPI ASINs): ${qpiAsins.size}`);
                }
                db.close();
              });
            }
          });
        }
      });
    });
  });
});

