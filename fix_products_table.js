const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('./amazon_vendor_central.db');

console.log('Fixing products table schema...\n');

db.serialize(() => {
  // First, check current structure
  db.all(`PRAGMA table_info(products)`, [], (err, columns) => {
    if (err) {
      console.error('Error getting table info:', err.message);
      db.close();
      return;
    }
    
    const columnNames = columns.map(c => c.name);
    console.log('Current columns:', columnNames.join(', '));
    
    const hasId = columnNames.includes('id');
    const hasPrimaryItemNumber = columnNames.includes('primary_item_number');
    
    console.log(`Has id column: ${hasId}`);
    console.log(`Has primary_item_number column: ${hasPrimaryItemNumber}\n`);
    
    if (hasId && hasPrimaryItemNumber) {
      console.log('✓ All required columns exist!');
      db.close();
      return;
    }
    
    // Need to recreate the table with proper schema
    console.log('Recreating products table with proper schema...');
    
    db.run('BEGIN TRANSACTION');
    
    // Rename old table
    db.run(`ALTER TABLE products RENAME TO products_old`, (err) => {
      if (err) {
        console.error('Error renaming table:', err.message);
        db.run('ROLLBACK');
        db.close();
        return;
      }
      
      // Create new table with correct schema
      db.run(`CREATE TABLE products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        asin TEXT UNIQUE NOT NULL,
        name TEXT,
        is_temp_asin BOOLEAN DEFAULT 0,
        primary_item_number TEXT,
        brand TEXT,
        age_grade TEXT,
        product_description TEXT,
        legal_name TEXT,
        upc_number TEXT,
        pim_spec_status TEXT,
        product_dev_status TEXT,
        package_length_cm REAL,
        package_width_cm REAL,
        package_height_cm REAL,
        package_weight_kg REAL,
        stage_1_idea_considered BOOLEAN DEFAULT 0,
        stage_1_brand TEXT,
        stage_1_description TEXT,
        stage_1_season_launch TEXT,
        stage_1_country TEXT,
        stage_1_item_number TEXT,
        stage_2_product_finalized BOOLEAN DEFAULT 0,
        stage_2_newly_finalized BOOLEAN DEFAULT 0,
        stage_3a_pricing_submitted BOOLEAN DEFAULT 0,
        stage_3b_pricing_approved BOOLEAN DEFAULT 0,
        stage_4_product_listed BOOLEAN DEFAULT 0,
        stage_5_product_ordered BOOLEAN DEFAULT 0,
        stage_6_product_online BOOLEAN DEFAULT 0,
        stage_7_end_of_life BOOLEAN DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`, (err) => {
        if (err) {
          console.error('Error creating new table:', err.message);
          db.run('ROLLBACK');
          db.close();
          return;
        }
        
        // Copy data from old table
        db.run(`
          INSERT INTO products (
            asin, name, legal_name, upc_number, brand, age_grade,
            product_description, pim_spec_status, product_dev_status,
            package_length_cm, package_width_cm, package_height_cm, package_weight_kg,
            stage_1_idea_considered, stage_1_brand, stage_1_description,
            stage_1_season_launch, stage_1_country, stage_1_item_number,
            stage_2_product_finalized, stage_2_newly_finalized,
            stage_3a_pricing_submitted, stage_3b_pricing_approved,
            stage_4_product_listed, stage_5_product_ordered, stage_6_product_online,
            is_temp_asin, created_at, updated_at
          )
          SELECT 
            asin, name, legal_name, upc_number, brand, age_grade,
            product_description, pim_spec_status, product_dev_status,
            package_length_cm, package_width_cm, package_height_cm, package_weight_kg,
            stage_1_idea_considered, stage_1_brand, stage_1_description,
            stage_1_season_launch, stage_1_country, stage_1_item_number,
            stage_2_product_finalized, stage_2_newly_finalized,
            stage_3a_pricing_submitted, stage_3b_pricing_approved,
            stage_4_product_listed, stage_5_product_ordered, stage_6_product_online,
            is_temp_asin, created_at, updated_at
          FROM products_old
        `, (err) => {
          if (err) {
            console.error('Error copying data:', err.message);
            db.run('ROLLBACK');
            db.close();
            return;
          }
          
          // Drop old table
          db.run('DROP TABLE products_old', (err) => {
            if (err) {
              console.error('Error dropping old table:', err.message);
              db.run('ROLLBACK');
              db.close();
              return;
            }
            
            // Update product_skus foreign keys
            db.all('SELECT * FROM product_skus', [], (err, skus) => {
              if (err) {
                console.error('Error reading product_skus:', err.message);
                db.run('ROLLBACK');
                db.close();
                return;
              }
              
              console.log(`Found ${skus.length} SKU records to update`);
              
              // We need to update product_id references from ASIN to new ID
              const updateStmt = db.prepare(`
                UPDATE product_skus 
                SET product_id = (SELECT id FROM products WHERE asin = ?)
                WHERE id = ?
              `);
              
              let updated = 0;
              skus.forEach((sku) => {
                // Get ASIN for this product_id (which is currently an old ID or ASIN string)
                db.get('SELECT asin FROM products WHERE id = ? OR asin = ?', [sku.product_id, sku.product_id], (err, product) => {
                  if (product) {
                    updateStmt.run([product.asin, sku.id], (err) => {
                      if (!err) updated++;
                    });
                  }
                });
              });
              
              updateStmt.finalize(() => {
                console.log(`Updated ${updated} product_skus records`);
                
                db.run('COMMIT', (err) => {
                  if (err) {
                    console.error('Error committing:', err.message);
                    db.run('ROLLBACK');
                  } else {
                    console.log('\n✓ Successfully recreated products table!');
                  }
                  db.close();
                });
              });
            });
          });
        });
      });
    });
  });
});

