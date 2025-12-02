const cron = require('node-cron');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const xlsx = require('xlsx');
const duckdb = require('duckdb');

class SyncScheduler {
  constructor(db) {
    this.db = db;
    this.isSyncing = false;
    this.dailyScheduler = null;
  }

  // Helper: Find or create product by ASIN
  findOrCreateProduct(asin, name, callback) {
    if (!asin) {
      // Generate temp ASIN
      this.db.get('SELECT counter FROM temp_asin_counter WHERE id = 1', (err, row) => {
        if (err) {
          callback(err, null);
          return;
        }
        const counter = row ? row.counter : 1;
        const tempAsin = `TEMP${String(counter).padStart(6, '0')}`;
        
        this.db.run('UPDATE temp_asin_counter SET counter = counter + 1 WHERE id = 1', (err) => {
          if (err) {
            callback(err, null);
            return;
          }
          
          this.db.run(
            'INSERT INTO products (asin, name, is_temp_asin) VALUES (?, ?, 1)',
            [tempAsin, name || tempAsin],
            function(err) {
              if (err) {
                callback(err, null);
              } else {
                callback(null, { id: this.lastID, asin: tempAsin, is_temp_asin: true });
              }
            }
          );
        });
      });
    } else {
      // Check if product exists with this ASIN
      this.db.get('SELECT * FROM products WHERE asin = ?', [asin], (err, product) => {
        if (err) {
          callback(err, null);
        } else if (product) {
          callback(null, product);
        } else {
          // Create new product
          this.db.run(
            'INSERT INTO products (asin, name, is_temp_asin) VALUES (?, ?, 0)',
            [asin, name || asin],
            function(err) {
              if (err) {
                callback(err, null);
              } else {
                callback(null, { id: this.lastID, asin: asin, is_temp_asin: false });
              }
            }
          );
        }
      });
    }
  }

  // Helper: Add SKU to product
  addSkuToProduct(productId, sku, isPrimary, source, callback) {
    this.db.run(
      'INSERT OR IGNORE INTO product_skus (product_id, sku, is_primary, source) VALUES (?, ?, ?, ?)',
      [productId, sku, isPrimary ? 1 : 0, source],
      function(err) {
        callback(err, this.changes);
      }
    );
  }

  // Sync VC Extract
  syncVcExtract() {
    return new Promise((resolve) => {
      const vcDir = 'A:\\ProcessOutput\\VC_Extracts\\Comparison_Extracts';

      if (!fs.existsSync(vcDir)) {
        console.log('[SYNC] VC directory not found, skipping...');
        resolve({ success: false, error: 'Directory not found' });
        return;
      }

      try {
        const files = fs.readdirSync(vcDir)
          .filter(f => f.startsWith('vc_extracts_') && f.endsWith('.parquet'))
          .sort()
          .reverse();

        if (files.length === 0) {
          console.log('[SYNC] No VC extract files found, skipping...');
          resolve({ success: false, error: 'No files found' });
          return;
        }

        const latestFile = path.join(vcDir, files[0]).replace(/\\/g, '/');
        console.log(`[SYNC] Reading VC extract: ${files[0]}`);

        const duckDb = new duckdb.Database(':memory:');

        const query = `
          SELECT DISTINCT 
            sku,
            summaries_0_asin as asin,
            summaries_0_itemName as item_name,
            summaries_0_status_0 as status,
            country
          FROM read_parquet('${latestFile}')
          WHERE sku IS NOT NULL AND summaries_0_asin IS NOT NULL
        `;

        duckDb.all(query, (err, rows) => {
          if (err) {
            console.error('[SYNC] VC Query Error:', err.message);
            duckDb.close();
            resolve({ success: false, error: err.message });
            return;
          }

          console.log(`[SYNC] Processing ${rows.length} VC records...`);
          let created = 0;
          let updated = 0;
          let processed = 0;

          if (rows.length === 0) {
            duckDb.close();
            resolve({ success: true, created: 0, updated: 0 });
            return;
          }

          rows.forEach(row => {
            const sku = row.sku;
            const asin = row.asin;
            const itemName = row.item_name;
            const vcListed = (row.status === 'SUCCESS' || row.status === 'COMPLETE') ? 1 : 0;

            this.findOrCreateProduct(asin, itemName, (err, product) => {
              if (err) {
                console.error(`[SYNC] Error creating product ${asin}:`, err.message);
                processed++;
                checkComplete();
                return;
              }

              // Update product with VC data
              this.db.run(
                `UPDATE products 
                 SET name = COALESCE(NULLIF(name, asin), ?),
                     stage_4_product_listed = ?,
                     is_temp_asin = 0,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE id = ?`,
                [itemName || asin, vcListed, product.id],
                (err) => {
                  if (err) {
                    console.error(`[SYNC] Error updating product ${asin}:`, err.message);
                  } else {
                    updated++;
                  }

                  // Add SKU to product
                  this.addSkuToProduct(product.id, sku, true, 'VC', (err) => {
                    if (err && !err.message.includes('UNIQUE constraint')) {
                      console.error(`[SYNC] Error adding SKU ${sku}:`, err.message);
                    }
                    processed++;
                    checkComplete();
                  });
                }
              );
            });
          });

          const checkComplete = () => {
            if (processed === rows.length) {
              duckDb.close();
              console.log(`[SYNC] VC Complete: ${updated} products updated`);
              resolve({ success: true, created, updated });
            }
          };
        });

      } catch (error) {
        console.error('[SYNC] VC Error:', error.message);
        resolve({ success: false, error: error.message });
      }
    });
  }

  // Sync QPI
  syncQpi() {
    return new Promise((resolve) => {
      const qpiPath = 'A:\\ProcessOutput\\QPI_Validation\\QPI_validation_full.csv';

      if (!fs.existsSync(qpiPath)) {
        console.log('[SYNC] QPI file not found, skipping...');
        resolve({ success: false, error: 'File not found' });
        return;
      }

      console.log('[SYNC] Reading QPI file...');
      const qpiRecords = [];

      fs.createReadStream(qpiPath)
        .pipe(csv())
        .on('data', (data) => {
          const sku = data['Item no'];
          const asin = data['ASIN'];
          if (sku || asin) {
            qpiRecords.push({ sku, asin });
          }
        })
        .on('end', () => {
          console.log(`[SYNC] Processing ${qpiRecords.length} QPI records...`);
          let updated = 0;
          let processed = 0;

          if (qpiRecords.length === 0) {
            resolve({ success: true, updated: 0 });
            return;
          }

          qpiRecords.forEach(record => {
            const { sku, asin } = record;

            if (asin) {
              // Find product by ASIN and mark as ordered
              this.db.run(
                `UPDATE products 
                 SET stage_5_product_ordered = 1,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE asin = ?`,
                [asin],
                function(err) {
                  if (err) {
                    console.error(`[SYNC] Error updating product ${asin}:`, err.message);
                  } else if (this.changes > 0) {
                    updated++;
                  }
                  processed++;
                  checkComplete();
                }
              );
            } else if (sku) {
              // Find product by SKU
              this.db.get(
                'SELECT product_id FROM product_skus WHERE sku = ?',
                [sku],
                (err, row) => {
                  if (err || !row) {
                    processed++;
                    checkComplete();
                    return;
                  }

                  this.db.run(
                    `UPDATE products 
                     SET stage_5_product_ordered = 1,
                         updated_at = CURRENT_TIMESTAMP
                     WHERE id = ?`,
                    [row.product_id],
                    function(err) {
                      if (err) {
                        console.error(`[SYNC] Error updating product:`, err.message);
                      } else if (this.changes > 0) {
                        updated++;
                      }
                      processed++;
                      checkComplete();
                    }
                  );
                }
              );
            } else {
              processed++;
              checkComplete();
            }
          });

          const checkComplete = () => {
            if (processed === qpiRecords.length) {
              console.log(`[SYNC] QPI Complete: ${updated} products marked as ordered`);
              resolve({ success: true, updated });
            }
          };
        })
        .on('error', (err) => {
          console.error('[SYNC] QPI Read Error:', err.message);
          resolve({ success: false, error: err.message });
        });
    });
  }

  // Sync PIM Extract
  syncPim() {
    return new Promise((resolve) => {
      const pimPath = 'A:\\Code\\InputFiles\\PIM Extract.xlsx';

      if (!fs.existsSync(pimPath)) {
        console.log('[SYNC] PIM file not found, skipping...');
        resolve({ success: false, error: 'File not found' });
        return;
      }

      try {
        console.log('[SYNC] Reading PIM Extract...');
        const workbook = xlsx.readFile(pimPath);
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        const data = xlsx.utils.sheet_to_json(worksheet);

        console.log(`[SYNC] Processing ${data.length} PIM records...`);
        let updated = 0;
        let processed = 0;

        if (data.length === 0) {
          resolve({ success: true, updated: 0 });
          return;
        }

        data.forEach((row) => {
          const itemNumber = row['Item Number'];
          if (!itemNumber) {
            processed++;
            checkComplete();
            return;
          }

          const updateData = {
            legal_name: row['Legal Name'] || null,
            upc_number: row['UPC Number'] || null,
            brand: row['Brand (Product Line)'] || null,
            age_grade: row['Age Grade'] || null,
            product_description: row['Product Description (internal)'] || null,
            pim_spec_status: row['Item Spec Sheet Status'] || null,
            product_dev_status: row['Product Development Status'] || null,
            package_length_cm: row['Single Package Size - Length (cm)'] || null,
            package_width_cm: row['Single Package Size - Width (cm)'] || null,
            package_height_cm: row['Single Package Size - Height (cm)'] || null,
            package_weight_kg: row['Single Package Size - Weight (kg)'] || null
          };

          const nameToUse = updateData.legal_name || itemNumber;
          const productFinalized = (updateData.product_dev_status === 'Finalized') ? 1 : 0;

          // Find product by SKU
          this.db.get(
            'SELECT ps.product_id, p.stage_2_product_finalized FROM product_skus ps JOIN products p ON ps.product_id = p.id WHERE ps.sku = ?',
            [itemNumber],
            (err, row) => {
              if (err || !row) {
                processed++;
                checkComplete();
                return;
              }

              const wasNotFinalized = row.stage_2_product_finalized === 0;
              const newlyFinalized = wasNotFinalized && productFinalized === 1 ? 1 : 0;

              this.db.run(
                `UPDATE products 
                 SET name = ?,
                     legal_name = ?,
                     upc_number = ?,
                     brand = ?,
                     age_grade = ?,
                     product_description = ?,
                     pim_spec_status = ?,
                     product_dev_status = ?,
                     package_length_cm = ?,
                     package_width_cm = ?,
                     package_height_cm = ?,
                     package_weight_kg = ?,
                     stage_2_product_finalized = ?,
                     stage_2_newly_finalized = ?,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE id = ?`,
                [
                  nameToUse,
                  updateData.legal_name,
                  updateData.upc_number,
                  updateData.brand,
                  updateData.age_grade,
                  updateData.product_description,
                  updateData.pim_spec_status,
                  updateData.product_dev_status,
                  updateData.package_length_cm,
                  updateData.package_width_cm,
                  updateData.package_height_cm,
                  updateData.package_weight_kg,
                  productFinalized,
                  newlyFinalized,
                  row.product_id
                ],
                function(err) {
                  if (err) {
                    console.error(`[SYNC] Error updating PIM for ${itemNumber}:`, err.message);
                  } else if (this.changes > 0) {
                    updated++;
                  }
                  processed++;
                  checkComplete();
                }
              );
            }
          );
        });

        const checkComplete = () => {
          if (processed === data.length) {
            console.log(`[SYNC] PIM Complete: ${updated} products updated`);
            resolve({ success: true, updated });
          }
        };

      } catch (error) {
        console.error('[SYNC] PIM Error:', error.message);
        resolve({ success: false, error: error.message });
      }
    });
  }

  // Run all syncs
  async syncAll() {
    if (this.isSyncing) {
      console.log('[SYNC] Already syncing, skipping...');
      return;
    }

    this.isSyncing = true;
    console.log('\n============================================================');
    console.log('STARTING DATA SYNC');
    console.log('============================================================');
    console.log('Timestamp:', new Date().toISOString());

    try {
      console.log('\n[SYNC] Starting VC Extract sync...');
      await this.syncVcExtract();
      
      console.log('\n[SYNC] Starting QPI sync...');
      await this.syncQpi();
      
      console.log('\n[SYNC] Starting PIM sync...');
      await this.syncPim();
      
    } catch (error) {
      console.error('[SYNC] Error during sync:', error.message);
    } finally {
      this.isSyncing = false;
      console.log('============================================================');
      console.log('SYNC COMPLETE');
      console.log('============================================================\n');
    }
  }

  // Start daily sync schedule
  startDailySync() {
    if (this.dailyScheduler) {
      return;
    }

    this.dailyScheduler = cron.schedule('0 2 * * *', () => {
      console.log('[SCHEDULER] Running daily sync...');
      this.syncAll();
    }, {
      scheduled: true,
      timezone: "America/New_York"
    });

    console.log('[SCHEDULER] Daily sync scheduled for 2:00 AM');
  }

  // Stop scheduler
  stop() {
    if (this.dailyScheduler) {
      this.dailyScheduler.stop();
      console.log('[SCHEDULER] Stopped');
    }
  }
}

module.exports = SyncScheduler;
