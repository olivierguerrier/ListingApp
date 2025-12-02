const fs = require('fs');
const csv = require('csv-parser');
const xlsx = require('xlsx');
const duckdb = require('duckdb');
const path = require('path');

class SyncScheduler {
  constructor(db) {
    this.db = db;
    this.syncInterval = null;
  }

  // Sync QPI data
  async syncQPI() {
    return new Promise((resolve) => {
      console.log('[SYNC] Starting QPI sync...');
      const qpiPath = 'A:\\ProcessOutput\\QPI_Validation\\QPI_validation_full.csv';

      if (!fs.existsSync(qpiPath)) {
        console.log('[SYNC] QPI file not found, skipping...');
        resolve({ success: false, error: 'File not found' });
        return;
      }

      const qpiSkus = new Set();

      fs.createReadStream(qpiPath)
        .pipe(csv())
        .on('data', (data) => {
          const sku = data['Item no'];
          const asin = data['ASIN'];
          if (sku) {
            qpiSkus.add(sku);
            
            // Update ASIN if available and item exists
            if (asin) {
              this.db.run(
                'UPDATE items SET asin = ? WHERE sku = ? AND (asin IS NULL OR asin = "")', 
                [asin, sku]
              );
            }
          }
        })
        .on('end', () => {
          // Mark all items in QPI as order_received = 1 and stage_5_product_ordered = 1
          const skuList = Array.from(qpiSkus);
          const placeholders = skuList.map(() => '?').join(',');
          
          this.db.run(
            `UPDATE items 
             SET order_received = 1, 
                 stage_5_product_ordered = 1, 
                 updated_at = CURRENT_TIMESTAMP 
             WHERE sku IN (${placeholders})`,
            skuList,
            (err) => {
              if (err) {
                console.error('[SYNC] QPI Error:', err.message);
                resolve({ success: false, error: err.message });
              } else {
                console.log(`[SYNC] QPI Complete: ${qpiSkus.size} items updated`);
                resolve({ success: true, updated: qpiSkus.size });
              }
            }
          );
        })
        .on('error', (err) => {
          console.error('[SYNC] QPI Read Error:', err.message);
          resolve({ success: false, error: err.message });
        });
    });
  }

  // Sync VC Extract data
  async syncVC() {
    return new Promise((resolve) => {
      console.log('[SYNC] Starting VC Extract sync...');
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
            summaries_0_status_0 as status
          FROM read_parquet('${latestFile}')
          WHERE sku IS NOT NULL
        `;

        duckDb.all(query, (err, rows) => {
          if (err) {
            console.error('[SYNC] VC Query Error:', err.message);
            duckDb.close();
            resolve({ success: false, error: err.message });
            return;
          }

          let updated = 0;
          let processed = 0;

          rows.forEach(row => {
            const sku = row.sku;
            const asin = row.asin;
            const vcSetup = row.status ? 1 : 0;

            this.db.run(
              `UPDATE items 
               SET vendor_central_setup = ?, 
                   asin = COALESCE(NULLIF(asin, ''), ?),
                   stage_4_product_listed = 1,
                   updated_at = CURRENT_TIMESTAMP
               WHERE sku = ?`,
              [vcSetup, asin, sku],
              function(err) {
                if (err) {
                  console.error(`[SYNC] VC Update Error for ${sku}:`, err.message);
                } else if (this.changes > 0) {
                  updated++;
                }
                processed++;

                if (processed === rows.length) {
                  duckDb.close();
                  console.log(`[SYNC] VC Complete: ${updated} items updated from ${rows.length} total`);
                  resolve({ success: true, updated, total: rows.length, file: files[0] });
                }
              }
            );
          });
        });

      } catch (error) {
        console.error('[SYNC] VC Error:', error.message);
        resolve({ success: false, error: error.message });
      }
    });
  }

  // Sync PIM Extract data
  async syncPIM() {
    return new Promise((resolve) => {
      console.log('[SYNC] Starting PIM Extract sync...');
      const pimPath = 'A:\\Code\\InputFiles\\PIM Extract.xlsx';

      if (!fs.existsSync(pimPath)) {
        console.log('[SYNC] PIM file not found, skipping...');
        resolve({ success: false, error: 'File not found' });
        return;
      }

      try {
        const workbook = xlsx.readFile(pimPath);
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        const data = xlsx.utils.sheet_to_json(worksheet);

        let updated = 0;
        let processed = 0;
        const totalRows = data.length;

        data.forEach((row) => {
          const itemNumber = row['Item Number'];
          
          if (!itemNumber) {
            processed++;
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
            case_pack: row['Case Pack'] || null,
            package_length_cm: row['Single Package Size - Length (cm)'] || null,
            package_width_cm: row['Single Package Size - Width (cm)'] || null,
            package_height_cm: row['Single Package Size - Height (cm)'] || null,
            package_weight_kg: row['Single Package Size - Weight (kg)'] || null
          };

          const nameToUse = updateData.legal_name || itemNumber;
          const stage2Finalized = (updateData.product_dev_status && 
                                   updateData.product_dev_status.toLowerCase() === 'finalized') ? 1 : 0;

          this.db.run(
            `UPDATE items 
             SET name = ?,
                 legal_name = ?,
                 upc_number = ?,
                 brand = ?,
                 age_grade = ?,
                 product_description = ?,
                 pim_spec_status = ?,
                 product_dev_status = ?,
                 case_pack = ?,
                 package_length_cm = ?,
                 package_width_cm = ?,
                 package_height_cm = ?,
                 package_weight_kg = ?,
                 stage_2_product_finalized = ?,
                 updated_at = CURRENT_TIMESTAMP
             WHERE sku = ?`,
            [
              nameToUse,
              updateData.legal_name,
              updateData.upc_number,
              updateData.brand,
              updateData.age_grade,
              updateData.product_description,
              updateData.pim_spec_status,
              updateData.product_dev_status,
              updateData.case_pack,
              updateData.package_length_cm,
              updateData.package_width_cm,
              updateData.package_height_cm,
              updateData.package_weight_kg,
              stage2Finalized,
              itemNumber
            ],
            function(err) {
              if (err) {
                console.error(`[SYNC] PIM Update Error for ${itemNumber}:`, err.message);
              } else if (this.changes > 0) {
                updated++;
              }
              processed++;

              if (processed === totalRows) {
                console.log(`[SYNC] PIM Complete: ${updated} items updated from ${totalRows} total`);
                resolve({ success: true, updated, total: totalRows });
              }
            }
          );
        });

      } catch (error) {
        console.error('[SYNC] PIM Error:', error.message);
        resolve({ success: false, error: error.message });
      }
    });
  }

  // Run all syncs
  async syncAll() {
    console.log('\n' + '='.repeat(60));
    console.log('STARTING AUTOMATIC DATA SYNC');
    console.log('='.repeat(60));
    console.log('Timestamp:', new Date().toISOString());
    
    const results = {
      timestamp: new Date().toISOString(),
      qpi: await this.syncQPI(),
      vc: await this.syncVC(),
      pim: await this.syncPIM()
    };

    console.log('='.repeat(60));
    console.log('SYNC COMPLETE');
    console.log('='.repeat(60) + '\n');

    return results;
  }

  // Start daily sync schedule (runs at 2 AM every day)
  startDailySync() {
    // Calculate time until next 2 AM
    const now = new Date();
    const next2AM = new Date();
    next2AM.setHours(2, 0, 0, 0);
    
    // If it's already past 2 AM today, schedule for tomorrow
    if (now > next2AM) {
      next2AM.setDate(next2AM.getDate() + 1);
    }

    const timeUntilSync = next2AM - now;
    
    console.log(`[SCHEDULER] Daily sync scheduled for ${next2AM.toLocaleString()}`);
    
    // Schedule first sync
    setTimeout(() => {
      this.syncAll();
      
      // Then run every 24 hours
      this.syncInterval = setInterval(() => {
        this.syncAll();
      }, 24 * 60 * 60 * 1000); // 24 hours
      
    }, timeUntilSync);
  }

  // Stop the scheduler
  stop() {
    if (this.syncInterval) {
      clearInterval(this.syncInterval);
      console.log('[SCHEDULER] Daily sync stopped');
    }
  }
}

module.exports = SyncScheduler;

