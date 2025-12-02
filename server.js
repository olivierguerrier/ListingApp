const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');
const csv = require('csv-parser');
const duckdb = require('duckdb');
const xlsx = require('xlsx');
const SyncScheduler = require('./syncScheduler');

const app = express();
const PORT = process.env.PORT || 7777;

let syncScheduler; // Global sync scheduler instance

// Middleware
app.use(cors());
app.use(bodyParser.json());
app.use(express.static('public'));

// Database connection
const db = new sqlite3.Database('./database.db', (err) => {
  if (err) {
    console.error('Error opening database:', err.message);
  } else {
    console.log('Connected to SQLite database');
    initializeDatabase();
  }
});

// Initialize database tables
function initializeDatabase() {
  db.serialize(() => {
    // Items table
    db.run(`CREATE TABLE IF NOT EXISTS items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sku TEXT UNIQUE NOT NULL,
      asin TEXT,
      name TEXT NOT NULL,
      dimensions TEXT,
      case_pack INTEGER,
      sioc_status TEXT,
      vendor_central_setup BOOLEAN DEFAULT 0,
      btr_submission BOOLEAN DEFAULT 0,
      btr_optional BOOLEAN DEFAULT 0,
      order_received BOOLEAN DEFAULT 0,
      order_shipped BOOLEAN DEFAULT 0,
      online_available BOOLEAN DEFAULT 0,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    // Migration: Add asin column if it doesn't exist
    db.run(`SELECT sql FROM sqlite_master WHERE name='items'`, (err, row) => {
      if (!err) {
        db.run(`PRAGMA table_info(items)`, [], (err, rows) => {
          if (!err) {
            db.all(`PRAGMA table_info(items)`, [], (err, columns) => {
              if (!err) {
                const hasAsin = columns.some(col => col.name === 'asin');
                if (!hasAsin) {
                  console.log('Adding asin column to items table...');
                  db.run(`ALTER TABLE items ADD COLUMN asin TEXT`, (err) => {
                    if (err) {
                      console.error('Error adding asin column:', err.message);
                    } else {
                      console.log('ASIN column added successfully');
                    }
                  });
                }
                
                // Migration: Add PIM-related columns
                const pimColumns = [
                  { name: 'legal_name', type: 'TEXT' },
                  { name: 'upc_number', type: 'TEXT' },
                  { name: 'brand', type: 'TEXT' },
                  { name: 'age_grade', type: 'TEXT' },
                  { name: 'product_description', type: 'TEXT' },
                  { name: 'pim_spec_status', type: 'TEXT' },
                  { name: 'product_dev_status', type: 'TEXT' },
                  { name: 'package_length_cm', type: 'REAL' },
                  { name: 'package_width_cm', type: 'REAL' },
                  { name: 'package_height_cm', type: 'REAL' },
                  { name: 'package_weight_kg', type: 'REAL' }
                ];
                
                pimColumns.forEach(col => {
                  const hasColumn = columns.some(c => c.name === col.name);
                  if (!hasColumn) {
                    db.run(`ALTER TABLE items ADD COLUMN ${col.name} ${col.type}`, (err) => {
                      if (err && !err.message.includes('duplicate column')) {
                        console.error(`Error adding ${col.name} column:`, err.message);
                      }
                    });
                  }
                });
                
                // Migration: Add new flow stage columns
                const flowColumns = [
                  { name: 'stage_1_idea_considered', type: 'BOOLEAN DEFAULT 0' },
                  { name: 'stage_1_brand', type: 'TEXT' },
                  { name: 'stage_1_description', type: 'TEXT' },
                  { name: 'stage_1_season_launch', type: 'TEXT' },
                  { name: 'stage_2_product_finalized', type: 'BOOLEAN DEFAULT 0' },
                  { name: 'stage_3a_pricing_submitted', type: 'BOOLEAN DEFAULT 0' },
                  { name: 'stage_3b_pricing_approved', type: 'BOOLEAN DEFAULT 0' },
                  { name: 'stage_4_product_listed', type: 'BOOLEAN DEFAULT 0' },
                  { name: 'stage_5_product_ordered', type: 'BOOLEAN DEFAULT 0' },
                  { name: 'stage_6_product_online', type: 'BOOLEAN DEFAULT 0' }
                ];
                
                flowColumns.forEach(col => {
                  const hasColumn = columns.some(c => c.name === col.name);
                  if (!hasColumn) {
                    db.run(`ALTER TABLE items ADD COLUMN ${col.name} ${col.type}`, (err) => {
                      if (err && !err.message.includes('duplicate column')) {
                        console.error(`Error adding ${col.name} column:`, err.message);
                      }
                    });
                  }
                });
              }
            });
          }
        });
      }
    });

    // Country pricing table
    db.run(`CREATE TABLE IF NOT EXISTS item_country_pricing (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      item_id INTEGER NOT NULL,
      country_code TEXT NOT NULL,
      retail_price DECIMAL(10, 2),
      sell_price DECIMAL(10, 2),
      currency TEXT NOT NULL,
      approval_status TEXT DEFAULT 'pending',
      approved_at DATETIME,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (item_id) REFERENCES items(id) ON DELETE CASCADE,
      UNIQUE(item_id, country_code)
    )`);

    // Flow stage history table
    db.run(`CREATE TABLE IF NOT EXISTS flow_stage_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      item_id INTEGER NOT NULL,
      stage_name TEXT NOT NULL,
      completed BOOLEAN DEFAULT 0,
      completed_at DATETIME,
      notes TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (item_id) REFERENCES items(id) ON DELETE CASCADE
    )`);

    console.log('Database tables initialized');
    
    // Initialize and start sync scheduler
    initializeSyncScheduler();
  });
}

// Initialize sync scheduler
function initializeSyncScheduler() {
  syncScheduler = new SyncScheduler(db);
  
  // Run initial sync on startup
  console.log('\n[STARTUP] Running initial data sync...');
  syncScheduler.syncAll().then(() => {
    console.log('[STARTUP] Initial sync complete\n');
    
    // Start daily sync schedule
    syncScheduler.startDailySync();
  });
}

// ============= API ENDPOINTS =============

// Get all items with their pricing status
app.get('/api/items', (req, res) => {
  const query = `
    SELECT 
      i.*,
      GROUP_CONCAT(
        json_object(
          'country', icp.country_code,
          'retail_price', icp.retail_price,
          'sell_price', icp.sell_price,
          'currency', icp.currency,
          'approval_status', icp.approval_status
        )
      ) as pricing_data
    FROM items i
    LEFT JOIN item_country_pricing icp ON i.id = icp.item_id
    GROUP BY i.id
    ORDER BY i.created_at DESC
  `;

  db.all(query, [], (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    
    // Parse pricing data JSON
    const items = rows.map(row => ({
      ...row,
      pricing_data: row.pricing_data ? JSON.parse(`[${row.pricing_data}]`) : []
    }));
    
    res.json(items);
  });
});

// Get single item with full details
app.get('/api/items/:id', (req, res) => {
  const itemId = req.params.id;

  db.get('SELECT * FROM items WHERE id = ?', [itemId], (err, item) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    if (!item) {
      res.status(404).json({ error: 'Item not found' });
      return;
    }

    // Get pricing data
    db.all(
      'SELECT * FROM item_country_pricing WHERE item_id = ?',
      [itemId],
      (err, pricing) => {
        if (err) {
          res.status(500).json({ error: err.message });
          return;
        }

        res.json({
          ...item,
          pricing: pricing
        });
      }
    );
  });
});

// Create new item
app.post('/api/items', (req, res) => {
  const { sku, asin, name, dimensions, case_pack, sioc_status, btr_optional } = req.body;

  if (!sku || !name) {
    res.status(400).json({ error: 'SKU and name are required' });
    return;
  }

  const query = `
    INSERT INTO items (sku, asin, name, dimensions, case_pack, sioc_status, btr_optional)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `;

  db.run(query, [sku, asin, name, dimensions, case_pack, sioc_status, btr_optional || 0], function(err) {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    res.json({ id: this.lastID, message: 'Item created successfully' });
  });
});

// Update item
app.put('/api/items/:id', (req, res) => {
  const itemId = req.params.id;
  const { asin, name, dimensions, case_pack, sioc_status, btr_optional } = req.body;

  const query = `
    UPDATE items 
    SET asin = ?, name = ?, dimensions = ?, case_pack = ?, sioc_status = ?, 
        btr_optional = ?, updated_at = CURRENT_TIMESTAMP
    WHERE id = ?
  `;

  db.run(query, [asin, name, dimensions, case_pack, sioc_status, btr_optional, itemId], function(err) {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    res.json({ message: 'Item updated successfully' });
  });
});

// Delete item
app.delete('/api/items/:id', (req, res) => {
  const itemId = req.params.id;

  db.run('DELETE FROM items WHERE id = ?', [itemId], function(err) {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    res.json({ message: 'Item deleted successfully' });
  });
});

// Add or update country pricing
app.post('/api/items/:id/pricing', (req, res) => {
  const itemId = req.params.id;
  const { country_code, retail_price, sell_price, currency } = req.body;

  if (!country_code || !currency) {
    res.status(400).json({ error: 'Country code and currency are required' });
    return;
  }

  const query = `
    INSERT INTO item_country_pricing (item_id, country_code, retail_price, sell_price, currency)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(item_id, country_code) 
    DO UPDATE SET 
      retail_price = excluded.retail_price,
      sell_price = excluded.sell_price,
      currency = excluded.currency,
      updated_at = CURRENT_TIMESTAMP
  `;

  db.run(query, [itemId, country_code, retail_price, sell_price, currency], function(err) {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    
    // Mark Stage 3a (Pricing Submitted) for this item
    db.run(
      'UPDATE items SET stage_3a_pricing_submitted = 1 WHERE id = ?',
      [itemId],
      (err) => {
        if (err) {
          console.error('Error updating stage 3a:', err.message);
        }
      }
    );
    
    res.json({ message: 'Pricing updated successfully' });
  });
});

// Approve pricing for a country
app.put('/api/items/:id/pricing/:country/approve', (req, res) => {
  const { id, country } = req.params;

  const query = `
    UPDATE item_country_pricing 
    SET approval_status = 'approved', 
        approved_at = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP
    WHERE item_id = ? AND country_code = ?
  `;

  db.run(query, [id, country], function(err) {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    
    // Check if all pricings for this item are approved
    db.get(
      'SELECT COUNT(*) as total, SUM(CASE WHEN approval_status = "approved" THEN 1 ELSE 0 END) as approved FROM item_country_pricing WHERE item_id = ?',
      [id],
      (err, row) => {
        if (!err && row.total > 0 && row.total === row.approved) {
          // All pricings approved, mark Stage 3b
          db.run('UPDATE items SET stage_3b_pricing_approved = 1 WHERE id = ?', [id]);
        }
      }
    );
    
    res.json({ message: 'Pricing approved successfully' });
  });
});

// Reject pricing for a country
app.put('/api/items/:id/pricing/:country/reject', (req, res) => {
  const { id, country } = req.params;

  const query = `
    UPDATE item_country_pricing 
    SET approval_status = 'rejected',
        updated_at = CURRENT_TIMESTAMP
    WHERE item_id = ? AND country_code = ?
  `;

  db.run(query, [id, country], function(err) {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    res.json({ message: 'Pricing rejected' });
  });
});

// Update Stage 1 (Idea Considered)
app.put('/api/items/:id/stage1', (req, res) => {
  const itemId = req.params.id;
  const { brand, description, season_launch } = req.body;

  const query = `
    UPDATE items 
    SET stage_1_idea_considered = 1,
        stage_1_brand = ?,
        stage_1_description = ?,
        stage_1_season_launch = ?,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = ?
  `;

  db.run(query, [brand, description, season_launch, itemId], function(err) {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    res.json({ message: 'Stage 1 updated successfully' });
  });
});

// Update flow stage
app.put('/api/items/:id/stage', (req, res) => {
  const itemId = req.params.id;
  const { stage, completed } = req.body;

  if (!stage) {
    res.status(400).json({ error: 'Stage name is required' });
    return;
  }

  const stageColumn = stage.toLowerCase().replace(/ /g, '_');
  const query = `UPDATE items SET ${stageColumn} = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`;

  db.run(query, [completed ? 1 : 0, itemId], function(err) {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }

    // Log in history
    if (completed) {
      db.run(
        'INSERT INTO flow_stage_history (item_id, stage_name, completed, completed_at) VALUES (?, ?, 1, CURRENT_TIMESTAMP)',
        [itemId, stage]
      );
    }

    res.json({ message: 'Stage updated successfully' });
  });
});

// Get flow stage history for an item
app.get('/api/items/:id/history', (req, res) => {
  const itemId = req.params.id;

  db.all(
    'SELECT * FROM flow_stage_history WHERE item_id = ? ORDER BY completed_at DESC',
    [itemId],
    (err, rows) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
      res.json(rows);
    }
  );
});

// Import items from QPI CSV
app.post('/api/import/qpi', (req, res) => {
  const qpiPath = 'A:\\ProcessOutput\\QPI_Validation\\QPI_validation_full.csv';

  if (!fs.existsSync(qpiPath)) {
    res.status(404).json({ error: 'QPI CSV file not found' });
    return;
  }

  const results = [];
  let imported = 0;
  let skipped = 0;
  let errors = 0;

  fs.createReadStream(qpiPath)
    .pipe(csv())
    .on('data', (data) => {
      const sku = data['Item no'];
      const asin = data['ASIN'];
      const name = data['Description'];
      const sioc_status = data['SIOC'] ? data['SIOC'].trim() : null;

      if (sku && name) {
        results.push({ sku, asin, name, sioc_status });
      }
    })
    .on('end', () => {
      // Insert items into database
      const insertStmt = db.prepare(`
        INSERT OR IGNORE INTO items (sku, asin, name, sioc_status, order_received)
        VALUES (?, ?, ?, ?, 1)
      `);

      results.forEach(item => {
        insertStmt.run([item.sku, item.asin, item.name, item.sioc_status], function(err) {
          if (err) {
            errors++;
          } else if (this.changes > 0) {
            imported++;
          } else {
            skipped++;
          }
        });
      });

      insertStmt.finalize(() => {
        res.json({
          message: 'QPI import completed',
          total: results.length,
          imported: imported,
          skipped: skipped,
          errors: errors
        });
      });
    })
    .on('error', (err) => {
      res.status(500).json({ error: 'Error reading CSV file: ' + err.message });
    });
});

// Sync order received status from QPI CSV
app.post('/api/sync/qpi', (req, res) => {
  const qpiPath = 'A:\\ProcessOutput\\QPI_Validation\\QPI_validation_full.csv';

  if (!fs.existsSync(qpiPath)) {
    res.status(404).json({ error: 'QPI CSV file not found' });
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
          db.run('UPDATE items SET asin = ? WHERE sku = ? AND (asin IS NULL OR asin = "")', [asin, sku]);
        }
      }
    })
    .on('end', () => {
      // Mark all items in QPI as order_received = 1 and stage_5_product_ordered = 1
      const skuList = Array.from(qpiSkus);
      const placeholders = skuList.map(() => '?').join(',');
      
      db.run(
        `UPDATE items 
         SET order_received = 1, 
             stage_5_product_ordered = 1, 
             updated_at = CURRENT_TIMESTAMP 
         WHERE sku IN (${placeholders})`,
        skuList,
        function(err) {
          if (err) {
            res.status(500).json({ error: err.message });
            return;
          }
          res.json({
            message: 'QPI sync completed',
            updated: this.changes,
            total_in_qpi: qpiSkus.size
          });
        }
      );
    })
    .on('error', (err) => {
      res.status(500).json({ error: 'Error reading CSV file: ' + err.message });
    });
});

// Sync Vendor Central status from parquet extract
app.post('/api/sync/vc', (req, res) => {
  const vcDir = 'A:\\ProcessOutput\\VC_Extracts\\Comparison_Extracts';
  
  try {
    // Find most recent VC extract file
    const files = fs.readdirSync(vcDir)
      .filter(f => f.startsWith('vc_extracts_') && f.endsWith('.parquet'))
      .sort()
      .reverse();
    
    if (files.length === 0) {
      res.status(404).json({ error: 'No VC extract files found' });
      return;
    }

    const latestFile = path.join(vcDir, files[0]).replace(/\\/g, '/');
    console.log('Reading VC extract:', latestFile);

    const duckDb = new duckdb.Database(':memory:');
    
    const query = `
      SELECT DISTINCT 
        sku,
        summaries_0_asin as asin,
        summaries_0_status_0 as status,
        country
      FROM '${latestFile}'
      WHERE sku IS NOT NULL AND sku != ''
    `;

    duckDb.all(query, (err, rows) => {
      if (err) {
        res.status(500).json({ error: 'Error reading parquet file: ' + err.message });
        duckDb.close();
        return;
      }

      console.log(`Found ${rows.length} SKUs in VC extract`);

      let updated = 0;
      let errors = 0;

      // Process each row
      rows.forEach(row => {
        const sku = row.sku;
        const asin = row.asin;
        const status = row.status;

        // Update item - mark as vendor_central_setup if it has a status
        const vcSetup = status ? 1 : 0;
        
        db.run(
          `UPDATE items 
           SET vendor_central_setup = ?, 
               asin = COALESCE(NULLIF(asin, ''), ?),
               stage_4_product_listed = 1,
               updated_at = CURRENT_TIMESTAMP
           WHERE sku = ?`,
          [vcSetup, asin, sku],
          function(err) {
            if (err) {
              errors++;
              console.error(`Error updating ${sku}:`, err.message);
            } else if (this.changes > 0) {
              updated++;
            }
          }
        );
      });

      // Give it a moment to finish all updates
      setTimeout(() => {
        duckDb.close();
        res.json({
          message: 'VC sync completed',
          file: files[0],
          total_in_vc: rows.length,
          updated: updated,
          errors: errors
        });
      }, 1000);
    });

  } catch (error) {
    res.status(500).json({ error: 'Error accessing VC extract: ' + error.message });
  }
});

// Sync PIM Extract data
app.post('/api/sync/pim', (req, res) => {
  const pimPath = 'A:\\Code\\InputFiles\\PIM Extract.xlsx';

  if (!fs.existsSync(pimPath)) {
    res.status(404).json({ error: 'PIM Extract file not found' });
    return;
  }

  try {
    console.log('Reading PIM Extract...');
    const workbook = xlsx.readFile(pimPath);
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const data = xlsx.utils.sheet_to_json(worksheet);

    let updated = 0;
    let notFound = 0;
    let errors = 0;

    data.forEach((row) => {
      const itemNumber = row['Item Number'];
      
      if (!itemNumber) {
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

      // Update item with name from Legal Name if available
      const nameToUse = updateData.legal_name || itemNumber;
      
      // Check if Stage 2 should be marked (Product Finalized)
      const stage2Finalized = (updateData.product_dev_status && 
                               updateData.product_dev_status.toLowerCase() === 'finalized') ? 1 : 0;

      db.run(
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
            errors++;
            console.error(`Error updating ${itemNumber}:`, err.message);
          } else if (this.changes > 0) {
            updated++;
          } else {
            notFound++;
          }
        }
      );
    });

    // Give it a moment to finish all updates
    setTimeout(() => {
      res.json({
        message: 'PIM sync completed',
        total_in_pim: data.length,
        updated: updated,
        not_found: notFound,
        errors: errors
      });
    }, 1000);

  } catch (error) {
    res.status(500).json({ error: 'Error reading PIM Extract: ' + error.message });
  }
});

// Export item list (SKUs and ASINs)
app.get('/api/export/items', (req, res) => {
  const format = req.query.format || 'json'; // json, csv, or txt
  const fields = req.query.fields || 'sku,asin'; // Which fields to export
  
  const query = `
    SELECT 
      sku,
      asin,
      name,
      vendor_central_setup,
      order_received,
      online_available
    FROM items
    ORDER BY sku
  `;
  
  db.all(query, [], (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }

    const fieldList = fields.split(',').map(f => f.trim());

    if (format === 'csv') {
      // CSV format
      const headers = fieldList.join(',');
      const csvRows = rows.map(row => {
        return fieldList.map(field => {
          const value = row[field] || '';
          // Escape commas and quotes in CSV
          return `"${String(value).replace(/"/g, '""')}"`;
        }).join(',');
      });
      
      const csvContent = [headers, ...csvRows].join('\n');
      
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', 'attachment; filename="items_export.csv"');
      res.send(csvContent);
      
    } else if (format === 'txt') {
      // Plain text format (one per line)
      const txtRows = rows.map(row => {
        return fieldList.map(field => row[field] || '').join('\t');
      });
      
      const txtContent = txtRows.join('\n');
      
      res.setHeader('Content-Type', 'text/plain');
      res.setHeader('Content-Disposition', 'attachment; filename="items_export.txt"');
      res.send(txtContent);
      
    } else {
      // JSON format (default)
      const jsonData = rows.map(row => {
        const obj = {};
        fieldList.forEach(field => {
          if (row.hasOwnProperty(field)) {
            obj[field] = row[field];
          }
        });
        return obj;
      });
      
      res.json({
        total: jsonData.length,
        items: jsonData
      });
    }
  });
});

// Serve frontend
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start server
app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\n[SHUTDOWN] Stopping sync scheduler...');
  if (syncScheduler) {
    syncScheduler.stop();
  }
  
  db.close((err) => {
    if (err) {
      console.error(err.message);
    }
    console.log('Database connection closed');
    process.exit(0);
  });
});

