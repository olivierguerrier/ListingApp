const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('database.db');

console.log('Checking asin_country_status.country_code values:');
db.all(`SELECT DISTINCT country_code FROM asin_country_status ORDER BY country_code LIMIT 30`, [], (err, rows) => {
    if (err) {
        console.error('Error:', err);
    } else {
        console.log('\nCountry codes in asin_country_status:');
        rows.forEach(r => console.log(' -', r.country_code));
    }
    
    // Check what the mapping looks like
    db.all(`SELECT country_code, marketplace FROM vendor_mapping WHERE marketplace = 'Canada'`, [], (err2, rows2) => {
        if (err2) {
            console.error('Error:', err2);
        } else {
            console.log('\nVendor mapping for Canada marketplace:');
            rows2.forEach(r => console.log(' -', r.country_code, '|', r.marketplace));
        }
        
        // Check a specific ASIN
        db.all(`SELECT asin, country_code FROM asin_country_status WHERE asin = 'B0CLKQRXF6'`, [], (err3, rows3) => {
            if (err3) {
                console.error('Error:', err3);
            } else {
                console.log('\nASIN B0CLKQRXF6 in asin_country_status:');
                rows3.forEach(r => console.log(' -', r.country_code));
            }
            db.close();
        });
    });
});

