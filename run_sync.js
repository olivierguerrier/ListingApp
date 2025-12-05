// Script to trigger data sync from the API
const API_BASE = 'http://localhost:7777/api';

async function syncData() {
  console.log('=== Starting Data Sync ===\n');
  
  try {
    // Step 1: Sync QPI data (creates products with primary_item_number and SKUs)
    console.log('Step 1: Syncing QPI data...');
    const qpiResponse = await fetch(`${API_BASE}/sync/qpi`, { method: 'POST' });
    const qpiResult = await qpiResponse.json();
    console.log('QPI sync result:', qpiResult);
    console.log('');
    
    // Wait a bit for async operations to complete
    await new Promise(resolve => setTimeout(resolve, 3000));
    
    // Step 2: Sync PIM data (populates item_numbers and updates products with brand, etc.)
    console.log('Step 2: Syncing PIM data...');
    const pimResponse = await fetch(`${API_BASE}/sync/pim`, { method: 'POST' });
    const pimResult = await pimResponse.json();
    console.log('PIM sync result:', pimResult);
    console.log('');
    
    console.log('=== Sync Complete ===');
    
  } catch (error) {
    console.error('Error during sync:', error.message);
  }
}

syncData();

