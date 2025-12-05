console.log('Testing Items API directly...');
fetch('http://localhost:7777/api/item-numbers?page=1&limit=10')
  .then(r => r.json())
  .then(d => console.log('Items API response:', d))
  .catch(e => console.error('Items API error:', e));
