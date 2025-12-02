# Amazon Vendor Central Item Tracker

A comprehensive web application for tracking item setup and flow management on Amazon Vendor Central. Built with Node.js, Express, and SQLite.

## Features

- **Item Management**: Track all your products with SKU, name, ASIN, dimensions, case pack, and SIOC status
- **Multi-Country Pricing**: Manage retail and sell prices for different countries
- **Pricing Approval Workflow**: Submit, approve, or reject pricing for each country
- **QPI Integration**: Import items and sync order status from QPI validation CSV
- **Vendor Central Integration**: Sync VC setup status from parquet extracts
- **Flow Stage Tracking**: Monitor progress through 5 stages:
  1. Vendor Central Setup
  2. Born to Run Submission (optional)
  3. Order Received
  4. Order Shipped
  5. Online Available
- **Search & Filter**: Quickly find items and filter by approval status
- **Modern UI**: Beautiful, responsive interface with Amazon-inspired design

## Data Sources

The application integrates with two external data sources:

1. **QPI Validation CSV**: `A:\ProcessOutput\QPI_Validation\QPI_validation_full.csv`
   - Provides SKUs with received orders
   - Includes ASIN and SIOC status
   - Automatically marks items as "Order Received"
  
2. **VC Extracts (Parquet)**: `A:\ProcessOutput\VC_Extracts\Comparison_Extracts\vc_extracts_*.parquet`
   - Most recent timestamped file is automatically selected
   - Confirms items are set up in Vendor Central
   - Updates ASINs and VC setup status

## Prerequisites

- Node.js (v14 or higher)
- npm (comes with Node.js)

## Installation

1. Install dependencies:
```bash
npm install
```

## Usage

1. Start the server:
```bash
npm start
```

For development with auto-reload:
```bash
npm run dev
```

2. Open your browser and navigate to:
```
http://localhost:7777
```

## Database

The application uses SQLite for data storage. The database file (`database.db`) will be automatically created on first run with the following tables:

- `items`: Product information and flow stage status
- `item_country_pricing`: Pricing data for each country
- `flow_stage_history`: Historical record of stage completions

## API Endpoints

### Items
- `GET /api/items` - Get all items with pricing data
- `GET /api/items/:id` - Get single item with full details
- `POST /api/items` - Create new item
- `PUT /api/items/:id` - Update item
- `DELETE /api/items/:id` - Delete item

### Pricing
- `POST /api/items/:id/pricing` - Add or update country pricing
- `PUT /api/items/:id/pricing/:country/approve` - Approve pricing
- `PUT /api/items/:id/pricing/:country/reject` - Reject pricing

### Flow Stages
- `PUT /api/items/:id/stage` - Update flow stage status
- `GET /api/items/:id/history` - Get stage history

### Data Integration
- `POST /api/import/qpi` - Import items from QPI CSV
- `POST /api/sync/qpi` - Sync order received status from QPI
- `POST /api/sync/vc` - Sync Vendor Central setup status from parquet extract

## Project Structure

```
amazon-vendor-central-tracker/
├── server.js           # Express server and API endpoints
├── package.json        # Dependencies and scripts
├── database.db         # SQLite database (auto-generated)
├── public/
│   ├── index.html     # Main HTML file
│   ├── styles.css     # Styling
│   └── app.js         # Frontend JavaScript
└── README.md          # This file
```

## Technologies Used

- **Backend**: Node.js, Express.js
- **Database**: SQLite3, DuckDB (for parquet reading)
- **Data Processing**: CSV Parser, Parquet support via DuckDB
- **Frontend**: Vanilla JavaScript, HTML5, CSS3
- **Additional**: CORS, Body-Parser

## License

ISC

