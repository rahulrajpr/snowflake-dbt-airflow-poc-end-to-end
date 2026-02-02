"""
GCS Data Generator and Uploader
================================

Generates financial data files and uploads to Google Cloud Storage

Features:
- Reads PostgreSQL to get current fund list (delta-aware)
- Generates NAV data for all funds
- Downloads real market index data from Yahoo Finance
- Generates fund metadata and ratings
- Uploads to GCS with date-based file naming
- Handles historical backfill (100 days)
- Prevents duplicates by checking existing files

"""
import os
import sys
import json
import csv
from datetime import datetime, timedelta
from decimal import Decimal
import logging
import io

# Third-party imports
import psycopg2
from google.cloud import storage
import yfinance as yf
import pandas as pd
from faker import Faker

# ============================================
# CONFIGURATION
# ============================================

# PostgreSQL Configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'finance_db',
    'user': 'postgres',
    'password': 'postgres'
}

# GCS Configuration

GCS_CREDENTIALS_PATH = 'gcp-gcs-credentials.json'  # Path to your service account JSON key

GCS_BUCKET_NAME = 'finance-data-landing-bucket-poc'  # Change if your bucket name is different

GCS_FUND_METADATA_FOLDER = 'fund-metadata'
GCS_MARKET_DATA_FOLDER = 'market-data'
GCS_NAV_DATA_FOLDER = 'nav-data'
GCS_RATINGS_FOLDER = 'ratings-data'

# Data Generation Configuration

HISTORICAL_DAYS = 100  # Number of days to backfill
START_DATE = datetime.now() - timedelta(days=HISTORICAL_DAYS)
END_DATE = datetime.now()

# Market indices to track

MARKET_INDICES = {
    'SP500': '^GSPC',      # S&P 500
    'NASDAQ': '^IXIC',     # NASDAQ Composite
    'DJIA': '^DJI',        # Dow Jones Industrial Average
    'RUSSELL2000': '^RUT'  # Russell 2000
}

# ============================================
# LOGGING SETUP
# ============================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gcs_data_generation.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============================================
# HELPER FUNCTIONS
# ============================================

def get_postgres_connection():
    """Connect to PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("[OK] PostgreSQL connection established")
        return conn
    except Exception as e:
        logger.error(f"[FAIL] PostgreSQL connection failed: {e}")
        sys.exit(1)

def get_gcs_client():
    """Initialize GCS client"""
    try:
        if not os.path.exists(GCS_CREDENTIALS_PATH):
            logger.error(f"[FAIL] GCS credentials file not found: {GCS_CREDENTIALS_PATH}")
            logger.error("Please download service account JSON key and save it as 'gcp-credentials.json'")
            sys.exit(1)
        
        # Set environment variable for authentication
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCS_CREDENTIALS_PATH
        
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        
        # Test bucket access
        if not bucket.exists():
            logger.error(f"[FAIL] GCS bucket does not exist: {GCS_BUCKET_NAME}")
            logger.error("Please create the bucket in GCP Console first")
            sys.exit(1)
        
        logger.info(f"[OK] GCS client initialized - Bucket: {GCS_BUCKET_NAME}")
        return client, bucket
    
    except Exception as e:
        logger.error(f"[FAIL] GCS client initialization failed: {e}")
        sys.exit(1)

def get_existing_gcs_files(bucket, prefix):
    """Get list of existing files in GCS folder"""
    blobs = bucket.list_blobs(prefix=prefix)
    existing_files = set()
    for blob in blobs:
        # Extract just the filename without path
        filename = blob.name.split('/')[-1]
        existing_files.add(filename)
    return existing_files

# ============================================
# DATA RETRIEVAL FROM POSTGRESQL
# ============================================

def get_fund_data_from_postgres():
    """Retrieve fund information from PostgreSQL"""
    logger.info("[START] Fetching fund data from PostgreSQL...")
    
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    # Get all active funds with their details
    query = """
        SELECT 
            f.fund_id,
            f.fund_code,
            f.fund_name,
            f.fund_category,
            f.inception_date,
            fm.first_name || ' ' || fm.last_name as manager_name
        FROM finance_schema.funds f
        LEFT JOIN finance_schema.fund_managers fm ON f.manager_id = fm.manager_id
        WHERE f.is_active = TRUE
        ORDER BY f.fund_id
    """
    
    cursor.execute(query)
    funds = cursor.fetchall()
    
    fund_list = []
    for fund in funds:
        fund_list.append({
            'fund_id': fund[0],
            'fund_code': fund[1],
            'fund_name': fund[2],
            'fund_category': fund[3],
            'inception_date': fund[4],
            'manager_name': fund[5]
        })
    
    cursor.close()
    conn.close()
    
    logger.info(f"[OK] Retrieved {len(fund_list)} funds from PostgreSQL")
    return fund_list

# ============================================
# NAV DATA GENERATION
# ============================================

def generate_nav_data(funds, date):
    """Generate realistic NAV data for all funds for a specific date"""
    
    nav_records = []
    
    for fund in funds:
        # Skip if fund wasn't launched yet
        if date.date() < fund['inception_date']:
            continue
        
        # Generate realistic NAV based on fund category
        if fund['fund_category'] == 'EQUITY':
            base_nav = 50.0
            volatility = 0.03  # 3% daily volatility
        elif fund['fund_category'] == 'DEBT':
            base_nav = 100.0
            volatility = 0.005  # 0.5% daily volatility
        elif fund['fund_category'] == 'HYBRID':
            base_nav = 75.0
            volatility = 0.015  # 1.5% daily volatility
        elif fund['fund_category'] == 'MONEY_MARKET':
            base_nav = 1000.0
            volatility = 0.0005  # 0.05% daily volatility
        else:  # INDEX
            base_nav = 100.0
            volatility = 0.02  # 2% daily volatility
        
        # Calculate NAV with some randomness
        days_since_inception = (date.date() - fund['inception_date']).days
        growth_factor = 1 + (0.08 * days_since_inception / 365)  # 8% annual growth
        
        import random
        random.seed(hash(fund['fund_code'] + str(date.date())))  # Consistent for same fund+date
        
        daily_change = random.uniform(-volatility, volatility)
        nav = round(base_nav * growth_factor * (1 + daily_change), 4)
        
        # Calculate total assets (AUM)
        total_assets = round(random.uniform(10_000_000, 1_000_000_000), 2)
        
        nav_records.append({
            'date': date.strftime('%Y-%m-%d'),
            'fund_code': fund['fund_code'],
            'fund_name': fund['fund_name'],
            'nav': nav,
            'change_percent': round(daily_change * 100, 2),
            'total_assets': total_assets
        })
    
    return nav_records

def upload_nav_data_to_gcs(bucket, nav_records, date):
    """Upload NAV data as CSV to GCS"""
    
    filename = f"nav_{date.strftime('%Y-%m-%d')}.csv"
    blob_path = f"{GCS_NAV_DATA_FOLDER}/{filename}"
    
    # Check if file already exists
    blob = bucket.blob(blob_path)
    if blob.exists():
        logger.info(f"[SKIP] NAV file already exists: {filename}")
        return False
    
    # Create CSV in memory
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=['date', 'fund_code', 'fund_name', 'nav', 'change_percent', 'total_assets'])
    writer.writeheader()
    writer.writerows(nav_records)
    
    # Upload to GCS
    blob.upload_from_string(output.getvalue(), content_type='text/csv')
    logger.info(f"[UPLOAD] NAV data uploaded: {blob_path} ({len(nav_records)} records)")
    return True

# ============================================
# MARKET INDEX DATA GENERATION
# ============================================

def download_market_data(date):
    """Download real market index data from Yahoo Finance"""
    
    market_data = {
        'date': date.strftime('%Y-%m-%d'),
        'indices': {}
    }
    
    for index_name, ticker in MARKET_INDICES.items():
        try:
            # Download data for the specific date
            ticker_obj = yf.Ticker(ticker)
            hist = ticker_obj.history(start=date, end=date + timedelta(days=1))
            
            if not hist.empty:
                close_price = round(hist['Close'].iloc[0], 2)
                
                # Get previous day's close for change calculation
                prev_date = date - timedelta(days=1)
                prev_hist = ticker_obj.history(start=prev_date, end=date)
                
                if len(prev_hist) >= 2:
                    prev_close = prev_hist['Close'].iloc[-2]
                    change_percent = round(((close_price - prev_close) / prev_close) * 100, 2)
                else:
                    change_percent = 0.0
                
                market_data['indices'][index_name] = {
                    'ticker': ticker,
                    'value': close_price,
                    'change_percent': change_percent
                }
                
                logger.info(f"[OK] {index_name}: {close_price} ({change_percent:+.2f}%)")
            else:
                # Fallback to synthetic data if market was closed
                logger.warning(f"[WARN] No data for {index_name} on {date.strftime('%Y-%m-%d')} (weekend/holiday)")
                market_data['indices'][index_name] = {
                    'ticker': ticker,
                    'value': None,
                    'change_percent': None,
                    'note': 'Market closed'
                }
        
        except Exception as e:
            logger.error(f"[ERROR] Failed to download {index_name}: {e}")
            market_data['indices'][index_name] = {
                'ticker': ticker,
                'value': None,
                'change_percent': None,
                'error': str(e)
            }
    
    return market_data

def upload_market_data_to_gcs(bucket, market_data, date):
    """Upload market index data as JSON to GCS"""
    
    filename = f"market_{date.strftime('%Y-%m-%d')}.json"
    blob_path = f"{GCS_MARKET_DATA_FOLDER}/{filename}"
    
    # Check if file already exists
    blob = bucket.blob(blob_path)
    if blob.exists():
        logger.info(f"[SKIP] Market data file already exists: {filename}")
        return False
    
    # Upload JSON to GCS
    blob.upload_from_string(
        json.dumps(market_data, indent=2),
        content_type='application/json'
    )
    logger.info(f"[UPLOAD] Market data uploaded: {blob_path}")
    return True

# ============================================
# FUND METADATA GENERATION
# ============================================

def generate_fund_metadata(funds):
    """Generate supplementary fund metadata"""
    
    metadata_records = []
    fake = Faker()
    Faker.seed(42)
    
    for fund in funds:
        metadata_records.append({
            'fund_code': fund['fund_code'],
            'fund_name': fund['fund_name'],
            'fund_category': fund['fund_category'],
            'inception_date': fund['inception_date'].strftime('%Y-%m-%d'),
            'manager_name': fund['manager_name'],
            'investment_objective': fake.sentence(nb_words=15),
            'minimum_investment': round(fake.random.uniform(1000, 10000), 2),
            'dividend_frequency': fake.random_element(['Quarterly', 'Annual', 'Monthly', 'None']),
            'risk_rating': fake.random_element(['Low', 'Moderate', 'High']),
            'tax_treatment': fake.random_element(['Tax-Exempt', 'Taxable', 'Tax-Deferred'])
        })
    
    return metadata_records

def upload_fund_metadata_to_gcs(bucket, metadata_records):
    """Upload fund metadata as CSV to GCS"""
    
    filename = "fund_attributes.csv"
    blob_path = f"{GCS_FUND_METADATA_FOLDER}/{filename}"
    
    # Check if file already exists
    blob = bucket.blob(blob_path)
    if blob.exists():
        logger.info(f"[SKIP] Fund metadata file already exists: {filename}")
        return False
    
    # Create CSV in memory
    output = io.StringIO()
    if metadata_records:
        writer = csv.DictWriter(output, fieldnames=metadata_records[0].keys())
        writer.writeheader()
        writer.writerows(metadata_records)
    
    # Upload to GCS
    blob.upload_from_string(output.getvalue(), content_type='text/csv')
    logger.info(f"[UPLOAD] Fund metadata uploaded: {blob_path} ({len(metadata_records)} records)")
    return True

# ============================================
# RATINGS DATA GENERATION
# ============================================

def generate_ratings_data(funds, date):
    """Generate mock ratings data (simulating external vendor API)"""
    
    import random
    random.seed(hash(str(date.date())))
    
    ratings = {
        'report_date': date.strftime('%Y-%m-%d'),
        'vendor': 'Morningstar Analytics (Simulated)',
        'ratings': []
    }
    
    for fund in funds:
        # Only include ~70% of funds in ratings (some funds unrated)
        if random.random() < 0.7:
            rating = {
                'fund_code': fund['fund_code'],
                'fund_name': fund['fund_name'],
                'overall_rating': random.randint(1, 5),  # 1-5 star rating
                'risk_score': round(random.uniform(1.0, 10.0), 2),
                'return_score': round(random.uniform(1.0, 10.0), 2),
                'expense_score': round(random.uniform(1.0, 10.0), 2),
                'category_rank': random.randint(1, 100),
                'analyst_outlook': random.choice(['Positive', 'Neutral', 'Negative'])
            }
            ratings['ratings'].append(rating)
    
    return ratings

def upload_ratings_data_to_gcs(bucket, ratings_data, date):
    """Upload ratings data as JSON to GCS"""
    
    # Monthly ratings (first day of month)
    filename = f"fund_ratings_{date.strftime('%Y-%m')}.json"
    blob_path = f"{GCS_RATINGS_FOLDER}/{filename}"
    
    # Check if file already exists
    blob = bucket.blob(blob_path)
    if blob.exists():
        logger.info(f"[SKIP] Ratings file already exists: {filename}")
        return False
    
    # Upload JSON to GCS
    blob.upload_from_string(
        json.dumps(ratings_data, indent=2),
        content_type='application/json'
    )
    logger.info(f"[UPLOAD] Ratings data uploaded: {blob_path} ({len(ratings_data['ratings'])} funds rated)")
    return True

# ============================================
# MAIN EXECUTION
# ============================================

def main():
    """Main execution flow"""
    
    logger.info("=" * 70)
    logger.info("GCS DATA GENERATOR - STARTING")
    logger.info("=" * 70)
    
    # Step 1: Initialize connections
    logger.info("\n[STEP 1] Initializing connections...")
    gcs_client, gcs_bucket = get_gcs_client()
    
    # Step 2: Get fund data from PostgreSQL
    logger.info("\n[STEP 2] Retrieving fund data from PostgreSQL...")
    funds = get_fund_data_from_postgres()
    
    if not funds:
        logger.error("[FAIL] No funds found in PostgreSQL. Run PostgreSQL data generation first.")
        sys.exit(1)
    
    # Step 3: Generate and upload fund metadata (one-time)
    logger.info("\n[STEP 3] Generating fund metadata...")
    metadata = generate_fund_metadata(funds)
    upload_fund_metadata_to_gcs(gcs_bucket, metadata)
    
    # Step 4: Generate historical data
    logger.info(f"\n[STEP 4] Generating {HISTORICAL_DAYS} days of historical data...")
    logger.info(f"Date range: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")
    
    current_date = START_DATE
    nav_uploaded_count = 0
    market_uploaded_count = 0
    ratings_uploaded_count = 0
    
    while current_date <= END_DATE:
        logger.info(f"\n--- Processing {current_date.strftime('%Y-%m-%d')} ---")
        
        # Generate and upload NAV data (daily)
        nav_records = generate_nav_data(funds, current_date)
        if upload_nav_data_to_gcs(gcs_bucket, nav_records, current_date):
            nav_uploaded_count += 1
        
        # Download and upload market data (daily)
        # Skip weekends for market data
        if current_date.weekday() < 5:  # Monday = 0, Friday = 4
            market_data = download_market_data(current_date)
            if upload_market_data_to_gcs(gcs_bucket, market_data, current_date):
                market_uploaded_count += 1
        
        # Generate and upload ratings (monthly - first day of month)
        if current_date.day == 1:
            ratings_data = generate_ratings_data(funds, current_date)
            if upload_ratings_data_to_gcs(gcs_bucket, ratings_data, current_date):
                ratings_uploaded_count += 1
        
        current_date += timedelta(days=1)
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("DATA GENERATION COMPLETE!")
    logger.info("=" * 70)
    logger.info(f"NAV files uploaded: {nav_uploaded_count}")
    logger.info(f"Market data files uploaded: {market_uploaded_count}")
    logger.info(f"Ratings files uploaded: {ratings_uploaded_count}")
    logger.info(f"Fund metadata files uploaded: 1")
    logger.info("=" * 70)
    logger.info(f"\nCheck your GCS bucket: gs://{GCS_BUCKET_NAME}/")
    logger.info("Next steps:")
    logger.info("1. Set up Snowflake external stage pointing to this bucket")
    logger.info("2. Configure Snowpipe for auto-ingestion")
    logger.info("3. Run dbt transformations")

if __name__ == "__main__":
    main()