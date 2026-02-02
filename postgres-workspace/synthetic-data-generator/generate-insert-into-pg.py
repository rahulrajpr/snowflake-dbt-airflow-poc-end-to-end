"""
Financial Platform Synthetic Data Generator
============================================
Generates realistic mutual fund investment data for PostgreSQL

Author: Data Engineering POC
Database: finance_db, Schema: finance_schema
"""

import psycopg2
from psycopg2.extras import execute_batch
from faker import Faker
import random
from datetime import datetime, timedelta
from decimal import Decimal
import logging
import sys

# ============================================
# CONFIGURATION
# ============================================

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'finance_db',
    'user': 'postgres',
    'password': 'postgres'
}

# Data volumes (adjust as needed)
TARGET_COUNTS = {
    'fund_managers': 50,
    'funds': 100,
    'investors': 1000,
    'accounts': 2500,
    'fund_holdings': 7500,
    'transactions': 50000,
    'account_balances_days': 100  # Days of historical balances
}

# Reference data
FUND_CATEGORIES = ['EQUITY', 'DEBT', 'HYBRID', 'MONEY_MARKET', 'INDEX']
FUND_TYPES = ['OPEN_ENDED', 'CLOSED_ENDED', 'ETF']
RISK_PROFILES = ['CONSERVATIVE', 'MODERATE', 'AGGRESSIVE']
CUSTOMER_SEGMENTS = ['RETAIL', 'HNI', 'INSTITUTIONAL']
ACCOUNT_TYPES = ['REGULAR', 'IRA', 'ROTH_IRA', 'JOINT', 'CORPORATE']
TRANSACTION_TYPES = ['BUY', 'SELL', 'DEPOSIT', 'WITHDRAWAL', 'DIVIDEND', 'TRANSFER_IN', 'TRANSFER_OUT']
CHANNELS = ['ONLINE', 'MOBILE', 'BRANCH', 'ADVISOR', 'AUTO']

# ============================================
# LOGGING SETUP (Windows-compatible)
# ============================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_generation.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============================================
# DATABASE CONNECTION
# ============================================

def get_connection():
    """Establish database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("[OK] Database connection established")
        return conn
    except Exception as e:
        logger.error(f"[FAIL] Database connection failed: {e}")
        sys.exit(1)

def get_current_counts(conn):
    """Get current row counts from all tables"""
    cursor = conn.cursor()
    counts = {}
    
    tables = ['fund_managers', 'funds', 'investors', 'accounts', 
              'fund_holdings', 'transactions', 'account_balances']
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM finance_schema.{table}")
        counts[table] = cursor.fetchone()[0]
        logger.info(f"[INFO] Current {table}: {counts[table]} rows")
    
    cursor.close()
    return counts

# ============================================
# DATA GENERATORS
# ============================================

fake = Faker()
Faker.seed(42)  # Reproducible but random

def generate_fund_managers(conn, current_count, target_count):
    """Generate fund manager records"""
    
    if current_count >= target_count:
        logger.info(f"[SKIP] fund_managers (already have {current_count})")
        return []
    
    to_generate = target_count - current_count
    logger.info(f"[START] Generating {to_generate} fund managers...")
    
    cursor = conn.cursor()
    managers = []
    
    specializations = ['Technology', 'Healthcare', 'Energy', 'Finance', 'Consumer Goods',
                      'Real Estate', 'Bonds', 'International Markets', 'Small Cap', 'Large Cap']
    
    for i in range(to_generate):
        manager_code = f"MGR{current_count + i + 1:04d}"
        
        manager = (
            manager_code,
            fake.first_name(),
            fake.last_name(),
            fake.company_email(),
            random.randint(5, 30),  # years_experience
            fake.random_element(['MBA Finance', 'CFA', 'PhD Economics', 'MS Finance']),
            random.choice(specializations),
            True,  # is_active
            fake.date_between(start_date='-20y', end_date='-1y')  # hire_date
        )
        managers.append(manager)
    
    # Insert
    insert_query = """
        INSERT INTO finance_schema.fund_managers 
        (manager_code, first_name, last_name, email, years_experience, 
         education, specialization, is_active, hire_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING manager_id
    """
    
    manager_ids = []
    for manager in managers:
        cursor.execute(insert_query, manager)
        manager_ids.append(cursor.fetchone()[0])
    
    conn.commit()
    cursor.close()
    logger.info(f"[DONE] Generated {len(manager_ids)} fund managers")
    
    return manager_ids


def generate_funds(conn, current_count, target_count):
    """Generate mutual fund records"""
    
    if current_count >= target_count:
        logger.info(f"[SKIP] funds (already have {current_count})")
        return []
    
    # Get all manager IDs
    cursor = conn.cursor()
    cursor.execute("SELECT manager_id FROM finance_schema.fund_managers")
    manager_ids = [row[0] for row in cursor.fetchall()]
    
    if not manager_ids:
        logger.error("[FAIL] No fund managers found. Generate managers first.")
        return []
    
    to_generate = target_count - current_count
    logger.info(f"[START] Generating {to_generate} funds...")
    
    funds = []
    fund_name_prefixes = ['Growth', 'Value', 'Dividend', 'Innovation', 'Global', 'Emerging', 
                          'Core', 'Premier', 'Select', 'Strategic', 'Dynamic', 'Alpha']
    fund_name_suffixes = ['Equity Fund', 'Bond Fund', 'Balanced Fund', 'Index Fund', 
                          'Income Fund', 'Opportunity Fund', 'Market Fund']
    
    for i in range(to_generate):
        fund_code = f"FND{current_count + i + 1:04d}"
        category = random.choice(FUND_CATEGORIES)
        
        # Generate appropriate fund name
        prefix = random.choice(fund_name_prefixes)
        if category == 'EQUITY':
            suffix = random.choice(['Equity Fund', 'Stock Fund', 'Growth Fund'])
        elif category == 'DEBT':
            suffix = random.choice(['Bond Fund', 'Income Fund', 'Fixed Income Fund'])
        elif category == 'HYBRID':
            suffix = 'Balanced Fund'
        elif category == 'MONEY_MARKET':
            suffix = 'Money Market Fund'
        else:
            suffix = 'Index Fund'
        
        fund_name = f"{prefix} {suffix}"
        
        fund = (
            fund_code,
            fund_name,
            category,
            random.choice(FUND_TYPES),
            random.choice(manager_ids),
            fake.date_between(start_date='-15y', end_date='-1y'),  # inception_date
            round(random.uniform(0.0050, 0.0250), 4),  # expense_ratio (0.5% to 2.5%)
            round(random.uniform(1000, 10000), 2),  # minimum_investment
            round(random.uniform(10, 5000), 2),  # aum_millions
            random.choice(['S&P 500', 'NASDAQ', 'Russell 2000', 'Bond Index', None]),
            True  # is_active
        )
        funds.append(fund)
    
    # Insert
    insert_query = """
        INSERT INTO finance_schema.funds 
        (fund_code, fund_name, fund_category, fund_type, manager_id, inception_date,
         expense_ratio, minimum_investment, aum_millions, benchmark_index, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING fund_id
    """
    
    fund_ids = []
    for fund in funds:
        cursor.execute(insert_query, fund)
        fund_ids.append(cursor.fetchone()[0])
    
    conn.commit()
    cursor.close()
    logger.info(f"[DONE] Generated {len(fund_ids)} funds")
    
    return fund_ids


def generate_investors(conn, current_count, target_count):
    """Generate investor records"""
    
    if current_count >= target_count:
        logger.info(f"[SKIP] investors (already have {current_count})")
        return []
    
    to_generate = target_count - current_count
    logger.info(f"[START] Generating {to_generate} investors...")
    
    cursor = conn.cursor()
    investors = []
    
    for i in range(to_generate):
        # Edge case: 5% inactive investors
        is_active = random.random() > 0.05
        
        # Edge case: institutional investors have different patterns
        segment = random.choices(
            CUSTOMER_SEGMENTS,
            weights=[70, 20, 10],  # 70% retail, 20% HNI, 10% institutional
            k=1
        )[0]
        
        investor = (
            fake.first_name(),
            fake.last_name(),
            fake.unique.email(),
            fake.phone_number()[:20],
            fake.date_of_birth(minimum_age=18, maximum_age=80),
            fake.date_between(start_date='-5y', end_date='today'),  # registration_date
            random.choice(['PENDING', 'APPROVED', 'APPROVED', 'APPROVED']),  # 75% approved
            random.choice(RISK_PROFILES),
            segment,
            fake.street_address(),
            fake.secondary_address() if random.random() > 0.5 else None,
            fake.city(),
            fake.state(),
            fake.postcode(),
            'USA',
            is_active
        )
        investors.append(investor)
    
    # Insert
    insert_query = """
        INSERT INTO finance_schema.investors 
        (first_name, last_name, email, phone, date_of_birth, registration_date,
         kyc_status, risk_profile, customer_segment, address_line1, address_line2,
         city, state, postal_code, country, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING investor_id
    """
    
    investor_ids = []
    execute_batch(cursor, insert_query, investors)
    cursor.execute("SELECT investor_id FROM finance_schema.investors ORDER BY investor_id DESC LIMIT %s", (to_generate,))
    investor_ids = [row[0] for row in cursor.fetchall()]
    
    conn.commit()
    cursor.close()
    logger.info(f"[DONE] Generated {len(investor_ids)} investors")
    
    return investor_ids


def generate_accounts(conn, current_count, target_count):
    """Generate account records"""
    
    if current_count >= target_count:
        logger.info(f"[SKIP] accounts (already have {current_count})")
        return []
    
    # Get all investor IDs
    cursor = conn.cursor()
    cursor.execute("SELECT investor_id, customer_segment FROM finance_schema.investors WHERE is_active = TRUE")
    investors = cursor.fetchall()
    
    if not investors:
        logger.error("[FAIL] No investors found. Generate investors first.")
        return []
    
    to_generate = target_count - current_count
    logger.info(f"[START] Generating {to_generate} accounts...")
    
    accounts = []
    
    for i in range(to_generate):
        investor_id, segment = random.choice(investors)
        
        # Account type distribution based on segment
        if segment == 'INSTITUTIONAL':
            account_type = 'CORPORATE'
        elif segment == 'HNI':
            account_type = random.choice(['REGULAR', 'IRA', 'JOINT'])
        else:
            account_type = random.choice(ACCOUNT_TYPES[:4])  # Exclude CORPORATE
        
        # Edge case: 5% suspended/closed accounts
        status_choice = random.choices(
            ['ACTIVE', 'SUSPENDED', 'CLOSED'],
            weights=[90, 5, 5],
            k=1
        )[0]
        
        opening_date = fake.date_between(start_date='-4y', end_date='today')
        closing_date = None
        if status_choice == 'CLOSED':
            closing_date = fake.date_between(start_date=opening_date, end_date='today')
        
        # Initial balance (will be updated by transactions)
        if segment == 'INSTITUTIONAL':
            initial_balance = round(random.uniform(500000, 5000000), 2)
        elif segment == 'HNI':
            initial_balance = round(random.uniform(100000, 500000), 2)
        else:
            initial_balance = round(random.uniform(5000, 100000), 2)
        
        # Edge case: some accounts with zero balance
        if random.random() < 0.10:
            initial_balance = 0.00
        
        account = (
            f"ACC{current_count + i + 1:08d}",
            investor_id,
            account_type,
            status_choice,
            opening_date,
            closing_date,
            initial_balance,
            'USD'
        )
        accounts.append(account)
    
    # Insert
    insert_query = """
        INSERT INTO finance_schema.accounts 
        (account_number, investor_id, account_type, account_status, 
         opening_date, closing_date, total_balance, currency)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING account_id
    """
    
    account_ids = []
    execute_batch(cursor, insert_query, accounts)
    cursor.execute("SELECT account_id FROM finance_schema.accounts ORDER BY account_id DESC LIMIT %s", (to_generate,))
    account_ids = [row[0] for row in cursor.fetchall()]
    
    conn.commit()
    cursor.close()
    logger.info(f"[DONE] Generated {len(account_ids)} accounts")
    
    return account_ids


def generate_fund_holdings(conn, current_count, target_count):
    """Generate fund holding records"""
    
    if current_count >= target_count:
        logger.info(f"[SKIP] fund_holdings (already have {current_count})")
        return []
    
    cursor = conn.cursor()
    
    # Get active accounts with balance > 0
    cursor.execute("""
        SELECT account_id, total_balance 
        FROM finance_schema.accounts 
        WHERE account_status = 'ACTIVE' AND total_balance > 0
    """)
    accounts = cursor.fetchall()
    
    # Get all active funds
    cursor.execute("SELECT fund_id FROM finance_schema.funds WHERE is_active = TRUE")
    fund_ids = [row[0] for row in cursor.fetchall()]
    
    if not accounts or not fund_ids:
        logger.error("[FAIL] No active accounts or funds found.")
        return []
    
    to_generate = target_count - current_count
    logger.info(f"[START] Generating {to_generate} fund holdings...")
    
    holdings = []
    used_combinations = set()
    
    # Get existing combinations to avoid duplicates
    cursor.execute("SELECT account_id, fund_id FROM finance_schema.fund_holdings")
    for row in cursor.fetchall():
        used_combinations.add((row[0], row[1]))
    
    attempts = 0
    max_attempts = to_generate * 3
    
    while len(holdings) < to_generate and attempts < max_attempts:
        attempts += 1
        account_id, total_balance = random.choice(accounts)
        fund_id = random.choice(fund_ids)
        
        # Check if combination already exists
        if (account_id, fund_id) in used_combinations:
            continue
        
        used_combinations.add((account_id, fund_id))
        
        # Convert Decimal to float for calculation
        total_balance_float = float(total_balance)
        
        # Allocate portion of account balance to this fund
        allocation_pct = random.uniform(0.10, 0.50)  # 10-50% of account
        holding_value = round(total_balance_float * allocation_pct, 2)
        
        # Generate realistic unit price (NAV)
        price_per_unit = round(random.uniform(10, 150), 4)
        units_held = round(holding_value / price_per_unit, 4)
        
        holding = (
            account_id,
            fund_id,
            units_held,
            price_per_unit,  # average_cost_per_unit
            holding_value,
            fake.date_between(start_date='-1y', end_date='today')  # last_transaction_date
        )
        holdings.append(holding)
    
    # Insert
    insert_query = """
        INSERT INTO finance_schema.fund_holdings 
        (account_id, fund_id, units_held, average_cost_per_unit, 
         current_value, last_transaction_date)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING holding_id
    """
    
    holding_ids = []
    execute_batch(cursor, insert_query, holdings)
    cursor.execute("SELECT holding_id FROM finance_schema.fund_holdings ORDER BY holding_id DESC LIMIT %s", (len(holdings),))
    holding_ids = [row[0] for row in cursor.fetchall()]
    
    conn.commit()
    cursor.close()
    logger.info(f"[DONE] Generated {len(holding_ids)} fund holdings")
    
    return holding_ids


def generate_transactions(conn, current_count, target_count):
    """Generate transaction records"""
    
    if current_count >= target_count:
        logger.info(f"[SKIP] transactions (already have {current_count})")
        return []
    
    cursor = conn.cursor()
    
    # Get active accounts
    cursor.execute("SELECT account_id FROM finance_schema.accounts WHERE account_status = 'ACTIVE'")
    account_ids = [row[0] for row in cursor.fetchall()]
    
    # Get active funds
    cursor.execute("SELECT fund_id FROM finance_schema.funds WHERE is_active = TRUE")
    fund_ids = [row[0] for row in cursor.fetchall()]
    
    if not account_ids or not fund_ids:
        logger.error("[FAIL] No active accounts or funds found.")
        return []
    
    to_generate = target_count - current_count
    logger.info(f"[START] Generating {to_generate} transactions...")
    
    transactions = []
    
    # Generate transactions over last 365 days
    start_date = datetime.now() - timedelta(days=365)
    
    for i in range(to_generate):
        txn_number = f"TXN{datetime.now().strftime('%Y%m%d')}-{current_count + i + 1:08d}"
        account_id = random.choice(account_ids)
        
        # Transaction type distribution
        txn_type = random.choices(
            TRANSACTION_TYPES,
            weights=[30, 20, 15, 10, 10, 10, 5],  # Favor BUY/SELL
            k=1
        )[0]
        
        # DEPOSIT/WITHDRAWAL don't need fund
        if txn_type in ['DEPOSIT', 'WITHDRAWAL']:
            fund_id = None
            units = None
            price_per_unit = None
            amount = round(random.uniform(500, 50000), 2)
        else:
            fund_id = random.choice(fund_ids)
            price_per_unit = round(random.uniform(10, 150), 4)
            units = round(random.uniform(10, 1000), 4)
            amount = round(units * price_per_unit, 2)
        
        txn_date = fake.date_between(start_date=start_date, end_date='today')
        settlement_date = txn_date + timedelta(days=random.randint(1, 3))
        
        # Fees (0.5% to 2%)
        fees = round(amount * random.uniform(0.005, 0.02), 2)
        tax_amount = round(amount * 0.01, 2) if txn_type == 'SELL' else 0.00
        net_amount = round(amount - fees - tax_amount, 2)
        
        # Edge case: 2% failed transactions
        if random.random() < 0.02:
            status = random.choice(['FAILED', 'REVERSED'])
        else:
            status = 'COMPLETED'
        
        transaction = (
            txn_number,
            account_id,
            fund_id,
            txn_type,
            txn_date,
            settlement_date,
            units,
            price_per_unit,
            amount,
            fees,
            tax_amount,
            net_amount,
            status,
            random.choice(CHANNELS),
            None  # notes
        )
        transactions.append(transaction)
    
    # Insert in batches
    insert_query = """
        INSERT INTO finance_schema.transactions 
        (transaction_number, account_id, fund_id, transaction_type, transaction_date,
         settlement_date, units, price_per_unit, amount, fees, tax_amount, net_amount,
         transaction_status, channel, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING transaction_id
    """
    
    transaction_ids = []
    batch_size = 1000
    for i in range(0, len(transactions), batch_size):
        batch = transactions[i:i + batch_size]
        execute_batch(cursor, insert_query, batch)
        logger.info(f"  [BATCH] Inserted batch {i//batch_size + 1}/{(len(transactions)-1)//batch_size + 1}")
    
    cursor.execute("SELECT transaction_id FROM finance_schema.transactions ORDER BY transaction_id DESC LIMIT %s", (to_generate,))
    transaction_ids = [row[0] for row in cursor.fetchall()]
    
    conn.commit()
    cursor.close()
    logger.info(f"[DONE] Generated {len(transaction_ids)} transactions")
    
    return transaction_ids


def generate_account_balances(conn, days_to_generate):
    """Generate daily account balance snapshots"""
    
    cursor = conn.cursor()
    
    # Get all accounts
    cursor.execute("SELECT account_id, total_balance, opening_date FROM finance_schema.accounts")
    accounts = cursor.fetchall()
    
    if not accounts:
        logger.error("[FAIL] No accounts found.")
        return []
    
    logger.info(f"[START] Generating {days_to_generate} days of balance history for {len(accounts)} accounts...")
    
    # Check existing balance dates
    cursor.execute("SELECT DISTINCT balance_date FROM finance_schema.account_balances ORDER BY balance_date DESC LIMIT 1")
    last_balance_date = cursor.fetchone()
    
    if last_balance_date:
        start_date = last_balance_date[0] + timedelta(days=1)
        logger.info(f"[INFO] Last balance date: {last_balance_date[0]}, starting from {start_date}")
    else:
        start_date = datetime.now().date() - timedelta(days=days_to_generate)
        logger.info(f"[INFO] No existing balances, starting from {start_date}")
    
    end_date = datetime.now().date()
    current_date = start_date
    
    balances = []
    total_records = 0
    
    while current_date <= end_date:
        for account_id, total_balance, opening_date in accounts:
            # Skip if account wasn't open yet
            if current_date < opening_date:
                continue
            
            # Check if balance already exists
            cursor.execute("""
                SELECT 1 FROM finance_schema.account_balances 
                WHERE account_id = %s AND balance_date = %s
            """, (account_id, current_date))
            
            if cursor.fetchone():
                continue  # Skip duplicate
            
            # Convert Decimal to float for calculation
            total_balance_float = float(total_balance)
            
            # Calculate daily balance changes
            daily_volatility = random.uniform(-0.02, 0.03)  # -2% to +3% daily
            opening_balance = total_balance_float
            net_deposits = round(random.uniform(0, 1000), 2) if random.random() > 0.8 else 0.00
            net_withdrawals = round(random.uniform(0, 500), 2) if random.random() > 0.9 else 0.00
            net_investment_gain = round(total_balance_float * daily_volatility, 2)
            closing_balance = round(opening_balance + net_deposits - net_withdrawals + net_investment_gain, 2)
            
            balance = (
                account_id,
                current_date,
                opening_balance,
                closing_balance,
                net_deposits,
                net_withdrawals,
                net_investment_gain
            )
            balances.append(balance)
            total_records += 1
        
        current_date += timedelta(days=1)
        
        # Insert in batches to avoid memory issues
        if len(balances) >= 10000:
            insert_query = """
                INSERT INTO finance_schema.account_balances 
                (account_id, balance_date, opening_balance, closing_balance,
                 net_deposits, net_withdrawals, net_investment_gain)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (account_id, balance_date) DO NOTHING
            """
            execute_batch(cursor, insert_query, balances)
            conn.commit()
            logger.info(f"  [BATCH] Inserted batch of {len(balances)} balance records")
            balances = []
    
    # Insert remaining
    if balances:
        insert_query = """
            INSERT INTO finance_schema.account_balances 
            (account_id, balance_date, opening_balance, closing_balance,
             net_deposits, net_withdrawals, net_investment_gain)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (account_id, balance_date) DO NOTHING
        """
        execute_batch(cursor, insert_query, balances)
        conn.commit()
    
    cursor.close()
    logger.info(f"[DONE] Generated {total_records} account balance records")
    
    return total_records


# ============================================
# VALIDATION
# ============================================

def validate_data(conn):
    """Run validation queries"""
    logger.info("[START] Running data validation...")
    
    cursor = conn.cursor()
    
    validations = [
        ("Orphaned accounts", "SELECT COUNT(*) FROM finance_schema.accounts a LEFT JOIN finance_schema.investors i ON a.investor_id = i.investor_id WHERE i.investor_id IS NULL"),
        ("Orphaned funds", "SELECT COUNT(*) FROM finance_schema.funds f LEFT JOIN finance_schema.fund_managers m ON f.manager_id = m.manager_id WHERE m.manager_id IS NULL"),
        ("Invalid holdings", "SELECT COUNT(*) FROM finance_schema.fund_holdings WHERE units_held <= 0"),
        ("Transactions with invalid accounts", "SELECT COUNT(*) FROM finance_schema.transactions t LEFT JOIN finance_schema.accounts a ON t.account_id = a.account_id WHERE a.account_id IS NULL"),
    ]
    
    all_valid = True
    for check_name, query in validations:
        cursor.execute(query)
        count = cursor.fetchone()[0]
        if count > 0:
            logger.warning(f"[WARN] {check_name}: {count} issues found")
            all_valid = False
        else:
            logger.info(f"[OK] {check_name}: PASS")
    
    cursor.close()
    
    if all_valid:
        logger.info("[DONE] All validation checks passed!")
    else:
        logger.warning("[WARN] Some validation issues found - review logs")
    
    return all_valid


# ============================================
# MAIN EXECUTION
# ============================================

def main():
    """Main execution flow"""
    
    logger.info("=" * 60)
    logger.info("FINANCIAL DATA GENERATOR - STARTING")
    logger.info("=" * 60)
    
    # Connect to database
    conn = get_connection()
    
    # Get current state
    logger.info("\n[INFO] Checking current database state...")
    current_counts = get_current_counts(conn)
    
    # Generate data in dependency order
    logger.info("\n" + "=" * 60)
    logger.info("STEP 1: FUND MANAGERS")
    logger.info("=" * 60)
    generate_fund_managers(conn, current_counts['fund_managers'], TARGET_COUNTS['fund_managers'])
    
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: FUNDS")
    logger.info("=" * 60)
    generate_funds(conn, current_counts['funds'], TARGET_COUNTS['funds'])
    
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: INVESTORS")
    logger.info("=" * 60)
    generate_investors(conn, current_counts['investors'], TARGET_COUNTS['investors'])
    
    logger.info("\n" + "=" * 60)
    logger.info("STEP 4: ACCOUNTS")
    logger.info("=" * 60)
    generate_accounts(conn, current_counts['accounts'], TARGET_COUNTS['accounts'])
    
    logger.info("\n" + "=" * 60)
    logger.info("STEP 5: FUND HOLDINGS")
    logger.info("=" * 60)
    generate_fund_holdings(conn, current_counts['fund_holdings'], TARGET_COUNTS['fund_holdings'])
    
    logger.info("\n" + "=" * 60)
    logger.info("STEP 6: TRANSACTIONS")
    logger.info("=" * 60)
    generate_transactions(conn, current_counts['transactions'], TARGET_COUNTS['transactions'])
    
    logger.info("\n" + "=" * 60)
    logger.info("STEP 7: ACCOUNT BALANCES")
    logger.info("=" * 60)
    generate_account_balances(conn, TARGET_COUNTS['account_balances_days'])
    
    # Final counts
    logger.info("\n" + "=" * 60)
    logger.info("FINAL DATABASE STATE")
    logger.info("=" * 60)
    final_counts = get_current_counts(conn)
    
    # Validation
    logger.info("\n" + "=" * 60)
    logger.info("DATA VALIDATION")
    logger.info("=" * 60)
    validate_data(conn)
    
    # Close connection
    conn.close()
    logger.info("\n" + "=" * 60)
    logger.info("DATA GENERATION COMPLETE!")
    logger.info("=" * 60)
    logger.info("Check data_generation.log for detailed logs")


if __name__ == "__main__":
    main()