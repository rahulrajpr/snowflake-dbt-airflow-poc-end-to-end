
-- ============================================
-- DATABASE SETUP
-- ============================================

CREATE DATABASE FINANCE_DB;

SELECT CURRENT_DATABASE()

-- ============================================
-- SCHEMA CREATION
-- ============================================

CREATE SCHEMA IF NOT EXISTS FINANCE_SCHEMA;

SET search_path TO FINANCE_SCHEMA;

SELECT CURRENT_SCHEMA()

-- ============================================
-- TABLE 1: INVESTORS (Customer Master)
-- ============================================

CREATE TABLE FINANCE_SCHEMA.investors 
	(
    investor_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    date_of_birth DATE,
    registration_date DATE NOT NULL DEFAULT CURRENT_DATE,
    kyc_status VARCHAR(20) DEFAULT 'PENDING' CHECK (kyc_status IN ('PENDING', 'APPROVED', 'REJECTED')),
    risk_profile VARCHAR(20) DEFAULT 'MODERATE' CHECK (risk_profile IN ('CONSERVATIVE', 'MODERATE', 'AGGRESSIVE')),
    customer_segment VARCHAR(20) DEFAULT 'RETAIL' CHECK (customer_segment IN ('RETAIL', 'HNI', 'INSTITUTIONAL')),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(50) DEFAULT 'USA',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

-- Index for CDC pattern

CREATE INDEX idx_investors_updated_at ON FINANCE_SCHEMA.investors(updated_at);
CREATE INDEX idx_investors_email ON FINANCE_SCHEMA.investors(email);

COMMENT ON TABLE FINANCE_SCHEMA.investors IS 'Customer master data - source for investor dimension';
COMMENT ON COLUMN FINANCE_SCHEMA.investors.updated_at IS 'CDC tracking field for incremental extraction';

-- ============================================
-- TABLE 2: FUND_MANAGERS (SCD Type 2 Demo)
-- ============================================

CREATE TABLE FINANCE_SCHEMA.fund_managers 
	(
    manager_id SERIAL PRIMARY KEY,
    manager_code VARCHAR(20) UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    years_experience INT,
    education VARCHAR(255),
    specialization VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    hire_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

CREATE INDEX idx_fund_managers_updated_at ON FINANCE_SCHEMA.fund_managers(updated_at);
CREATE INDEX idx_fund_managers_code ON FINANCE_SCHEMA.fund_managers(manager_code);

COMMENT ON TABLE FINANCE_SCHEMA.fund_managers IS 'Fund manager master - demonstrates SCD Type 2 in dbt';

-- ============================================
-- TABLE 3: FUNDS (Mutual Fund Master)
-- ============================================

CREATE TABLE FINANCE_SCHEMA.funds 
	(
    fund_id SERIAL PRIMARY KEY,
    fund_code VARCHAR(20) UNIQUE NOT NULL,
    fund_name VARCHAR(255) NOT NULL,
    fund_category VARCHAR(50) CHECK (fund_category IN ('EQUITY', 'DEBT', 'HYBRID', 'MONEY_MARKET', 'INDEX')),
    fund_type VARCHAR(20) CHECK (fund_type IN ('OPEN_ENDED', 'CLOSED_ENDED', 'ETF')),
    manager_id INT REFERENCES FINANCE_SCHEMA.fund_managers(manager_id),
    inception_date DATE NOT NULL,
    expense_ratio DECIMAL(5,4), -- e.g., 0.0125 = 1.25%
    minimum_investment DECIMAL(15,2) DEFAULT 1000.00,
    aum_millions DECIMAL(15,2), -- Assets Under Management in millions
    benchmark_index VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

CREATE INDEX idx_funds_updated_at ON FINANCE_SCHEMA.funds(updated_at);
CREATE INDEX idx_funds_category ON FINANCE_SCHEMA.funds(fund_category);
CREATE INDEX idx_funds_manager ON FINANCE_SCHEMA.funds(manager_id);

COMMENT ON TABLE FINANCE_SCHEMA.funds IS 'Mutual fund master data - relatively static';

-- ============================================
-- TABLE 4: ACCOUNTS (Investment Accounts)
-- ============================================

CREATE TABLE FINANCE_SCHEMA.accounts 
	(
    account_id SERIAL PRIMARY KEY,
    account_number VARCHAR(20) UNIQUE NOT NULL,
    investor_id INT NOT NULL REFERENCES FINANCE_SCHEMA.investors(investor_id),
    account_type VARCHAR(20) DEFAULT 'REGULAR' CHECK (account_type IN ('REGULAR', 'IRA', 'ROTH_IRA', 'JOINT', 'CORPORATE')),
    account_status VARCHAR(20) DEFAULT 'ACTIVE' CHECK (account_status IN ('ACTIVE', 'SUSPENDED', 'CLOSED')),
    opening_date DATE NOT NULL DEFAULT CURRENT_DATE,
    closing_date DATE,
    total_balance DECIMAL(15,2) DEFAULT 0.00,
    currency VARCHAR(3) DEFAULT 'USD',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

CREATE INDEX idx_accounts_updated_at ON FINANCE_SCHEMA.accounts(updated_at);
CREATE INDEX idx_accounts_investor ON FINANCE_SCHEMA.accounts(investor_id);
CREATE INDEX idx_accounts_status ON FINANCE_SCHEMA.accounts(account_status);

COMMENT ON TABLE FINANCE_SCHEMA.accounts IS 'Investment accounts - one investor can have multiple accounts';

-- ============================================
-- TABLE 5: FUND_HOLDINGS (Bridge Table)
-- ============================================

CREATE TABLE FINANCE_SCHEMA.fund_holdings 
	(
    holding_id SERIAL PRIMARY KEY,
    account_id INT NOT NULL REFERENCES FINANCE_SCHEMA.accounts(account_id),
    fund_id INT NOT NULL REFERENCES FINANCE_SCHEMA.funds(fund_id),
    units_held DECIMAL(15,4) NOT NULL DEFAULT 0.0000,
    average_cost_per_unit DECIMAL(15,4),
    current_value DECIMAL(15,2),
    last_transaction_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(account_id, fund_id)
	);

CREATE INDEX idx_fund_holdings_updated_at ON FINANCE_SCHEMA.fund_holdings(updated_at);
CREATE INDEX idx_fund_holdings_account ON FINANCE_SCHEMA.fund_holdings(account_id);
CREATE INDEX idx_fund_holdings_fund ON FINANCE_SCHEMA.fund_holdings(fund_id);

COMMENT ON TABLE FINANCE_SCHEMA.fund_holdings IS 'Current fund positions per account - updated by transactions';

-- ============================================
-- TABLE 6: TRANSACTIONS (High-Volume Fact)
-- ============================================

CREATE TABLE FINANCE_SCHEMA.transactions 
	(
    transaction_id SERIAL PRIMARY KEY,
    transaction_number VARCHAR(30) UNIQUE NOT NULL,
    account_id INT NOT NULL REFERENCES FINANCE_SCHEMA.accounts(account_id),
    fund_id INT REFERENCES FINANCE_SCHEMA.funds(fund_id),
    transaction_type VARCHAR(20) NOT NULL CHECK (transaction_type IN ('BUY', 'SELL', 'DEPOSIT', 'WITHDRAWAL', 'DIVIDEND', 'TRANSFER_IN', 'TRANSFER_OUT')),
    transaction_date DATE NOT NULL,
    settlement_date DATE,
    units DECIMAL(15,4), -- For BUY/SELL
    price_per_unit DECIMAL(15,4), -- NAV at transaction time
    amount DECIMAL(15,2) NOT NULL, -- Total transaction amount
    fees DECIMAL(15,2) DEFAULT 0.00,
    tax_amount DECIMAL(15,2) DEFAULT 0.00,
    net_amount DECIMAL(15,2), -- amount - fees - tax
    transaction_status VARCHAR(20) DEFAULT 'COMPLETED' CHECK (transaction_status IN ('PENDING', 'COMPLETED', 'FAILED', 'REVERSED')),
    channel VARCHAR(20) CHECK (channel IN ('ONLINE', 'MOBILE', 'BRANCH', 'ADVISOR', 'AUTO')),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

CREATE INDEX idx_transactions_updated_at ON FINANCE_SCHEMA.transactions(updated_at);
CREATE INDEX idx_transactions_date ON FINANCE_SCHEMA.transactions(transaction_date);
CREATE INDEX idx_transactions_account ON FINANCE_SCHEMA.transactions(account_id);
CREATE INDEX idx_transactions_fund ON FINANCE_SCHEMA.transactions(fund_id);
CREATE INDEX idx_transactions_type ON FINANCE_SCHEMA.transactions(transaction_type);

COMMENT ON TABLE FINANCE_SCHEMA.transactions IS 'High-volume transaction fact table - primary source for incremental CDC';

-- ============================================
-- TABLE 7: ACCOUNT_BALANCES (Daily Snapshots)
-- ============================================

CREATE TABLE FINANCE_SCHEMA.account_balances 
	(
    balance_id SERIAL PRIMARY KEY,
    account_id INT NOT NULL REFERENCES FINANCE_SCHEMA.accounts(account_id),
    balance_date DATE NOT NULL,
    opening_balance DECIMAL(15,2),
    closing_balance DECIMAL(15,2),
    net_deposits DECIMAL(15,2) DEFAULT 0.00,
    net_withdrawals DECIMAL(15,2) DEFAULT 0.00,
    net_investment_gain DECIMAL(15,2) DEFAULT 0.00,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(account_id, balance_date)
	);

CREATE INDEX idx_account_balances_date ON FINANCE_SCHEMA.account_balances(balance_date);
CREATE INDEX idx_account_balances_account ON FINANCE_SCHEMA.account_balances(account_id);

COMMENT ON TABLE FINANCE_SCHEMA.account_balances IS 'Daily balance snapshots for historical tracking';


-- ============================================
-- TRIGGER FUNCTIONS FOR UPDATED_AT
-- ============================================

CREATE OR REPLACE FUNCTION FINANCE_SCHEMA.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply triggers to all tables

CREATE TRIGGER update_investors_updated_at BEFORE UPDATE ON FINANCE_SCHEMA.investors
    FOR EACH ROW EXECUTE FUNCTION FINANCE_SCHEMA.update_updated_at_column();

CREATE TRIGGER update_fund_managers_updated_at BEFORE UPDATE ON FINANCE_SCHEMA.fund_managers
    FOR EACH ROW EXECUTE FUNCTION FINANCE_SCHEMA.update_updated_at_column();

CREATE TRIGGER update_funds_updated_at BEFORE UPDATE ON FINANCE_SCHEMA.funds
    FOR EACH ROW EXECUTE FUNCTION FINANCE_SCHEMA.update_updated_at_column();

CREATE TRIGGER update_accounts_updated_at BEFORE UPDATE ON FINANCE_SCHEMA.accounts
    FOR EACH ROW EXECUTE FUNCTION FINANCE_SCHEMA.update_updated_at_column();

CREATE TRIGGER update_fund_holdings_updated_at BEFORE UPDATE ON FINANCE_SCHEMA.fund_holdings
    FOR EACH ROW EXECUTE FUNCTION FINANCE_SCHEMA.update_updated_at_column();

CREATE TRIGGER update_transactions_updated_at BEFORE UPDATE ON FINANCE_SCHEMA.transactions
    FOR EACH ROW EXECUTE FUNCTION FINANCE_SCHEMA.update_updated_at_column();

CREATE TRIGGER update_account_balances_updated_at BEFORE UPDATE ON FINANCE_SCHEMA.account_balances
    FOR EACH ROW EXECUTE FUNCTION FINANCE_SCHEMA.update_updated_at_column();

-- ============================================
-- VERIFICATION QUERIES
-- ============================================

-- Check all tables created
SELECT schemaname, tablename 
FROM pg_tables 
WHERE schemaname = 'finance_schema'
ORDER BY tablename;

-- Check indexes

SELECT schemaname, tablename, indexname 
FROM pg_indexes 
WHERE schemaname = 'finance_schema'
ORDER BY tablename, indexname;

-- Check foreign key relationships

SELECT
    tc.table_name, 
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY' 
    AND tc.table_schema = 'finance_schema'
ORDER BY tc.table_name;

```