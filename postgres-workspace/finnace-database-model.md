# Investment Platform Data Model
## Understanding Our Mutual Fund Business Through Data

---

## The Business Story

Imagine a mutual fund investment platform where investors open accounts, buy and sell fund units, and watch their portfolios grow over time. Our database captures this entire investment journey.

```mermaid
erDiagram
    INVESTORS ||--o{ ACCOUNTS : "opens"
    ACCOUNTS ||--o{ TRANSACTIONS : "executes"
    ACCOUNTS ||--o{ FUND_HOLDINGS : "holds"
    FUNDS ||--o{ TRANSACTIONS : "involved in"
    FUNDS ||--o{ FUND_HOLDINGS : "part of"
    FUND_MANAGERS ||--o{ FUNDS : "manages"

    INVESTORS {
        name "John Smith"
        email "john@email.com"
        risk_profile "Aggressive"
        segment "Retail"
    }

    ACCOUNTS {
        account_number "ACC20250001"
        type "IRA"
        balance "$125,430"
        status "Active"
    }

    FUNDS {
        fund_name "Growth Equity Fund"
        category "EQUITY"
        manager "Sarah Johnson"
        aum "$500M"
    }

    TRANSACTIONS {
        type "BUY"
        amount "$5,000"
        date "2025-02-01"
        units "125.5"
    }

    FUND_HOLDINGS {
        fund "Growth Equity"
        units "1,250.75"
        value "$52,000"
    }

    FUND_MANAGERS {
        name "Sarah Johnson"
        experience "15 years"
        specialization "Tech Stocks"
    }
```

---

## The Core Entities

### **INVESTORS** - Our Customers

These are the people and institutions who invest with us. Each investor has a profile that helps us understand their investment preferences and serve them better.

**What We Track:**
- Personal details (name, email, contact info)
- Investment personality (conservative, moderate, or aggressive)
- Customer type (retail investor, high net worth, or institutional)
- When they joined us
- Compliance status (KYC approval)

**Why It Matters:**
Understanding our investors helps us recommend suitable funds, personalize communication, and ensure we're meeting compliance requirements.

**Example:** Jane is a moderate-risk retail investor who joined in 2024. She prefers balanced funds with steady returns.

---

### **FUND_MANAGERS** - The Investment Professionals

The people behind the funds - experienced professionals who make investment decisions on behalf of fund investors.

**What We Track:**
- Manager identity and contact
- Years of experience
- Education background
- Area of expertise (technology, healthcare, bonds, etc.)
- Which funds they currently manage

**Why It Matters:**
Investors often make decisions based on a manager's track record. We need to track when managers change roles to analyze fund performance attribution.

**Example:** Michael Chen has 20 years of experience specializing in Asian markets. He manages our Asia Growth Fund.

---

### **FUNDS** - The Investment Products

The mutual funds we offer to investors. Each fund has distinct characteristics that appeal to different investor types.

**What We Track:**
- Fund name and identification code
- Investment category (stocks, bonds, mixed, money market)
- Who manages it
- When it was launched
- Cost structure (expense ratio)
- Total assets managed
- Benchmark for comparison

**Why It Matters:**
This is our product catalog. Investors choose funds based on these characteristics. We compare performance and analyze which products are growing.

**Example:** "Tech Innovation Fund" - An equity fund focused on technology stocks, managed by Sarah Johnson, with $450M in assets.

---

### **ACCOUNTS** - Investment Containers

When investors join us, they open accounts. One investor might have multiple accounts for different purposes (retirement, regular savings, etc.).

**What We Track:**
- Account number (unique identifier)
- Who owns it
- Account type (regular, IRA, joint, etc.)
- Current total balance
- Account status (active, closed, suspended)
- When it was opened

**Why It Matters:**
Accounts are where the money lives. Different account types have different tax implications and restrictions.

**Example:** John has two accounts - one IRA for retirement ($180K) and one regular account for short-term goals ($45K).

---

### **FUND_HOLDINGS** - What's in Each Portfolio

The bridge between accounts and funds. This shows exactly which funds each account owns and how much.

**What We Track:**
- Which account owns which fund
- How many units they hold
- What they paid on average per unit (cost basis)
- Current market value
- When they last traded this fund

**Why It Matters:**
This is the current portfolio snapshot. We use this to show investors their holdings, calculate gains/losses, and understand diversification.

**Example:** John's IRA holds 450 units of Growth Fund (worth $22,500) and 800 units of Bond Fund (worth $24,000).

---

### **TRANSACTIONS** - The Activity Ledger

Every action taken on accounts - the complete history of all investment activities.

**What We Track:**
- Transaction type (buy, sell, deposit, withdrawal, dividend)
- Which account and which fund (if applicable)
- Date it happened
- How many units traded
- Price per unit at that time
- Total amount
- Fees charged
- Final net amount

**Why It Matters:**
This is our audit trail. It shows investor behavior patterns, generates fee revenue, and is required for regulatory reporting. It's append-only - we never change history.

**Example:** On Feb 1st, Jane bought 125.5 units of Tech Fund at $40/unit for a total of $5,020 including $20 in fees.

---

### **ACCOUNT_BALANCES** - Daily Snapshots

End-of-day balance records for every account, every single day. Like taking a photograph of each account at closing time.

**What We Track:**
- Account and date
- Balance at start of day
- Balance at end of day
- Money deposited that day
- Money withdrawn that day
- Investment gains or losses from market movement

**Why It Matters:**
Historical tracking for trend analysis. We can show investors how their accounts have grown over time and calculate total returns accurately.

**Example:** On Jan 31st, John's IRA opened at $179,500, received no deposits, and closed at $180,200 due to market gains of $700.

---

## How They Connect

**The Investment Journey:**

1. **Jane signs up** → Creates INVESTOR record
2. **Opens an account** → Creates ACCOUNT linked to her investor profile
3. **Deposits $10,000** → Creates TRANSACTION (type: DEPOSIT)
4. **Buys Growth Fund** → Creates TRANSACTION (type: BUY) and updates FUND_HOLDINGS
5. **End of day** → System creates ACCOUNT_BALANCE snapshot
6. **Market moves** → FUND_HOLDINGS current_value updates daily
7. **Checks portfolio** → Views her FUND_HOLDINGS showing current positions

**The Manager Relationship:**
- FUND_MANAGERS manage multiple FUNDS
- When a manager changes roles, we track the history for performance analysis

**The Portfolio View:**
- Each ACCOUNT can hold multiple FUNDS (through FUND_HOLDINGS)
- Each FUND appears in many ACCOUNTS (popular funds are widely held)

---

## Data Freshness & Updates

**Static Data** (changes rarely):
- Fund Manager information - only when someone switches roles
- Fund characteristics - quarterly updates for AUM and expense ratios

**Slow-Moving Data** (changes occasionally):
- Investor profiles - when addresses change or risk profiles update
- Account information - when balances change or status updates

**Active Data** (changes constantly):
- Transactions - new ones every minute during trading hours
- Fund Holdings - updated every time there's a transaction
- Account Balances - new snapshot every single day for every account

---

## Business Questions We Can Answer

**Customer Analytics:**
- Who are our most valuable investors?
- Which customer segments are growing fastest?
- What's the average account size by investor type?

**Product Performance:**
- Which funds are attracting the most new money?
- How does each fund perform vs. its benchmark?
- Which manager's funds have the best returns?

**Activity Patterns:**
- When do investors typically buy vs. sell?
- What's the average transaction size?
- How often do investors rebalance their portfolios?

**Business Health:**
- What's our total Assets Under Management?
- How much fee revenue did we generate this quarter?
- Are account balances growing or shrinking?

---

## Data Quality Rules

**Investor Rules:**
- Every investor must have a unique email
- Risk profile must be valid (Conservative, Moderate, or Aggressive)

**Account Rules:**
- Every account belongs to exactly one investor
- Account number must be unique
- Only one balance snapshot per account per day

**Transaction Rules:**
- Every transaction must reference a valid account
- Transaction amounts must match units × price
- Transactions are immutable - we never change history

**Holdings Rules:**
- An account can hold the same fund only once (no duplicates)
- Holdings are recalculated from transactions

---

## Expected Data Volumes (POC)

For demonstration purposes, we're simulating:

- **1,000 investors** (like a small advisory firm)
- **2,500 accounts** (investors have 2-3 accounts on average)
- **100 mutual funds** (typical product lineup)
- **50 fund managers** (realistic management team)
- **50,000 historical transactions** (about 1 year of activity)
- **250,000 daily balance records** (2,500 accounts × 100 days)

This gives us realistic patterns without overwhelming the POC infrastructure.

---

## What This Enables

This data model supports the entire POC pipeline:

1. **Source System Simulation** - Behaves like a real transaction system
2. **Incremental Data Extraction** - Only pull what's changed since last time
3. **Historical Analysis** - Track trends over time
4. **Portfolio Analytics** - Current positions and performance
5. **Customer Segmentation** - Group investors by behavior
6. **Regulatory Reporting** - Complete audit trail
7. **Dashboard Visualization** - Feed business intelligence tools

The design mirrors real-world mutual fund platforms while remaining simple enough to understand and demonstrate in 10 days.