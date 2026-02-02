# GCS Data Sources - Business & Technical Documentation
## Financial Data Landing Zone - Morgan Stanley POC

**Document Purpose:** This document provides business context and technical specifications for all data files in Google Cloud Storage, enabling stakeholders to understand the data model, business logic, and relationships within the mutual fund investment platform.

**Target Audience:** Directors, Architects, Business Analysts, Data Engineers

**Last Updated:** February 2, 2026

---

## Executive Summary

### Project Context
This POC demonstrates an end-to-end data platform for a **mutual fund investment platform**, simulating the complete investor journey from account opening through portfolio management and performance tracking.

### Data Sources Overview

**GCS Bucket:** `gs://finance-data-landing-bucket-poc/`

| Folder | Business Purpose | Frequency | Files | Owner |
|--------|------------------|-----------|-------|-------|
| `nav-data/` | Daily fund pricing & valuation | Daily | 101 | Fund Operations |
| `market-data/` | Benchmark indices | Daily (weekdays) | 71 | Market Data |
| `fund-metadata/` | Product catalog | Monthly | 1 | Product Mgmt |
| `ratings-data/` | Quality assessments | Monthly | 4 | Research |

### Key Metrics
- **Historical Depth:** 100 days (Oct 25, 2025 - Feb 2, 2026)
- **Fund Universe:** 100 mutual funds across 5 categories
- **Total Files:** 177 files
- **Data Volume:** ~10 MB

---

## Business Context: The Investment Lifecycle

### Data Touchpoints Across Customer Journey

```
INVESTOR JOURNEY
│
├─ 1. DISCOVERY & RESEARCH
│   ├─ fund-metadata/     → Browse product catalog
│   ├─ ratings-data/      → Check quality scores  
│   └─ market-data/       → Understand market context
│
├─ 2. INVESTMENT DECISION
│   ├─ nav-data/          → Current pricing
│   └─ Historical NAV     → Performance analysis
│
├─ 3. PORTFOLIO MANAGEMENT
│   ├─ Daily NAV updates  → Portfolio valuation
│   └─ Market benchmarks  → Relative performance
│
└─ 4. REPORTING & ANALYTICS
    └─ All sources combined → Comprehensive insights
```

### Business Questions Answered

**For Investors:**
- What is my portfolio worth today?
- How are my funds performing vs. market?
- Which funds are top-rated?
- What products match my risk profile?

**For Portfolio Managers:**
- Which funds are outperforming/underperforming?
- How do returns correlate with market movements?
- What is the volatility of each fund?

**For Executives:**
- Total Assets Under Management trends
- Product mix analysis
- Market share and positioning
- Risk-adjusted returns by category

**For Compliance:**
- Daily pricing accuracy
- Fund categorization compliance
- Rating coverage validation

---

## Data Architecture

### Integration with PostgreSQL

```
COMPLETE DATA ECOSYSTEM

PostgreSQL (Transactional)          GCS (Reference Data)
├─ investors                        ├─ nav-data/
├─ accounts                         ├─ market-data/
├─ fund_holdings                    ├─ fund-metadata/
├─ transactions                     └─ ratings-data/
├─ funds ◄──────fund_code──────────────┘
└─ fund_managers                    

            ▼
    Snowflake (Analytics)
    └─ Unified analytical view
```

**Critical Join Key:** `fund_code` (e.g., "FND0001") connects all data sources

---

## File Specifications

### 1. NAV Data (Daily Fund Pricing)

**📁 Location:** `nav-data/`  
**📄 File Pattern:** `nav_YYYY-MM-DD.csv`  
**📊 Records per file:** 100 (one per fund)  
**🔄 Update:** Daily after market close

#### Business Purpose
Net Asset Value (NAV) is the **per-unit price** of a mutual fund. This is the CORE pricing data that determines:
- Buy/sell prices for investors
- Daily portfolio valuations
- Performance calculations
- Asset under management tracking

#### File Structure
```csv
date,fund_code,fund_name,nav,change_percent,total_assets
2026-02-02,FND0001,Growth Equity Fund,52.3456,0.23,125000000.00
2026-02-02,FND0002,Value Bond Fund,102.4578,-0.15,85000000.00
```

#### Field Definitions

| Field | Type | Definition | Business Rules |
|-------|------|------------|----------------|
| **date** | DATE | NAV calculation date | End-of-day valuation |
| **fund_code** | VARCHAR | Unique fund ID | Joins to PostgreSQL funds |
| **fund_name** | VARCHAR | Display name | For readability |
| **nav** | DECIMAL(15,4) | Price per unit | Always positive, 4 decimals |
| **change_percent** | DECIMAL(5,2) | Daily % change | Shows volatility |
| **total_assets** | DECIMAL(15,2) | Total AUM (USD) | Fund size indicator |

#### Business Logic

**NAV Calculation:**
```
NAV = (Total Assets - Total Liabilities) / Outstanding Units
```

**Category-Specific Patterns:**
- **EQUITY:** High volatility (±3% daily), base ~$50
- **DEBT:** Low volatility (±0.5% daily), base ~$100
- **HYBRID:** Medium volatility (±1.5% daily), base ~$75
- **MONEY_MARKET:** Minimal volatility (±0.05%), base ~$1,000
- **INDEX:** Market-like (±2% daily), base ~$100

**Fund Size Categories:**
- Small: $10M-$50M (newer/niche)
- Mid: $50M-$250M (established)
- Large: $250M-$1B (flagship)

#### Business Use Cases

| Use Case | Formula | Value |
|----------|---------|-------|
| Portfolio Valuation | `SUM(units × nav)` | Customer balance |
| Performance | `(nav_today - nav_30d) / nav_30d` | Returns |
| Volatility | `STDDEV(change_percent)` | Risk |
| AUM Trends | `SUM(total_assets) by date` | Growth |

#### Data Quality Rules
✅ NAV must be > 0  
✅ All 100 funds in each file  
✅ Change % between -10% and +10%  
✅ AUM should not jump >20% day-over-day

---

### 2. Market Data (Benchmark Indices)

**📁 Location:** `market-data/`  
**📄 File Pattern:** `market_YYYY-MM-DD.json`  
**📊 Indices tracked:** 4 (S&P 500, NASDAQ, DJIA, Russell 2000)  
**🔄 Update:** Daily (weekdays only have real data)

#### Business Purpose
Market indices provide **performance benchmarks** for evaluating funds.

**Example:** If a fund returns +5%:
- S&P 500 is +8% → Fund underperformed
- S&P 500 is +2% → Fund outperformed (+3% alpha)

This enables **relative performance analysis** and **market context**.

#### File Structure
```json
{
  "date": "2026-02-02",
  "indices": {
    "SP500": {
      "ticker": "^GSPC",
      "value": 4875.23,
      "change_percent": 0.45
    },
    "NASDAQ": {
      "ticker": "^IXIC",
      "value": 15234.56,
      "change_percent": 0.67
    }
  }
}
```

#### Index Definitions

| Index | Full Name | Represents | Benchmark For |
|-------|-----------|------------|---------------|
| **SP500** | S&P 500 | 500 largest US companies | Large-cap equity funds |
| **NASDAQ** | NASDAQ Composite | Tech-heavy index | Growth/tech funds |
| **DJIA** | Dow Jones Ind. Avg | 30 blue-chip stocks | Value/dividend funds |
| **RUSSELL2000** | Russell 2000 | Small-cap companies | Small-cap funds |

#### Field Definitions

| Field | Type | Definition | Notes |
|-------|------|------------|-------|
| **date** | DATE | Market date | Trading days only |
| **ticker** | VARCHAR | Yahoo Finance symbol | e.g., ^GSPC |
| **value** | DECIMAL | Index closing value | Market-determined |
| **change_percent** | DECIMAL | Daily % change | Can be negative |
| **note** | VARCHAR | Status | "Market closed" for weekends |

#### Data Source
- **Primary:** Yahoo Finance API (real data)
- **Update:** After 4:00 PM EST market close
- **Weekends:** Files exist but show "Market closed"

#### Business Use Cases

| Use Case | Calculation | Insight |
|----------|-------------|---------|
| Alpha | `fund_return - index_return` | Outperformance |
| Beta | `COVAR(fund, index) / VAR(index)` | Market sensitivity |
| Context | Join by date | "Fund up 2%, market down 1%" |
| Correlation | `CORR(fund, index)` | Risk factor |

#### Benchmark Assignment

| Fund Category | Primary Benchmark | Why |
|---------------|------------------|-----|
| EQUITY | S&P 500 | Broad market |
| DEBT | N/A | Bonds not equities |
| HYBRID | S&P 500 (weighted) | Equity portion |
| INDEX | Corresponding index | Direct tracking |
| MONEY_MARKET | N/A | Short-term |

#### Quality Rules
✅ Weekday files have numeric values  
✅ Weekend files show "Market closed"  
✅ Values in historical ranges  
✅ Change % aligns with value movement

---

### 3. Fund Metadata (Product Catalog)

**📁 Location:** `fund-metadata/`  
**📄 File:** `fund_attributes.csv` (single file)  
**📊 Records:** 100 (all funds)  
**🔄 Update:** Monthly

#### Business Purpose
Extended fund attributes for:
- Customer fact sheets
- Compliance disclosures  
- Product comparison tools
- Marketing materials
- Suitability assessments

#### File Structure
```csv
fund_code,fund_name,fund_category,inception_date,manager_name,investment_objective,minimum_investment,dividend_frequency,risk_rating,tax_treatment
FND0001,Growth Equity Fund,EQUITY,2020-01-15,John Smith,"Long-term capital growth",5000.00,Quarterly,High,Taxable
```

#### Field Definitions

| Field | Type | Business Definition | Example |
|-------|------|---------------------|---------|
| **fund_code** | VARCHAR | Unique ID | FND0001 |
| **fund_name** | VARCHAR | Marketing name | Growth Equity Fund |
| **fund_category** | VARCHAR | Asset class | EQUITY |
| **inception_date** | DATE | Launch date | 2020-01-15 |
| **manager_name** | VARCHAR | Portfolio manager | John Smith |
| **investment_objective** | TEXT | Strategy | "To achieve growth..." |
| **minimum_investment** | DECIMAL | Entry threshold | 5000.00 |
| **dividend_frequency** | VARCHAR | Distribution | Quarterly |
| **risk_rating** | VARCHAR | Risk class | High |
| **tax_treatment** | VARCHAR | Tax category | Taxable |

#### Business Classifications

**Risk Rating:**
- **Low:** Money market, short bonds (preservation)
- **Moderate:** Balanced, investment-grade (growth)
- **High:** Equity, high-yield (appreciation)

**Minimum Investment:**
- Retail: $1K-$5K (individuals)
- Premium: $5K-$10K (affluent)
- Institutional: $10K+ (institutions)

**Dividend Frequency:**
- **None:** Growth funds (reinvest)
- **Monthly:** Income funds (retirees)
- **Quarterly:** Balanced (most common)
- **Annual:** Tax-efficient

**Tax Treatment:**
- **Taxable:** Standard (most funds)
- **Tax-Exempt:** Municipal bonds
- **Tax-Deferred:** Retirement accounts

#### Business Use Cases

| Use Case | Query | Value |
|----------|-------|-------|
| Product Finder | Filter by risk/category | Help investors choose |
| Suitability | `WHERE min_inv <= balance` | Eligible products |
| Income Planning | `WHERE dividend = 'Monthly'` | Income seekers |
| Diversification | Group by risk/category | Portfolio construction |

#### Regulatory Compliance
Supports:
- SEC Form N-1A (prospectus)
- FINRA suitability rules
- Advertising regulations

#### Quality Rules
✅ All fund_codes exist in PostgreSQL  
✅ inception_date in past  
✅ minimum_investment > 0  
✅ risk_rating in (Low, Moderate, High)  
✅ No nulls in required fields

---

### 4. Ratings Data (External Quality Assessment)

**📁 Location:** `ratings-data/`  
**📄 File Pattern:** `fund_ratings_YYYY-MM.json`  
**📊 Coverage:** ~70% of funds (realistic)  
**🔄 Update:** Monthly (1st of month)

#### Business Purpose
Independent fund ratings provide:
- Third-party validation
- Competitive benchmarking
- Marketing support
- Risk identification
- Due diligence data

#### File Structure
```json
{
  "report_date": "2026-02-01",
  "vendor": "Morningstar Analytics (Simulated)",
  "ratings": [
    {
      "fund_code": "FND0001",
      "fund_name": "Growth Equity Fund",
      "overall_rating": 4,
      "risk_score": 7.5,
      "return_score": 8.2,
      "expense_score": 6.8,
      "category_rank": 15,
      "analyst_outlook": "Positive"
    }
  ]
}
```

#### Field Definitions

| Field | Type | Scale | Interpretation |
|-------|------|-------|----------------|
| **overall_rating** | INT | 1-5 stars | 5 = Elite, 1 = Poor |
| **risk_score** | DECIMAL | 1.0-10.0 | 10 = Highest risk |
| **return_score** | DECIMAL | 1.0-10.0 | 10 = Best returns |
| **expense_score** | DECIMAL | 1.0-10.0 | 10 = Lowest fees |
| **category_rank** | INT | 1-100 | 1 = Top of category |
| **analyst_outlook** | VARCHAR | Pos/Neut/Neg | Forward view |

#### Rating Methodology

**Star System:**
- **5 Stars:** Top 10% (Elite)
- **4 Stars:** Next 22.5% (Above average)
- **3 Stars:** Middle 35% (Average)
- **2 Stars:** Next 22.5% (Below average)
- **1 Star:** Bottom 10% (Poor)

**Component Scores (1-10):**

**Risk Score:**
- 1-3: Very Low (money market)
- 4-6: Moderate (balanced)
- 7-10: High (equity, high-yield)

**Return Score:**
- 1-3: Underperforming
- 4-6: Meeting expectations
- 7-10: Outperforming

**Expense Score:**
- 1-3: High fees (>1.5%)
- 4-6: Average (0.75%-1.5%)
- 7-10: Low fees (<0.75%)

**Category Rank:**
- 1-20: Top quintile (institutional quality)
- 21-40: Above average
- 41-60: Average
- 61-80: Below average
- 81-100: Bottom quintile (red flag)

**Analyst Outlook:**
- **Positive:** Expect improvement
- **Neutral:** Status quo expected
- **Negative:** Performance concerns

#### Business Use Cases

| Use Case | Filter | Purpose |
|----------|--------|---------|
| Screening | `rating >= 4` | Quality filter |
| Risk Check | `risk_score > 8` | High-risk alert |
| Marketing | `rating = 5` | Promote top funds |
| Competitive | `ORDER BY rank` | Market position |

#### Business Examples

| Profile | Meaning | Action |
|---------|---------|--------|
| 5-star, Low Risk, Top 5 | **Exceptional** | Core holding |
| 3-star, High Risk, Rank 50 | **Volatile** | Aggressive only |
| 2-star, Rank 85 | **Underperformer** | Consider exit |

#### Coverage Notes
- ~70% rated (realistic - not all funds rated)
- Unrated: Newer funds (<3 years) or small (<$10M)
- Update: Monthly (more frequent than real quarterly)

#### Quality Rules
✅ overall_rating in 1-5  
✅ Scores in 1.0-10.0  
✅ Rank in 1-100  
✅ Outlook in valid values  
✅ Rated funds exist in PostgreSQL

---

## Data Lineage & Flow

### End-to-End Architecture

```
SOURCE SYSTEMS
├─ PostgreSQL (Internal)
│  └─ Transactional data
└─ GCS (External)
   ├─ nav-data/ (Fund accounting)
   ├─ market-data/ (Yahoo Finance)
   ├─ fund-metadata/ (Product mgmt)
   └─ ratings-data/ (Analysts)

        ▼ Meltano

SNOWFLAKE RAW
├─ raw.investors
├─ raw.accounts
├─ raw.nav_data
├─ raw.market_indices
└─ raw.ratings

        ▼ dbt

SNOWFLAKE STAGING
├─ stg_investors
├─ stg_nav_data
└─ stg_market_indices

        ▼ dbt

SNOWFLAKE MARTS
├─ dim_funds (SCD Type 2)
├─ fact_nav_daily
├─ fact_market_indices
└─ dim_fund_ratings

        ▼ Superset

DASHBOARDS
├─ Portfolio Performance
├─ Fund Comparison
└─ Market Analysis
```

---

## Business Rules & Data Quality

### Critical Business Rules

1. **NAV Integrity:**
   - NAV must be positive
   - Daily changes within reasonable bounds
   - All funds must have NAV each day

2. **Market Data Consistency:**
   - Real data on trading days only
   - Weekend/holiday files acceptable with "closed" note
   - Values must be in historical ranges

3. **Rating Coverage:**
   - 60-80% coverage is expected
   - All rated funds must exist in master
   - Ratings must be current (within 90 days)

4. **Metadata Completeness:**
   - No nulls in regulatory fields
   - Minimum investment must be positive
   - Inception date must precede first NAV

### Data Quality Framework

| Check | Frequency | Action if Failed |
|-------|-----------|------------------|
| NAV completeness | Daily | Alert fund ops |
| Price reasonableness | Daily | Manual review |
| Market data freshness | Daily | Check API |
| Rating currency | Monthly | Request update |
| Metadata consistency | Monthly | Reconcile with master |

---

## Sample Analytics & Queries

### 1. Portfolio Valuation

```sql
-- Calculate total portfolio value
SELECT 
    i.investor_name,
    a.account_number,
    SUM(h.units_held * n.nav) as portfolio_value
FROM accounts a
JOIN investors i ON a.investor_id = i.investor_id
JOIN fund_holdings h ON a.account_id = h.account_id
JOIN nav_data n ON h.fund_code = n.fund_code 
    AND n.date = CURRENT_DATE
GROUP BY 1, 2;
```

### 2. Fund Performance vs. Benchmark

```sql
-- Compare fund to S&P 500
SELECT 
    f.fund_name,
    n.change_percent as fund_return,
    m.sp500_change as market_return,
    (n.change_percent - m.sp500_change) as alpha
FROM nav_data n
JOIN funds f ON n.fund_code = f.fund_code
JOIN market_indices m ON n.date = m.date
WHERE n.date = CURRENT_DATE
ORDER BY alpha DESC;
```

### 3. Top-Rated Funds

```sql
-- Find 5-star funds in each category
SELECT 
    fm.fund_category,
    r.fund_name,
    r.overall_rating,
    r.category_rank
FROM ratings r
JOIN fund_metadata fm ON r.fund_code = fm.fund_code
WHERE r.overall_rating = 5
ORDER BY fm.fund_category, r.category_rank;
```

### 4. Risk-Adjusted Returns

```sql
-- Calculate Sharpe ratio proxy
SELECT 
    f.fund_code,
    f.fund_name,
    AVG(n.change_percent) as avg_return,
    STDDEV(n.change_percent) as volatility,
    AVG(n.change_percent) / STDDEV(n.change_percent) as risk_adj_return
FROM nav_data n
JOIN funds f ON n.fund_code = f.fund_code
WHERE n.date >= DATEADD(day, -90, CURRENT_DATE)
GROUP BY 1, 2
ORDER BY risk_adj_return DESC;
```

---

## Appendix: File Inventory

### Complete File List

**nav-data/ (101 files)**
```
nav_2025-10-25.csv through nav_2026-02-02.csv
Daily files for 100 days
Each file: ~100 records (one per fund)
Average file size: ~10 KB
```

**market-data/ (71 files)**
```
market_2025-10-27.csv through market_2026-02-02.json
Weekday files only (no weekends initially, but all days present)
Each file: 4 index records
Average file size: ~1 KB
```

**fund-metadata/ (1 file)**
```
fund_attributes.csv
100 records (all funds)
File size: ~15 KB
```

**ratings-data/ (4 files)**
```
fund_ratings_2025-11.json
fund_ratings_2025-12.json
fund_ratings_2026-01.json
fund_ratings_2026-02.json
Monthly files
Each file: ~70 ratings (70% coverage)
Average file size: ~5 KB
```

---

## Contact & Support

**For Business Questions:**
- Product Management: fund-metadata/, ratings interpretation
- Fund Operations: NAV data accuracy, pricing
- Investment Research: ratings methodology, benchmarks

**For Technical Questions:**
- Data Engineering: file formats, ingestion issues
- Data Architecture: integration patterns, lineage
- Data Quality: validation rules, exception handling

**For Access & Permissions:**
- GCP Administrator: GCS bucket access
- Data Governance: PII handling, compliance

---

**Document Version:** 1.0  
**Prepared for:** Morgan Stanley POC Review  
**Review Date:** February 2, 2026  
**Next Review:** March 2, 2026 (post-Snowflake integration)