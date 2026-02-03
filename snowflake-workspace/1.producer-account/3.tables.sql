
USE DATABASE FINANCE_DB;

--==============================
-- SHOW TABLES
--==============================

USE SCHEMA GCS_RAW;

SHOW TABLES;

--==============================
-- CREATE TABLES
--==============================


-------------------------------
-->> LANDING TABLES FROM GCS
-------------------------------

-->> FUND METADATA, CSV SOURCE

CREATE OR REPLACE TABLE GCS_RAW.FUND_METADATA 
	(
    FUND_CODE               VARCHAR(20),
    FUND_NAME               VARCHAR(200),
    FUND_CATEGORY           VARCHAR(50),
    INCEPTION_DATE          DATE,
    MANAGER_NAME            VARCHAR(100),
    INVESTMENT_OBJECTIVE    VARCHAR,
    MINIMUM_INVESTMENT      NUMBER(12,2),
    DIVIDEND_FREQUENCY      VARCHAR(50),
    RISK_RATING             VARCHAR(20),
    TAX_TREATMENT           VARCHAR(50),
    
      -- Metadata columns
    
    SOURCE_FILE_NAME               VARCHAR(500),
    SOURCE_FILE_ROW_NUMBER         NUMBER,
    FILE_CONTENT_KEY               VARCHAR(500),
    FILE_LAST_MODIFIED_TIMESTAMP   TIMESTAMP_NTZ,
    LOAD_TIMESTAMP                 TIMESTAMP_NTZ,
    LOAD_SOURCE                    VARCHAR(100)
	);

--->> NAV_DATA, CSV SOURCE

CREATE OR REPLACE TABLE GCS_RAW.NAV_DATA 
	(
    DATE                DATE,
    FUND_CODE           VARCHAR(20),
    FUND_NAME           VARCHAR(200),
    NAV                 NUMBER(18,4),
    CHANGE_PERCENT      NUMBER(6,2),
    TOTAL_ASSETS        NUMBER(20,2),
    
    -- Metadata columns
    
    SOURCE_FILE_NAME               VARCHAR(500),
    SOURCE_FILE_ROW_NUMBER         NUMBER,
    FILE_CONTENT_KEY               VARCHAR(500),
    FILE_LAST_MODIFIED_TIMESTAMP   TIMESTAMP_NTZ,
    LOAD_TIMESTAMP                 TIMESTAMP_NTZ,
    LOAD_SOURCE                    VARCHAR(100)
	);

--->> MARKET DATA (RAW JSON)
    
CREATE OR REPLACE TABLE GCS_RAW.MARKET_DATA_RAW_JSON
	(
    JSON_CONTENT                    OBJECT,          
    SOURCE_FILE_NAME                VARCHAR,
    SOURCE_FILE_ROW_NUMBER          NUMBER,
    FILE_CONTENT_KEY                VARCHAR,
    FILE_LAST_MODIFIED_TIMESTAMP    TIMESTAMP_LTZ,
    LOAD_TIMESTAMP                  TIMESTAMP_LTZ,
    LOAD_SOURCE                     VARCHAR
	);


--->> RATINGS (RAW JSON)

CREATE OR REPLACE TABLE GCS_RAW.RATINGS_DATA_RAW_JSON
	(
    JSON_CONTENT                    OBJECT,          
    SOURCE_FILE_NAME                VARCHAR,
    SOURCE_FILE_ROW_NUMBER          NUMBER,
    FILE_CONTENT_KEY                VARCHAR,
    FILE_LAST_MODIFIED_TIMESTAMP    TIMESTAMP_LTZ,
    LOAD_TIMESTAMP                  TIMESTAMP_LTZ,
    LOAD_SOURCE                     VARCHAR
	);

--==============================
-- VARIFYING TABLES
--==============================

USE SCHEMA GCS_RAW;

SHOW TABLES;

--==============================
-- END OF THE DOCUMENT
--==============================