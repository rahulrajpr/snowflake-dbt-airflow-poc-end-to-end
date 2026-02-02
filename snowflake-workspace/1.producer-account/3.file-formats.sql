
USE DATABASE FINANCE_DB;

--==============================
-- SEE ALL FILE FORMATES
--==============================

SHOW FILE FORMATS

--==============================
-- CREATE THE FILE FORMATS
--==============================

CREATE OR REPLACE FILE FORMAT POSTGRES_RAW.POSTGRES_RAW_TABLES_CSV_FORMAT
TYPE = CSV
FIELD_DELIMITER = ',';

--==============================
-- VERIFY FILE FORMATS
--==============================

SELECT CURRENT_DATABASE(),CURRENT_SCHEMA()

SHOW FILE FORMATS


