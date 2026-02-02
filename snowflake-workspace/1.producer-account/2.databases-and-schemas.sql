--===============================
-- SHOW DATABASES 
--===============================

SHOW DATABASES;

SHOW SCHEMAS;

--===============================
-- CREATE DATABASES AND SCHEMA
--===============================

CREATE DATABASE IF NOT EXISTS FINANCE_DB;

USE DATABASE FINANCE_DB;

CREATE SCHEMA IF NOT EXISTS FINANCE_DB.POSTGRES_RAW;

USE SCHEMA FINANCE_DB.POSTGRES_RAW;

SELECT CURRENT_ACCOUNT(), CURRENT_DATABASE(), CURRENT_SCHEMA();

--===============================
-- VERIFY THE SAME
--===============================

SHOW DATABASES

SHOW SCHEMAS

--===============================
-- END OF DATABASES AND SCHEMAS
--===============================

