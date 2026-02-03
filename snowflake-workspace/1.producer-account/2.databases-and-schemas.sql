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

SHOW SCHEMAS;

CREATE SCHEMA IF NOT EXISTS FINANCE_DB.POSTGRES_RAW;

CREATE SCHEMA IF NOT EXISTS FINANCE_DB.GCS_RAW;

--===============================
-- VERIFY THE SAME
--===============================

SHOW DATABASES

SHOW SCHEMAS

--===============================
-- END OF DATABASES AND SCHEMAS
--===============================

