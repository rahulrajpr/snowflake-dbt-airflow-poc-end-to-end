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

---------------------------------------------------------
--> SCHEMA FOR RAW DATA FROM POSTGRES AND APIS (IN GCP)
---------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS FINANCE_DB.POSTGRES_RAW;

CREATE SCHEMA IF NOT EXISTS FINANCE_DB.GCS_RAW;

--------------------------------------------------------------
--> UNIFIED SCHEMA FOR ALL RAW TABLES WITH MIN TRANSFORMATION
-------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS FINANCE_DB.LANDING;

--------------------------------------------------------------
--> SCHEMA , USED DBT DEVELOPMENT
-------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS FINANCE_DB.STAGING;

CREATE SCHEMA IF NOT EXISTS FINANCE_DB.INTERMEDIATE;

CREATE SCHEMA IF NOT EXISTS FINANCE_DB.ANALYTICS;

--===============================
-- VERIFY THE SAME
--===============================

SHOW DATABASES

SHOW SCHEMAS

--===============================
-- END OF DATABASES AND SCHEMAS
--===============================