--===============================
-- SHOW DATABASES 
--===============================

SHOW DATABASES 

--===============================
-- CREATE DATABASES AND SCHEMA
--===============================

CREATE DATABASE IF NOT EXISTS POC_DATABASE;
USE DATABASE POC_DATABASE;

CREATE SCHEMA IF NOT EXISTS POC_DATABASE.POC_SCHEMA;
USE SCHEMA POC_DATABASE.POC_SCHEMA;

SELECT CURRENT_ACCOUNT(), CURRENT_DATABASE(), CURRENT_SCHEMA();

--===============================
-- VERIFY THE SAME
--===============================

SHOW DATABASES

SHOW SCHEMAS

--===============================
-- END OF DATABASES AND SCHEMAS
--===============================

