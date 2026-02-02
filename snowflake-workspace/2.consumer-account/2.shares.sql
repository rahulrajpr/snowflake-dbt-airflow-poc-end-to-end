--=========================================
-- SEE THE CONNECTION AND SESSION DETAILS
--=========================================

SELECT CURRENT_REGION(), CURRENT_ACCOUNT(), CURRENT_ROLE(), CURRENT_USER(), CURRENT_WAREHOUSE();

--====================================
-- ROUTE TO ROLES AND PERMISSIONS
--====================================

USE ROLE ACCOUNTADMIN;

USE DATABASE POC_DATABASE;

USE SCHEMA POC_SCHEMA;

SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();

--====================================
-- SEE THE INBOUND SAHRES
--====================================

SHOW SHARES;

DESC SHARE UAHYRSV.ZJ51694.POC_SHARE; -- MAY OR MAY NOT WORK BASED ON THE PERMISIONS

--====================================
-- CREATING DATABASE FROM SHARE
--====================================

CREATE OR REPLACE DATABASE POC_DATABASE_SHARED
FROM SHARE UAHYRSV.ZJ51694.POC_SHARE

--====================================
-- VERIYING SHARE DATABASE
--====================================

SELECT *
FROM POC_DATABASE_SHARED.POC_SCHEMA.TEST_INTO

--====================================
-- END OF SHARES
--====================================
