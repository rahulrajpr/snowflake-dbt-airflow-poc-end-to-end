
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
-- CREATE SHARE
--====================================

CREATE OR REPLACE SHARE POC_SHARE;

--====================================
-- GRANT THE PERSMISSIONS
--====================================

GRANT USAGE ON DATABASE POC_DATABASE TO SHARE POC_SHARE;

GRANT USAGE ON SCHEMA POC_DATABASE.POC_SCHEMA TO SHARE POC_SHARE;

GRANT SELECT ON ALL TABLES IN SCHEMA POC_DATABASE.POC_SCHEMA TO SHARE POC_SHARE;

--====================================
-- SET THE CONSUMER
--====================================

ALTER SHARE POC_SHARE 
	SET SHARE_RESTRICTIONS = FALSE,
	ACCOUNT = 'FCQXOSZ.GL87342',
	SECURE_OBJECTS_ONLY = TRUE,
	COMMENT = 'OVERWRITING THE RESTRICIONS, PROVIDER - BUSINESS CRITICAL AND CONSUMER - STANDARD'
	
--====================================
-- VERIFYING THE SHARES
--====================================
	
SHOW SHARES;

DESCRIBE SHARE POC_SHARE;
