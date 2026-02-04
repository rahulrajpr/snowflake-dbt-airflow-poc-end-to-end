
USE DATABASE FINANCE_DB;

--==============================
-- SEE ALL STORAGE INTEGRATIONS
--==============================

SHOW STORAGE INTEGRATIONS

--==============================
-- CREATESTORAGE INTEGRATIONS
--==============================

-- GCS STORAGE -> finance-data-landing-bucket-poc

CREATE OR REPLACE STORAGE INTEGRATION GCS_FINANCE_DATA_LANDING_STORAGE_INT
ENABLED = TRUE
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'GCS'
STORAGE_ALLOWED_LOCATIONS = ('gcs://finance-data-landing-bucket-poc')

-- ussed snoflake_staging role in the gcp to give access along with 'STORAGE_GCP_SERVICE_ACCOUNT' value as principal

DESCRIBE STORAGE INTEGRATION GCS_FINANCE_DATA_LANDING_STORAGE_INT;


--==============================
-- VERIFY INTGERATIONS
--==============================

SHOW STORAGE INTEGRATIONS

--==============================
-- END OF THE DOCUMENT
--==============================


