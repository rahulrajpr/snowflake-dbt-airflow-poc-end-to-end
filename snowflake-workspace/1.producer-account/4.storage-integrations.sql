
USE DATABASE FINANCE_DB;

--==============================
-- SEE ALL STORAGE INTEGRATIONS
--==============================

SHOW STORAGE INTEGRATIONS

--==============================
-- CREATESTORAGE INTEGRATIONS
--==============================

-- DROP STORAGE INTEGRATION MELTANO_STAGING_STORAGE_INT;

CREATE OR REPLACE STORAGE INTEGRATION MELTANO_STAGING_STORAGE_INT
ENABLED = TRUE
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'GCS'
STORAGE_ALLOWED_LOCATIONS = ('gcs://finance-data-landing-bucket-poc/meltano-staging')

-- ussed snoflake_staging role in the gcp to give access along with 'STORAGE_GCP_SERVICE_ACCOUNT' value as principal

DESCRIBE STORAGE INTEGRATION MELTANO_STAGING_STORAGE_INT;

--==============================
-- VERIFY INTGERATIONS
--==============================

SHOW STORAGE INTEGRATIONS

--==============================
-- END OF THE DOCUMENT
--==============================


