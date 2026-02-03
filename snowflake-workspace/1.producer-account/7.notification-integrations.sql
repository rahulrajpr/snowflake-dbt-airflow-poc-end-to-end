
USE DATABASE FINANCE_DB;

--==============================
-- SEE ALL STORAGE INTEGRATIONS
--==============================

SHOW NOTIFICATION INTEGRATIONS

--==================================
-- CREATE NOTIFIACTION INTEGRATION
--=================================

-- subscription name created in gcp :
-- projects/snowflake-dbt-poc/subscriptions/subscription-finance-data-landing-bucket

CREATE OR REPLACE NOTIFICATION INTEGRATION GCS_FINANCE_DATA_LANDING_NOTIFICATION_INT
ENABLED = TRUE
TYPE = QUEUE
NOTIFICATION_PROVIDER = GCP_PUBSUB
GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/snowflake-dbt-poc/subscriptions/subscription-finance-data-landing-bucket'

--

DESCRIBE NOTIFICATION INTEGRATION GCS_FINANCE_DATA_LANDING_NOTIFICATION_INT

-- took the GCP_PUBSUB_SERVICE_ACCOUNT value TO be used AS principle FOR the GCS PubSub subscription
-- Role used will be PubSub Admin Role

-- Similary go to admin > Grant Access > copy-paste the above value in principle and choose role Monitoring Virwer

--=================================
-- VERIFY
--=================================

SHOW NOTIFICATION INTEGRATIONS

--=================================
-- END OF THE DOCUMENT
--=================================

