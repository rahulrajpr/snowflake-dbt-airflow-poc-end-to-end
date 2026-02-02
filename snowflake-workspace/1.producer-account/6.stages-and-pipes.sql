--===============================
-- SHOW THE COMPONENTS 
--===============================

-- INTEGRATIONS

SHOW STORAGE INTEGRATIONS;

SHOW NOTIFICATION INTEGRATIONS;

-- FILE FORMATS 

SHOW FILE FORMATS;

-- STAGES

SHOW STAGES

-- PIPES

SHOW PIPES 

--====================================
-- CREATING THE STORAGE INTEGRATIONS 
--====================================

CREATE  STORAGE INTEGRATION IF NOT EXISTS GCS_TEST_BUCKET_STORAGE_INT
ENABLED = TRUE
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'GCS'
STORAGE_ALLOWED_LOCATIONS = ('gcs://rahul-test-bucket/test-folder');

DESC INTEGRATION GCS_TEST_BUCKET_STORAGE_INT;

--====================================
-- CREATING THE FILE FORMAT
--====================================

CREATE FILE FORMAT IF NOT EXISTS CSV_COMMA
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1;

--====================================
-- CREATING EXTRNAL STAGE
--====================================

CREATE OR REPLACE STAGE GCS_TEST_BUCKET_STAGE
STORAGE_INTEGRATION = GCS_TEST_BUCKET_STORAGE_INT
FILE_FORMAT = CSV_COMMA
URL = 'gcs://rahulraj-test-bucket/test-folder';

-- List the files in the staging location

LIST @GCS_TEST_BUCKET_STAGE;

-- Verifing the stage

SELECT METADATA$FILENAME, COUNT(*)
FROM @GCS_TEST_BUCKET_STAGE
GROUP BY METADATA$FILENAME;

-- ===================================================
-- RUN THE FOLLOWING COMMAND ON Google Cloud CLI
-- ===================================================

-- reference : https://sfc-gh-dwilczak.github.io/tutorials/clouds/google/storage/

gsutil notification create -t test-bucket-notification -f json gs://rahulraj-test-bucket/

-- crate subscription in the notication get the subscription name that will be used for the notification integration

--====================================
-- CREATE NOTIFICATION INTEGRATION
--====================================

CREATE OR REPLACE NOTIFICATION INTEGRATION GCS_TEST_BUCKET_NOTIFICATION_INT
ENABLED = TRUE
TYPE = QUEUE
NOTIFICATION_PROVIDER = GCP_PUBSUB
GCP_PUBSUB_SUBSCRIPTION_NAME = 'PROJECTS/PROJECT-541C492C-1E5B-48D2-B85/SUBSCRIPTIONS/TEST-BUCKET-NOTIFICATION-SUBSCRIPTION';

-- Decribring the notification integration

DESC INTEGRATION GCS_TEST_BUCKET_NOTIFICATION_INT;

-- GET the GCP_PUBSUB_SERVICE_ACCOUNT Value the above

-- ===================================================
-- MAKE CHANGES IN THE GCP NOTIFIACTION (SUB/PUB)
-- ===================================================

-- Go to GCP Sub/Pub and go the subsription and add principle with the GCP_PUBSUB_SERVICE_ACCOUNT Value 

-- ===================================================
-- CREATE PIPE
-- ===================================================

CREATE OR REPLACE PIPE TEST_PIPE
INTEGRATION = GCS_TEST_BUCKET_NOTIFICATION_INT
AUTO_INGEST = TRUE
AS
COPY INTO TEST_INTO_PIPE_TABLE
FROM
(
SELECT
    $1  AS COUNTRY,
    $2  AS COUNTRY_TYPE,
    $3  AS EU_MEMBER,
    $4  AS EUROZONE_MEMBER,
    $5  AS YEAR,
    $6  AS QUARTER_NUM,
    $7  AS QUARTER,
    $8  AS PRICE_INDEX,
    $9  AS QUARTERLY_CHANGE_PCT,
    $10 AS YEARLY_CHANGE_PCT,
    $11 AS PRICE_CHANGE_SINCE_2015_PCT,
    $12 AS DATA_QUALITY,

    METADATA$FILENAME         AS SOURCE_FILE_NAME,
    METADATA$FILE_ROW_NUMBER AS SOURCE_ROW_NUMBER,
    CURRENT_TIMESTAMP()      AS LOAD_TIME

FROM @GCS_TEST_BUCKET_STAGE
);

-- ===================================================
-- VERIFY PIPES
-- ===================================================

SHOW PIPES;

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY;

--

SELECT * FROM TEST_INTO_PIPE_TABLE;

SELECT SOURCE_FILE_NAME, COUNT(1) AS CNT
FROM TEST_INTO_PIPE_TABLE
GROUP BY SOURCE_FILE_NAME;

-- ===================================================
-- END OF PIPES
-- ===================================================
