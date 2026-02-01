

create or replace warehouse poc_injestion_warehouse
with 
warehouse_size = small
auto_suspend = 180
auto_resume = True;

--
create or replace warehouse poc_adhoc_warehouse
with 
warehouse_size = small
auto_suspend = 180
auto_resume = True;

--

create or replace database poc_database;
use database poc_database;

create or replace schema poc_database.poc_schema;
use schema poc_database.poc_schema;

select current_account(), current_database(), current_schema();

create or replace storage integration gcs_test_bucket_storage_int
enabled = True
type = external_stage
storage_provider = 'GCS'
storage_allowed_locations = ('gcs://rahulraj-test-bucket/test-folder');

desc integration gcs_test_bucket_storage_int;

create or replace file format csv_comma
type = CSV
field_delimiter = ','
skip_header = 1 ;

create or replace stage gcs_test_bucket_stage
storage_integration = gcs_test_bucket_int
file_format = csv_comma
url = 'gcs://rahulraj-test-bucket/test-folder';

list @gcs_test_bucket_stage;

use warehouse poc_injestion_warehouse;

select count(*)
from @gcs_test_bucket_stage;

create or replace table test_into as 
select 
    $1  AS country,
    $2  AS country_type,
    $3  AS eu_member,
    $4  AS eurozone_member,
    $5  AS year,
    $6  AS quarter_num,
    $7  AS quarter,
    $8  AS price_index,
    $9  AS quarterly_change_pct,
    $10 AS yearly_change_pct,
    $11 AS price_change_since_2015_pct,
    $12 AS data_quality
from @gcs_test_bucket_stage;

select *
from test_into

create or replace table test_into_pipe_table like test_into;

select * 
from test_into_pipe_table;


-- gsutil notification create -t test-bucket-notification -f json gs://rahulraj-test-bucket/

create or replace notification integration gcs_test_bucket_notification_int
enabled = True
type = queue
notification_provider = gcp_pubsub
gcp_pubsub_subscription_name = 'projects/project-541c492c-1e5b-48d2-b85/subscriptions/test-bucket-notification-subscription';

desc integration gcs_test_bucket_notification_int;

--

ALTER TABLE test_into_pipe_table ADD COLUMN source_file_name STRING;
ALTER TABLE test_into_pipe_table ADD COLUMN source_row_number NUMBER;
ALTER TABLE test_into_pipe_table ADD COLUMN load_time TIMESTAMP;

---

create or replace pipe test_pipe
integration = gcs_test_bucket_notification_int
auto_ingest = True
as 
copy into test_into_pipe_table
from 
(
select 
    $1  AS country,
    $2  AS country_type,
    $3  AS eu_member,
    $4  AS eurozone_member,
    $5  AS year,
    $6  AS quarter_num,
    $7  AS quarter,
    $8  AS price_index,
    $9  AS quarterly_change_pct,
    $10 AS yearly_change_pct,
    $11 AS price_change_since_2015_pct,
    $12 AS data_quality,

    METADATA$FILENAME           AS source_file_name,
    METADATA$FILE_ROW_NUMBER   AS source_row_number,
    CURRENT_TIMESTAMP()        AS load_time
    
from @gcs_test_bucket_stage
);

--

SHOW pipes

select * from POC_DATABASE.INFORMATION_SCHEMA.COPY_HISTORY
select * from SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY

select * from POC_DATABASE.INFORMATION_SCHEMA.PIPES

--

select *
from test_into_pipe_table

--

select source_file_name, count(1) as cnt
from test_into_pipe_table
group by source_file_name
