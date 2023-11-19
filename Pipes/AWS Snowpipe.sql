-- PIPES & INTEGRATION & EXTERNAL STAGE Objects
-- + AWS S3 Bucket
--------------------------
-- Used resources:
-- Snowflake ASW integration documentation: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
-- Youtube tutorial: https://www.youtube.com/watch?v=b5iqAiiHvPI&list=LL&index=4
--------------------------

USE STAGEDB;

-- A storage integration is a Snowflake object that stores a generated identity and access management (IAM) user for your S3 cloud storage, along with an optional set of allowed or blocked storage locations (i.e. buckets)

CREATE or REPLACE STORAGE INTEGRATION s3_lha_bucket
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::074287118213:role/snowflakeRole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-lha-playground/', 's3://snowflake-lha-playground/csv/','s3://snowflake-lha-playground/json/');

-- Execute the DESCRIBE INTEGRATION command to retrieve the ARN for the AWS IAM user that was created automatically for your Snowflake account:
-- STORAGE_AWS_IAM_USER_ARN
-- STORAGE_AWS_EXTERNAL_ID

DESC INTEGRATION s3_lha_bucket;

-- Create an external (i.e. S3) stage that references the storage integration you created

CREATE or REPLACE STAGE my_s3_stage_csv
  STORAGE_INTEGRATION = s3_lha_bucket
  URL = 's3://snowflake-lha-playground/data/csv'
  FILE_FORMAT = (type = 'csv');

  
desc stage my_s3_stage;

list @my_s3_stage;


-- Create table 
CREATE OR REPLACE TABLE OceansCsvS3Table (
Ocean string,
Area number,
AverageDepth number,
DeepestPoint number
)

-- Create pipe to automate data ingestion from s3 to snowflake
create or replace pipe my_s3_pipe auto_ingest = true as
copy into OceansCsvS3Table
from @my_s3_stage
on_error = continue;

show pipes;

-- check the table if the data is loaded by snowpipe
select * from OceansCsvS3Table;




