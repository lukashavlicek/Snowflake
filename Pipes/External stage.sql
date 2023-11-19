-- Query external stage
-- Stage is created in AWS Pipes worksheet
-------------------------------------------------

-- CSV FORMAT 

USE STAGEDB;

list @my_s3_stage;

SELECT 
$1, 
$2,
$3,
$4
FROM @my_s3_stage
(file_format => 'MyCsvForStages');

-- CREATE EXTERNAL TABLE
create or replace external table csv_table_external (
Name string AS (value:c1::varchar),
Area number AS (value:c2::number),
AvgDepth number AS (value:c3::number),
DeepestPoint number AS (value:c4::number)
)
with location = @my_s3_stage
auto_refresh = false
file_format = 'MyCsvForStages'


-- query from the external table
select * from csv_table_external

-- value of each row + filename
select value, metadata$filename from csv_table_external

--------------------------------------------

-- JSON FORMAT

DESC INTEGRATION s3_lha_bucket;

-- Create a stage for the S3
CREATE or REPLACE STAGE my_s3_stage_json
  STORAGE_INTEGRATION = s3_lha_bucket
  URL = 's3://snowflake-lha-playground/data/json'
  FILE_FORMAT = (type = 'json');

desc stage my_s3_stage_json;

-- list the stage
list @my_s3_stage_json;

-- Query the external stage
SELECT 
arr.value:Name::string as Name,
arr.value:"Area (sq. km)"::number as Area,
arr.value:"Deepest Point (m)"::number as DeepestPoint,
arr.value:"Average Depth (m)"::number as AverageDepth
FROM @my_s3_stage_json,
lateral flatten ( input => $1 ) arr;


-- Create external table
create or replace table json_table_s3 (
json_col variant
)

select * from json_table_s3;

-- Create pipe to automate data ingestion from s3 to snowflake
create or replace pipe my_s3_pipe_json auto_ingest = true as
copy into json_table_s3
from @my_s3_stage_json
on_error = continue;

show pipes;

select * from json_table_s3;

-- check the status of the pipe
-- executionState: RUNNING
select SYSTEM$PIPE_STATUS('stagedb.public.my_s3_pipe_json');


-- Copy data from stage to external table
-- Copy statement does not suppor flatten function
/*copy into json_table_external
FROM (SELECT 
arr.value:Name::string as Name,
arr.value:"Area (sq. km)"::number as Area,
arr.value:"Deepest Point (m)"::number as DeepestPoint,
arr.value:"Average Depth (m)"::number as AverageDepth
FROM @my_s3_stage_json (file_format => (type = 'json')),
lateral flatten ( input => $1 )  arr)*/






