-- Pipes database
USE Pipes;


-- Create a stage for the S3 parent folder
CREATE or REPLACE STAGE my_s3_data_json
  STORAGE_INTEGRATION = s3_lha_bucket
  URL = 's3://snowflake-lha-playground/data/json'
  FILE_FORMAT = (type = 'json');


-- list the stage
list @my_s3_data_json;


-- Create table 1 for beers json
create or replace table s3_data_beers (
json_col variant
);

-- Create table 2 for oceans json
create or replace table s3_data_oceans (
json_col variant
);

-- Create a pipe for json/beers AWS S3 subfolder
create or replace pipe s3_pipe_beers auto_ingest = true as
copy into s3_data_beers
from @my_s3_data_json/beers
file_format = 'my_json_ff'
on_error = continue;


-- Create second pipe for json/oceans AWS S3 subfolder
create or replace pipe s3_pipe_oceans auto_ingest = true as
copy into s3_data_oceans
from @my_s3_data_json/oceans
file_format = 'my_json_ff'
on_error = continue;

-- 2 pipes created
show pipes;

-- pipe status 
select SYSTEM$PIPE_STATUS('pipes.public.s3_pipe_beers');

-- data
select * from s3_data_beers;
select * from s3_data_oceans;

-- create schema for relational data
create schema reldata;

-- create destination table for BEERS by flattening the json
create table pipes.reldata.beers as
select 
b.value as "array",
b.value:id::int as id,
b.value:name::string as name,
b.value:brewery::string as brewery,
b.value:style::string as style,
b.value:abv::number as abv
from s3_data_beers
, lateral flatten (input => json_col:beers) b;

select * from pipes.reldata.beers;

-- create destination table for OCEANS by flattening the json
create table pipes.reldata.oceans as
select 
o.value:Name::string as Name,
o.value:"Area (sq. km)"::number as Area,
o.value:"Deepest Point (m)"::number as DeepestPoint,
o.value:"Average Depth (m)"::number as AverageDepth
from s3_data_oceans
, lateral flatten ( input => $1 ) o;

select * from pipes.reldata.oceans;

