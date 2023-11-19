USE DATABASE JSON_DB;

-- CREATE EXTERNAL STAGE AND LOAD S3 BUCKET
CREATE OR REPLACE stage my_ext_stage
    url = 's3://awsglue-datasets/examples/us-legislators/all'
    comment = 'US Legislator Data';

DESC stage my_ext_stage;

-- there are 6 files in total
list @my_ext_stage;

-- create a file format before accessing the data
CREATE OR REPLACE file format json_file_format
    type = 'json'
    compression = 'AUTO'


DESC file format json_file_format;

-- run query on external table
SELECT * FROM @my_ext_stage/areas.json (file_format => 'json_file_format') t;

-- $ notation to access the data from external stage
SELECT 
t.$1:id::string as id,
t.$1:name::string as name,
t.$1:type::string as type
FROM @my_ext_stage/areas.json (file_format => 'json_file_format') t;


-- creating an internal table with variant column
CREATE OR REPLACE TABLE area_json (area_json variant);

COPY INTO area_json from @my_ext_stage/areas.json
    file_format = 'json_file_format'


SELECT * FROM area_json;

-- accessing the data from internal stage
SELECT 
area_json:id::string as id,
area_json:name::string as name,
area_json:type::string as type
FROM area_json;
