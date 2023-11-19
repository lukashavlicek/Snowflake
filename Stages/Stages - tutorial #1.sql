-------------------------------------------------
-- STAGES
-- temp storages -> must be removed after the files are loaded to destinational location
-- There are three types of internal stages
-- File format needs to be created before the copy command
-- 1) User stage (automatically created for every snowflake user) 
-- Cannot be shared between users not even account admin can access
-- Careful: All the worksheets are also contained in the user stage (do not remove them)
-- PUT file://c:/myData/myCSV.csv @~;
-- list @~ (tilde)

-- 2) Table stage
-- each table has a stage allocated to it, can be accessed by multiple users
-- has no storage limit
-- PUT file://c:/myData/myCSV.csv @%myTable;
-- List @%

-- 3) Named Internal Stages
-- actual defined database objects in Snowflake
-- users with granted CREATE STAGE rights can create them
-- can be created as temporary stages (dropped at the end of the session)
-- list @stage_name;

-------------------------------------------------
-- PUT COMMNAD (SnowSQL CLI)
-- PUT command in Snowflake uploads (i.e. stages) data files from a local folder on client machine into one of the internal Snowflake stages
-------------------------------------------------
-- GET COMMAND (SnowSQL CLI)
-- GET Command in Snowflake downloads data files from one of the internal Snowflake stages to a local folder on a client machine
-------------------------------------------------
--COPY INTO COMMAND (SQL)
-- COPY INTO command in Snowflake loads data from staged files to an existing table and vice versa. 
-- copy into EMPLOYEE from @~/my_stage_dir/input.csv;
-------------------------------------------------

-- Create new csv file format
CREATE OR REPLACE file format MyCsvForStages
type = 'CSV'
field_delimiter = ','
skip_header = 1;

-- Create new named stage with the file format
-- You can force specific file format with the stage during creation of the stage
-- If file format is not specified, it needs to be specified when copying data from stage into a table
create stage my_tdf_csv_stage file_format = MyCsvForStages

--------------------------------------------------------------------------
-- csv file loaded from local folder through put command from snowsql cli
--------------------------------------------------------------------------

-- list content of a stage
list @my_tdf_csv_stage;
show stages;

-- query data from internal stage (before loading into a table)
-- CSV file have $1, $2, $3 notation for each column
-- Parquet files have only $1 associated with each key
SELECT 
metadata$filename as filename,
metadata$file_row_number as rownum,
t.$2 as rank, -- col1
t.$3 as rider, -- col2
t.$4 as rider_no, -- col3
t.$5 as team,
t.$6 as time,
t.$7 as time_gap,
t.$13 as gap_seconds
FROM @my_tdf_csv_stage (file_format => 'MyCsvForStages') t;


-- create snowflake destination table for data loading
CREATE OR REPLACE table TdF_Women_Results (
Rank int,
Rider string,
Rider_no int,
Team string,
Time string,
Time_gap string,
Gap_in_seconds int)

desc table TdF_Women_Results;

-- load data from internal stage into the destination table (COPY INTO)
COPY INTO TdF_Women_Results
FROM 
(SELECT 
t.$2 as rank, -- col1
t.$3 as rider, -- col2
t.$4 as rider_no, -- col3
t.$5 as team,
t.$6 as time,
t.$7 as time_gap,
t.$13 as gap_seconds
FROM @my_tdf_csv_stage (file_format => 'MyCsvForStages') t);

-- query the destination table
SELECT * FROM TdF_Women_Results;


-- remove @@my_tdf_csv_stage;
-- drop stage @@my_tdf_csv_stage;

---------------------------------

CREATE OR REPLACE table demo_table (
RowNum int,
Rider string)

SELECT COUNT(*) FROM @my_tdf_csv_stage;

COPY INTO demo_table
FROM
(
select
metadata$file_row_number as soubor,
$3 as rider
from @my_tdf_csv_stage
);

SELECT * from demo_table



