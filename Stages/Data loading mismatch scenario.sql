USE STAGEDB;

-- crate table that contains only two columns, .csv contains four columns
CREATE OR REPLACE TABLE Oceans_col_mismatch (
Ocean string,
Area number
--AverageDepth number,
--DeepestPoint number
)

SELECT * FROM Oceans_col_mismatch;

-- Putting data into user-stage
-- SnowSql: PUT file://C:\Snowflake\load_temp\Oceans_1.csv @~/CsvMismatch;

list @~/CsvMismatch;

-------------------------------------------------
---- error_on_column_count_mismatch = true ------
-- create csv file format with error_on_column_count_mismatch set to true
-- if there is an column mismatch error, loading into a table will error
-- it will load only data that fit the table definition

create or replace file format csv_col_mismatch_ff
type = 'csv'
compression = 'none'
field_delimiter = ','
error_on_column_count_mismatch = true
skip_header = 1;

copy into Oceans_col_mismatch
from @~/CsvMismatch
file_format = csv_col_mismatch_ff
on_error = 'abort_statement';

-------------------------------------------------
---- error_on_column_count_mismatch = false -----
-- create csv file format with error_on_column_count_mismatch set to false
-- it will load only data that fit the table definition

create or replace file format csv_col_mismatch_false
type = 'csv'
compression = 'none'
field_delimiter = ','
error_on_column_count_mismatch = false
field_optionally_enclosed_by = '\042'
skip_header = 1;

copy into Oceans_col_mismatch
from @~/CsvMismatch/Oceans_1.csv.gz
file_format = csv_col_mismatch_false
on_error = 'abort_statement'
pattern = '.*[.]csv';

select * from Oceans_col_mismatch;
-- Validating loaded data
-- does not work together with data transformation 

USE STAGEDB;

create or replace table csv_validation_mode (
city string,
country string,
capital boolean
);

select * from csv_validation_mode;

-- load data into user-stage
-- SnowSql: PUT file://C:\Snowflake\load_temp\csv_validation_mode.csv @~/csv-validaton-mode;

list @~/csv-validaton-mode;
select $1, $2, $3 from @~/csv-validaton-mode;

--remove @~/csv-validaton-mode;

-- load data to table with validation mode, capital does not fit the boolean data type of the table (Yes vs True)
-- if validation mode is on and there is an issue, data is not loaded and an error is displayed
copy into csv_validation_mode
from @~/csv-validaton-mode
file_format = ( type = 'csv')
on_error = 'continue'
validation_mode = return_errors

-- we can also try to load and test only certain amount of rows with validation_mode = return_2_rows
copy into csv_validation_mode
from @~/csv-validaton-mode
file_format = ( type = 'csv')
on_error = 'continue'
validation_mode = return_2_rows

select * from csv_validation_mode;

-- if validation_mode is not on, and on_error = 'continue' table is loaded by only with data without any error
-- status = PARTIALLY_LOADED
copy into csv_validation_mode
from @~/csv-validaton-mode
file_format = ( type = 'csv')
on_error = 'continue'
--validation_mode = return_2_rows

select * from csv_validation_mode;
