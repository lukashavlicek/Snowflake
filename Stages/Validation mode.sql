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
