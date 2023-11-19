
CREATE schema rb;

USE StreamsDB;
USE schema rb;

-- create a file format to avoid column mismatch issue while loading the data
create or replace file format csv_col_mismatch_ff
type = 'csv'
compression = 'none'
field_delimiter = ','
error_on_column_count_mismatch = false
skip_header = 1;


-- create raw/transient table
CREATE or REPLACE transient table road_bikes_raw (
    ID int,
    Brand string,
    Model string,
    Frame string,
    FrameSize string,
    WheelSize string,
    NumberOfGears string,
    Price number
);


show tables;

select * from road_bikes_raw;

-- create dim table 
CREATE or REPLACE table road_bikes_dim (
    ID int,
    Brand string,
    Model string,
    Frame string,
    FrameSize string,
    WheelSize string,
    NumberOfGears string,
    Price number
);


-- first insert to load the data
INSERT INTO road_bikes_dim SELECT * FROM road_bikes_raw;

-- create stream object on a raw table
create or replace stream stream_bikes_raw on table road_bikes_raw;

show streams;

-- update data in raw table
select * from road_bikes_raw;

update road_bikes_raw
set frame = 'Aluminum'
where brand = 'Giant'

delete from road_bikes_raw where id = 18;

insert into road_bikes_raw (id, brand, model, frame, framesize, wheelsize, numberofgears, price)
values (19, 'Standerd', 'Standerd-T', 'Carbon', '54', '28', '24', 4500);

-- total six rows in a stream
select * from stream_bikes_raw;

-- merge data to consume data from the stream and update changes to the dim table
MERGE INTO road_bikes_dim dim USING stream_bikes_raw str on dim.id = str.id
WHEN MATCHED AND metadata$action = 'DELETE' AND metadata$isupdate = 'FALSE'
THEN DELETE
WHEN MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'TRUE'
THEN UPDATE SET dim.brand = str.brand, dim.model = str.model, dim.frame = str.frame, dim.framesize = str.framesize, dim.wheelsize = str.wheelsize, dim.numberofgears = str.numberofgears, dim.price = str.price
WHEN NOT MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'FALSE'
THEN INSERT (id, brand, model, frame, framesize, wheelsize, numberofgears, price) VALUES (str.id, str.brand, str.model, str.frame, str.framesize, str.wheelsize, str.numberofgears, str.price);

-- stream has been consumned = empty
select * from stream_bikes_raw;

-- dim table is updated
select * from road_bikes_dim;
USE StreamsDB;

--create employees_raw table
CREATE OR REPLACE TABLE EMPLOYEES_RAW (
    ID int,
    NAME VARCHAR(50),
    SALARY NUMBER
);

--Insert three records into table
INSERT INTO EMPLOYEES_RAW VALUES (101,'Tony',25000);
INSERT INTO EMPLOYEES_RAW VALUES (102,'Chris',55000);
INSERT INTO EMPLOYEES_RAW VALUES (103,'Bruce',40000);

select * from EMPLOYEES_RAW;

--create employees table
CREATE OR REPLACE TABLE EMPLOYEES (
    ID NUMBER,
    NAME VARCHAR(50),
    SALARY NUMBER
);

--Inserting initial set of records from raw table
INSERT INTO EMPLOYEES SELECT * FROM EMPLOYEES_RAW;

select * from EMPLOYEES;

--create stream
CREATE OR REPLACE STREAM MY_STREAM ON TABLE EMPLOYEES_RAW;

show streams;

--Insert two records
INSERT INTO EMPLOYEES_RAW VALUES (104,'Clark',35000);
INSERT INTO EMPLOYEES_RAW VALUES (105,'Steve',30000);--Update two records
UPDATE EMPLOYEES_RAW SET SALARY = '50000' WHERE ID = '102';
UPDATE EMPLOYEES_RAW SET SALARY = '45000' WHERE ID = '103';

-- Recognize different operations
--INSERT
SELECT * FROM MY_STREAM
WHERE metadata$action = 'INSERT'
AND metadata$isupdate = 'FALSE';

--UPDATE
SELECT * FROM MY_STREAM
WHERE metadata$action = 'INSERT'
AND metadata$isupdate = 'TRUE';

--DELETE
SELECT * FROM MY_STREAM
WHERE metadata$action = 'DELETE'
AND metadata$isupdate = 'FALSE';


select * from EMPLOYEES;
select * from MY_STREAM;


-- = CONSUMING THE STREAM DATA
-- Finally we can use a MERGE statement with the Stream using these filters to perform the insert, update and delete operations on target table as shown below.

MERGE INTO EMPLOYEES a USING MY_STREAM b ON a.ID = b.ID
WHEN MATCHED AND metadata$action = 'DELETE' AND metadata$isupdate = 'FALSE'
THEN DELETE
WHEN MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'TRUE'
THEN UPDATE SET a.NAME = b. NAME, a.SALARY = b.SALARY
WHEN NOT MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'FALSE'
THEN INSERT (ID, NAME, SALARY) VALUES (b.ID, b.NAME, b.SALARY)
;

-- Now that we have consumed the stream in a DML transaction, the stream now do not return any records and is set to new offset. So if you need to consume for multiple downstream --- systems from a stream, build multiple streams on the table, one for each consumer.

select * from MY_STREAM;

-- STALENESS
-- A stream becomes stale when its offset is outside of the data retention period for its source table. When a stream becomes stale, the historical data for the source table is -- no longer accessible, including any unconsumed change records.
-- The period is extended to the stream’s offset, up to a maximum of 14 days by default, regardless of the Snowflake edition for your account.