-- STREAMS & CHANGE DATA CAPTURE

CREATE or REPLACE DATABASE StreamsDB;

USE StreamsDB;

-- create test table
CREATE TABLE Oceans_table CLONE STAGEDB.PUBLIC.OCEANS;

-- query the test table
select * from Oceans_table;

-- create stream object on the Oceans_stream table
CREATE or REPLACE STREAM Oceans_stream ON TABLE Oceans_table;

-- query the stream object
-- Stream object captures all the data changes on the source table + three metadata columns about each action
select * from Oceans_stream;


-- alter the source table and perform DML operation (Insert, Update or Delete)
UPDATE Oceans_table
set DeepestPoint = 10912
where Ocean = 'Pacific Ocean'

DELETE FROM Oceans_table WHERE Ocean = 'Southern Ocean'

INSERT INTO Oceans_table VALUES ('Inserted Ocean', 2892729, 2484, 74490)

-- see the change via stream object
select * from Oceans_table;
select * from Oceans_stream;

-- Insert and Delete operations capture each one row (Action = Insert or Delete)
-- Update operations captures two rows Deete and then Insert


-- create append_only stream objects (tracks only inserted columns)
-- Each source table can have multiple stream objects associated
CREATE or REPLACE STREAM Oceans_append_stream ON TABLE Oceans_table append_only =  true;

INSERT INTO Oceans_table VALUES ('Another Inserted Ocean', 2892729, 2484, 74490)

select * from Oceans_stream;
select * from Oceans_append_stream;

show streams;
desc stream Oceans_stream;
desc stream Oceans_append_stream; -- mode = append_only

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