-- Time travel vs Fail Safe
-- Time travel is set in snowflake, fail safe is lost data that can no longer be recovered through time travel (Snowflake charges additional cost => data lost prevention)


CREATE DATABASE TT;

use TT;

-- create table
-- maximum data retention time for standard edition is 1 day (default settings), for enterprise edition it is up to 60 days
create or replace table tt_table_1 (
    ID int,
    Value string
)
data_retention_time_in_days = 1
;

show tables; -- retention time value

-- insert some test data into the table
insert into tt_table_1
values (1, 'Pes'), (2, 'Kocka'), (3, 'Ptak'), (4, 'Tygr');

-- 4 rows
select * from tt_table_1;


-- delete one row
delete from tt_table_1 where ID = 4; 
-- Query ID: 01af1eef-0000-7e06-0000-5c5900066096

-- update one row
update tt_table_1 set value = 'Zelva' where ID = 3;
-- Query ID: 01af1ef0-0000-7e06-0000-5c590006609e

-- use extensice sql to query tim travel data based on specific Query ID
-- at keyword
select * from tt_table_1
at(statement => '01af1ef0-0000-7e06-0000-5c590006609e');

-- before keyword
select * from tt_table_1
before(statement => '01af1ef0-0000-7e06-0000-5c590006609e');


-- droping & undroping (you can drop & undrop tables, schemas, database)

drop table tt_table_1; -- dropped

-- oops need it back
undrop table tt_table_1; -- restored

select * from tt_table_1;

drop database tt;

undrop database tt;


create table tt_table_1_clone clone tt_table_1;
select * from tt_table_1_clone
drop table tt_table_1_clone


