
CREATE or REPLACE DATABASE TasksDB;

USE TasksDB;

-- create table
CREATE or REPLACE table CyclingClubs (
   ID int,
   Club string,
   Country string,
   insert_time timestamp default current_timestamp()
);

-- crate sequence
CREATE or REPLACE sequence cc_seq
    start 1
    increment 1
    comment = 'Sequence for CyclingClubs table ID'

-- create a (root) TASK
-- Task Limitations:
-- 1) Only one SQL statement is allowed for each task
-- 2) Schedule paremeter only takes minutes
-- 3) The minimum value is 1 minute, you cannot go below this number. Maximum value is total 8 days (11520 minutes).
-- 4) Once task is created, it is in suspended state and needs to be resumed manually by the user (permission needs to be granted or by accountAdmin)

CREATE or REPLACE task cc_task
    warehouse = compute_wh
    schedule = '1 minute'
        as
    insert into CyclingClubs (id, Club, Country, insert_time)
    values (cc_seq.nextval, 'Some Club', 'Some Country', current_date())

-- suspended state
show tasks;
desc task cc_task;

-- resume the task
alter task cc_task resume;

-- task has been resumed
show tasks;

-- query tables
select * from CyclingClubs;

-- information table
select * from table(information_schema.task_history()) where name = 'CC_TASK' order by scheduled_time;

-- suspend the task again
alter task cc_task suspend;
show tasks;

---------------------------
-- CRON STYLE scheduling

-- create a task that runs every 5min but only every Saturday
CREATE or REPLACE task cc_Sat_5min_task
    warehouse = compute_wh
    schedule = 'USING CRON 5 * * * SAT Europe/Berlin'
        as
    insert into CyclingClubs (id, Club, Country, insert_time)
    values (cc_seq.nextval, 'Some Club', 'Some Country', current_date())

-- resume the task
show tasks;
alter task cc_Sat_5min_task resume;


-- query tables
select * from CyclingClubs;

-- information table
select * from table(information_schema.task_history()) where name = 'CC_SAT_5MIN_TASK' order by scheduled_time;

alter task cc_Sat_5min_task suspend;
