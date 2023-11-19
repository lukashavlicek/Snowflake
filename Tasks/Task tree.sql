
USE TasksDB;

-- create table
CREATE or REPLACE table TaskTree_Table (
   ID int,
   TaskLevel string,
   Club string,
   Country string,
   insert_time timestamp default current_timestamp()
);

-- crate sequence
CREATE or REPLACE sequence taskTree_seq
    start 1
    increment 1
    comment = 'Sequence for TaskTree_Table table ID'

-- create a parent task
CREATE or REPLACE task parent_task
    warehouse = compute_wh
    schedule = '1 minute'
        as
    insert into TaskTree_Table (id, TaskLevel, Club, Country, insert_time)
    values (taskTree_seq.nextval, 'PARENT Task Level', 'Some Club', 'Some Country', current_date())

-- create child tasks using AFTER keyword (after parent task)
CREATE or REPLACE task child_task01
warehouse = compute_wh
after parent_task
    as
insert into TaskTree_Table (id, TaskLevel, Club, Country, insert_time)
values (taskTree_seq.nextval, 'Child Level 1', 'Some Club 1', 'Some Country 1', current_date())

CREATE or REPLACE task child_task02
warehouse = compute_wh
after parent_task
    as
insert into TaskTree_Table (id, TaskLevel, Club, Country, insert_time)
values (taskTree_seq.nextval, 'Child Level 1 (02)', 'Some Club 2', 'Some Country 2', current_date())


CREATE or REPLACE task child_task03
warehouse = compute_wh
after child_task01
    as
insert into TaskTree_Table (id, TaskLevel, Club, Country, insert_time)
values (taskTree_seq.nextval, 'Child Level 2', 'Some Club 3', 'Some Country 3', current_date())


-- only parent task needs to be resumed
alter task parent_task resume;

show tasks;

-- query tables
select * from TaskTree_Table order by id desc;

-- information table
select * from table(information_schema.task_history()) where name = 'PARENT_TASK' order by scheduled_time;


---- resume taks tree
alter task child_task01 resume;
alter task child_task02 resume;
alter task child_task03 resume;
alter task parent_task resume;

-- suspent task tree
alter task parent_task suspend;
alter task child_task01 suspend;
alter task child_task02 suspend;
alter task child_task03 suspend;


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
