
USE SnowflakeScripting;

-- create a dummy table for testing purposes
CREATE or REPLACE transient TABLE test_data (id int, profit number);

INSERT INTO test_data VALUES (1, 200), (2, 400), (3, 600), (4, 800);

-- 4 recors inserted
select * from test_data;


-----------------------------------
-- Cursors
-----------------------------------

declare
    id int default 0;
    minimum_id int default 2;
    c1 cursor for select id from test_data where id > ?;
begin
    open c1 using (minimum_id);
    fetch c1 into id;
    return id;
end;


--------------------------------------
-- resultsets
--------------------------------------
-- dynamis sql

declare
    res resultset;
    col_name varchar;
    select_statement varchar;
begin
    col_name := 'id';
    select_statement := 'SELECT ' || col_name || ' FROM test_data';
    res := (execute immediate :select_statement);
    return table(res);
end;


--------------------------------------
-- error handling
-- EXCEPTION block
--------------------------------------

-- Other exception
begin
    select * from non_existent_table;
exception
    when other then
    return object_construct('Error_type', 'MY_EXCEPTION',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
end;


-- Custom exception defined
declare
    my_exception exception (-20002, 'My CUSTOM exception message!');
begin
    let should_raise_exception := true;
    if (should_raise_exception) then
        raise my_exception;
    end if;
exception
    when my_exception then
    return object_construct('Error_type', 'MY_EXCEPTION',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
end;
    


