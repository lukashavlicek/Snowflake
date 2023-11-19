CREATE DATABASE SnowflakeScripting;
USE SnowflakeScripting;

create or replace table message(name string, message string);

-- Anonymous block
-- Variable needs to be binded when inserted into a column (:var_name)
-- Exceptions pre-defined vs user-defined (can happen in declaration block, execution block or even exception block itself)

declare 
    var_name string := 'Declaration block';
    result resultset;
begin
    delete from message;
    insert into message values(:var_name,'Variable assigned in the declaration block.');
    var_name := 'Reassignment';
    insert into message values(:var_name, 'Variable reassignment in the execution block.');
        -- nested block with declaration of the same variable
        declare
            var_name string := 'Nested';
        begin
            insert into message values(:var_name,'Variable from the nested block.');
        end;
    result := (select * from message);
    return table(result);
end;


-- Dynamic variables (variable binding)
declare
    var_name string;
begin
    select name into :var_name from message limit 1;
    return :var_name;
end;

