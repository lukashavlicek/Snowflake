CREATE OR REPLACE DATABASE JSON_DB;

USE JSON_DB;

CREATE OR REPLACE TABLE json_table (json_data variant);

DESC TABLE json_table;

-- 1st record
INSERT INTO json_table 
SELECT parse_json(
'{
    employee: {
    "name": "Lukas",
    "age": 28,
    "height": 5.11,
    "married": true,
    "has_kids": true,
    "stock_options": null,
    "email": "luke@future.au",
    "phone": [
      "98383-98303",
      "8048-84804"
    ],
    "Address": {
     "street": "Svatopluka Cecha",
     "city": "Brno",
     "state": "CZ"
        }
      }
    }'
    )


-- 2nd record
INSERT INTO json_table 
SELECT parse_json(
'{
    employee: {
    "name": "Peter",
    "age": 29,
    "height": 5.11,
    "married": true,
    "has_kids": true,
    "stock_options": null,
    "email": "petee@future.au",
    "phone": [
      "98383-98303",
      "8048-84804"
    ],
    "Address": {
     "street": "Kocianka",
     "city": "Brno",
     "state": "CZ"
        }
      }
    }'
    )

    
SELECT * FROM json_table;

-- Extract each element
SELECT 
json_data:employee.name::string as Name,
json_data:employee.email::string as Email,
json_data:employee.married as Married,
json_data:employee.phone[0]::string as phone_1,
json_data:employee.phone[1]::string as phone_2,
json_data:employee.Address.street::string as street,
json_data:employee.Address.city::string as city,
json_data:employee.Address.state::string as state
FROM json_table


-- Insert JSON data into relational table
-- Create sequence to generate IDs for PK and FK relationship
CREATE OR REPLACE SEQUENCE emp_seq
    start 1
    increment 1
    comment = 'employee seqeuence';


CREATE OR REPLACE SEQUENCE phone_seq
    start 1
    increment 1
    comment = 'phone seqeuence';


CREATE OR REPLACE SEQUENCE address_seq
    start 1
    increment 1
    comment = 'address seqeuence';


-- Create tables
CREATE OR REPLACE TABLE employee (
emp_pk integer default emp_seq.nextval,
name string,
age number(3),
is_married boolean,
has_kids boolean,
stock_options integer,
email varchar(100)
);


CREATE OR REPLACE TABLE emp_phones (
phone_pk integer default phone_seq.nextval,
emp_fk number,
phone_type varchar(20),
phone varchar(30)
);


CREATE OR REPLACE TABLE emp_address (
address_pk integer default address_seq.nextval,
emp_fk number,
street_address varchar(200),
city varchar(50),
state varchar(50)
);

-- INSERT Statements

-- EMPLOYEE TABLE
INSERT INTO employee (name, age, is_married, has_kids, stock_options, email)
SELECT 
json_data:employee.name::string as name,
json_data:employee.age::number as age,
json_data:employee.married::boolean as is_married,
json_data:employee.has_kids::boolean as has_kids,
json_data:employee.stock_options::number as stock_options,
json_data:employee.email::string as email
FROM json_table

SELECT * FROM employee;

-- EMPLYOEE PHONE TABLE
INSERT INTO emp_phones (emp_fk, phone_type, phone)
SELECT E.emp_pk,
'home_phone' as home_phone,
JT.json_data:employee.phone[0]::string as home_number
FROM json_table JT
JOIN employee E
ON JT.json_data:employee.email = E.email

UNION ALL

SELECT E.emp_pk,
'work_phone' as work_phone,
JT.json_data:employee.phone[1]::string as work_number
FROM json_table JT
JOIN employee E
ON JT.json_data:employee.email = E.email

SELECT * FROM emp_phones;


-- EMPLOYEE ADDRESS TABLE
INSERT INTO emp_address (emp_fk, street_address, city, state)
SELECT E.emp_pk,
JT.json_data:employee.Address.street::string as street_address,
JT.json_data:employee.Address.city::string as city,
JT.json_data:employee.Address.state::string as state
FROM json_table JT
JOIN employee E
ON JT.json_data:employee.email = E.email


SELECT * FROM emp_address;

-- JOIN THE TABLES TOGETHER
SELECT *
FROM employee E
JOIN emp_address EA
ON E.emp_pk = EA.emp_fk;
