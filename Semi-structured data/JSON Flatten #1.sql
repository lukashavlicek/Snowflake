
-- CREATE TABLE WITH A JSON COLUMN
CREATE OR REPLACE TABLE json_table (id INT, json_data VARIANT);

-- VARIANT DATA TYPE = SEMI-STRUCTURED DATA

-- Insert JSON data into a table
-- parse_json function must be used to validace the json data and insert it into a variant data type column
INSERT INTO json_table (json_data)
SELECT parse_json(Column1) from values
('{"Name":"Lukas", "Age": 28, "City":"Brno"}'),
('{"Name":"Petee", "Age": 29, "City":"Brno"}')

SELECT * FROM json_table

desc table json_table

-- Inserting multiple json values into a table
INSERT INTO json_table (id, json_data)
SELECT 1, parse_json('{"Name":"Lukas", "Age": 29, "City":"Brno"}')

-- Query the JSON data
SELECT json_data:Name as Name,
json_data:Age as Age,
json_data:City as City
FROM json_table

-- Using the Flatten function

CREATE TABLE json_table_flatten CLONE json_table;

INSERT INTO json_table_flatten (id, json_data)
SELECT 1, parse_json('{"Name":{"First":"Lukas","Last":"Hav"},"Age": 29, "City":"Brno"}')

SELECT * FROM json_table_flatten

SELECT * FROM json_table_flatten,
TABLE (flatten(json_table_flatten.json_data))

-- Converting JSON data to specific data types
INSERT INTO json_table (id, json_data)
SELECT 2, parse_json('{"Name":{"First":"Lukas","Last":"Hav"},"Age": 29, "City":"Brno", "DE": true}')

SELECT * FROM json_table

SELECT json_data:Name as Name,
TO_NUMBER(json_data:Age) as Age,
TO_BOOLEAN(json_data:DE) as Is_DE
FROM json_table
WHERE ID = 2

-- LATERAL FLATTEN
CREATE OR REPLACE TABLE json_table (id INT, json_data VARIANT);

INSERT INTO json_table (id, json_data)
SELECT 1, parse_json('{"Name":"Lukas", "Age": 29, "City":"Brno", "phones": 
                                                                 [{"type":"work", "phone": "00-11"}, 
                                                                 {"type":"home", "phone": "00-22"}]}')

SELECT * FROM json_table

SELECT DISTINCT json_data:Name as Name,
TO_NUMBER(json_data:Age) as Age,
phone.value:"type"::string as phone_type,
phone.value:"phone"::string as phone_number
FROM json_table as src,
LATERAL FLATTEN (input => src.json_data:phones) phone


-- Extract the first phone number
SELECT DISTINCT json_data:Name::string as Name,
TO_NUMBER(json_data:Age) as Age,
json_data:phones[0].type::string as phone_type,
json_data:phones[0].phone::string as phone_number
FROM json_table 
