CREATE OR REPLACE TABLE json_table (json_data VARIANT);

DESC TABLE json_table;

-- Select individual values from inserted JSON data
-- 1
SELECT 
json_data:quiz.sport.q1.question::string as question,
json_data:quiz.sport.q1.options[2]::string as answer
FROM json_table

--2
SELECT 
json_data:quiz.maths.q1.question::string as question,
json_data:quiz.maths.q1.options[2]::string as answer
FROM json_table

--3
SELECT 
json_data:quiz.maths.q1.question::string as question,
json_data:quiz.maths.q1.options[2]::number my_answer,
json_data:quiz.maths.q1.answer::number as json_key_answer
FROM json_table



-- Insert JSON data into a table
-- parse_json function must be used to validace the json data and insert it into a variant data type column
INSERT INTO json_table (json_data)
SELECT parse_json('{
    "quiz": {
        "sport": {
            "q1": {
                "question": "Which one is correct team name in NBA?",
                "options": [
                    "New York Bulls",
                    "Los Angeles Kings",
                    "Golden State Warriros",
                    "Huston Rocket"
                ],
                "answer": "Huston Rocket"
            }
        },
        "maths": {
            "q1": {
                "question": "5 + 7 = ?",
                "options": [
                    "10",
                    "11",
                    "12",
                    "13"
                ],
                "answer": "12"
            },
            "q2": {
                "question": "12 - 8 = ?",
                "options": [
                    "1",
                    "2",
                    "3",
                    "4"
                ],
                "answer": "4"
            }
        }
    }
}')


SELECT * FROM json_table;



