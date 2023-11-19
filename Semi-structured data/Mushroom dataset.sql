--------------------------------------------------------------------------
-- JSON
-- {} = Object -> key:value pairs
-- [] = Array -> index
-- JSON is a combination of arrays [] and objects {}
-- root element followed by child elements
-- JSON does not support date data types - it presents them as strings
-- Simple / Complex / Sparse (null values) / Inconsistent JSON
---------------------------------------------------------------------------

CREATE OR REPLACE TABLE TdF_JSON (json_col variant);

DESC TABLE TdF_JSON;

-- parse_json()
-- cannot take more than 8MB of data

INSERT INTO TdF_JSON 
SELECT parse_json('{
  "tour_de_france": {
    "year": 2023,
    "top_ten_male_winners": [
      {
        "name": "Ethan Smith",
        "age": 28,
        "country": "United States",
        "club": "Team Cyclone",
        "general_classification_time": "85:32:14",
        "stages_won": 3
      },
      {
        "name": "Luis Hernandez",
        "age": 31,
        "country": "Spain",
        "club": "Team Velocity",
        "general_classification_time": "85:36:20",
        "stages_won": 2
      },
      {
        "name": "Matteo Bianchi",
        "age": 29,
        "country": "Italy",
        "club": "Team Alpino",
        "general_classification_time": "85:45:55",
        "stages_won": 4
      },
      {
        "name": "Oliver Andersen",
        "age": 27,
        "country": "Denmark",
        "club": "Team Nordic",
        "general_classification_time": "85:48:02",
        "stages_won": 0
      },
      {
        "name": "Carlos Rodriguez",
        "age": 30,
        "country": "Colombia",
        "club": "Team Andes",
        "general_classification_time": "85:55:45",
        "stages_won": 3
      },
      {
        "name": "Hiroshi Tanaka",
        "age": 32,
        "country": "Japan",
        "club": "Team Sakura",
        "general_classification_time": "86:02:16",
        "stages_won": 0
      },
      {
        "name": "Alexis Dubois",
        "age": 28,
        "country": "France",
        "club": "Team Éclat",
        "general_classification_time": "86:10:30",
        "stages_won": 1
      }
    ]
  }
}
')

SELECT * FROM TdF_JSON;

--------------------------------------
-- colon notation vs brackets notation
--------------------------------------
SELECT 
json_col:tour_de_france.top_ten_male_winners[0].name::string as Winner,
json_col:tour_de_france.top_ten_male_winners[0].general_classification_time::string as GC_Time
FROM TdF_JSON
UNION ALL
SELECT 
json_col['tour_de_france']['top_ten_male_winners'][0]['name']::string as Winner,
json_col['tour_de_france']['top_ten_male_winners'][0]['general_classification_time']::string as GC_Time
FROM TdF_JSON;


-- check_json() function to check the validity of json files
-- returns null if valid
select check_json(json_col) from TdF_JSON;

-- TODO: json_extract_path_text() function

---------------------------------------------------
------------- FLATTEN FUNCTION---------------------
---------------------------------------------------
-- Table function
-- Could be applied to other data type than JSON (arrays)
-- Flatten function needs to be used especially with nested function where the structure is not consistent and index-based approach cannot be used

-- Simple flatten applied on an ARRAY

-- create an array
SELECT array_construct('Tadej Pogacar', 'MVDP', 'Jonas Vingegaard', 'Wout Van Aert')

-- applaying flatten function to transfer array into table form
SELECT * FROM TABLE(flatten ( input => array_construct('Tadej Pogacar', 'MVDP', 'Jonas Vingegaard', 'Wout Van Aert')))

-- applaying flatten on a select from a table
CREATE OR REPLACE transient table ToursWon (
    ID int,
    Winner string,
    Tour array
);

INSERT INTO ToursWon 
SELECT 1, 'UAE', array_construct('Flenders');

INSERT INTO ToursWon 
SELECT 2, 'Jumbo', array_construct('Tour de France', 'Giro', 'Vuelta')

SELECT * 
FROM ToursWon TW,
lateral flatten( input => TW.Tour) ToursFlattened;

-- select winner for each tour
SELECT TW.Winner,
        ToursFlattened.Value::string as Tour
FROM ToursWon TW,
lateral flatten( input => TW.Tour, mode => 'array') ToursFlattened;

------------------------------------------------------------
-- Flatten function applied on object (JSON object)
-- Flatten function basically generates a temporary table that flattens the object and joins it together with the table

SELECT object_construct('UAE', 'Tadej Pogacar', 'Alpecin', 'MVDP', 'Jumbo', 'Jonas Vingegaard')

-- applaying flatten function to transfer object into table form
SELECT * FROM TABLE(flatten ( input => object_construct('UAE', 'Tadej Pogacar', 'Alpecin', 'MVDP', 'Jumbo', 'Jonas Vingegaard')))

SELECT key, value::string as Club FROM TABLE(flatten ( input => object_construct('UAE', 'Tadej Pogacar', 'Alpecin', 'MVDP', 'Jumbo', 'Jonas Vingegaard'), mode => 'object'))

-- table with object data type
CREATE OR REPLACE transient table ToursWonObject (
    ID int,
    Winner string,
    Tour object
);

INSERT INTO ToursWonObject 
SELECT 1, 'UAE', object_construct('Flenders', 'Netherlands');

INSERT INTO ToursWonObject
SELECT 2, 'Jumbo', object_construct('Tour de France', 'France', 'Giro', 'Italia', 'Vuelta', 'Spain')

SELECT * FROM ToursWonObject 

SELECT TW.Winner AS Team,
        ToursFlattened.key as Tour,
        ToursFlattened.Value::string as Country
FROM ToursWonObject TW,
lateral flatten( input => TW.Tour) ToursFlattened;

---------------------------------------------------
--------- flatten object & array toghether---------
---------------------------------------------------

-- table contains both object and array column
CREATE OR REPLACE TABLE JumboVisma (
    club string,
    manager string,
    country string,
    leader object,
    grandTours array
)

INSERT INTO JumboVisma
SELECT 'JumboVisma', 
        'Richard Plugge', '
         Netherlands',
         object_construct('Leader One', 'Jonas Vingegaard', 'Leader Two', 'Primoz Roglic'),
         array_construct('Giro de Italia', 'Tour de France', 'La Vuelta')


SELECT * FROM JumboVisma



-- Use lateral flatten for both object and array
SELECT club, manager, country, leader.key, leader.value::string as teamLeader, GT.index, GT.value::string as GrandTour
FROM JumboVisma JV,
lateral flatten ( input => JV.leader) leader,
lateral flatten ( input => JV.grandTours) GT
WHERE GT.Index = 1 -- Tour de France

----------------------------------------
----flatten function applied on JSON----
----------------------------------------

-- outer paramter = LEFT JOIN, includes empty elements

SELECT 
flattenedTable.value:name::string as name,
flattenedTable.value:club::string as club
FROM TDF_JSON TDF,
lateral flatten ( input => TDF.json_col:tour_de_france.top_ten_male_winners) flattenedTable;


------- JSON MUSHROOMS PRACTISE SAMPLE -----------

CREATE OR REPLACE TABLE JsonMushrooms ( json_col variant);

INSERT INTO JsonMushrooms
SELECT parse_json('{
  "poisonous_mushrooms": [
    {
      "name": "Amanita phalloides",
      "common_names": ["Death Cap", "Destroying Angel"],
      "description": "One of the deadliest mushrooms, responsible for numerous poisonings and fatalities.",
      "toxicity": "Highly toxic",
      "appearance": {
        "cap": "Pale to olive green, often with white patches",
        "gills": "White",
        "stem": "White with a bulbous base"
      },
      "habitat": ["Deciduous forests", "Grassy areas"],
      "symptoms": ["Nausea", "Vomiting", "Diarrhea", "Liver and kidney damage"],
      "growth_environments": ["Under trees", "Near roots"],
      "countries": ["Europe", "North America", "Asia"]
    },
    {
      "name": "Conocybe filaris",
      "common_names": ["Deadly Galerina"],
      "description": "Contains the same deadly toxins as the Amanita species.",
      "toxicity": "Highly toxic",
      "appearance": {
        "cap": "Conical, rusty brown",
        "gills": "Rusty brown",
        "stem": "Rusty brown with a ring"
      },
      "habitat": ["Grassy areas", "Lawns", "Mulched gardens"],
      "symptoms": ["Abdominal pain", "Nausea", "Vomiting", "Liver failure"],
      "growth_environments": ["Lawns", "Mulched areas"],
      "countries": ["North America", "Europe", "Australia"]
    },
    {
      "name": "Gyromitra esculenta",
      "common_names": ["False Morel"],
      "description": "Contains a toxin that can cause severe liver damage even after cooking.",
      "toxicity": "Toxic",
      "appearance": {
        "cap": "Irregular and brain-like, wrinkled",
        "gills": "Absent",
        "stem": "Irregular, often attached at the top"
      },
      "habitat": ["Coniferous forests", "Wooded areas"],
      "symptoms": ["Nausea", "Vomiting", "Diarrhea", "Dizziness", "Seizures"],
      "growth_environments": ["Forest floors", "Near fallen trees"],
      "countries": ["North America", "Europe", "Asia"]
    },
    {
      "name": "Cortinarius rubellus",
      "common_names": ["Deadly Webcap"],
      "description": "Contains a toxin that damages the kidneys and can be lethal.",
      "toxicity": "Highly toxic",
      "appearance": {
        "cap": "Deep red to orange-brown",
        "gills": "Purple-brown",
        "stem": "Orange with a web-like veil"
      },
      "habitat": ["Mossy areas", "Decaying wood"],
      "symptoms": ["Nausea", "Vomiting", "Kidney failure", "Coma"],
      "growth_environments": ["Damp areas", "Forest undergrowth"],
      "countries": ["Europe", "North America"]
    }
  ]
}');

SELECT * FROM JsonMushrooms;

SELECT flattenedShrooms.*
FROM JsonMushrooms as Shrooms,
lateral flatten ( input => json_col:poisonous_mushrooms) flattenedShrooms

-- lateral faltten applied on commonnames array, and countries array together with where clause
SELECT PM.value:name::string as latinName,
CommonNames.value::string as commonName,
Countries.value::string as country,
PM.value:description::string as description,
PM.value:appearance.cap::string as cap,
PM.value:appearance.gills::string as gills,
PM.value:appearance.stem::string as stem
FROM JsonMushrooms as Shrooms,
lateral flatten ( input => json_col:poisonous_mushrooms) PM,
lateral flatten ( input => PM.value:common_names) CommonNames,
lateral flatten ( input => PM.value:countries) Countries
where Country = 'Europe' AND
PM.value:toxicity::string = 'Highly toxic';





