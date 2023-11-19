USE STAGEDB;

-- crate table that to load data to
CREATE OR REPLACE TABLE Oceans_duplicate (
Ocean string,
Area number,
AverageDepth number,
DeepestPoint number
)

SELECT * FROM Oceans_duplicate;

-- load csv file that contain duplicates into table-stage of the table
-- Putting data into table-stage
-- SnowSql: PUT file://C:\Snowflake\load_temp\Oceans_duplicate_records.csv @%Oceans_duplicate;

list @%Oceans_duplicate;
SELECT * FROM @%Oceans_duplicate;

-- copy data into table but remove duplicates
copy into Oceans_duplicate
from (
    select distinct * from @%Oceans_duplicate
)
file_format = (type = 'csv')
on_error = 'continue'
force = true

-- duplicate records are removed
SELECT * FROM Oceans_duplicate;
