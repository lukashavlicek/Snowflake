
-- Create new DB
CREATE or REPLACE DATABASE RoadBikesETL;

-- Create schema / layers
CREATE or REPLACE SCHEMA RAW;
CREATE or REPLACE SCHEMA DIM;

USE RoadBikesETL;
USE SCHEMA RAW;


-- create a file format to avoid column mismatch issue while loading the data
create or replace file format csv_col_mismatch_ff
type = 'csv'
compression = 'none'
field_delimiter = ','
error_on_column_count_mismatch = false
skip_header = 1;

show file formats;


-- create transient table for Products
-- ProductID,Brand,Model,FrameMaterial,WheelSize,Price
CREATE or REPLACE transient table Raw_Products (
    ProductId int,
    Brand string,
    Model string,
    Frame string,
    WheelSize string,
    Price number
);


-- create transient table for fact table 
CREATE or REPLACE transient table Fct_Orders (
    OrderId int,
    OrderDate date,
    ProductId int,
    Quantity number,
    ShopId int
);

-- create transient table for Shops
CREATE or REPLACE transient table Raw_Shops (
    ShopId int,
    ShopName string,
    Location string
);

show tables;

-- first load to the tables via WebUI
-- after successful first load, create streams
create or replace stream raw_products_strm on table Raw_products append_only = true;

create or replace stream fct_orders_strm on table Fct_orders append_only = true;

create or replace stream raw_shops_strm on table Raw_shops append_only = true;

show streams;


select * from raw_products_strm
select * from raw_shops_strm
select * from fct_orders_strm


