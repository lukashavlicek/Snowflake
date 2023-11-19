
-- Set context
USE RoadBikesETL;
USE SCHEMA DIM;

-- Create tables to move data from raw layer to dimensions

-- Dim_Products
CREATE or REPLACE table Dim_Products (
    ProductId int,
    Brand string,
    Model string,
    Frame string,
    WheelSize string,
    Price number
);


-- Fct_Orders
CREATE or REPLACE table Fct_Orders (
    OrderId int,
    OrderDate date,
    ProductId int,
    Quantity number,
    ShopId int
);

-- Dim_Shops
CREATE or REPLACE transient table Dim_Shops (
    ShopId int,
    ShopName string,
    Location string
);

show tables;

-- first load - insert data from raw tables
insert into DIM_PRODUCTS select * from raw.RAW_PRODUCTS;
insert into FCT_ORDERS select * from raw.FCT_ORDERS;
insert into DIM_SHOPS select * from raw.RAW_SHOPS;

select * from DIM_PRODUCTS;
select * from FCT_ORDERS;
select * from DIM_SHOPS;

-- Create tasks that consume data from streams in raw schema and perform DML operations on dimension tables
-- Product dimension task
create or replace task dim_products_task
    warehouse = compute_wh
    schedule = '1 minute'
when
    system$stream_has_data('roadbikesetl.raw.raw_products_strm')
as
    merge into roadbikesetl.dim.dim_products dim using roadbikesetl.raw.raw_products_strm strm on dim.ProductId = strm.ProductId
    WHEN MATCHED AND metadata$action = 'DELETE' AND metadata$isupdate = 'FALSE'
    THEN DELETE
    WHEN MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'TRUE'
    THEN UPDATE SET dim.brand = strm.brand, dim.model = strm.model, dim.frame = strm.frame, dim.wheelsize = strm.wheelsize, dim.price = strm.price
    WHEN NOT MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'FALSE'
    THEN INSERT (ProductId, brand, model, frame, wheelsize, price) VALUES (strm.ProductId, strm.brand, strm.model, strm.frame, strm.wheelsize, strm.price);

-- Shop dimension task
create or replace task dim_shop_task
    warehouse = compute_wh
    schedule = '2 minute'
when
    system$stream_has_data('roadbikesetl.raw.raw_shops_strm')
as
    merge into roadbikesetl.dim.dim_shops dim using roadbikesetl.raw.raw_shops_strm strm on dim.Shopid = strm.ShopId
    WHEN MATCHED AND metadata$action = 'DELETE' AND metadata$isupdate = 'FALSE'
    THEN DELETE
    WHEN MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'TRUE'
    THEN UPDATE SET dim.ShopName = strm.ShopName, dim.Location = strm.Location
    WHEN NOT MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'FALSE'
    THEN INSERT (ShopId, ShopName, Location) VALUES (strm.ShopId, strm.ShopName, strm.Location);


-- fact table task
create or replace task fct_orders_task
    warehouse = compute_wh
    schedule = '3 minute'
when
    system$stream_has_data('roadbikesetl.raw.fct_orders_strm')
as
    merge into roadbikesetl.dim.fct_orders dim using roadbikesetl.raw.fct_orders_strm strm on dim.OrderId = strm.OrderId
    WHEN MATCHED AND metadata$action = 'DELETE' AND metadata$isupdate = 'FALSE'
    THEN DELETE
    WHEN MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'TRUE'
    THEN UPDATE SET dim.orderDate = strm.orderDate, dim.ProductId = strm.ProductId, dim.Quantity = strm.Quantity, dim.ShopId = strm.ShopId
    WHEN NOT MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'FALSE'
    THEN INSERT (OrderId, OrderDate, ProductId, Quantity, ShopId) VALUES (strm.OrderId, strm.OrderDate, strm.ProductId, strm.Quantity, strm.ShopId);


show tasks;

-- resume tasks
alter task dim_products_task resume;
alter task dim_shop_task resume;
alter task fct_orders_task resume;


------------------------------------------
select count(*) from dim_products
select count(*) from dim_shops
select count(*) from fct_orders

select * from dim_products
select * from dim_shops
select * from fct_orders

