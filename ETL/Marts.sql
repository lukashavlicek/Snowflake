USE RoadBikesETL;
USE SCHEMA DIM;

select * from dim_products
select * from dim_shops
select * from fct_orders


select * from fct_orders where shopid = 103

select distinct shopid from fct_orders

------------------------------------------------------

-- 1) Which shop sold the most bikes
select 
s.shopid as shop_id,
s.shopname as shop_name,
s.location as location,
SUM(o.quantity) as quantity

from fct_orders o join dim_shops s
    on o.shopid = s.shopid
group by s.shopid, s.shopname, s.location


-- 2) How much revenue each shop made on bikes
select 
s.shopid as shop_id,
s.shopname as shop_name,
--s.location as location,
SUM(o.quantity * p.price) as revenue

from fct_orders o join dim_shops s on o.shopid = s.shopid
                  join dim_products p on o.productid = p.productid
    
group by s.shopid, s.shopname
order by revenue desc;