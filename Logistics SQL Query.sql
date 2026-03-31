----check tables
select * from final
---Row count check
select count(*) from final
-----null check
select * from final
where origin is null
---Duplicate Check
select origin ,count(*)
from final
group by origin
having count(*)>1

----Chrck Your Data Set
select * from raw

---check Your Real Columns
select * from raw limit 10
-----Basic Data
select shipment_id,cost,distance_km
from raw
------ETA Accuracy
select *,
1- abs(actual_days-eta_days)/eta_days as assuracy
from raw
-----Before/ After Stage 
select *, 
case
when(1- abs(actual_days-eta_days) / eta_days)< 0.8 then'Before'
else 'After'
end as stage
from raw
-----ETA Columns
select 
avg(cost) as avg_cost,
avg(distance_km) as avg_distance
from raw

-----create clean data 
create table clean_data
with 
(
format='parquet',
external_location= 's3://supplychain-clean-data/clean/'
)
as
select 
delivery_date,
estimated_date,
cost,
1- abs(date_diff('day',delivery_date,estimated_date))
/ date_diff('day',estimated_date,delivery_date) as accuracy 
from raw
where cost is not null
----Add Stage(Before/After)
SELECT *,
    CASE 
        WHEN accuracy < 0.8 THEN 'Before'
        ELSE 'After'
    END AS stage
FROM clean_data;
-----Cost Analysis Query
SELECT
    stage,
    AVG(cost) AS avg_cost
FROM clean_data
GROUP BY stage;
-----ETA accuracy Query
SELECT
    stage,
    AVG(accuracy) * 100 AS accuracy_percent
FROM clean_data
GROUP BY stage;