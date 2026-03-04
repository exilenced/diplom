truncate table driver_activity cascade;
truncate table trips cascade;
truncate table drivers cascade;

select 'drivers' as table_name, count(*) from drivers
union all
select 'trips', count(*) from trips
union all
select 'driver_activity', count(*) from driver_activity;