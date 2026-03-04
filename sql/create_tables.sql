-- sql/create_tables.sql
-- Создание таблиц для дипломного проекта

-- Удаляем таблицы, если существуют
drop table if exists driver_activity cascade;
drop table if exists trips cascade;
drop table if exists drivers cascade;

-- Таблица водителей
create table drivers (
    driver_id uuid primary key default gen_random_uuid(),
    name varchar(100) not null,
    phone varchar(20) unique not null,
    city varchar(50) not null,
    registration_date timestamp not null default current_timestamp,
    status varchar(20) not null check (status in ('active', 'inactive', 'blocked')),
    rating decimal(3,2) check (rating >= 0 and rating <= 5),
    total_trips int default 0,
    created_at timestamp default current_timestamp,
    updated_at timestamp default current_timestamp
);

-- Таблица поездок
create table trips (
    trip_id uuid primary key default gen_random_uuid(),
    driver_id uuid not null references drivers(driver_id) on delete cascade,
    trip_date timestamp not null,
    city varchar(50) not null,
    distance_km decimal(5,2) check (distance_km >= 0),
    duration_min int check (duration_min >= 0),
    fare_amount decimal(8,2) check (fare_amount >= 0),
    commission decimal(8,2) check (commission >= 0),
    driver_payout decimal(8,2) check (driver_payout >= 0),
    rating decimal(3,2) check (rating >= 0 and rating <= 5),
    created_at timestamp default current_timestamp
);

-- Таблица активности
create table driver_activity (
    activity_id uuid primary key default gen_random_uuid(),
    driver_id uuid not null references drivers(driver_id) on delete cascade,
    date date not null,
    trips_count int default 0 check (trips_count >= 0),
    online_hours decimal(4,2) default 0 check (online_hours >= 0),
    earnings decimal(10,2) default 0 check (earnings >= 0),
    accepted_orders int default 0 check (accepted_orders >= 0),
    rejected_orders int default 0 check (rejected_orders >= 0),
    created_at timestamp default current_timestamp,
    unique(driver_id, date)
);

-- Индексы
create index idx_drivers_city on drivers(city);
create index idx_drivers_status on drivers(status);
create index idx_trips_driver_id on trips(driver_id);
create index idx_trips_trip_date on trips(trip_date);
create index idx_activity_driver_id on driver_activity(driver_id);
create index idx_activity_date on driver_activity(date);