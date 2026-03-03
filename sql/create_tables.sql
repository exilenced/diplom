--sql/create_tables.sql
-- Создание таблиц
-- Автор: Артём Мазур
-- Дата: 04.04.2026

--Удалить таблицы для пересоздания
drop table if exists driver_activity cascade;
drop table if exists trips cascade;
drop table if exists drivers cascade;

-- Таблица водителей
create table drivers (
    driver_id UUID primary key default gen_random_uuid(),
    name varchar(100) not null,
    phone varchar(20) unique not null,
    city varchar(50) not null,
    registration_date timestamp not null default current_timestamp,
    status varchar(20) not null check (status IN ('active', 'inactive', 'blocked')),
    rating decimal(3, 2) check (rating >= 0 and rating <= 5),
    total_trips int default 0,
    created_at timestamp default current_timestamp,
    updated_at timestamp default current_timestamp
);

--Индексы для ускорения запросов
create index idx_drivers_city on drivers(city);
create index idx_drivers_status on drivers(status);
create index idx_drivers_registration on drivers(registration_date);

-- Таблица поездок
create table trips (
    trip_id UUID primary key default gen_random_uuid(),
    driver_id UUID not null references drivers(driver_id) on delete cascade,
    trip_date timestamp not null,
    city varchar(50) not null,
    distance_km decimal(5, 2) check (distance_km >= 0),
    duration_min int check (duration_min >= 0),
    fare_amount decimal(8, 2) check (fare_amount >= 0),
    commission decimal(8, 2) check (commission >= 0),
    driver_payout decimal(8, 2) check (driver_payout >= 0),
    rating decimal(3, 2) check (rating >= 0 and rating <= 5),
    created_at timestamp default current_timestamp
);

--Индексы
CREATE INDEX idx_trips_driver_id ON trips(driver_id);
CREATE INDEX idx_trips_trip_date ON trips(trip_date);
CREATE INDEX idx_trips_city ON trips(city);
CREATE INDEX idx_trips_driver_date ON trips(driver_id, trip_date);

-- Таблица дневной активности
CREATE TABLE driver_activity (
    activity_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    driver_id UUID NOT NULL REFERENCES drivers(driver_id) ON DELETE CASCADE,
    date DATE NOT NULL,
    trips_count INT DEFAULT 0 CHECK (trips_count >= 0),
    online_hours DECIMAL(4,2) DEFAULT 0 CHECK (online_hours >= 0),
    earnings DECIMAL(10,2) DEFAULT 0 CHECK (earnings >= 0),
    accepted_orders INT DEFAULT 0 CHECK (accepted_orders >= 0),
    rejected_orders INT DEFAULT 0 CHECK (rejected_orders >= 0),
    
    -- Метаданные
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Уникальность: один водитель - одна запись на день
    UNIQUE(driver_id, date)
);

-- Индексы
CREATE INDEX idx_activity_driver_id ON driver_activity(driver_id);
CREATE INDEX idx_activity_date ON driver_activity(date);
CREATE INDEX idx_activity_driver_date ON driver_activity(driver_id, date);

-- Автообновление updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Триггер для drivers
CREATE TRIGGER update_drivers_updated_at
    BEFORE UPDATE ON drivers
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Комментарии
COMMENT ON TABLE drivers IS 'Водители такси';
COMMENT ON COLUMN drivers.status IS 'Статус: active - активен, inactive - не активен, blocked - заблокирован';
COMMENT ON COLUMN drivers.rating IS 'Средний рейтинг водителя (0-5)';

COMMENT ON TABLE trips IS 'Поездки';
COMMENT ON COLUMN trips.fare_amount IS 'Стоимость поездки для пассажира';
COMMENT ON COLUMN trips.commission IS 'Комиссия агрегатора';
COMMENT ON COLUMN trips.driver_payout IS 'Выплата водителю';

COMMENT ON TABLE driver_activity IS 'Ежедневная агрегированная активность водителей';
COMMENT ON COLUMN driver_activity.accepted_orders IS 'Принятые заказы';
COMMENT ON COLUMN driver_activity.rejected_orders IS 'Отклоненные заказы';

-- Проверка
-- INSERT INTO drivers (name, phone, city, status, rating, total_trips)
-- VALUES ('Тестовый Водитель', '+7-900-123-45-67', 'Москва', 'active', 4.5, 0);