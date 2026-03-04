# src/data_generation/generate_data.py
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import uuid
import os
from pathlib import Path

DATA_DIR = os.environ.get('DATA_DIR', 'data/generated')

FAKER_SEED = 42
RANDOM_SEED = 42
NP_RANDOM_SEED = 42

fake = Faker('ru_RU')
Faker.seed(FAKER_SEED)
random.seed(RANDOM_SEED)
np.random.seed(NP_RANDOM_SEED)

CITIES = ['Москва', 'Санкт-Петербург', 'Казань', 'Екатеринбург', 'Новосибирск', 'Тюмень']
STATUSES = ['active', 'inactive', 'blocked']
STATUS_WEIGHTS = [0.7, 0.25, 0.05]  # 70% активных, 25% неактивных, 5% заблокированных

CITY_WEIGHTS = [0.3, 0.2, 0.15, 0.15, 0.1, 0.1]

def generate_drivers(n=5000):
    """
    Генерирует DataFrame с водителями и возвращает список сгенерированных ID
    """
    print(f"generating {n} drivers...")
    
    drivers = []
    driver_ids = []
    
    for i in range(n):
        driver_id = str(uuid.uuid4())
        driver_ids.append(driver_id)

        phone = fake.phone_number()

        city = random.choices(CITIES, weights=CITY_WEIGHTS)[0]

        registration_date = fake.date_time_between(
            start_date='-2y', 
            end_date='now'
        )

        status = random.choices(STATUSES, weights=STATUS_WEIGHTS)[0]

        rating = round(np.random.normal(4.5, 0.3), 2)
        rating = max(3.0, min(5.0, rating))

        total_trips = 0
        
        drivers.append({
            'driver_id': driver_id,
            'name': fake.name(),
            'phone': phone,
            'city': city,
            'registration_date': registration_date,
            'status': status,
            'rating': rating,
            'total_trips': total_trips
        })
    
    df = pd.DataFrame(drivers)
    print(f"generated {len(df)} drivers")
    print(f"generated {len(driver_ids)} unique IDs")
    return df, driver_ids

def generate_trips(driver_ids, n=50000):
    """
    Генерирует поездки для водителей, используя ТОЛЬКО переданные driver_ids
    """
    print(f"generating {n} trips...")
    
    trips = []
    
    if not driver_ids:
        print("no driver_ids to generate trips")
        return pd.DataFrame()
    
    print(f"using pool of {len(driver_ids)} drivers")

    end_date = datetime.now()
    start_date = end_date - timedelta(days=180)

    driver_trip_counts = {did: 0 for did in driver_ids}
    
    for i in range(n):
        driver_id = random.choice(driver_ids)
        driver_trip_counts[driver_id] += 1

        trip_date = fake.date_time_between(start_date=start_date, end_date=end_date)

        city = random.choices(CITIES, weights=CITY_WEIGHTS)[0]

        distance_km = round(np.random.lognormal(mean=2.0, sigma=0.7), 1)
        distance_km = max(0.5, min(100, distance_km))

        duration_min = int(distance_km * 3 + np.random.normal(0, 5))
        duration_min = max(5, duration_min)

        fare_amount = round(40 * distance_km + np.random.normal(50, 20), 2)
        fare_amount = max(100, fare_amount)

        commission_rate = random.uniform(0.15, 0.25)
        commission = round(fare_amount * commission_rate, 2)
        driver_payout = round(fare_amount - commission, 2)

        rating = round(np.random.normal(4.7, 0.2), 2)
        rating = max(1.0, min(5.0, rating))
        
        trips.append({
            'trip_id': str(uuid.uuid4()),
            'driver_id': driver_id,
            'trip_date': trip_date,
            'city': city,
            'distance_km': distance_km,
            'duration_min': duration_min,
            'fare_amount': fare_amount,
            'commission': commission,
            'driver_payout': driver_payout,
            'rating': rating
        })
    
    df = pd.DataFrame(trips)

    active_drivers = sum(1 for count in driver_trip_counts.values() if count > 0)
    print(f"generated {len(df)} trips")
    print(f"active drivers: {active_drivers} out of {len(driver_ids)}")
    print(f"avg trips per driver: {n/active_drivers:.1f}")
    
    return df

def generate_driver_activity(driver_ids, trips_df):
    """
    Агрегирует поездки в дневную активность водителей
    Использует временную колонку, чтобы не изменять исходный trips_df
    """
    print("generating drivers daily activity")
    
    if trips_df.empty:
        print("no trips to generate activity")
        return pd.DataFrame()

    trips_with_date = trips_df.copy()
    trips_with_date['_temp_date'] = pd.to_datetime(trips_with_date['trip_date']).dt.date

    activity = trips_with_date.groupby(['driver_id', '_temp_date']).agg({
        'duration_min': 'sum',
        'driver_payout': 'sum',
        'trip_id': 'count'
    }).rename(columns={
        'duration_min': 'online_hours',
        'driver_payout': 'earnings',
        'trip_id': 'trips_count'
    }).reset_index()

    activity.rename(columns={'_temp_date': 'date'}, inplace=True)

    activity['online_hours'] = (activity['online_hours'] / 60).round(2)

    np.random.seed(NP_RANDOM_SEED)
    activity['accepted_orders'] = activity['trips_count'] + np.random.poisson(2, len(activity))
    activity['rejected_orders'] = np.random.poisson(1, len(activity))

    activity['activity_id'] = [str(uuid.uuid4()) for _ in range(len(activity))]

    activity = activity[[
        'activity_id', 'driver_id', 'date', 'trips_count', 
        'online_hours', 'earnings', 'accepted_orders', 'rejected_orders'
    ]]

    activity_drivers = set(activity['driver_id'].unique())
    valid_drivers = set(driver_ids)
    invalid_drivers = activity_drivers - valid_drivers
    
    if invalid_drivers:
        print(f"found {len(invalid_drivers)} invalid driver_ids in activity, filtering...")
        activity = activity[activity['driver_id'].isin(valid_drivers)]
    
    print(f"generated {len(activity)} activities")
    print(f"included: {len(activity['driver_id'].unique())} drivers")
    
    return activity

def save_to_csv(df, filename, folder=None):
    """
    Сохраняет DataFrame в CSV
    """
    if folder is None:
        folder = DATA_DIR

    Path(folder).mkdir(parents=True, exist_ok=True)

    filepath = os.path.join(folder, filename)

    df.to_csv(filepath, index=False, encoding='utf-8')
    print(f"saved to {filepath} ({len(df):,} rows)")

def verify_consistency(drivers_df, trips_df, activity_df):
    """
    Проверяет консистентность данных между файлами
    """
    print("\nconsistency check")
    
    drivers_ids = set(drivers_df['driver_id'])
    trips_ids = set(trips_df['driver_id'].unique())
    activity_ids = set(activity_df['driver_id'].unique())
    
    print(f"unique driver_id in drivers: {len(drivers_ids):,}")
    print(f"unique driver_id in trips: {len(trips_ids):,}")
    print(f"unique driver_id in activity: {len(activity_ids):,}")

    missing_in_drivers = trips_ids - drivers_ids
    if missing_in_drivers:
        print(f"error: {len(missing_in_drivers)} driver_id from trips are in drivers")
        print(f"examples: {list(missing_in_drivers)[:3]}")
    else:
        print(f"all driver_ids from trips are in drivers")

    missing_in_activity = activity_ids - drivers_ids
    if missing_in_activity:
        print(f"error: {len(missing_in_activity)} driver_id from activity aren't in drivers")
        print(f"examples: {list(missing_in_activity)[:3]}")
    else:
        print(f"all driver_id from activity are in drivers")

    duplicate_phones = drivers_df[drivers_df.duplicated('phone', keep=False)]
    if not duplicate_phones.empty:
        print(f"error: found {len(duplicate_phones)} phone duplicates")
    else:
        print(f"all phones are unique")
    
    return len(missing_in_drivers) == 0 and len(missing_in_activity) == 0 and duplicate_phones.empty

def main():

    print("test data generation start")

    N_DRIVERS = 5000
    N_TRIPS = 50000

    drivers_df, driver_ids = generate_drivers(N_DRIVERS)

    trips_df = generate_trips(driver_ids, N_TRIPS)

    trips_per_driver = trips_df.groupby('driver_id').size()
    drivers_df['total_trips'] = drivers_df['driver_id'].map(trips_per_driver).fillna(0).astype(int)

    activity_df = generate_driver_activity(driver_ids, trips_df)

    if not verify_consistency(drivers_df, trips_df, activity_df):
        print("\nconsistency issues found")
        print("fix before continue")
        return

    print("\nsaving files")
    save_to_csv(drivers_df, 'drivers.csv')
    save_to_csv(trips_df, 'trips.csv')
    save_to_csv(activity_df, 'driver_activity.csv')

    print("\nstats:")
    print(f"drivers: {len(drivers_df):,}")
    print(f"active: {len(drivers_df[drivers_df['status']=='active']):,}")
    print(f"inactive: {len(drivers_df[drivers_df['status']=='inactive']):,}")
    print(f"blocked: {len(drivers_df[drivers_df['status']=='blocked']):,}")
    print(f"trips: {len(trips_df):,}")
    print(f"columns: {list(trips_df.columns)}")
    print(f"avg cost: {trips_df['fare_amount'].mean():.2f} руб")
    print(f"avg distance: {trips_df['distance_km'].mean():.1f} км")
    print(f"unique drivers in trips: {len(trips_df['driver_id'].unique()):,}")
    print(f"activity rows: {len(activity_df):,}")
    print(f"unique drivers in activity: {len(activity_df['driver_id'].unique()):,}")
    print(f"period: {activity_df['date'].min()} - {activity_df['date'].max()}")
    print("generation success")

if __name__ == "__main__":
    main()