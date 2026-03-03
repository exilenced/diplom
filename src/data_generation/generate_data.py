import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import uuid
import os
from pathlib import Path

fake = Faker('ru_RU')
Faker.seed(42)
random.seed(42)
np.random.seed(42)

CITIES = ["Москва", "Санкт-Петербург", "Казань", "Екатеринбург", "Новосибирск", "Тюмень"]
STATUSES = ['active', 'inactive', 'blocked']
STATUS_WEIGHTS = [0.7, 0.25, 0.05] # Распределение статусов по популяции водителей

def generate_drivers(n=5000):
    """
    Генерирует DataFrame с водителями
    
    Parameters:
    -----------
    n : int
        Количество водителей
    
    Returns:
    --------
    pd.DataFrame
    """
    print(f"generating {n} drivers...")
    
    drivers = []

    for i in range(n):
        phone = fake.phone_number()

        city = random.choices(
            CITIES,
            weights=[0.3, 0.2, 0.15, 0.15, 0.1, 0.1]
        )[0]

        registration_date = fake.date_time_between(
            start_date='-2y',
            end_date='now'
        )

        status = random.choices(STATUSES, weights=STATUS_WEIGHTS)[0]
        # нормальное распределение вокруг 4.5
        rating = round(np.random.normal(4.5, 0.3), 2)
        rating = max(3.0, min(5.0, rating)) # от 3 до 5

        total_trips = 0

        drivers.append({
            'driver_id': str(uuid.uuid4()),
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
    return df

def generate_trips(drivers_df, n=50000):
    """
    Генерирует поездки для водителей

    Parameters:
    -----------
    drivers_df : pd.DataFrame
        DataFrame с водителями
    n : int
        Количество поездок
    
    Returns:
    --------
    pd.DataFrame
    """
    print(f"generating {n} trips...")

    trips = []

    active_drivers = drivers_df[drivers_df['status'] == 'active']['driver_id'].tolist()

    if not active_drivers:
        print("no active drivers, take all")
        active_drivers = drivers_df['driver_id'].tolist()

    end_date = datetime.now()
    start_date = end_date - timedelta(days=100)

    for i in range(n):
        driver_id = random.choice(active_drivers)

        trip_date = fake.date_time_between(start_date=start_date, end_date=end_date)

        city = random.choice(CITIES)

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
    print(f"generated {len(df)} trips")
    return df

def generate_driver_activity(drivers_df, trips_df):
    """
    Агрегирует поездки в дневную активность водителей
    
    Parameters:
    -----------
    drivers_df : pd.DataFrame
        DataFrame с водителями
    trips_df : pd.DataFrame
        DataFrame с поездками
        
    Returns:
    --------
    pd.DataFrame
    """
    print("📊 Генерируем дневную активность водителей...")
    
    # Преобразуем trip_date в дату
    trips_df['date'] = pd.to_datetime(trips_df['trip_date']).dt.date
    
    # Группируем по водителю и дате
    activity = trips_df.groupby(['driver_id', 'date']).agg({
        'trip_id': 'count',  # количество поездок
        'duration_min': 'sum',  # общее время онлайн (грубо)
        'driver_payout': 'sum',  # заработок
        'trip_id': lambda x: len(x)  # count поездок
    }).rename(columns={
        'trip_id': 'trips_count',
        'duration_min': 'online_hours',
        'driver_payout': 'earnings'
    }).reset_index()
    
    # Переводим минуты в часы
    activity['online_hours'] = (activity['online_hours'] / 60).round(2)
    
    # Генерируем принятые/отклоненные заказы
    # В реальности это отдельные данные, но мы сэмулируем
    np.random.seed(42)
    activity['accepted_orders'] = activity['trips_count'] + np.random.poisson(2, len(activity))
    activity['rejected_orders'] = np.random.poisson(1, len(activity))
    
    # Добавляем activity_id
    activity['activity_id'] = [str(uuid.uuid4()) for _ in range(len(activity))]
    
    # Переставляем колонки в нужном порядке
    activity = activity[[
        'activity_id', 'driver_id', 'date', 'trips_count', 
        'online_hours', 'earnings', 'accepted_orders', 'rejected_orders'
    ]]
    
    # Для водителей без поездок генерируем "пустые" дни
    all_drivers = drivers_df['driver_id'].unique()
    all_dates = pd.date_range(
        start=trips_df['trip_date'].min().date(),
        end=trips_df['trip_date'].max().date()
    ).date
    
    # TODO: можно добавить заполнение пропусков, но пока оставим так
    
    print(f"   ✅ Сгенерировано {len(activity)} записей активности")
    return activity

def save_to_csv(df, filename, folder='data/generated'):
    """
    Сохраняет DataFrame в CSV
    
    Parameters:
    -----------
    df : pd.DataFrame
        Данные для сохранения
    filename : str
        Имя файла
    folder : str
        Папка для сохранения
    """
    # Создаем папку, если нет
    Path(folder).mkdir(parents=True, exist_ok=True)
    
    # Полный путь
    filepath = os.path.join(folder, filename)
    
    # Сохраняем
    df.to_csv(filepath, index=False, encoding='utf-8')
    print(f"   💾 Сохранено в {filepath} ({len(df)} записей)")

def main():
    """
    Главная функция генерации данных
    """
    print("="*50)
    print("🚀 НАЧАЛО ГЕНЕРАЦИИ ТЕСТОВЫХ ДАННЫХ")
    print("="*50)
    
    # Параметры генерации
    N_DRIVERS = 5000
    N_TRIPS = 50000
    
    # 1. Генерируем водителей
    drivers_df = generate_drivers(N_DRIVERS)
    
    # 2. Генерируем поездки
    trips_df = generate_trips(drivers_df, N_TRIPS)
    
    # 3. Обновляем total_trips в drivers_df
    trips_per_driver = trips_df.groupby('driver_id').size()
    drivers_df['total_trips'] = drivers_df['driver_id'].map(trips_per_driver).fillna(0).astype(int)
    
    # 4. Генерируем активность
    activity_df = generate_driver_activity(drivers_df, trips_df)
    
    # 5. Сохраняем в CSV
    print("\n💾 СОХРАНЕНИЕ ФАЙЛОВ")
    print("-"*50)
    save_to_csv(drivers_df, 'drivers.csv')
    save_to_csv(trips_df, 'trips.csv')
    save_to_csv(activity_df, 'driver_activity.csv')
    
    # 6. Статистика
    print("\n📊 ИТОГОВАЯ СТАТИСТИКА")
    print("-"*50)
    print(f"Водители: {len(drivers_df)}")
    print(f"  - Активных: {len(drivers_df[drivers_df['status']=='active'])}")
    print(f"  - Неактивных: {len(drivers_df[drivers_df['status']=='inactive'])}")
    print(f"  - Заблокированных: {len(drivers_df[drivers_df['status']=='blocked'])}")
    print(f"Поездки: {len(trips_df)}")
    print(f"  - Средняя стоимость: {trips_df['fare_amount'].mean():.2f} руб")
    print(f"  - Среднее расстояние: {trips_df['distance_km'].mean():.1f} км")
    print(f"Записи активности: {len(activity_df)}")
    print(f"  - Период: {activity_df['date'].min()} - {activity_df['date'].max()}")
    print("="*50)
    print("✅ ГЕНЕРАЦИЯ ЗАВЕРШЕНА")
    print("="*50)

if __name__ == "__main__":
    main()