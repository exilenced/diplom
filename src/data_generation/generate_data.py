# src/data_generation/generate_data.py
import random
import numpy as np
from datetime import datetime, timedelta, date
from typing import List, Tuple, Optional
import pandas as pd
from faker import Faker
from tqdm import tqdm
import logging

from src.models import Driver, Trip, DriverActivity
from src.utils.config import config

logger = logging.getLogger(__name__)
fake = Faker('ru_RU')

CITIES = ['Москва', 'Санкт-Петербург', 'Казань', 'Екатеринбург', 'Новосибирск']
STATUSES = ['active', 'inactive', 'blocked']
CITY_MULTIPLIERS = {
    'Москва': 2.0,
    'Санкт-Петербург': 1.5,
    'Казань': 1.2,
    'Екатеринбург': 1.1,
    'Новосибирск': 1.0
}

class DataGenerator:
    """
    Генератор синтетических данных с контролируемым оттоком
    """
    
    def __init__(self, seed: int = 42):
        random.seed(seed)
        np.random.seed(seed)
        fake.seed_instance(seed)
        self.current_date = datetime.now()
    
    def generate_driver(self, is_churned: bool = False) -> Driver:
        """
        Генерация одного водителя с возможностью задать статус оттока
        """
        city = random.choice(CITIES)
        
        # Регистрация от 1 до 2 лет назад
        registration_date = fake.date_time_between(
            start_date='-2y',
            end_date='-60d'
        )
        
        # Определяем статус на основе флага оттока
        if is_churned:
            status = 'inactive'
            # Ушедшие водители имеют чуть ниже рейтинг
            rating = round(random.uniform(3.2, 4.5), 2)
        else:
            status = random.choices(
                ['active', 'inactive', 'blocked'],
                weights=[0.85, 0.1, 0.05]
            )[0]
            rating = round(random.uniform(3.8, 5.0), 2)
        
        return Driver(
            name=fake.name(),
            phone=fake.unique.phone_number(),
            city=city,
            registration_date=registration_date,
            status=status,
            rating=rating,
            total_trips=0  # будет обновлено позже
        )
    
    def generate_drivers(
        self, 
        n: int, 
        churn_rate: float = 0.2,
        city_churn_weights: Optional[dict] = None
    ) -> List[Driver]:
        """
        Генерация списка водителей с контролируемым оттоком
        
        Args:
            n: количество водителей
            churn_rate: желаемый процент ушедших водителей
            city_churn_weights: веса оттока по городам (для реализма)
        """
        fake.unique.clear()
        drivers = []
        
        # По умолчанию: в Москве меньше отток, в регионах больше
        if city_churn_weights is None:
            city_churn_weights = {
                'Москва': 0.1,
                'Санкт-Петербург': 0.15,
                'Казань': 0.2,
                'Екатеринбург': 0.22,
                'Новосибирск': 0.25
            }
        
        # Определяем, сколько водителей должно быть ушедшими в каждом городе
        city_counts = {city: 0 for city in CITIES}
        churned_by_city = {city: 0 for city in CITIES}
        
        # Распределяем водителей по городам
        for _ in range(n):
            city = random.choice(CITIES)
            city_counts[city] += 1
        
        # Рассчитываем целевое количество ушедших по городам
        for city in CITIES:
            target_churned = int(city_counts[city] * city_churn_weights[city])
            churned_by_city[city] = target_churned
        
        # Генерируем водителей
        for city in CITIES:
            for i in range(city_counts[city]):
                # Этот водитель будет ушедшим?
                is_churned = i < churned_by_city[city]
                driver = self.generate_driver(is_churned)
                # Важно: город может измениться в generate_driver, поэтому устанавливаем явно
                driver.city = city
                drivers.append(driver)
        
        # Перемешиваем, чтобы ушедшие не были сгруппированы
        random.shuffle(drivers)
        
        logger.info(f"Generated {len(drivers)} drivers with overall churn rate: "
                   f"{sum(1 for d in drivers if d.status == 'inactive')/len(drivers):.2%}")
        
        return drivers
    
    def generate_trip(self, driver: Driver, trip_date: datetime = None) -> Trip:
        """
        Генерация одной поездки
        """
        if trip_date is None:
            trip_date = fake.date_time_between(
                start_date=driver.registration_date,
                end_date='now'
            )
        
        base_fare = random.uniform(200, 800)
        city_multiplier = CITY_MULTIPLIERS[driver.city]
        
        distance = random.uniform(2, 50)
        duration = int(distance * random.uniform(2, 5))
        
        fare_amount = round(base_fare * city_multiplier, 2)
        commission = round(fare_amount * random.uniform(0.1, 0.2), 2)
        driver_payout = round(fare_amount - commission, 2)
        
        # Оценка зависит от рейтинга водителя
        if random.random() < 0.7:
            # Водители с высоким рейтингом получают хорошие оценки
            rating = max(1, min(5, round(random.gauss(driver.rating, 0.3), 1)))
        else:
            rating = None
        
        return Trip(
            driver_id=driver.driver_id,
            city=driver.city,
            distance_km=round(distance, 1),
            duration_min=duration,
            fare_amount=fare_amount,
            commission=commission,
            driver_payout=driver_payout,
            rating=rating,
            trip_date=trip_date
        )
    
    def generate_trips_for_driver(
        self,
        driver: Driver,
        n_trips: int,
        end_date: Optional[datetime] = None
    ) -> List[Trip]:
        """
        Генерация поездок для одного водителя с возможностью задать конечную дату
        """
        if end_date is None:
            end_date = self.current_date
        
        trips = []
        start_date = driver.registration_date
        
        # Если водитель ушедший, его последняя поездка была 31-90 дней назад
        if driver.status == 'inactive':
            # Сдвигаем end_date в прошлое
            days_since_last_trip = random.randint(31, 90)
            end_date = self.current_date - timedelta(days=days_since_last_trip)
        
        for _ in range(n_trips):
            trip_date = fake.date_time_between(
                start_date=start_date,
                end_date=end_date
            )
            trips.append(self.generate_trip(driver, trip_date))
        
        trips.sort(key=lambda x: x.trip_date)
        return trips
    
    def generate_trips(
        self,
        drivers: List[Driver],
        total_trips: int
    ) -> List[Trip]:
        """
        Генерация поездок для всех водителей с учетом их статуса
        """
        all_trips = []
        
        # Активные водители получают больше поездок
        driver_weights = []
        for driver in drivers:
            if driver.status == 'active':
                # Активные: вес 2-5 в зависимости от рейтинга
                weight = 2 + int(driver.rating * 10)
            elif driver.status == 'inactive':
                # Ушедшие: вес 0.2-0.5 (мало поездок, все в прошлом)
                weight = 0.2 + (driver.rating / 10)
            else:  # blocked
                weight = 0.1
            
            driver_weights.append(weight)
        
        # Нормализуем веса
        total_weight = sum(driver_weights)
        driver_trip_counts = [
            int((w / total_weight) * total_trips)
            for w in driver_weights
        ]
        
        # Генерируем поездки
        for driver, n_trips in tqdm(
            zip(drivers, driver_trip_counts),
            desc="Generating trips",
            total=len(drivers)
        ):
            if n_trips > 0:
                trips = self.generate_trips_for_driver(driver, n_trips)
                all_trips.extend(trips)
                
                # Обновляем total_trips у водителя
                driver.total_trips += n_trips
        
        return all_trips
    
    def generate_activity(
        self,
        drivers: List[Driver],
        trips: List[Trip]
    ) -> List[DriverActivity]:
        """
        Генерация дневной активности на основе поездок
        """
        from collections import defaultdict
        
        trips_by_driver_day = defaultdict(list)
        
        for trip in trips:
            trip_date = trip.trip_date.date()
            trips_by_driver_day[(trip.driver_id, trip_date)].append(trip)
        
        activities = []
        
        for (driver_id, day), day_trips in tqdm(
            trips_by_driver_day.items(),
            desc="Generating activities"
        ):
            driver = next(d for d in drivers if d.driver_id == driver_id)
            
            n_trips = len(day_trips)
            total_earnings = sum(t.driver_payout for t in day_trips)
            
            online_hours = n_trips * random.uniform(0.3, 0.6)
            
            # Активные водители лучше принимают заказы
            if driver.status == 'active':
                acceptance_rate = random.uniform(0.85, 0.98)
            else:
                acceptance_rate = random.uniform(0.7, 0.9)
            
            total_orders = int(n_trips / acceptance_rate)
            accepted = n_trips
            rejected = total_orders - n_trips
            
            activity = DriverActivity(
                driver_id=driver_id,
                date=day,
                trips_count=n_trips,
                online_hours=round(online_hours, 1),
                earnings=round(total_earnings, 2),
                accepted_orders=accepted,
                rejected_orders=rejected
            )
            activities.append(activity)
        
        return activities
    
    def add_churn_column(self, drivers: List[Driver], trips: List[Trip]) -> pd.DataFrame:
        """
        Добавляет целевую переменную is_churn в датафрейм водителей
        """
        # Преобразуем водителей в DataFrame
        df_drivers = pd.DataFrame([d.to_dict() for d in drivers])
        
        # Группируем поездки по водителям, находим последнюю дату
        if trips:
            df_trips = pd.DataFrame([t.to_dict() for t in trips])
            last_trip_dates = df_trips.groupby('driver_id')['trip_date'].max().reset_index()
            last_trip_dates.columns = ['driver_id', 'last_trip_date']
            
            # Объединяем с водителями
            df_drivers = df_drivers.merge(last_trip_dates, on='driver_id', how='left')
        else:
            df_drivers['last_trip_date'] = None
        
        # Определяем churn: нет поездок в последние 30 дней
        df_drivers['days_since_last_trip'] = (
            self.current_date - pd.to_datetime(df_drivers['last_trip_date'])
        ).dt.days
        
        # Если нет поездок вообще или последняя была >30 дней назад
        df_drivers['is_churn'] = (
            df_drivers['days_since_last_trip'].isna() | 
            (df_drivers['days_since_last_trip'] > 30)
        ).astype(int)
        
        # Логируем распределение
        churn_counts = df_drivers['is_churn'].value_counts()
        logger.info(f"Churn distribution: {churn_counts.to_dict()}")
        logger.info(f"Churn rate: {df_drivers['is_churn'].mean():.2%}")
        
        return df_drivers
    
    def save_to_csv(
        self,
        drivers: List[Driver],
        trips: List[Trip],
        activities: List[DriverActivity],
        output_dir: str
    ):
        """
        Сохранение данных в CSV
        """
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        # Сохраняем основные данные
        pd.DataFrame([d.to_dict() for d in drivers]).to_csv(
            f"{output_dir}/drivers.csv",
            index=False
        )
        
        pd.DataFrame([t.to_dict() for t in trips]).to_csv(
            f"{output_dir}/trips.csv",
            index=False
        )
        
        pd.DataFrame([a.to_dict() for a in activities]).to_csv(
            f"{output_dir}/driver_activity.csv",
            index=False
        )
        
        # Сохраняем датафрейм с целевой переменной для ML
        df_with_churn = self.add_churn_column(drivers, trips)
        df_with_churn.to_csv(
            f"{output_dir}/drivers_with_churn.csv",
            index=False
        )
        
        logger.info(f"Saved {len(drivers)} drivers, {len(trips)} trips, {len(activities)} activities")
        logger.info(f"ML-ready data saved to {output_dir}/drivers_with_churn.csv")

def main(
    n_drivers: int = 1000,
    n_trips: int = 50000,
    churn_rate: float = 0.2,
    output_dir: Optional[str] = None
):
    """
    Основная функция генерации с контролируемым оттоком
    
    Args:
        n_drivers: количество водителей
        n_trips: общее количество поездок
        churn_rate: желаемый процент ушедших водителей
        output_dir: директория для сохранения (по умолчанию config.DATA_DIR)
    """
    if output_dir is None:
        output_dir = config.DATA_DIR
    
    generator = DataGenerator()
    
    print(f"Generating {n_drivers} drivers with target churn rate: {churn_rate:.1%}")
    drivers = generator.generate_drivers(n_drivers, churn_rate=churn_rate)
    
    print(f"Generating {n_trips} trips...")
    trips = generator.generate_trips(drivers, n_trips)
    
    print("Generating activities...")
    activities = generator.generate_activity(drivers, trips)
    
    # Сохраняем данные
    generator.save_to_csv(drivers, trips, activities, output_dir)
    
    print(f"\n✅ Data generation complete!")
    print(f"  - Drivers: {len(drivers)}")
    print(f"  - Trips: {len(trips)}")
    print(f"  - Activities: {len(activities)}")
    print(f"  - Saved to: {output_dir}")
    
    return drivers, trips, activities

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Generate synthetic taxi data')
    parser.add_argument('--drivers', type=int, default=1000, help='Number of drivers')
    parser.add_argument('--trips', type=int, default=50000, help='Number of trips')
    parser.add_argument('--churn', type=float, default=0.2, help='Target churn rate')
    parser.add_argument('--seed', type=int, default=42, help='Random seed')
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    
    main(
        n_drivers=args.drivers,
        n_trips=args.trips,
        churn_rate=args.churn
    )