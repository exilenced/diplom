# tests/test_data_generation.py
import pytest
import pandas as pd
import uuid
from src.data_generation.generate_data import (
    generate_drivers, 
    generate_trips, 
    generate_driver_activity,
    verify_consistency
)

class TestDataGeneration:
    """Тесты генерации данных"""
    
    def test_generate_drivers(self):
        """Проверка генерации водителей"""
        df, driver_ids = generate_drivers(n=100)
        
        assert len(df) == 100
        assert len(driver_ids) == 100
        assert all(df['driver_id'].isin(driver_ids))
        assert all(df['phone'].notna())
        assert all(df['status'].isin(['active', 'inactive', 'blocked']))
        assert all((df['rating'] >= 3.0) & (df['rating'] <= 5.0))
        
        # Проверка формата UUID
        for driver_id in driver_ids:
            # Проверяем, что это валидный UUID
            uuid_obj = uuid.UUID(driver_id)
            assert str(uuid_obj) == driver_id
    
    def test_generate_trips(self, sample_drivers_data):
        """Проверка генерации поездок"""
        driver_ids = sample_drivers_data['driver_id'].tolist()
        trips_df = generate_trips(driver_ids, n=500)
        
        assert len(trips_df) == 500
        assert all(trips_df['driver_id'].isin(driver_ids))
        assert all(trips_df['fare_amount'] > 0)
        assert all(trips_df['commission'] > 0)
        assert all(trips_df['driver_payout'] > 0)
        assert 'date' not in trips_df.columns  # Критическая проверка!
        
        # Проверка формата trip_id
        for trip_id in trips_df['trip_id']:
            uuid_obj = uuid.UUID(trip_id)
            assert str(uuid_obj) == trip_id
    
    def test_generate_activity(self, sample_drivers_data, sample_trips_data):
        """Проверка генерации активности"""
        driver_ids = sample_drivers_data['driver_id'].tolist()
        activity_df = generate_driver_activity(driver_ids, sample_trips_data)
        
        assert not activity_df.empty
        assert all(activity_df['driver_id'].isin(driver_ids))
        assert all(activity_df['trips_count'] > 0)
        assert all(activity_df['earnings'] > 0)
        
        # Проверка формата activity_id
        for activity_id in activity_df['activity_id']:
            uuid_obj = uuid.UUID(activity_id)
            assert str(uuid_obj) == activity_id
    
    def test_consistency_check(self, sample_drivers_data, sample_trips_data):
        """Проверка функции проверки консистентности"""
        driver_ids = sample_drivers_data['driver_id'].tolist()
        activity_df = generate_driver_activity(driver_ids, sample_trips_data)
        
        # Должно быть True для консистентных данных
        assert verify_consistency(
            sample_drivers_data, 
            sample_trips_data, 
            activity_df
        ) is True
        
        # Проверка на неконсистентных данных
        bad_trips = sample_trips_data.copy()
        # Меняем один driver_id на несуществующий
        bad_trips.loc[0, 'driver_id'] = str(uuid.uuid4())
        
        assert verify_consistency(
            sample_drivers_data,
            bad_trips,
            activity_df
        ) is False