import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import tempfile
import shutil
from sqlalchemy import create_engine, text
from uuid import uuid4
from datetime import datetime, timedelta

from src.utils.config import config
from src.database.connection import db

@pytest.fixture
def temp_data_dir():
    """
    creates temp dir for test csv files
    auto deletes after test
    """
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)

@pytest.fixture
def sample_drivers_data():
    """
    Создает тестовый DataFrame с водителями.
    Возвращает 10 тестовых записей с реальными UUID.
    """
    n_drivers = 10
    cities = ['Москва', 'СПб', 'Казань', 'Екб', 'Нск']
    statuses = ['active', 'inactive', 'blocked']
    
    data = {
        'driver_id': [str(uuid4()) for _ in range(n_drivers)],
        'name': [f'Driver_{i}' for i in range(n_drivers)],
        'phone': [f'+7{900+i:07d}' for i in range(n_drivers)],
        'city': np.random.choice(cities, n_drivers),
        'registration_date': [
            datetime.now() - timedelta(days=np.random.randint(1, 365))
            for _ in range(n_drivers)
        ],
        'status': np.random.choice(statuses, n_drivers, p=[0.7, 0.2, 0.1]),
        'rating': np.round(np.random.uniform(3.5, 5.0, n_drivers), 2),  # ИСПРАВЛЕНО
        'total_trips': np.random.randint(0, 1000, n_drivers)
    }
    return pd.DataFrame(data)
@pytest.fixture
def sample_trips_data(sample_drivers_data):
    """
    creates test trips DF
    uses driver_id from sample_drivers_data
    """

    n_trips = 50
    driver_ids = sample_drivers_data['driver_id'].tolist()
    cities = ['Москва', 'СПб', 'Казань', 'Екб', 'Нск']
    
    data = {
        'trip_id': [str(uuid4()) for _ in range(n_trips)],
        'driver_id': np.random.choice(driver_ids, n_trips),
        'trip_date': [
            datetime.now() - timedelta(days=np.random.randint(0, 30))
            for _ in range(n_trips)
        ],
        'city': np.random.choice(cities, n_trips),
        'distance_km': np.round(np.random.uniform(1, 50, n_trips), 1),
        'duration_min': np.random.randint(5, 120, n_trips),
        'fare_amount': np.round(np.random.uniform(100, 2000, n_trips), 2),
        'commission': np.round(np.random.uniform(10, 200, n_trips), 2),
        'driver_payout': np.round(np.random.uniform(90, 1800, n_trips), 2),
        'rating': np.round(np.random.uniform(1, 5, n_trips), 1)
    }
    return pd.DataFrame(data)

@pytest.fixture(scope="session")
def test_db_engine():
    """
    creates test db connection
    uses test db for tests
    """

    test_db_name = f"{config.DB_NAME}_test"

    root_url = f"postgresql://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}:{config.DB_PORT}/postgres"
    root_engine = create_engine(root_url)

    with root_engine.connect() as conn:
        conn.execute(text('commit'))
        conn.execute(text(f"drop database if exists {test_db_name}"))
        conn.execute(text('commit'))

        conn.execute(text(f'create database {test_db_name}'))
        conn.execute(text('commit'))
    
    test_url = f"postgresql://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}:{config.DB_PORT}/{test_db_name}"
    engine = create_engine(test_url)

    from src.database.models import Base
    Base.metadata.create_all(engine)

    yield engine

    engine.dispose()
    with root_engine.connect() as conn:
        conn.execute(text('commit'))
        conn.execute(text(f'drop database if exists {test_db_name}'))
        conn.execute(text('commit'))

@pytest.fixture
def test_db_session(test_db_engine):
    """
    creates test session
    each test gets clean session
    """
    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker(bind=test_db_engine)
    session = Session()

    yield session

    session.rollback()
    session.close()

@pytest.fixture
def clean_tables(test_db_engine):
    """
    clears all tables before test
    used for tests, that require clean tables
    """
    # Очищаем ДО теста
    with test_db_engine.connect() as conn:
        conn.execute(text("set constraints all deferred"))
        conn.execute(text("truncate table driver_activity cascade"))
        conn.execute(text("truncate table trips cascade"))
        conn.execute(text("truncate table drivers cascade"))
        conn.execute(text("set constraints all immediate"))
        conn.commit()
    
    yield

@pytest.fixture
def etl_loader(temp_data_dir, test_db_engine):
    """
    creates DataLoader configured for test db and temp dir
    """

    from src.etl.load_to_postgres import DataLoader

    original_data_dir = config.DATA_DIR
    config.DATA_DIR = temp_data_dir

    db._engine = test_db_engine

    loader = DataLoader()
    loader.data_dir = temp_data_dir

    yield loader
    config.DATA_DIR = original_data_dir
    db._engine = None

@pytest.fixture(autouse=True)
def reset_db_singleton():
    """Автоматически сбрасывает синглтон БД перед каждым тестом"""
    from src.database.connection import DatabaseConnection
    DatabaseConnection._instance = None
    DatabaseConnection._engine = None
    yield