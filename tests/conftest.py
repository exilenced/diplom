# tests/conftest.py
import pytest
import pandas as pd
import numpy as np
import uuid
from pathlib import Path
import tempfile
import shutil
from sqlalchemy import create_engine, text

from src.database.connection import db
from src.utils.config import config

@pytest.fixture(scope="function")
def test_db():
    """Фикстура для тестовой БД с правильной очисткой"""
    # Генерируем уникальное имя для тестовой БД
    test_db_name = f"test_diplom_{np.random.randint(1000, 9999)}"
    
    # Подключаемся к стандартной postgres БД для создания тестовой
    admin_engine = create_engine(
        f"postgresql://{config.DB_USER}:{config.DB_PASSWORD}@localhost:5432/postgres"
    )
    
    # Убиваем все соединения к тестовой БД, если она существует
    with admin_engine.connect() as conn:
        conn.execute(text("COMMIT"))
        # Принудительно закрываем все соединения к БД, которую хотим удалить
        try:
            conn.execute(text(f"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = '{test_db_name}'
                AND pid <> pg_backend_pid()
            """))
        except Exception:
            pass  # БД может еще не существовать
        conn.execute(text("COMMIT"))
        conn.execute(text(f"DROP DATABASE IF EXISTS {test_db_name}"))
        conn.execute(text(f"CREATE DATABASE {test_db_name}"))
    
    # Создаем engine для тестовой БД
    test_engine = create_engine(
        f"postgresql://{config.DB_USER}:{config.DB_PASSWORD}@localhost:5432/{test_db_name}"
    )
    
    # Создаем таблицы из SQL файла
    sql_path = Path(__file__).parent.parent / 'sql' / 'create_tables.sql'
    with open(sql_path, 'r', encoding='utf-8') as f:
        sql = f.read()
    
    with test_engine.connect() as conn:
        for statement in sql.split(';'):
            if statement.strip():
                conn.execute(text(statement))
        conn.commit()
    
    # Сохраняем оригинальный engine и подменяем
    original_engine = db.engine
    db.engine = test_engine
    
    yield test_engine
    
    # Возвращаем оригинальный engine и закрываем все соединения
    db.engine = original_engine
    test_engine.dispose()
    
    # Удаляем тестовую БД
    with admin_engine.connect() as conn:
        conn.execute(text("COMMIT"))
        # Снова убиваем все соединения перед удалением
        conn.execute(text(f"""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = '{test_db_name}'
        """))
        conn.execute(text("COMMIT"))
        conn.execute(text(f"DROP DATABASE IF EXISTS {test_db_name}"))

@pytest.fixture
def temp_csv_dir():
    """Временная директория для CSV файлов"""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)

@pytest.fixture
def sample_drivers_data():
    """Пример данных водителей для тестов с реальными UUID"""
    np.random.seed(42)
    n = 100
    
    # Генерируем реальные UUID
    driver_ids = [str(uuid.uuid4()) for _ in range(n)]
    
    # Генерируем телефоны
    phones = [f"+7-999-{i:03d}-{i:02d}" for i in range(n)]
    
    # Даты регистрации
    registration_dates = pd.date_range('2024-01-01', periods=n)
    
    # Статусы
    statuses = np.random.choice(['active', 'inactive'], n, p=[0.8, 0.2])
    
    # Рейтинги
    ratings = np.round(np.random.uniform(3.5, 5.0, n), 2)
    
    # Количество поездок
    total_trips = np.random.poisson(50, n)
    
    return pd.DataFrame({
        'driver_id': driver_ids,
        'name': [f"Driver {i}" for i in range(n)],
        'phone': phones,
        'city': np.random.choice(['Москва', 'Санкт-Петербург', 'Казань', 'Екатеринбург'], n),
        'registration_date': registration_dates,
        'status': statuses,
        'rating': ratings,
        'total_trips': total_trips
    })

@pytest.fixture
def sample_trips_data(sample_drivers_data):
    """Пример данных поездок для тестов с реальными UUID"""
    n = 200
    driver_ids = sample_drivers_data['driver_id'].tolist()
    
    # Генерируем trip_id как UUID
    trip_ids = [str(uuid.uuid4()) for _ in range(n)]
    
    # Даты поездок
    trip_dates = pd.date_range('2025-01-01', periods=n)
    
    # Города
    cities = np.random.choice(['Москва', 'Санкт-Петербург', 'Казань', 'Екатеринбург'], n)
    
    # Расстояния
    distances = np.round(np.random.uniform(1, 30, n), 1)
    
    # Длительность
    durations = np.random.randint(5, 60, n)
    
    # Стоимость
    fares = np.round(np.random.uniform(200, 1500, n), 2)
    
    # Комиссия (15-25% от стоимости)
    commissions = np.round(fares * np.random.uniform(0.15, 0.25, n), 2)
    
    # Выплата водителю
    payouts = fares - commissions
    
    # Рейтинг поездки
    ratings = np.round(np.random.uniform(3.5, 5.0, n), 2)
    
    return pd.DataFrame({
        'trip_id': trip_ids,
        'driver_id': np.random.choice(driver_ids, n),
        'trip_date': trip_dates,
        'city': cities,
        'distance_km': distances,
        'duration_min': durations,
        'fare_amount': fares,
        'commission': commissions,
        'driver_payout': payouts,
        'rating': ratings
    })