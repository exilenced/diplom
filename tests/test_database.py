# tests/test_database.py
import pytest
from sqlalchemy import text
from src.database.connection import db
from src.utils.config import config

class TestDatabaseConnection:
    """Тесты подключения к БД"""
    
    def test_connection(self):
        """Проверка, что подключение работает"""
        assert db.test_connection() is True
    
    def test_engine_creation(self):
        """Проверка создания engine"""
        assert db.engine is not None
    
    def test_execute_query(self):
        """Проверка выполнения простого запроса"""
        with db.get_connection() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            assert result == 1

class TestDatabaseSchema:
    """Тесты схемы БД"""
    
    def test_tables_exist(self, test_db):
        """Проверка, что все таблицы созданы"""
        with test_db.connect() as conn:
            tables = conn.execute(
                text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
            ).fetchall()
            table_names = [t[0] for t in tables]
            
            expected = ['drivers', 'trips', 'driver_activity']
            for table in expected:
                assert table in table_names
    
    def test_drivers_schema(self, test_db):
        """Проверка схемы таблицы drivers"""
        with test_db.connect() as conn:
            columns = conn.execute(
                text("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name='drivers'
                """)
            ).fetchall()
            
            column_dict = {col[0]: col[1] for col in columns}
            expected = {
                'driver_id': 'uuid',
                'name': 'character varying',
                'phone': 'character varying',
                'city': 'character varying',
                'status': 'character varying',
                'rating': 'numeric'
            }
            
            for col, dtype in expected.items():
                assert col in column_dict
                assert dtype in column_dict[col]
    
    def test_trips_schema(self, test_db):
        """Проверка схемы таблицы trips"""
        with test_db.connect() as conn:
            columns = conn.execute(
                text("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name='trips'
                """)
            ).fetchall()
            
            column_names = [col[0] for col in columns]
            assert 'trip_id' in column_names
            assert 'driver_id' in column_names
            assert 'trip_date' in column_names
            assert 'fare_amount' in column_names
            assert 'date' not in column_names  # Важно!
    
    def test_driver_activity_schema(self, test_db):
        """Проверка схемы таблицы driver_activity"""
        with test_db.connect() as conn:
            columns = conn.execute(
                text("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name='driver_activity'
                """)
            ).fetchall()
            
            column_names = [col[0] for col in columns]
            assert 'activity_id' in column_names
            assert 'driver_id' in column_names
            assert 'date' in column_names  # Здесь date нужна!
            assert 'trips_count' in column_names