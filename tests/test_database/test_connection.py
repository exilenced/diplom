# tests/test_database/test_connection.py
import pytest
from sqlalchemy import text
from src.database.connection import db
from src.utils.config import config

class TestDatabaseConnection:
    """Тесты для подключения к БД - только полезные сценарии"""
    
    def test_connection_test(self):
        """Проверка, что подключение работает"""
        assert db.test_connection() is True
    
    def test_get_connection(self):
        """Проверка получения соединения"""
        with db.get_connection() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            assert result == 1
    
    def test_df_to_table(self, test_db_engine, sample_drivers_data, clean_tables):
        """Проверка загрузки DataFrame в таблицу"""
        # Сохраняем оригинальный engine
        original_engine = db._engine
        db._engine = test_db_engine
        
        # Очищаем таблицу
        with db.get_connection() as conn:
            conn.execute(text("DROP TABLE IF EXISTS drivers CASCADE"))
            conn.execute(text("""
                CREATE TABLE drivers (
                    driver_id VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    phone VARCHAR UNIQUE,
                    city VARCHAR,
                    registration_date TIMESTAMP,
                    status VARCHAR,
                    rating DECIMAL,
                    total_trips INTEGER
                )
            """))
            conn.commit()
        
        # Загружаем данные
        success = db.df_to_table(sample_drivers_data, 'drivers', if_exists='append')
        assert success is True
        
        # Проверяем количество
        with db.get_connection() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM drivers")).scalar()
            assert count == len(sample_drivers_data)
        
        # Восстанавливаем
        db._engine = original_engine