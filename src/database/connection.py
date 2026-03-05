# src/database/connection.py
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from contextlib import contextmanager
import pandas as pd
from ..utils.config import config
import logging

logger = logging.getLogger(__name__)

class DatabaseConnection:
    """Класс для работы с PostgreSQL через SQLAlchemy"""
    
    def __init__(self):
        self.engine = None
        self._init_engine()
    
    def _init_engine(self):
        """Инициализация SQLAlchemy engine"""
        try:
            self.engine = create_engine(
                config.database_url,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                echo=False
            )
            logger.info("sqlalchemy engine created successfully")
        except Exception as e:
            logger.error(f"failed to create engine: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """Контекстный менеджер для получения соединения"""
        conn = None
        try:
            conn = self.engine.connect()
            yield conn
        except Exception as e:
            logger.error(f"db error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def get_engine(self) -> Engine:
        """Получить SQLAlchemy engine"""
        return self.engine
    
    def test_connection(self) -> bool:
        """Тест подключения"""
        try:
            with self.get_connection() as conn:
                result = conn.execute(text("select 1")).scalar()
                return result == 1
        except Exception as e:
            logger.error(f"connection test failed: {e}")
            return False
    
    def execute_query(self, query: str, params: dict = None):
        """Выполнить произвольный запрос"""
        from sqlalchemy import text
        with self.get_connection() as conn:
            return conn.execute(text(query), params or {})
    
    def df_to_table(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append', **kwargs):
        """
        Загрузить DataFrame в таблицу
        
        Parameters:
        -----------
        df : pd.DataFrame
            Данные для загрузки
        table_name : str
            Имя таблицы
        if_exists : str
            'fail', 'replace', 'append'
        """
        try:
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists=if_exists,
                index=False,
                method='multi',  # ускоряет вставку
                chunksize=1000,  # по 1000 записей за раз
                **kwargs
            )
            logger.info(f"loaded {len(df)} rows into {table_name}")
            return True
        except Exception as e:
            logger.error(f"failed to load data into {table_name}: {e}")
            return False
    @property
    def connection_url(self) -> str:
        return str(self.engine.url) if self.engine else None

db = DatabaseConnection()