# src/utils/config.py
import os
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

class Config:
    # PostgreSQL
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'diplom')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')
    
    # Project
    PROJECT_NAME = os.getenv('PROJECT_NAME', 'diplom')
    ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # Paths
    BASE_DIR = Path(__file__).parent.parent.parent
    DATA_DIR = BASE_DIR / 'data' / 'generated'
    LOG_DIR = BASE_DIR / 'logs'
    
    @property
    def database_url(self) -> str:
        """Получить URL для подключения к БД"""
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    @property
    def database_url_psycopg(self) -> str:
        """URL для psycopg2 (без экранирования)"""
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

config = Config()

config.LOG_DIR.mkdir(parents=True, exist_ok=True)