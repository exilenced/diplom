# tests/test_utils/test_config.py
import pytest
import os
from pathlib import Path
from src.utils.config import config

class TestConfig:
    """Тесты для конфигурации - только полезные сценарии"""
    
    def test_config_loading(self):
        """Проверка загрузки конфигурации"""
        assert config.DB_HOST is not None
        assert config.DB_PORT is not None
        assert config.DB_NAME is not None
        assert config.DB_USER is not None
    
    def test_database_url(self):
        """Проверка формирования URL для БД"""
        url = config.database_url
        assert url.startswith('postgresql://')
        assert config.DB_USER in url
        assert config.DB_NAME in url
    
    def test_project_paths(self):
        """Проверка корректности путей проекта"""
        assert isinstance(config.BASE_DIR, Path)
        assert config.BASE_DIR.exists()
        assert config.LOG_DIR.exists()