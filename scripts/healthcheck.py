# scripts/healthcheck.py
#!/usr/bin/env python
"""
Проверка здоровья базы данных и подключений
"""

import sys

from src.database.connection import db
from src.utils.config import config
import logging
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_database():
    """Проверка подключения к БД"""
    print("\nchecking db connection")
    print(f"Host: {config.DB_HOST}")
    print(f"Database: {config.DB_NAME}")
    print(f"User: {config.DB_USER}")
    
    if db.test_connection():
        print("connection successful")
        return True
    else:
        print("connection failed")
        return False

def check_tables():
    """Проверка наличия таблиц"""
    print("\nchecking tables")
    
    with db.get_connection() as conn:
        result = conn.execute(
            text("select table_name from information_schema.tables where table_schema='public'")
        )
        tables = [row[0] for row in result]
        
        if not tables:
            print("no tables found")
            return False
        
        print(f"found tables: {', '.join(tables)}")

        required_tables = ['drivers', 'trips', 'driver_activity']
        for table in required_tables:
            if table in tables:
                count = conn.execute(text(f"select count(*) from {table}")).scalar()
                print(f"{table}: {count} rows")
            else:
                print(f"{table}: missing")
        
        return all(table in tables for table in required_tables)

def main():
    print("\ndb healthcheck")
    
    db_ok = check_database()
    if not db_ok:
        print("\ndb connection failed. Exiting.")
        sys.exit(1)
    
    tables_ok = check_tables()

    if db_ok and tables_ok:
        print("all systems operational")
        sys.exit(0)
    else:
        print("some checks failed")
        sys.exit(1)

if __name__ == "__main__":
    main()