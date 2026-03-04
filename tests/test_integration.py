# tests/test_integration.py
import pytest
import subprocess
import sys
from pathlib import Path
from sqlalchemy import text
import shutil
import os

class TestPipelineIntegration:
    """Интеграционные тесты всего пайплайна"""
    
    def test_generate_script_runs(self):
        """Проверка, что скрипт генерации запускается"""
        result = subprocess.run(
            [sys.executable, "src/data_generation/generate_data.py"],
            capture_output=True,
            text=True
        )
        assert result.returncode == 0
    
    def test_etl_script_runs(self, test_db):
        """Проверка, что ETL скрипт запускается на чистой БД"""
        from src.database.connection import db
        from src.utils.config import config

        test_db_url = str(test_db.url)
        test_db_name = test_db.url.database
        
        print(f"\nusing test db: {test_db_name}")

        original_engine = db.engine
        original_db_name = config.DB_NAME
        
        try:
            config.DB_NAME = test_db_name

            db.engine = test_db

            with db.get_connection() as conn:

                conn.execute(text("drop table if exists driver_activity cascade;"))
                conn.execute(text("drop table if exists trips cascade;"))
                conn.execute(text("drop table if exists drivers cascade;"))
                conn.commit()

                sql_path = Path(__file__).parent.parent / 'sql' / 'create_tables.sql'
                with open(sql_path, 'r', encoding='utf-8') as f:
                    sql = f.read()
                    for statement in sql.split(';'):
                        if statement.strip():
                            conn.execute(text(statement))
                    conn.commit()

            with db.get_connection() as conn:
                drivers_count = conn.execute(text("select count(*) from drivers")).scalar()
                trips_count = conn.execute(text("select count(*) from trips")).scalar()
                activity_count = conn.execute(text("select count(*) from driver_activity")).scalar()
                
                print(f"\ninitial counts in {test_db_name}: drivers: {drivers_count}, trips: {trips_count}, activity: {activity_count}")
                assert drivers_count == 0
                assert trips_count == 0
                assert activity_count == 0

            csv_dir = Path("data/generated")
            if csv_dir.exists():
                shutil.rmtree(csv_dir)
            csv_dir.mkdir(parents=True)

            from src.data_generation.generate_data import main as generate_main
            generate_main()

            env = os.environ.copy()
            env['DB_NAME'] = test_db_name
            env['PYTHONPATH'] = str(Path.cwd())
            
            result = subprocess.run(
                [sys.executable, "-m", "src.etl.load_to_postgres"],
                capture_output=True,
                text=True,
                env=env
            )

            print("\netl stdout:", result.stdout)
            print("etl stderr:", result.stderr)

            assert result.returncode == 0
            output = result.stdout + result.stderr

            with db.get_connection() as conn:
                drivers_count = conn.execute(text("select count(*) from drivers")).scalar()
                trips_count = conn.execute(text("select count(*) from trips")).scalar()
                activity_count = conn.execute(text("select count(*) from driver_activity")).scalar()
                
                print(f"\nfinal counts in {test_db_name}: drivers: {drivers_count}, trips: {trips_count}, activity: {activity_count}")
                assert drivers_count == 5000
                assert trips_count == 50000
                assert activity_count > 0
                
        finally:
            db.engine = original_engine
            config.DB_NAME = original_db_name
    
    def test_full_pipeline(self, tmp_path):
        """Полный тест пайплайна во временной директории"""
        temp_data = tmp_path / "data" / "generated"
        temp_data.mkdir(parents=True)
        
        env = os.environ.copy()
        env['DATA_DIR'] = str(temp_data)
        env['PYTHONPATH'] = str(Path.cwd())
        
        result = subprocess.run(
            [sys.executable, "src/data_generation/generate_data.py"],
            capture_output=True,
            text=True,
            env=env,
            cwd=Path.cwd()
        )
        
        assert result.returncode == 0

        assert (temp_data / "drivers.csv").exists(), f"file not found in {temp_data}"
        assert (temp_data / "trips.csv").exists()
        assert (temp_data / "driver_activity.csv").exists()

        assert (temp_data / "drivers.csv").stat().st_size > 0
        assert (temp_data / "trips.csv").stat().st_size > 0
        assert (temp_data / "driver_activity.csv").stat().st_size > 0