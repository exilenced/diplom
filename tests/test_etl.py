# tests/test_etl.py
import pytest
import pandas as pd
from pathlib import Path
from sqlalchemy import text
from src.etl.load_to_postgres import DataLoader
from src.database.connection import db

class TestETL:
    """Тесты ETL пайплайна"""
    
    def test_loader_init(self):
        """Проверка инициализации загрузчика"""
        loader = DataLoader()
        expected_path = Path.cwd() / "data" / "generated"
        assert loader.data_dir == expected_path
    
    def test_read_csv(self, temp_csv_dir, sample_drivers_data):
        """Проверка чтения CSV"""
        csv_path = temp_csv_dir / "drivers.csv"
        sample_drivers_data.to_csv(csv_path, index=False)

        loader = DataLoader()
        original_dir = loader.data_dir
        loader.data_dir = temp_csv_dir
        
        df = loader._read_csv('drivers.csv')
        assert df is not None
        assert len(df) == len(sample_drivers_data)
        
        loader.data_dir = original_dir
    
    def test_filter_duplicates_method(self, sample_drivers_data, test_db):
        """Проверка метода _filter_duplicates напрямую"""
        loader = DataLoader()

        with db.get_connection() as conn:
            conn.execute(text("truncate table driver_activity cascade;"))
            conn.execute(text("truncate table trips cascade;"))
            conn.execute(text("truncate table drivers cascade;"))
            conn.commit()

        first_half = sample_drivers_data.iloc[:50].copy()
        with db.get_connection() as conn:
            first_half.to_sql('drivers', conn, if_exists='append', index=False)

        df_with_dupe = pd.concat([
            sample_drivers_data,
            sample_drivers_data.iloc[[0]]
        ], ignore_index=True)

        filtered_df = loader._filter_duplicates(df_with_dupe, 'drivers', 'driver_id')

        assert len(filtered_df) == 50
    
    def test_duplicate_handling_in_load_drivers(self, sample_drivers_data, test_db):
        """Проверка обработки дубликатов в load_drivers"""
        loader = DataLoader()

        with db.get_connection() as conn:
            conn.execute(text("truncate table driver_activity cascade;"))
            conn.execute(text("truncate table trips cascade;"))
            conn.execute(text("truncate table drivers cascade;"))
            conn.commit()

        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            df_with_dupe = pd.concat([
                sample_drivers_data,
                sample_drivers_data.iloc[[0]]
            ], ignore_index=True)
            
            df_with_dupe.to_csv(tmp_path / 'drivers.csv', index=False)

            original_dir = loader.data_dir
            loader.data_dir = tmp_path
            
            result1 = loader.load_drivers()
            assert result1 is True

            with db.get_connection() as conn:
                count = conn.execute(text("select count(*) from drivers")).scalar()
                assert count == 100, f"expected 100, got {count}"

            result2 = loader.load_drivers()
            assert result2 is True

            with db.get_connection() as conn:
                count = conn.execute(text("select count(*) from drivers")).scalar()
                assert count == 100, f"expected 100, got {count}"
            
            loader.data_dir = original_dir
    
    def test_duplicate_handling_in_load_trips(self, sample_drivers_data, sample_trips_data, test_db):
        """Проверка обработки дубликатов в load_trips"""
        loader = DataLoader()

        with db.get_connection() as conn:
            conn.execute(text("truncate table driver_activity cascade;"))
            conn.execute(text("truncate table trips cascade;"))
            conn.execute(text("truncate table drivers cascade;"))
            conn.commit()

            sample_drivers_data.to_sql('drivers', conn, if_exists='append', index=False)
        
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            trips_with_dupe = pd.concat([
                sample_trips_data,
                sample_trips_data.iloc[[0]]
            ], ignore_index=True)
            
            trips_with_dupe.to_csv(tmp_path / 'trips.csv', index=False)

            original_dir = loader.data_dir
            loader.data_dir = tmp_path

            result1 = loader.load_trips()
            assert result1 is True

            with db.get_connection() as conn:
                count = conn.execute(text("select count(*) from trips")).scalar()
                assert count == len(sample_trips_data), f"expected {len(sample_trips_data)}, got {count}"

            result2 = loader.load_trips()
            assert result2 is True

            with db.get_connection() as conn:
                count = conn.execute(text("select count(*) from trips")).scalar()
                assert count == len(sample_trips_data), f"expected {len(sample_trips_data)}, got {count}"
            
            loader.data_dir = original_dir
    
    @pytest.mark.integration
    def test_full_etl_flow(self, test_db, temp_csv_dir, sample_drivers_data, sample_trips_data):
        """Интеграционный тест всего ETL"""
        sample_drivers_data.to_csv(temp_csv_dir / 'drivers.csv', index=False)
        sample_trips_data.to_csv(temp_csv_dir / 'trips.csv', index=False)

        from src.data_generation.generate_data import generate_driver_activity
        activity_df = generate_driver_activity(
            sample_drivers_data['driver_id'].tolist(),
            sample_trips_data
        )
        activity_df.to_csv(temp_csv_dir / 'driver_activity.csv', index=False)

        loader = DataLoader()
        original_dir = loader.data_dir
        loader.data_dir = temp_csv_dir
        
        success = loader.run_all()
        assert success is True

        with test_db.connect() as conn:
            drivers = pd.read_sql("select count(*) from drivers", conn).iloc[0,0]
            trips = pd.read_sql("select count(*) from trips", conn).iloc[0,0]
            activity = pd.read_sql("select count(*) from driver_activity", conn).iloc[0,0]
            
            assert drivers == len(sample_drivers_data)
            assert trips == len(sample_trips_data)
            assert activity > 0
        
        loader.data_dir = original_dir