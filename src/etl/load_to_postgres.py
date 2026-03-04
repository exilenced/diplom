# src/etl/load_to_postgres.py
import pandas as pd
import logging
from typing import Optional
from sqlalchemy import text
from src.database.connection import db
from src.utils.config import config
import os

if 'DB_NAME' in os.environ:
    config.DB_NAME = os.environ['DB_NAME']
    from src.database.connection import db
    db._init_engine()

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self):
        self.data_dir = config.DATA_DIR
        
    def _read_csv(self, filename: str) -> Optional[pd.DataFrame]:
        filepath = self.data_dir / filename
        if not filepath.exists():
            logger.error(f"file not found: {filepath}")
            return None
        
        try:
            df = pd.read_csv(filepath)
            for col in df.columns:
                if 'id' in col.lower():
                    df[col] = df[col].astype(str)
            logger.info(f"read {len(df)} rows from {filename}")
            return df
        except Exception as e:
            logger.error(f"error reading {filename}: {e}")
            return None
    
    def _filter_duplicates(self, df: pd.DataFrame, table_name: str, id_column: str) -> pd.DataFrame:
        """
        Фильтрует дубликаты:
        1. Удаляет дубликаты внутри самого CSV по id_column
        2. Удаляет записи, которые уже есть в БД
        """
        before_dedup = len(df)
        df = df.drop_duplicates(subset=[id_column], keep='first')
        after_dedup = len(df)
        if before_dedup > after_dedup:
            logger.info(f"removed {before_dedup - after_dedup} internal duplicates from CSV by {id_column}")

        try:
            with db.get_connection() as conn:
                query = f"select {id_column} from {table_name}"
                existing = pd.read_sql(query, conn)
                
                if not existing.empty:
                    existing_ids = set(existing[id_column].astype(str))

                    before = len(df)
                    df = df[~df[id_column].isin(existing_ids)]
                    after = len(df)
                    
                    logger.info(f"filtered {before - after} records already in DB from {table_name}")
                else:
                    logger.info(f"no existing records in {table_name}, keeping all unique from CSV")
                    
        except Exception as e:
            logger.warning(f"Error checking duplicates in {table_name}: {e}")
        
        return df
    
    def load_drivers(self, if_exists: str = 'append') -> bool:
        logger.info("="*50)
        logger.info("Loading drivers...")
        logger.info("="*50)
        
        df = self._read_csv('drivers.csv')
        if df is None:
            return False

        df['driver_id'] = df['driver_id'].astype(str)
        
        logger.info(f"original rows in CSV: {len(df)}")
        logger.info(f"unique driver_ids in CSV: {df['driver_id'].nunique()}")

        dupes_in_csv = df.duplicated(subset=['driver_id']).sum()
        logger.info(f"duplicates within CSV: {dupes_in_csv}")

        logger.info("calling _filter_duplicates...")
        df = self._filter_duplicates(df, 'drivers', 'driver_id')
        logger.info(f"after _filter_duplicates: {len(df)} rows")

        if not df.empty:
            with db.get_connection() as conn:
                existing_phones = pd.read_sql("select phone from drivers", conn)
                if not existing_phones.empty:
                    existing_phone_set = set(existing_phones['phone'].astype(str))
                    before = len(df)
                    df = df[~df['phone'].astype(str).isin(existing_phone_set)]
                    after = len(df)
                    logger.info(f"filtered {before - after} rows by existing phone numbers")
        
        if df.empty:
            logger.info("no new drivers to load")
            return True
        
        logger.info(f"loading {len(df)} new drivers...")
        return db.df_to_table(df, 'drivers', if_exists=if_exists)
    
    def load_trips(self, if_exists: str = 'append') -> bool:
        logger.info("loading trips...")
        
        df = self._read_csv('trips.csv')
        if df is None:
            return False

        df['driver_id'] = df['driver_id'].astype(str)
        df['trip_id'] = df['trip_id'].astype(str)
        
        logger.info(f"total trips in CSV: {len(df)}")

        with db.get_connection() as conn:
            existing_drivers = pd.read_sql("select driver_id from drivers", conn)
            
            if existing_drivers.empty:
                logger.error("no drivers found in database. Load drivers first.")
                return False
            
            existing_drivers['driver_id'] = existing_drivers['driver_id'].astype(str)
            valid_driver_ids = set(existing_drivers['driver_id'])
            
            logger.info(f"Found {len(valid_driver_ids)} valid drivers in DB")

            csv_driver_ids = set(df['driver_id'].unique())
            logger.info(f"Unique drivers in CSV: {len(csv_driver_ids)}")

            logger.info(f"Sample DB driver_ids: {list(valid_driver_ids)[:3]}")
            logger.info(f"Sample CSV driver_ids: {list(csv_driver_ids)[:3]}")

            common_ids = valid_driver_ids & csv_driver_ids
            logger.info(f"Common driver_ids: {len(common_ids)}")
            
            if len(common_ids) == 0:
                logger.error("no common driver_ids found!")
                logger.error("this means uuids in CSV don't match those in DB")
                return False

            before = len(df)
            df = df[df['driver_id'].isin(valid_driver_ids)]
            after = len(df)
            
            logger.info(f"filtered {before - after} trips with invalid driver_ids")
            
            if df.empty:
                logger.error("no valid trips left after filtering!")
                return False

        df = self._filter_duplicates(df, 'trips', 'trip_id')
        
        if df.empty:
            logger.info("no new trips to load")
            return True
        
        logger.info(f"loading {len(df)} new trips...")
        return db.df_to_table(df, 'trips', if_exists=if_exists)
    
    def load_driver_activity(self, if_exists: str = 'append') -> bool:
        logger.info("loading driver activity...")

        df = self._read_csv('driver_activity.csv')
        if df is None:
            return False

        df['driver_id'] = df['driver_id'].astype(str)
        df['activity_id'] = df['activity_id'].astype(str)
        df['date'] = pd.to_datetime(df['date']).dt.date
        
        logger.info(f"original driver_activity shape: {df.shape}")

        with db.get_connection() as conn:
            existing_drivers = pd.read_sql("select driver_id from drivers", conn)
            
            if existing_drivers.empty:
                logger.error("no drivers found in database!")
                return False
            
            existing_drivers['driver_id'] = existing_drivers['driver_id'].astype(str)
            valid_driver_ids = set(existing_drivers['driver_id'])
            
            csv_driver_ids = set(df['driver_id'].unique())
            common_ids = valid_driver_ids & csv_driver_ids
            logger.info(f"common driver_ids in activity: {len(common_ids)}")
            
            if len(common_ids) == 0:
                logger.error("no common driver_ids in activity!")
                return False

            before = len(df)
            df = df[df['driver_id'].isin(valid_driver_ids)]
            after = len(df)
            
            logger.info(f"filtered {before - after} rows with invalid driver_ids")
            
            if df.empty:
                logger.error("no valid activity rows left!")
                return False

        with db.get_connection() as conn:
            existing = pd.read_sql(
                "select driver_id, date from driver_activity", 
                conn
            )
            if not existing.empty:
                existing['driver_id'] = existing['driver_id'].astype(str)
                existing['date'] = pd.to_datetime(existing['date']).dt.date

                df['key'] = df['driver_id'].astype(str) + '_' + df['date'].astype(str)
                existing['key'] = existing['driver_id'].astype(str) + '_' + existing['date'].astype(str)
                
                existing_keys = set(existing['key'])
                before = len(df)
                df = df[~df['key'].isin(existing_keys)]
                after = len(df)
                
                logger.info(f"filtered {before - after} existing activity records")
                df = df.drop(columns=['key'])
        
        if df.empty:
            logger.info("no new activity to load")
            return True
        
        logger.info(f"loading {len(df)} new activity records...")
        return db.df_to_table(df, 'driver_activity', if_exists=if_exists)
    
    def run_all(self) -> bool:
        logger.info("starting etl pipeline")
        
        if not db.test_connection():
            logger.error("db connection failed")
            return False
        
        stages = [
            ("drivers", self.load_drivers),
            ("trips", self.load_trips),
            ("driver_activity", self.load_driver_activity)
        ]
        
        overall_success = True
        
        for stage_name, stage_func in stages:
            logger.info(f"\n--- Stage: {stage_name} ---")
            try:
                stage_success = stage_func()
                if not stage_success:
                    logger.error(f"stage {stage_name} failed")
                    overall_success = False
                else:
                    logger.info(f"stage {stage_name} completed")
            except Exception as e:
                logger.error(f"stage {stage_name} crashed: {e}")
                import traceback
                traceback.print_exc()
                overall_success = False
        
        if overall_success:
            logger.info("etl pipeline completed")
        else:
            logger.error("etl pipeline got errors")

        return overall_success

def main():
    loader = DataLoader()
    return loader.run_all()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()