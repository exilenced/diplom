import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

import pandas as pd
from sqlalchemy import text

root_dir = Path(__file__).parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

log_dir = Path(__file__).parent / 'logs'
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

from src.data_generation.generate_data import DataGenerator, main as generate_main
from src.etl.load_to_postgres import DataLoader, main as etl_main
from src.database.connection import db
from src.utils.config import config

class PipelineOrchestrator:
    """
    data processing pipeline orchestrator
    """

    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.stats = {}

    def run_healthcheck(self) -> bool:
        """
        healthcheck before start
        """
        self.logger.info("running healthcheck...")

        checks = {
            'database': self._check_database(),
            'data_directory': self._check_data_directory(),
        }

        all_passed = all(checks.values())

        if all_passed:
            self.logger.info("all healthchecks passed")
        else:
            self.logger.error("some healthchecks failed: ")
            for check, status in checks.items():
                status_symbol = "V" if status else "X"
                self.logger.error(f"{status_symbol} {check}")
        return all_passed
    
    def _check_database(self) -> bool:
        """
        db connection check
        """
        try:
            if db.test_connection():
                with db.get_connection() as conn:
                    tables = pd.read_sql("""
                                         select table_name
                                         from information_schema.tables
                                         where table_schema = 'public'
                                        """, conn)
                    required_tables = ['drivers', 'trips', 'driver_activity']
                    existing_tables = tables['table_name'].tolist()

                    for table in required_tables:
                        if table in existing_tables:
                            count = pd.read_sql(f"select count(*) from {table}", conn).iloc[0,0]
                            self.logger.info(f"table {table}: {count} rows")
                        else:
                            self.logger.warning(f"table {table} does not exist yet")
                return True
            return False
        except Exception as e:
            self.logger.error(f"db check failed: {e}")
            return False
    def _check_data_directory(self) -> bool:
        """
        data directory check
        """
        try:
            if config.DATA_DIR.exists():
                self.logger.info(f"data directory exists: {config.DATA_DIR}")
                test_file = config.DATA_DIR / '.write_test'
                test_file.touch()
                test_file.unlink()
                return True
            else:
                self.logger.warning(f"data directory does not exist: {config.DATA_DIR}")
                config.DATA_DIR.mkdir(parents=True, exist_ok=True)
                self.logger.info(f"created data directory: {config.DATA_DIR}")
                return True
        except Exception as e:
            self.logger.error(f"data directory check failed: {e}")
            return False
    
    def run_data_generation(self, n_drivers: int = 1000, n_trips: int = 50000) -> bool:
        """
        data gen start
        """
        self.logger.info(f"starting data generation: {n_drivers} drivers, {n_trips} trips")

        try:
            drivers, trips, activities = generate_main(n_drivers, n_trips)

            self.stats.update({
                'drivers_generated': len(drivers),
                'trips_generated': len(trips),
                'activities_generated': len(activities)
            })

            self.logger.info(
                f"data generation completed: "
                f"{len(drivers)} drivers, {len(trips)} trips, {len(activities)} activities"
            )
            return True
        except Exception as e:
            self.logger.error(f"data generation failed: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def run_etl(self) -> bool:
        """
        etl pipeline start
        """

        self.logger.info("starting ETL pipeline...")

        try:
            required_files = ['drivers.csv', 'trips.csv', 'driver_activity.csv']
            missing_files = []

            for file in required_files:
                if not (config.DATA_DIR / file).exists:
                    missing_files.append(file)
            
            if missing_files:
                self.logger.error(f"missing required files: {missing_files}")
                self.logger.error("please run data generation first")
                return False
            loader = DataLoader()
            success = loader.run_all()

            if success:
                with db.get_connection() as conn:
                    for table in ['drivers', 'trips', 'driver_activity']:
                        count = conn.execute(text(f"select count(*) from {table}")).scalar()
                        self.stats[f'{table}_after_etl'] = count
                
                self.logger.info(f"etl completed. DB stats: {self.stats}")
            else:
                self.logger.error("elt failed")
            return success
        except Exception as e:
            self.logger.error(f"etl pipeline failed: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def print_summary(self):
        """
        display stats
        """

        self.logger.info('\npipeline summary\n')
        if self.stats:
            for key, value in self.stats.items():
                self.logger.info(f"{key}: {value}")
        else:
            self.logger.info('no stats viable')

def main():
    """
    pipeline main func
    """
    logger.info('starting data pipeline')

    orchestrator = PipelineOrchestrator()

    pipeline_stages = [
        ('Healthcheck', orchestrator.run_healthcheck),
        ('Data Generation', lambda: orchestrator.run_data_generation(
            n_drivers=1000,
            n_trips=50000
        )),
        ('ETL', orchestrator.run_etl),
    ]

    for stage_name, stage_func in pipeline_stages:
        logger.info(f"\n Stage: {stage_name}\n")

        try:
            stage_success = stage_func()

            if not stage_success:
                logger.error(f"pipeline stopped at {stage_name}")
                orchestrator.print_summary()
                sys.exit(1)
            logger.info(f"stage completed: {stage_name}")
        except KeyboardInterrupt:
            logger.info('\n pipeline interrupted by user')
            orchestrator.print_summary()
            sys.exit(130)
        except Exception as e:
            logger.error(f"unexpected error in {stage_name}: {e}")
            import traceback
            traceback.print_exc()
            orchestrator.print_summary()
            sys.exit(1)
        
        logger.info('pipeline completed successfully')
        orchestrator.print_summary()

        logger.info(f'generated data: {config.DATA_DIR}')
        logger.info(f'pipeline log: {log_file}')
        logger.info(f'database: {config.DB_NAME}@{config.DB_HOST}')

if __name__ == "__main__":
    main()