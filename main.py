# main.py (обновленная версия)
#!/usr/bin/env python
"""
Главный оркестратор проекта
Запускает полный цикл: генерация данных -> ETL -> (позже ML)
"""
import sys
import logging
from pathlib import Path
from datetime import datetime

# Настройка логирования
log_dir = Path(__file__).parent / 'logs'
log_dir.mkdir(exist_ok=True)

log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.DEBUG,  # Сменили на DEBUG для деталей
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def run_data_generation():
    """Запуск генерации данных"""
    logger.info("📊 Starting data generation...")
    print("\n=== DEBUG: Entering run_data_generation ===")  # Принудительный вывод
    try:
        from src.data_generation.generate_data import main as generate_main
        print("=== DEBUG: Import successful, calling generate_main() ===")
        generate_main()
        print("=== DEBUG: generate_main() completed ===")
        logger.info("✅ Data generation completed")
        return True
    except Exception as e:
        print(f"=== DEBUG: Exception in data generation: {e} ===")
        logger.error(f"❌ Data generation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def run_etl():
    """Запуск ETL пайплайна"""
    logger.info("🔄 Starting ETL pipeline...")
    print("\n=== DEBUG: Entering run_etl ===")
    try:
        from src.etl.load_to_postgres import main as etl_main
        print("=== DEBUG: Import successful, calling etl_main() ===")
        etl_main()
        print("=== DEBUG: etl_main() completed ===")
        logger.info("✅ ETL pipeline completed")
        return True
    except Exception as e:
        print(f"=== DEBUG: Exception in ETL: {e} ===")
        logger.error(f"❌ ETL pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def run_healthcheck():
    """Проверка здоровья системы"""
    logger.info("🏥 Running healthcheck...")
    print("\n=== DEBUG: Entering run_healthcheck ===")
    try:
        from scripts.healthcheck import main as healthcheck_main
        print("=== DEBUG: Import successful, calling healthcheck_main() ===")
        healthcheck_main()
        print("=== DEBUG: healthcheck_main() completed ===")
        return True
    except Exception as e:
        print(f"=== DEBUG: Exception in healthcheck: {e} ===")
        logger.error(f"❌ Healthcheck failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Главная функция"""
    logger.info("="*60)
    logger.info("🚀 STARTING PIPELINE")
    logger.info("="*60)
    
    stages = [
        ("Healthcheck", run_healthcheck),
        ("Data Generation", run_data_generation),
        ("ETL", run_etl)
    ]
    
    for i, (stage_name, stage_func) in enumerate(stages):
        logger.info(f"\n--- Stage {i+1}/{len(stages)}: {stage_name} ---")
        print(f"\n=== DEBUG: About to run stage: {stage_name} ===")
        
        result = stage_func()
        
        print(f"=== DEBUG: Stage {stage_name} returned: {result} ===")
        
        if not result:
            logger.error(f"❌ Pipeline stopped at {stage_name}")
            print(f"=== DEBUG: Pipeline stopping at {stage_name} ===")
            sys.exit(1)
        
        logger.info(f"✅ {stage_name} completed")
        print(f"=== DEBUG: Stage {stage_name} completed successfully ===\n")
    
    logger.info("="*60)
    logger.info("✅ PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("="*60)

if __name__ == "__main__":
    main()