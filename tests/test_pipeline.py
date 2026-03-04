# scripts/test_pipeline.py
#!/usr/bin/env python
"""
Полный тест пайплайна: генерация -> ETL -> проверка
"""
import subprocess
import time
import re


from src.database.connection import db
from sqlalchemy import text
import pandas as pd

class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'

def print_step(title):
    print(f"\n{Colors.CYAN}▶ {title}{Colors.END}")

def print_success(msg):
    print(f"  {Colors.GREEN}✅ {msg}{Colors.END}")

def print_error(msg):
    print(f"  {Colors.RED}❌ {msg}{Colors.END}")

def run_cmd(cmd, capture=True):
    """Запуск команды и возврат вывода"""
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            capture_output=capture, 
            text=True,
            encoding='utf-8'
        )
        return result.stdout, result.stderr, result.returncode
    except Exception as e:
        return "", str(e), 1

def main():
    start_time = time.time()
    has_errors = False
    
    print(f"{Colors.BOLD}{Colors.HEADER}============================================================{Colors.END}")
    print(f"{Colors.BOLD}{Colors.HEADER}🧪 TESTING COMPLETE PIPELINE{Colors.END}")
    print(f"{Colors.BOLD}{Colors.HEADER}============================================================{Colors.END}")
    
    # 1. Проверка структуры
    print_step("CHECKING PROJECT STRUCTURE")
    required_dirs = [
        "src/data_generation",
        "src/etl",
        "src/database",
        "src/utils",
        "data/generated",
        "sql",
        "scripts"
    ]
    
    for dir_path in required_dirs:
        if Path(dir_path).exists():
            print_success(f"Directory exists: {dir_path}")
        else:
            print_error(f"Directory missing: {dir_path}")
            has_errors = True
    
    # 2. Подключение к БД
    print_step("TESTING DATABASE CONNECTION")
    if db.test_connection():
        print_success("Database connection successful")
    else:
        print_error("Database connection failed")
        has_errors = True
    
    # 3. Очистка
    print_step("CLEANING OLD DATA")
    
    # Удаляем CSV
    csv_files = list(Path("data/generated").glob("*.csv"))
    if csv_files:
        for f in csv_files:
            f.unlink()
        print_success(f"Removed {len(csv_files)} old CSV files")
    else:
        print_success("No old CSV files found")
    
    # Очищаем таблицы
    try:
        with db.get_connection() as conn:
            conn.execute(text("TRUNCATE TABLE driver_activity CASCADE;"))
            conn.execute(text("TRUNCATE TABLE trips CASCADE;"))
            conn.execute(text("TRUNCATE TABLE drivers CASCADE;"))
            conn.commit()
        print_success("Database tables truncated")
    except Exception as e:
        print_error(f"Failed to truncate tables: {e}")
        has_errors = True
    
    # 4. Генерация данных
    print_step("GENERATING TEST DATA")
    stdout, stderr, code = run_cmd("python src/data_generation/generate_data.py")
    
    if code == 0 and "ГЕНЕРАЦИЯ ЗАВЕРШЕНА УСПЕШНО" in stdout:
        print_success("Data generation completed")
        
        # Извлекаем статистику
        drivers = re.search(r"Водители: ([\d,]+)", stdout)
        trips = re.search(r"Поездки: ([\d,]+)", stdout)
        activity = re.search(r"Записи активности: ([\d,]+)", stdout)
        
        if drivers:
            print_success(f"  Drivers: {drivers.group(1)}")
        if trips:
            print_success(f"  Trips: {trips.group(1)}")
        if activity:
            print_success(f"  Activity: {activity.group(1)}")
    else:
        print_error("Data generation failed")
        if stderr:
            print(stderr)
        has_errors = True
    
    # 5. Проверка CSV
    print_step("CHECKING CSV CONSISTENCY")
    stdout, stderr, code = run_cmd("python scripts/check_csv_consistency.py")
    
    if code == 0 and "ALL CHECKS PASSED" in stdout:
        print_success("CSV consistency check passed")
        
        # Проверка на колонку date
        try:
            trips_df = pd.read_csv("data/generated/trips.csv")
            if 'date' not in trips_df.columns:
                print_success("  No 'date' column in trips.csv")
            else:
                print_error("  Found 'date' column in trips.csv!")
                has_errors = True
        except Exception as e:
            print_error(f"  Failed to read trips.csv: {e}")
            has_errors = True
    else:
        print_error("CSV consistency check failed")
        if stderr:
            print(stderr)
        has_errors = True
    
    # 6. ETL - ИСПРАВЛЕНО
    print_step("RUNNING ETL PIPELINE")
    stdout, stderr, code = run_cmd("python -m src.etl.load_to_postgres")
    
    # Проверяем по коду возврата, а не по тексту
    if code == 0:
        print_success("ETL pipeline completed (exit code 0)")
        # Для информации покажем последние строки
        last_lines = stdout.strip().split('\n')[-3:]
        print_success("  Last output:")
        for line in last_lines:
            if line.strip():
                print(f"    {line}")
    else:
        print_error(f"ETL pipeline failed (exit code: {code})")
        if stderr:
            print(stderr)
        if stdout:
            print("\n" + stdout[-500:])  # Последние 500 символов
        has_errors = True
    
    # 7. Проверка в БД
    print_step("VERIFYING DATABASE LOAD")
    try:
        with db.get_connection() as conn:
            drivers = conn.execute(text("SELECT COUNT(*) FROM drivers")).scalar()
            trips = conn.execute(text("SELECT COUNT(*) FROM trips")).scalar()
            activity = conn.execute(text("SELECT COUNT(*) FROM driver_activity")).scalar()
            
            orphan_trips = conn.execute(text("""
                SELECT COUNT(*) FROM trips t 
                LEFT JOIN drivers d ON t.driver_id = d.driver_id 
                WHERE d.driver_id IS NULL
            """)).scalar()
            
            orphan_activity = conn.execute(text("""
                SELECT COUNT(*) FROM driver_activity a 
                LEFT JOIN drivers d ON a.driver_id = d.driver_id 
                WHERE d.driver_id IS NULL
            """)).scalar()
        
        print_success("Database final state:")
        print_success(f"  Drivers: {drivers:,} rows")
        print_success(f"  Trips: {trips:,} rows")
        print_success(f"  Activity: {activity:,} rows")
        
        if orphan_trips == 0:
            print_success("  No orphan trips (all driver_ids valid)")
        else:
            print_error(f"  Found {orphan_trips} orphan trips!")
            has_errors = True
        
        if orphan_activity == 0:
            print_success("  No orphan activity records")
        else:
            print_error(f"  Found {orphan_activity} orphan activity records!")
            has_errors = True
            
    except Exception as e:
        print_error(f"Failed to verify database: {e}")
        has_errors = True
    
    # 8. Дополнительные проверки
    print_step("CHECKING DATA RELATIONSHIPS")
    try:
        with db.get_connection() as conn:
            inactive = conn.execute(text("""
                SELECT COUNT(*) FROM drivers d 
                LEFT JOIN trips t ON d.driver_id = t.driver_id 
                WHERE t.trip_id IS NULL
            """)).scalar()
            
            active = conn.execute(text("""
                SELECT COUNT(DISTINCT driver_id) FROM trips
            """)).scalar()
            
            revenue = conn.execute(text("""
                SELECT SUM(fare_amount) FROM trips
            """)).scalar() or 0
        
        print_success(f"  Active drivers (with trips): {active:,}")
        print_success(f"  Inactive drivers: {inactive:,}")
        print_success(f"  Total revenue: {revenue:,.2f} руб")
        
        # Проверка консистентности (активные + неактивные = всего водителей)
        if active + inactive == drivers:
            print_success("  Driver counts are consistent")
        else:
            print_error(f"  Driver count mismatch: active({active}) + inactive({inactive}) != total({drivers})")
            has_errors = True
            
    except Exception as e:
        print_error(f"Failed to check relationships: {e}")
        has_errors = True
    
    # Итог
    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    
    print(f"\n{Colors.BOLD}{Colors.HEADER}============================================================{Colors.END}")
    if not has_errors:
        print(f"{Colors.BOLD}{Colors.GREEN}✅ ALL TESTS PASSED SUCCESSFULLY{Colors.END}")
        print(f"{Colors.GREEN}  Drivers: {drivers:,} rows{Colors.END}")
        print(f"{Colors.GREEN}  Trips: {trips:,} rows{Colors.END}")
        print(f"{Colors.GREEN}  Activity: {activity:,} rows{Colors.END}")
        print(f"{Colors.GREEN}  Time: {minutes}:{seconds:02d} min{Colors.END}")
    else:
        print(f"{Colors.BOLD}{Colors.RED}❌ TESTS COMPLETED WITH ERRORS{Colors.END}")
        print(f"{Colors.RED}  Check the errors above{Colors.END}")
    
    print(f"{Colors.BOLD}{Colors.HEADER}============================================================{Colors.END}")
    print(f"\nTotal execution time: {minutes}:{seconds:02d} minutes")
    
    sys.exit(1 if has_errors else 0)

if __name__ == "__main__":
    main()