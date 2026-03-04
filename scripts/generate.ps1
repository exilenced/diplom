python -c "
from src.database.connection import db
from sqlalchemy import text
with db.get_connection() as conn:
    conn.execute(text('TRUNCATE TABLE driver_activity CASCADE;'))
    conn.execute(text('TRUNCATE TABLE trips CASCADE;'))
    conn.execute(text('TRUNCATE TABLE drivers CASCADE;'))
    conn.commit()
    print('✅ Tables truncated')
"

Remove-Item data\generated\*.csv

python src/data_generation/generate_data.py

python scripts/check_csv_consistency.py

python -m src.etl.load_to_postgres