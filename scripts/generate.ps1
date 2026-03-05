python -c "
from src.database.connection import db
from sqlalchemy import text
with db.get_connection() as conn:
    conn.execute(text('truncate table driver_activity cascade;'))
    conn.execute(text('truncate table trips cascade;'))
    conn.execute(text('truncate table drivers cascade;'))
    conn.commit()
    print('tables truncated')
"

Remove-Item data\generated\*.csv

python src/data_generation/generate_data.py

python scripts/check_csv_consistency.py

python -m src.etl.load_to_postgres