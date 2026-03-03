import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

try:
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    conn.autocommit = True
    cursor = conn.cursor()

    with open('sql/create_tables.sql', 'r', encoding='utf-8') as f:
        sql_script = f.read()
        cursor.execute(sql_script)
        print('tables created successfully')

        cursor.close()
        conn.close()
except Exception as e:
    print(f"error: {e}")