import time
import psycopg2
from create import create_table
from populate import populate_table
from select import select_table
from config import db_config

conn = None
retries = 15

while retries:
    try:
        conn = psycopg2.connect(**db_config)
        break
    except Exception as err:
        print(err)
        retries -= 1
        print('Retries -', retries)
        time.sleep(1)

try:
    create_table(conn)
    populate_table(conn)
    select_table(conn)
except Exception as err:
    print(err)
finally:
    if conn is not None:
        conn.close()
