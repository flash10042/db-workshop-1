import psycopg2
from config import db_config

conn = psycopg2.connect(**db_config)
cur = conn.cursor()
cur.execute("DROP TABLE tbl_zno_res")
cur.execute("DROP TABLE tbl_zno_temp")
cur.close()
conn.commit()
conn.close()
