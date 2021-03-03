import psycopg2
import csv
import os
from config import db_config

RESULT_FILE = 'result.csv'
QUERY = '''
    SELECT 
        zno_year, 
        max(physBall100), 
        max(physBall12), 
        max(physBall) 
    FROM tbl_zno_res 
    WHERE physTestStatus = 'Зараховано' 
    GROUP BY zno_year
'''
COLUMNS = ['Year', '100-200 Grade', 'DPA Grade', 'Test Grade']


def select_table(conn):
    cur = conn.cursor()

    cur.execute(QUERY)
    res = cur.fetchall()

    with open(os.path.join('data', RESULT_FILE), 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile, dialect='excel')
        csv_writer.writerow(COLUMNS)
        csv_writer.writerows(res)
    print(f'Select operation finished. The result is written to {RESULT_FILE}.')

    cur.close()


if __name__ == '__main__':
    conn = psycopg2.connect(**db_config)
    select_table(conn)
    conn.close()
