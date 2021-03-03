import os
import re
import csv
import time
import json
import psycopg2
import psycopg2.extras
from config import db_config

BATCH_SIZE = 50
QUERY = (
    '''
        INSERT INTO tbl_zno_res (
            zno_year,
            OUTID,
            Birth,
            SEXTYPENAME,
            REGNAME,
            AREANAME,
            TERNAME,
            REGTYPENAME,
            TerTypeName,
            ClassProfileNAME,
            ClassLangName,
            EONAME,
            EOTYPENAME,
            EORegName,
            EOAreaName,
            EOTerName,
            EOParent,
            UkrTest,
            UkrTestStatus,
            UkrBall100,
            UkrBall12,
            UkrBall,
            UkrAdaptScale,
            UkrPTName,
            UkrPTRegName,
            UkrPTAreaName,
            UkrPTTerName,
            histTest,
            HistLang,
            histTestStatus,
            histBall100,
            histBall12,
            histBall,
            histPTName,
            histPTRegName,
            histPTAreaName,
            histPTTerName,
            mathTest,
            mathLang,
            mathTestStatus,
            mathBall100,
            mathBall12,
            mathBall,
            mathPTName,
            mathPTRegName,
            mathPTAreaName,
            mathPTTerName,
            physTest,
            physLang,
            physTestStatus,
            physBall100,
            physBall12,
            physBall,
            physPTName,
            physPTRegName,
            physPTAreaName,
            physPTTerName,
            chemTest,
            chemLang,
            chemTestStatus,
            chemBall100,
            chemBall12,
            chemBall,
            chemPTName,
            chemPTRegName,
            chemPTAreaName,
            chemPTTerName,
            bioTest,
            bioLang,
            bioTestStatus,
            bioBall100,
            bioBall12,
            bioBall,
            bioPTName,
            bioPTRegName,
            bioPTAreaName,
            bioPTTerName,
            geoTest,
            geoLang,
            geoTestStatus,
            geoBall100,
            geoBall12,
            geoBall,
            geoPTName,
            geoPTRegName,
            geoPTAreaName,
            geoPTTerName,
            engTest,
            engTestStatus,
            engBall100,
            engBall12,
            engDPALevel,
            engBall,
            engPTName,
            engPTRegName,
            engPTAreaName,
            engPTTerName,
            fraTest,
            fraTestStatus,
            fraBall100,
            fraBall12,
            fraDPALevel,
            fraBall,
            fraPTName,
            fraPTRegName,
            fraPTAreaName,
            fraPTTerName,
            deuTest,
            deuTestStatus,
            deuBall100,
            deuBall12,
            deuDPALevel,
            deuBall,
            deuPTName,
            deuPTRegName,
            deuPTAreaName,
            deuPTTerName,
            spaTest,
            spaTestStatus,
            spaBall100,
            spaBall12,
            spaDPALevel,
            spaBall,
            spaPTName,
            spaPTRegName,
            spaPTAreaName,
            spaPTTerName
        ) VALUES %s
    ''',
    '''
        UPDATE tbl_zno_temp SET record_id = %s, is_done = %s WHERE zno_year = %s
    '''
)
NUMERIC_COLS = [1, 18, 19, 20, 21, 29, 30, 31, 39, 40, 41, 49, 50, 51, 59, 60, 61, 69, 70, 71, 79, 80, 81, 88, 89, 91, 98, 99, 101, 108, 109, 111, 118, 119, 121]


def populate_table(conn):
    cur = conn.cursor()

    executions_time = dict()
    for file_name in os.listdir('data'):
        year = re.findall(r'Odata(\d{4})File.csv', file_name)
        if year:
            with open(os.path.join('data', file_name), encoding='cp1251') as csvfile:
                csv_reader = csv.reader(csvfile, delimiter=';')
                # SKIP HEADER
                next(csv_reader)

                start_time = time.time()
                idx = 0
                batch = list()

                cur.execute('SELECT record_id, is_done FROM tbl_zno_temp WHERE zno_year = %s', year)
                res = cur.fetchone()

                if res is None:
                    cur.execute('INSERT INTO tbl_zno_temp (zno_year, record_id, is_done) VALUES (%s, %s, %s)', year + [idx, False])
                else:
                    if res[-1]:
                        print(f'File {file_name} has been already processed. Going to next...')
                        continue
                    for row in csv_reader:
                        idx += 1
                        if idx >= res[0]:
                            break

                print(f'Start exporting data from {file_name}')
                for row in csv_reader:
                    # PREPROCESS
                    for i in range(len(row)):
                        if row[i] == 'null':
                            row[i] = None
                        else:
                            if i in NUMERIC_COLS:
                                row[i] = row[i].replace(',', '.')
                                row[i] = float(row[i])

                    idx += 1
                    batch.append(year + row)
                    if not idx % BATCH_SIZE:
                        psycopg2.extras.execute_values(cur, QUERY[0], batch)
                        cur.execute(QUERY[1], [idx, False] + year)
                        batch = list()
                        conn.commit()
                if batch:
                    psycopg2.extras.execute_values(cur, QUERY[0], batch)
                    cur.execute(QUERY[1], [idx, True] + year)
                    batch = list()
                    conn.commit()
                exec_time = time.time() - start_time
                print(f'File {file_name} done. Execution time - {exec_time} sec.')
                executions_time[file_name] = exec_time

    print('All files have been iterated.')
    cur.close()

    if executions_time:
        dump = json.dumps(executions_time)
        with open(os.path.join('data', 'insert_time.json'), 'w') as file:
            file.write(dump)


if __name__ == '__main__':
    conn = psycopg2.connect(**db_config)
    populate_table(conn)
    conn.close()
