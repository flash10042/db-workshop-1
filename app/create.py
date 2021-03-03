import psycopg2
from config import db_config

CREATE_QUERY = (
    '''
        CREATE TABLE IF NOT EXISTS tbl_zno_res (
            record_id SERIAL PRIMARY KEY,
            zno_year INTEGER,
            OUTID VARCHAR(255),
            Birth NUMERIC,
            SEXTYPENAME VARCHAR(255),
            REGNAME VARCHAR(255),
            AREANAME VARCHAR(255),
            TERNAME VARCHAR(255),
            REGTYPENAME VARCHAR(255),
            TerTypeName VARCHAR(255),
            ClassProfileNAME VARCHAR(255),
            ClassLangName VARCHAR(255),
            EONAME VARCHAR(255),
            EOTYPENAME VARCHAR(255),
            EORegName VARCHAR(255),
            EOAreaName VARCHAR(255),
            EOTerName VARCHAR(255),
            EOParent VARCHAR(255),
            UkrTest VARCHAR(255),
            UkrTestStatus VARCHAR(255),
            UkrBall100 NUMERIC,
            UkrBall12 NUMERIC,
            UkrBall NUMERIC,
            UkrAdaptScale NUMERIC,
            UkrPTName VARCHAR(255),
            UkrPTRegName VARCHAR(255),
            UkrPTAreaName VARCHAR(255),
            UkrPTTerName VARCHAR(255),
            histTest VARCHAR(255),
            HistLang VARCHAR(255),
            histTestStatus VARCHAR(255),
            histBall100 NUMERIC,
            histBall12 NUMERIC,
            histBall NUMERIC,
            histPTName VARCHAR(255),
            histPTRegName VARCHAR(255),
            histPTAreaName VARCHAR(255),
            histPTTerName VARCHAR(255),
            mathTest VARCHAR(255),
            mathLang VARCHAR(255),
            mathTestStatus VARCHAR(255),
            mathBall100 NUMERIC,
            mathBall12 NUMERIC,
            mathBall NUMERIC,
            mathPTName VARCHAR(255),
            mathPTRegName VARCHAR(255),
            mathPTAreaName VARCHAR(255),
            mathPTTerName VARCHAR(255),
            physTest VARCHAR(255),
            physLang VARCHAR(255),
            physTestStatus VARCHAR(255),
            physBall100 NUMERIC,
            physBall12 NUMERIC,
            physBall NUMERIC,
            physPTName VARCHAR(255),
            physPTRegName VARCHAR(255),
            physPTAreaName VARCHAR(255),
            physPTTerName VARCHAR(255),
            chemTest VARCHAR(255),
            chemLang VARCHAR(255),
            chemTestStatus VARCHAR(255),
            chemBall100 NUMERIC,
            chemBall12 NUMERIC,
            chemBall NUMERIC,
            chemPTName VARCHAR(255),
            chemPTRegName VARCHAR(255),
            chemPTAreaName VARCHAR(255),
            chemPTTerName VARCHAR(255),
            bioTest VARCHAR(255),
            bioLang VARCHAR(255),
            bioTestStatus VARCHAR(255),
            bioBall100 NUMERIC,
            bioBall12 NUMERIC,
            bioBall NUMERIC,
            bioPTName VARCHAR(255),
            bioPTRegName VARCHAR(255),
            bioPTAreaName VARCHAR(255),
            bioPTTerName VARCHAR(255),
            geoTest VARCHAR(255),
            geoLang VARCHAR(255),
            geoTestStatus VARCHAR(255),
            geoBall100 NUMERIC,
            geoBall12 NUMERIC,
            geoBall NUMERIC,
            geoPTName VARCHAR(255),
            geoPTRegName VARCHAR(255),
            geoPTAreaName VARCHAR(255),
            geoPTTerName VARCHAR(255),
            engTest VARCHAR(255),
            engTestStatus VARCHAR(255),
            engBall100 NUMERIC,
            engBall12 NUMERIC,
            engDPALevel VARCHAR(255),
            engBall NUMERIC,
            engPTName VARCHAR(255),
            engPTRegName VARCHAR(255),
            engPTAreaName VARCHAR(255),
            engPTTerName VARCHAR(255),
            fraTest VARCHAR(255),
            fraTestStatus VARCHAR(255),
            fraBall100 NUMERIC,
            fraBall12 NUMERIC,
            fraDPALevel VARCHAR(255),
            fraBall NUMERIC,
            fraPTName VARCHAR(255),
            fraPTRegName VARCHAR(255),
            fraPTAreaName VARCHAR(255),
            fraPTTerName VARCHAR(255),
            deuTest VARCHAR(255),
            deuTestStatus VARCHAR(255),
            deuBall100 NUMERIC,
            deuBall12 NUMERIC,
            deuDPALevel VARCHAR(255),
            deuBall NUMERIC,
            deuPTName VARCHAR(255),
            deuPTRegName VARCHAR(255),
            deuPTAreaName VARCHAR(255),
            deuPTTerName VARCHAR(255),
            spaTest VARCHAR(255),
            spaTestStatus VARCHAR(255),
            spaBall100 NUMERIC,
            spaBall12 NUMERIC,
            spaDPALevel VARCHAR(255),
            spaBall NUMERIC,
            spaPTName VARCHAR(255),
            spaPTRegName VARCHAR(255),
            spaPTAreaName VARCHAR(255),
            spaPTTerName VARCHAR(255)
        )
    ''',
    '''
        CREATE TABLE IF NOT EXISTS tbl_zno_temp (
            zno_year INTEGER PRIMARY KEY,
            record_id INTEGER,
            is_done BOOLEAN
        )
    '''
)


def create_table(conn):
    cur = conn.cursor()
    for query in CREATE_QUERY:
        cur.execute(query)
    cur.close()
    conn.commit()
    print('Tables created.')


if __name__ == '__main__':
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        create_table(conn)
    except Exception as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
