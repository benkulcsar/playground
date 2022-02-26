import os
from datetime import datetime, timedelta
import psycopg2


def db_connect():
    conn = psycopg2.connect(host=os.environ['PG_DB_HOST'],
                            port=os.environ['PG_DB_PORT'],
                            database=os.environ['PG_DB_NAME'],
                            user=os.environ['PG_DB_USER'],
                            password=os.environ['PG_DB_PASSWORD'])
    return conn


def db_close(conn):
    conn.close()


def execute_sql(sql, rows=None):
    conn = db_connect()
    cur = conn.cursor()

    if rows is None:
        cur.execute(sql)
    else:
        cur.executemany(sql, rows)

    conn.commit()
    try:
        result = cur.fetchall()
    except Exception:
        result = None
    db_close(conn)
    return result


def create_partition(table_name, partition_prefix, date_stamp):

    partition_name = partition_prefix + date_stamp.replace("-", "_")

    date_stamp_next_date = (datetime.today() + timedelta(days=1)) \
        .strftime("%Y-%m-%d")
    
    create_partition_sql = f"""
        CREATE TABLE IF NOT EXISTS {partition_name}
        PARTITION OF {table_name}
        FOR VALUES FROM ('{date_stamp}') TO ('{date_stamp_next_date}');
    """
    execute_sql(sql=create_partition_sql)