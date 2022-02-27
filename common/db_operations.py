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
    date_stamp_next_date = (
        datetime.strptime(date_stamp, "%Y-%m-%d") 
        + timedelta(days=1)).strftime("%Y-%m-%d")
    
    create_partition_sql = f"""
        CREATE TABLE IF NOT EXISTS {partition_name}
        PARTITION OF {table_name}
        FOR VALUES FROM ('{date_stamp}') TO ('{date_stamp_next_date}');
    """
    execute_sql(sql=create_partition_sql)


def get_missing_date_hours(source_table_name,
                           source_date_field,
                           source_hour_field,
                           target_table_name, 
                           target_date_field, 
                           target_hour_field, 
                           lookback_days):
    diff_sql = f"""
        with src as (
            select distinct 
                {source_date_field} as dt, 
                {source_hour_field} as hr
            from {source_table_name} 
            where {source_date_field} > now()::DATE - {lookback_days}
        ),

        tgt as (
            select distinct 
                {target_date_field} as dt, 
                {target_hour_field} as hr
            from {target_table_name} 
            where {target_date_field} > now()::DATE - {lookback_days}
        )

        select 
            src.dt::VARCHAR,
            src.hr::INTEGER
        from src
        left join tgt
        on src.dt=tgt.dt
        and src.hr=tgt.hr
        where tgt.dt is null
        and tgt.hr is null
        order by 1,2;
    """
    missing_date_hours = execute_sql(sql=diff_sql)
    return missing_date_hours