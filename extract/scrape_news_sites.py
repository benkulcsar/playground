import os
import json
from datetime import datetime, timedelta
from title_list_scraper import get_title_list_from_site
from db_operations import execute_sql


# Global vars
table_name = 'scraped_titles'
path = os.path.dirname(os.path.abspath(__file__)) + '/'
site_list_with_parse_logic_file = path + 'site_list.json'


def create_table():
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            scrape_date date,
            scrape_time time,
            site VARCHAR,
            title VARCHAR)
        PARTITION BY RANGE (scrape_date);
    """
    execute_sql(sql=create_sql)


def create_partition(date_stamp, date_stamp_next_date):
    partition_name = "st_" + date_stamp.replace("-", "_")
    create_partition_sql = f"""
        CREATE TABLE IF NOT EXISTS {partition_name}
        PARTITION OF {table_name}
        FOR VALUES FROM ('{date_stamp}') TO ('{date_stamp_next_date}');
    """
    execute_sql(sql=create_partition_sql)


def insert_data(date_stamp, time_stamp, site, title_list):
    rows = []
    for title in title_list:
        rows.append([date_stamp, time_stamp, site, title])
    insert_sql = f"""
        INSERT INTO {table_name}
            (scrape_date,scrape_time,site,title)
            VALUES (%s,%s,%s,%s)
    """
    execute_sql(sql=insert_sql, rows=rows)


def pipeline():
    create_table()

    date_stamp = datetime.today().strftime("%Y-%m-%d")
    date_stamp_next_date = (datetime.today() + timedelta(days=1)) \
        .strftime("%Y-%m-%d")
    create_partition(date_stamp, date_stamp_next_date)

    with open(site_list_with_parse_logic_file, 'r') as file:
        site_list_with_parse_logic = json.load(file)

    for site_with_parse_logic in site_list_with_parse_logic:
        time_stamp = datetime.now().strftime("%H:%M:%S")
        title_list = get_title_list_from_site(site_with_parse_logic)
        insert_data(date_stamp,
                    time_stamp,
                    site_with_parse_logic['name'],
                    title_list)


if __name__ == '__main__':
    pipeline()
