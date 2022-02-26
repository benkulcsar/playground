import os
import sys
import json
from datetime import datetime

from news_sites_extract_modules.title_list_scraper import get_title_list_from_site
from common.db_operations import execute_sql, create_partition

# Global constants
TABLE_NAME_BASE = 'scraped_titles'
PARTITION_PREFIX_BASE ='st_'
SITE_LIST_PATH = os.path.dirname(os.path.abspath(__file__)) + '/news_sites_extract_modules/'
SITE_LIST_PARSE_LOGIC_FILE = SITE_LIST_PATH + 'site_list.json'


def create_table(table_name):
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            scrape_date date,
            scrape_time time,
            site VARCHAR,
            title VARCHAR)
        PARTITION BY RANGE (scrape_date);
    """
    execute_sql(sql=create_sql)


def insert_data(table_name, date_stamp, time_stamp, site, title_list):
    rows = []
    for title in title_list:
        rows.append([date_stamp, time_stamp, site, title])
    insert_sql = f"""
        INSERT INTO {table_name}
            (scrape_date,scrape_time,site,title)
            VALUES (%s,%s,%s,%s)
    """
    execute_sql(sql=insert_sql, rows=rows)


def pipeline(is_test):
    table_name = TABLE_NAME_BASE if not is_test else 'test_' + TABLE_NAME_BASE
    partition_prefix = PARTITION_PREFIX_BASE if not is_test else 'test_' + PARTITION_PREFIX_BASE
    date_stamp = datetime.today().strftime("%Y-%m-%d")

    create_table(table_name)
    create_partition(table_name, partition_prefix, date_stamp)

    with open(SITE_LIST_PARSE_LOGIC_FILE, 'r') as file:
        site_list_with_parse_logic = json.load(file)

    for site_with_parse_logic in site_list_with_parse_logic:
        time_stamp = datetime.now().strftime("%H:%M:%S")
        title_list = get_title_list_from_site(site_with_parse_logic)
        insert_data(table_name,
                    date_stamp,
                    time_stamp,
                    site_with_parse_logic['name'],
                    title_list)
        
        if is_test: break


if __name__ == '__main__':
    is_test = any(test_flag in sys.argv for test_flag in ['test', '--test'])
    pipeline(is_test)
