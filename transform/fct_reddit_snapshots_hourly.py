import sys
from datetime import datetime

from common.db_operations import execute_sql, create_partition


# Global constants
SOURCE_TABLE_NAME_BASE = 'reddit_data'
TARGET_TABLE_NAME_BASE = 'fct_reddit_snapshots_hourly'
TARGET_PARTITION_PREFIX_BASE ='frsh_'

# For a given snapshotted_on and snapshotted_hour (for now, hardcode it)
# (later: figure out which "snapshotted_on" and "snapshotted_hour" combinations we need)
snapshotted_on = '2022-02-26'
snapshotted_hour = 18


def create_table(table_name):
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            snapshotted_on DATE,
            snapshotted_hour INTEGER,
            post_id VARCHAR,
            snapshotted_at TIME,
            subreddit VARCHAR,
            upvote_count INTEGER,
            downvote_count INTEGER,
            upvote_ratio FLOAT,
            comment_count INTEGER,
            award_count INTEGER,
            post_created_at VARCHAR,
	        PRIMARY KEY (snapshotted_on, snapshotted_hour, post_id))
	    PARTITION BY RANGE (snapshotted_on);
    """
    print(create_sql)
    execute_sql(sql=create_sql)


def delete_existing_target_rows(target_table_name, snapshotted_on, snapshotted_hour):
    delete_sql = f"""
        DELETE FROM {target_table_name}
        WHERE snapshotted_on = '{snapshotted_on}'
        AND snapshotted_hour = {snapshotted_hour}
    """
    execute_sql(sql=delete_sql)


def transform_reddit_data(source_table_name, 
                          target_table_name,
                          snapshotted_on, 
                          snapshotted_hour):
    transform_sql = f"""
        insert into {target_table_name}(snapshotted_on,
                                        snapshotted_hour,
                                        post_id,
                                        subreddit,
                                        upvote_count,
                                        downvote_count,
                                        upvote_ratio,
                                        comment_count,
                                        award_count,
                                        post_created_at,
                                        snapshotted_at)

        with stg_reddit_data as (
            select
                apicall_date as snapshotted_on,
                apicall_time as snapshotted_at,
                subreddit,
                name as post_id,
                ups as upvote_count,
                downs as downvote_count,
                created_utc,
                upvote_ratio,
                num_comments as comment_count,
                total_awards_received as award_count,
                post_rank,
                created_at as post_created_at,
                extract(hour from apicall_time) as snapshotted_hour
            from 
                {source_table_name}
        )

        select distinct on (1,2,3)
            snapshotted_on,
            snapshotted_hour,
            post_id,
            subreddit,
            upvote_count,
            downvote_count,
            upvote_ratio,
            comment_count,
            award_count,
            post_created_at,
            snapshotted_at
        from 
            stg_reddit_data
		where 
			snapshotted_on = '{snapshotted_on}'
		and
			snapshotted_hour = {snapshotted_hour}
        order by 
            1, 2, 3, snapshotted_at desc
    """
    execute_sql(sql=transform_sql)


def pipeline(is_test):
    source_table_name = SOURCE_TABLE_NAME_BASE if not is_test else 'test_' + SOURCE_TABLE_NAME_BASE
    target_table_name = TARGET_TABLE_NAME_BASE if not is_test else 'test_' + TARGET_TABLE_NAME_BASE
    target_partition_prefix = TARGET_PARTITION_PREFIX_BASE if not is_test else 'test_' + TARGET_PARTITION_PREFIX_BASE

    # date_stamp = datetime.today().strftime("%Y-%m-%d")

    create_table(target_table_name)
    create_partition(target_table_name, target_partition_prefix, snapshotted_on)

    delete_existing_target_rows(target_table_name, snapshotted_on, snapshotted_hour)

    transform_reddit_data(source_table_name, 
                          target_table_name,
                          snapshotted_on, 
                          snapshotted_hour)


if __name__ == '__main__':
    is_test = any(test_flag in sys.argv for test_flag in ['test', '--test'])
    pipeline(is_test)