import sys
from datetime import datetime

from common.db_operations import execute_sql, create_partition, get_missing_date_hours


# Global constants
SOURCE_TABLE_NAME_BASE = 'fct_reddit_snapshots_hourly'
TARGET_TABLE_NAME_BASE = 'agg_subreddit_metrics_hourly'

SOURCE_DATE_FIELD = 'snapshotted_on'
SOURCE_HOUR_FIELD = 'snapshotted_hour'
TARGET_DATE_FIELD = 'snapshotted_on'
TARGET_HOUR_FIELD = 'snapshotted_hour'
AGG_BACKFILL_LOOKBACK_DAYS = 28
AGG_SELF_JOIN_LOOKBACK_DAYS = 2


def create_table(table_name):
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            subreddit VARCHAR,
            snapshotted_on DATE,
            snapshotted_hour INTEGER,
            overlapping_posts_between_snapshots INTEGER,
            upvotes INTEGER,
            downvotes INTEGER,
            comments INTEGER,
            awards INTEGER
        );
    """
    execute_sql(sql=create_sql)


def aggregate_reddit_data(source_table_name, 
                          target_table_name,
                          snapshotted_on, 
                          snapshotted_hour):
    transform_sql = f"""
        insert into {target_table_name}(subreddit,
                                        snapshotted_on,
                                        snapshotted_hour,
                                        overlapping_posts_between_snapshots,
                                        upvotes,
                                        downvotes,
                                        comments,
                                        awards)
        with fct_reddit_post_snapshots_hourly_ranked as (
            select
                *,
                dense_rank() over(partition by subreddit
                    order by snapshotted_on, snapshotted_hour)
                    as date_hour_rank
            from 
                {source_table_name}
            where 
                snapshotted_on BETWEEN '{snapshotted_on}'::DATE - {AGG_SELF_JOIN_LOOKBACK_DAYS} AND '{snapshotted_on}'
        ),

        reddit_metric_diffs_from_consecutive_snapshots as (
            select
                current_snapshot.subreddit,
                current_snapshot.snapshotted_on,
                current_snapshot.snapshotted_hour,

                current_snapshot.upvote_count
                    - previous_snapshot.upvote_count
                        as new_upvotes,
                
                current_snapshot.downvote_count
                    - previous_snapshot.downvote_count
                        as new_downvotes,

                current_snapshot.comment_count
                    - previous_snapshot.comment_count
                        as new_comments,

                current_snapshot.award_count
                    - previous_snapshot.award_count
                        as new_awards
            
            from  -- self join
                fct_reddit_post_snapshots_hourly_ranked as previous_snapshot
                inner join fct_reddit_post_snapshots_hourly_ranked as current_snapshot
                on previous_snapshot.post_id=current_snapshot.post_id
                and previous_snapshot.date_hour_rank=current_snapshot.date_hour_rank-1
            where
                current_snapshot.snapshotted_on = '{snapshotted_on}'
            and 
                current_snapshot.snapshotted_hour = {snapshotted_hour}
            )

        select
            subreddit,
            snapshotted_on,
            snapshotted_hour,
            count(1) as overlapping_posts_between_snapshots,
            sum(new_upvotes) as new_upvotes,
            sum(new_downvotes) as new_downvotes,
            sum(new_comments) as new_comments,
            sum(new_awards) as new_awards
        from 
            reddit_metric_diffs_from_consecutive_snapshots
        group by 1, 2, 3
        order by 1, 2, 3
    """
    execute_sql(sql=transform_sql)


def pipeline(is_test):
    source_table_name = SOURCE_TABLE_NAME_BASE if not is_test else 'test_' + SOURCE_TABLE_NAME_BASE
    target_table_name = TARGET_TABLE_NAME_BASE if not is_test else 'test_' + TARGET_TABLE_NAME_BASE

    create_table(target_table_name)

    missing_date_hours = get_missing_date_hours(source_table_name=source_table_name,
                                                source_date_field=SOURCE_DATE_FIELD,
                                                source_hour_field=SOURCE_HOUR_FIELD,
                                                target_table_name=target_table_name, 
                                                target_date_field=TARGET_DATE_FIELD, 
                                                target_hour_field=TARGET_HOUR_FIELD, 
                                                lookback_days=AGG_BACKFILL_LOOKBACK_DAYS)

    for date_hour in missing_date_hours:
        aggregate_reddit_data(source_table_name, 
                              target_table_name,
                              snapshotted_on=date_hour[0], 
                              snapshotted_hour=date_hour[1])


if __name__ == '__main__':
    is_test = any(test_flag in sys.argv for test_flag in ['test', '--test'])
    pipeline(is_test)