import sys
from datetime import datetime

from common.db_operations import execute_sql, create_partition


# Global constants
SOURCE_TABLE_NAME_BASE = 'fct_reddit_snapshots_hourly'
TARGET_TABLE_NAME_BASE = 'agg_subreddit_metrics_hourly'
AGG_BACKFILL_LOOKBACK_DAYS = 3


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
    print(create_sql)
    execute_sql(sql=create_sql)


def get_missing_date_hours(source_table_name, target_table_name):
    diff_sql = f"""
        with date_hours_in_fact_table as (
            select distinct 
                snapshotted_on, 
                snapshotted_hour
            from {source_table_name} 
            where snapshotted_on > now()::DATE - {AGG_BACKFILL_LOOKBACK_DAYS}
        ),

        date_hours_in_agg_table as (
            select distinct 
                snapshotted_on, 
                snapshotted_hour
            from {target_table_name} 
            where snapshotted_on > now()::DATE - {AGG_BACKFILL_LOOKBACK_DAYS}
        )

        select 
            f.snapshotted_on::VARCHAR,
            f.snapshotted_hour::INTEGER
        from date_hours_in_fact_table f
        left join date_hours_in_agg_table a
        on f.snapshotted_on=a.snapshotted_on
        and f.snapshotted_hour=a.snapshotted_hour
        where a.snapshotted_on is null
        and a.snapshotted_hour is null
        order by 1,2;
    """
    missing_date_hours = execute_sql(sql=diff_sql)
    return missing_date_hours


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
                    order by snapshotted_hour)
                    as date_hour_rank
            from 
                {source_table_name}
            where 
                snapshotted_on='{snapshotted_on}'
            and 
                snapshotted_hour IN ({snapshotted_hour-1},{snapshotted_hour})
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

    missing_date_hours = get_missing_date_hours(source_table_name, target_table_name)

    for date_hour in missing_date_hours:
        aggregate_reddit_data(source_table_name, 
                              target_table_name,
                              snapshotted_on=date_hour[0], 
                              snapshotted_hour=date_hour[1])


if __name__ == '__main__':
    is_test = any(test_flag in sys.argv for test_flag in ['test', '--test'])
    pipeline(is_test)