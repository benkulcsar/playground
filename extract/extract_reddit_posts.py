import sys
from datetime import datetime

from common.db_operations import execute_sql, create_partition
from reddit_posts_extract_modules.reddit_api_interface import api_auth, fetch_posts

# Global constants
POSTS_TO_FETCH = 100
TABLE_NAME_BASE = 'reddit_data'
PARTITION_PREFIX_BASE = "rd_"
SUBREDDITS = ('australia,unitedkingdom,russia,poland,india,canada,germany,'
              'france,dataisbeautiful,funny,gaming,aww,Music,pics,'
              'worldnews,science,todayilearned,movies,videos,news,'
              'Showerthoughts,EarthPorn,hungary,datascience,romania,'
              'czech,Austria,de,europe,Polska,wallstreetbets,memes,'
              'nosleep,personalfinance,politics')


def create_table(table_name):
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            apicall_date date,
            apicall_time time,
            subreddit VARCHAR,
            name VARCHAR,
            ups INTEGER,
            downs INTEGER,
            created_utc FLOAT,
            upvote_ratio FLOAT,
            num_comments INTEGER,
            total_awards_received INTEGER,
            post_rank SMALLINT,
            created_at VARCHAR)
        PARTITION BY RANGE (apicall_date);
    """
    execute_sql(sql=create_sql)


def insert_data(table_name, filtered_enriched_post_list):
    insert_sql = f"""
        INSERT INTO {table_name}
            (subreddit,name,ups,created_utc,upvote_ratio,num_comments,
             total_awards_received,post_rank,apicall_date,apicall_time,
             created_at,downs)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    execute_sql(sql=insert_sql, rows=filtered_enriched_post_list)


def filter_enrich_post_list(post_list):
    post_fields_to_keep = ['subreddit', 'name', 'ups', 'created_utc',
                           'upvote_ratio', 'num_comments',
                           'total_awards_received']
    filtered_enriched_post_list = []
    post_rank = 1

    for post in post_list:
        filtered_enriched_post = []
        for field in post_fields_to_keep:
            if field in post['data'].keys():
                filtered_enriched_post.append(post['data'][field])
            else:
                filtered_enriched_post.append(None)

        filtered_enriched_post.append(post_rank)
        filtered_enriched_post.append(datetime.now().strftime('%Y-%m-%d'))
        filtered_enriched_post.append(datetime.now().strftime('%H:%M:%S'))
        filtered_enriched_post.append(
            datetime.fromtimestamp(post['data']['created_utc'])
            .strftime('%Y-%m-%d %H:%M:%S'))

        d = post['data']  # Single letter variable for readability
        downs_est = int(
            (d['ups'] - d['ups']*d['upvote_ratio']) / d['upvote_ratio'])

        filtered_enriched_post.append(downs_est)

        filtered_enriched_post_list.append(filtered_enriched_post)
        post_rank += 1

    return filtered_enriched_post_list


def pipeline(is_test):
    table_name = TABLE_NAME_BASE if not is_test else 'test_' + TABLE_NAME_BASE
    partition_prefix = PARTITION_PREFIX_BASE if not is_test else 'test_' + PARTITION_PREFIX_BASE
    date_stamp = datetime.today().strftime("%Y-%m-%d")
    
    create_table(table_name)
    create_partition(table_name, partition_prefix, date_stamp)

    subreddit_list = sorted(list(set(SUBREDDITS.split(','))))

    TOKEN = api_auth()

    for subreddit in subreddit_list:
        post_list = fetch_posts(subreddit=subreddit,
                                post_count=POSTS_TO_FETCH,
                                TOKEN=TOKEN)

        filtered_enriched_post_list = filter_enrich_post_list(post_list)

        insert_data(table_name, filtered_enriched_post_list)

        if is_test: break


if __name__ == '__main__':
    is_test = any(test_flag in sys.argv for test_flag in ['test', '--test'])
    pipeline(is_test)
