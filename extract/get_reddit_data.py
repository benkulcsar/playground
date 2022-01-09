from datetime import datetime, timedelta
from db_operations import execute_sql
from reddit_api_interface import api_auth, fetch_posts

# Global vars
posts_to_fetch = 100
table_name = 'reddit_data'
subreddits = ('australia,unitedkingdom,russia,poland,india,canada,germany,'
              'france,dataisbeautiful,funny,gaming,aww,Music,pics,'
              'worldnews,science,todayilearned,movies,videos,news,'
              'Showerthoughts,EarthPorn,hungary,datascience,romania,'
              'czech,Austria,de,europe,Polska,wallstreetbets,memes,'
              'nosleep,personalfinance,politics')


def create_table():
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


def create_partition(date_stamp, date_stamp_next_date):
    partition_name = "rd_" + date_stamp.replace("-", "_")
    create_partition_sql = f"""
        CREATE TABLE IF NOT EXISTS {partition_name}
        PARTITION OF {table_name}
        FOR VALUES FROM ('{date_stamp}') TO ('{date_stamp_next_date}');
    """
    execute_sql(sql=create_partition_sql)


def insert_data(filtered_enriched_post_list):
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

        d = post['data']
        downs_est = int(
            (d['ups'] - d['ups']*d['upvote_ratio']) / d['upvote_ratio'])

        filtered_enriched_post.append(downs_est)

        filtered_enriched_post_list.append(filtered_enriched_post)
        post_rank += 1

    return filtered_enriched_post_list


def pipeline():
    create_table()

    date_stamp = datetime.today().strftime("%Y-%m-%d")
    date_stamp_next_date = (datetime.today() + timedelta(days=1)) \
        .strftime("%Y-%m-%d")
    create_partition(date_stamp, date_stamp_next_date)

    subreddit_list = sorted(list(set(subreddits.split(','))))

    TOKEN = api_auth()

    for subreddit in subreddit_list:
        post_list = fetch_posts(subreddit=subreddit,
                                post_count=posts_to_fetch,
                                TOKEN=TOKEN)

        filtered_enriched_post_list = filter_enrich_post_list(post_list)

        insert_data(filtered_enriched_post_list)


if __name__ == '__main__':
    pipeline()
