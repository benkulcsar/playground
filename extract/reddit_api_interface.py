import requests
import os


def api_auth():

    data = {'grant_type': os.environ['PG_REDDIT_GRANT_TYPE'],
            'username': os.environ['PG_REDDIT_USERNAME'],
            'password': os.environ['PG_REDDIT_PASSWORD']}
    auth = requests.auth.HTTPBasicAuth(
        os.environ['PG_REDDIT_CLIENT_ID'],
        os.environ['PG_REDDIT_SECRET_TOKEN'])
    headers = {'User-Agent': os.environ['PG_REDDIT_USER_AGENT']}

    res = requests.post('https://www.reddit.com/api/v1/access_token',
                        auth=auth, data=data, headers=headers)
    TOKEN = res.json()['access_token']
    return TOKEN


def fetch_posts(subreddit, post_count, TOKEN):

    post_count = min(100, post_count)
    headers = {'User-Agent': os.environ['PG_REDDIT_USER_AGENT'],
               **{'Authorization': f'bearer {TOKEN}'}}
    params = {'limit': post_count}
    url = 'https://oauth.reddit.com/r/' + subreddit + '/new'

    res = requests.get(url,
                       headers=headers,
                       params=params)
    return res.json()['data']['children']
