#! /usr/bin/python3

###################
# Download comments and submissions from a list of subreddits
###################


# Initial start from
# source 1: https://medium.com/@pasdan/how-to-scrap-reddit-using-pushshift-io-via-python-a3ebcc9b83f4
# and source 2: https://medium.com/@RareLoot/using-pushshifts-api-to-extract-reddit-submissions-fb517b286563

import os
import pandas as pd
import requests
import json
import time
import datetime
import sqlite3

# Select columns for the submissions and comments to download
default_columns = ['id', 'author', 'author_created_utc', 'author_fullname',
                   'created_utc',  'score', 'stickied', 'subreddit',
                   'subreddit_id', 'send_replies', 'permalink', 'retrieved_on',
                   'locked']
submission_columns = default_columns +\
                     ['title', 'url', 'domain', 'is_reddit_media_domain', 'full_link', 
                     'num_comments', 'num_crossposts', 'over_18', 'selftext', 'spoiler', 
                     'pinned', 'parent_whitelist_status', 'whitelist_status',
                     'is_crosspostable', 'is_self', 'is_video', 'thumbnail',
                     'subreddit_type',]
comment_columns = default_columns +\
                  ['body', 'controversiality', 'distinguished', 'edited',
                  'link_id', 'nest_level', 'parent_id','reply_delay', 
                  'user_removed', 'is_submitter',]


def get_url_request(url):
    """
    Get the text element of a URL request. 
    If ANY error occurs it is retried 10 seconds later.
    """
    while True:
        try:
            _r = requests.get(url)
            _data = json.loads(_r.text)
        except:
            print('Some error appeared. Wait for 10 sec., then retry.')
            time.sleep(10)
            continue
        break
    return _data


# Get the server rate limit per minute for pushshift API
def get_rate_limit() -> int:
    _data = get_url_request('https://api.pushshift.io/meta')
    return int(_data['server_ratelimit_per_minute'])


SERVER_RATELIMIT = get_rate_limit()


def get_entries(query: str = None, sub: str = None, after: int = 0, before: int = None,
                post_type: str = 'submission', size: int = 100, verbose: bool = False):
    _url = 'https://api.pushshift.io/reddit/search/{type:s}/?size={size:d}'.format(type=post_type, size=size)
    if query:
        _url += 'title=' + str(query) + '&'
    _url += '&after=' + str(after)
    if before is not None:
        _url += '&before=' + str(before)
    if sub:
        _url += '&subreddit=' + str(sub)
    if verbose:
        print(_url)
    _data = get_url_request(_url)
    return _data['data']


def download_data(con: sqlite3.Connection, query: str = None, sub: str = None, after: int = 0, before: int = None,
                  post_type: str = 'submission', verbose: bool = False) -> None:
    _request_times = []

    if post_type == 'submission':
        _columns = submission_columns
    elif post_type == 'comment':
        _columns = comment_columns
    else:
        raise IOError

    while True:
        _request_times.append(time.time())
        _data = get_entries(query=query, sub=sub, after=after, before=before, post_type=post_type, verbose=verbose)
        _df = pd.DataFrame(_data)

        if verbose:
            print('Number of entries: ', _df.shape[0], '\n')

        if _df.empty:
            break
        else:
            # Dirty trick to avoid key errors
            for _c in _columns:
                if _c not in _df.columns:
                    _df[_c] = None
            
            after = _data[-1]['created_utc']
            if before:
                if after > before:
                    _df = _df[_df['created_utc'] < before]
            _df[_columns].to_sql(name=post_type, con=con, if_exists='append', index=False)
            
        # Last time stamp
        if verbose:
            print(str(datetime.datetime.fromtimestamp(_data[-1]['created_utc'])))
        if before:
            if after > before:
                break

        # Ensures that the rate limit per minute is not hit
        if len(_request_times) >= SERVER_RATELIMIT:
            _time_last_downloads = _request_times[-1] - _request_times[-SERVER_RATELIMIT]
            if _time_last_downloads < 58:
                _time_sleep = 62 - _time_last_downloads
                print('Sleep for ', int(_time_sleep), ' seconds')
                time.sleep(_time_sleep)


if __name__ == '__main__':

    import config

    # Nov 6, 2020, 12:00:00 AM GMT
    stop_timestamp = 1604620800

    # Subreddit to query
    # subreddit_list = list(config.get_subreddits_df().dropna()['name'])
    subreddit_list = list(pd.read_csv('./data/subreddits_extended_marchal.csv',).dropna()['name'])

    # Check that database directory exists
    os.makedirs(config.db_dir, exist_ok=True)

    # Create database and connect
    conn = sqlite3.connect(config.db_path.format(version=4))
    c = conn.cursor()
    
    q_table_exists = "select count(name) from sqlite_master where type='table' and name=?"

    # Will run until all posts have been gathered from the 'after' date up until before date
    for subreddit in subreddit_list:
        print(subreddit)
        for kind in config.POST_TYPES:
            # Check first if table exists
            table_exists = c.execute(
                "select count(name) from sqlite_master where type='table' and name=?", (kind,)
                ).fetchone()[0]
            if table_exists:
                q_utc_of_last_post = f"select max(`created_utc`) from `{kind}` where `subreddit` is ?"
                c.execute(q_utc_of_last_post, (subreddit,))
                # if kind == 'submission':
                # elif kind == 'comment':
                #     c.execute(q_utc_of_last_post, (subreddit,))
                # else:
                #     continue
                start_at = c.fetchone()[0]
                start_at = start_at if start_at else 0
            else:
                start_at = 0
            print('\t', kind, '\n\tLast timestamp found: ', start_at,)
            download_data(
                con=conn,
                sub=subreddit,
                after=start_at,
                before=stop_timestamp,
                post_type=kind,
                verbose=True
            )

    c.close()
    conn.close()
