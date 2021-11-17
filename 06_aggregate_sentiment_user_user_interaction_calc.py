#! /usr/bin/python3

import numpy as np
import pandas as pd
import sqlite3
from tqdm import tqdm

import config

timestamps = np.loadtxt('./data/timestamps.txt', dtype=np.int32)
subreddits_df = pd.read_csv('./data/subreddits.csv',).dropna().set_index('name')
moderate_subs = subreddits_df[subreddits_df['political_ideology']=='moderate'].index

con = sqlite3.connect(config.db_path)
cur = con.cursor()

df_seq = pd.read_csv(config.db_dir + "submission_classification_by_history.csv", index_col="link_id")

submission_df = pd.read_sql("""select id, created_utc from submission""", con=con)
submission_df['created_utc'] = submission_df['created_utc'].astype(int)
submission_df['link_id'] = 't3_' + submission_df['id']
submission_df = submission_df.set_index('link_id')

helper_df = pd.read_sql(
    """select link_id, min(created_utc) created_utc from comment group by link_id""",
    con=con,
)
helper_df = helper_df.set_index('link_id')
helper_df = helper_df.astype(int)

def get_submission_timestamp(idx: str):
    if idx in submission_df.index:
        return submission_df.loc[idx, 'created_utc']
    else:
        return helper_df.loc[idx, 'created_utc']
    
def is_moderate(x):
    return x in moderate_subs

created_utc = [get_submission_timestamp(i) for i in tqdm(df_seq.index)]
df_seq['created_utc'] = created_utc

_min_count = 10
_min_ratio_classified = .5
idxs = [is_moderate(i['subreddit']) for _, i in tqdm(df_seq.iterrows())]
df_seq = df_seq[
    (idxs) & 
    (df_seq['count']>=_min_count) &
    (df_seq['ratio_classified']>_min_ratio_classified)
]

base_query = """select * from comment_sentiment where link_id in ({seq})"""
out = []

# _s = 6
for _s in range(2, 7):
    print('\nThreshold parameter: ', _s)
    # Both parties
    _df_heterogeneous = df_seq[np.abs(df_seq['dem-rep-classification']) <= 1./_s]
    # Just one of the parties is predominantly present
    _df_homogeneous = df_seq[np.abs(df_seq['dem-rep-classification']) >= 1 - (1./_s)]
    # Just Democrats
    _df_democrat = df_seq[df_seq['dem-rep-classification'] <= -1 + (1./_s)]
    # Just Republicans
    _df_republican = df_seq[df_seq['dem-rep-classification'] >= 1 - (1./_s)]

    _df_collection = {
            'heterogeneous': _df_heterogeneous,
            'homogeneous': _df_homogeneous,
            'democrat': _df_democrat,
            'republican': _df_republican,
    }
    for k, v in _df_collection.items():
        print(f"{k} submissions:  ", len(v))

    for _idx in np.arange(len(timestamps)-1):
        for k, v in _df_collection.items():
            args = v[v['created_utc'].between(timestamps[_idx], timestamps[_idx+1])].index
            _query = base_query.format(seq=','.join(['?']*len(args)))
            _df_sentiment_scores = pd.read_sql(_query, con=con, params=args)
            
            _tmp = _df_sentiment_scores.describe().T
            _tmp.index = _tmp.index.rename('measurement')
            _tmp['From'] = timestamps[_idx]
            _tmp['To'] = timestamps[_idx+1]
            _tmp['Type'] = k
            _tmp['Threshold'] = _s
            _tmp = _tmp.reset_index()
            out.append(_tmp)


column_order = ['From', 'To','Type', 'Threshold', 'measurement', 'count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max']
out_df = pd.concat(out).dropna(thresh=7).reset_index()
out_df = out_df[column_order]
out_df.to_csv(config.db_dir + 'aggregated_sentiment_scores_user_user.csv', index=False, header=True)
