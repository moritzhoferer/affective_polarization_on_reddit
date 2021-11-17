#! /usr/bin/python

import os
import pandas as pd
import sqlite3

import multiprocessing
# from joblib import Parallel, delayed
from tqdm import tqdm
num_cores = multiprocessing.cpu_count()

import config

con = sqlite3.connect(config.db_path)
cur = con.cursor()

user_classification = pd.read_csv(config.db_dir + 'user_classification.csv', index_col='author_fullname')
classifier_df = user_classification[user_classification['dem-rep-classification'].notna()]

classification_threshold = 1/3

def classify_function(r):
    try:
        if (f := classifier_df.loc[r[0], 'dem-rep-classification']) <= -classification_threshold:
            v = 'democrat'
        elif f < classification_threshold:
            v = 'neutral'
        else:
            v = 'republican'
    except KeyError:
        v = None
    return (r[1], v)

sub = pd.read_sql(
    """select link_id, subreddit, count(*) count from comment group by link_id""",
    con=con,
)
if sum(sub['link_id'].duplicated()) == 0:
    sub = sub.set_index('link_id')
    print('No duplicates :)')

labels = ['democrat', 'neutral', 'republican']
for l in labels:
    sub[l] = 0

cur.execute("""select author_fullname, link_id from comment where author_fullname is not Null""")

# PARALLEL
chunk_size = int(1e7)
pool = multiprocessing.Pool(num_cores)
while (chunck := cur.fetchmany(size=chunk_size)):
    print(len(chunck))
    out = pool.map(classify_function, chunck)
    df = pd.DataFrame(out, columns=['link_id', 'classification'])
    counts = df.dropna().value_counts()
    for idx, val in counts.iteritems():
        sub.loc[idx[0], idx[1]] += val
pool.close()
pool.join()

# Aggregate variables
sub['ratio_classified'] = sub[['democrat', 'republican']].sum(axis=1)/sub['count']
sub['dem-rep-classification'] = (sub['republican'] - sub['democrat'])/sub[['democrat', 'republican']].sum(axis=1)

# Initial creation of the submission
submission_df = pd.read_sql("""select id, created_utc from submission""", con=con)
submission_df['created_utc'] = submission_df['created_utc'].astype(int)
submission_df['link_id'] = 't3_' + submission_df['id']
submission_df = submission_df.set_index('link_id')

# Helper dataframe that gives the timestamp of the first comment
helper_df = pd.read_sql(
    """select link_id, min(created_utc) created_utc from comment group by link_id""",
    con=con,
)
helper_df = helper_df.set_index('link_id')
helper_df = helper_df.astype(int)

# Check if the timestamp of the submission is availalbe and return it. Otherwise, return the timestamp of the first comment
def get_submission_timestamp(idx: str):
    if idx in submission_df.index:
        return submission_df.loc[idx, 'created_utc']
    else:
        return helper_df.loc[idx, 'created_utc']

created_utc = [get_submission_timestamp(i) for i in tqdm(sub.index)]
sub['created_utc'] = created_utc

# Average sentiment per submission (link_id)
_query = """SELECT link_id, count(*) count_analyzed, avg(neg) neg, avg(neu) neu, avg(pos) pos, avg(compound) compound from comment_sentiment group by link_id"""
_df_avg_scores = pd.read_sql(_query, con=con).set_index('link_id')
sub = sub.join(_df_avg_scores, how='outer')

output_path = config.db_dir + 'submission_classification_by_history.csv'
sub.to_csv(output_path)
