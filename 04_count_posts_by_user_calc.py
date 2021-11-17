#! /usr/bin/python3

########################################
#  Count posts by author_fullname and subreddit and do the user classification
########################################

import pandas as pd
import sqlite3

import config

con_merged = sqlite3.connect(config.db_path)

db_count_path = config.db_dir + 'post_counts_by_user.db'
con_count = sqlite3.connect(db_count_path)

q_c_post_counts = """
select author_fullname, subreddit, count(*) count 
from comment 
where author_fullname is not null group by author_fullname, subreddit 
"""
q_s_post_counts = """
select author_fullname, subreddit, count(*) count 
from submission 
where author_fullname is not null group by author_fullname, subreddit 
"""

queries = {'comment': q_c_post_counts, 'submission': q_s_post_counts}

for k, v in queries.items():
    df = pd.read_sql(v, con=con_merged,)
    df = df.pivot(columns='subreddit', index='author_fullname', values='count')
    
    # Save to db
    df.to_sql(con=con_count, name=k, if_exists='replace', index=True)

df_subs = pd.read_csv('./data/subreddits.csv').dropna()
df = pd.read_sql("""select * from comment""", con=con_count, index_col='author_fullname')

dict_= {}
for _cat in df_subs['political_ideology'].unique():
    _cols = df_subs['name'][df_subs['political_ideology']==_cat].values
    _cols = [c for c in _cols if c in df.columns]
    dict_[_cat] = df[_cols].sum(axis=1)

df = pd.concat(dict_,axis=1)

df['dem-rep-classification'] = (df['republican'] - df['democrat']) / (df['republican'] + df['democrat'])

# Save data
df.to_csv(config.db_dir + 'user_classification.csv', index=True,)
df.to_sql(con=con_count, name='user_classification', if_exists='replace', index=True)

# Close connection to database
con_count.close()
