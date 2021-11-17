#! /usr/bin/python3

import os
import gc
import datetime
import numpy as np
import pandas as pd
import sqlite3

import config

# Index [9] for Wednesday, November 3, 2010
timestamps = np.loadtxt('./data/timestamps.txt', dtype=np.int32)
# Smaller intervals because of RAM issues
timestamps = np.linspace(timestamps[2], timestamps[-1], 300, dtype=int)
# np.arange(timestamps[2], timestamps[-1]+1, 3600*24*16)   
db_name = 'reddit'

# db_path_list = [config.db_dir+i for i in os.listdir(config.db_dir) if db_name in i and 'MERGED' not in i]
db_path_list = [config.db_dir + db_name + '_v{}.db'.format(i) for i in range(10)]
db_path_list = [i for i in db_path_list if os.path.exists(i)]
print(db_path_list)
con_list = [sqlite3.connect(i) for i in db_path_list]
cur_list = [c.cursor() for c in con_list]

q_s_duplicated = """SELECT id, COUNT(*) c FROM submission GROUP BY id having c>1"""
q_c_duplicated = """SELECT id, COUNT(*) c FROM comment GROUP BY id having c>1"""
queries = {'comment': q_c_duplicated, 'submission': q_s_duplicated}
tester = [len(cur.fetchall())>0 for cur in cur_list for q in queries.values()]
if any(tester):
    print('!!! WARNING !!!')
else:
    print('all tables are good (no duplicates)')

q_s = """select * from submission where created_utc >= ? and created_utc < ?"""
q_c = """select * from comment where created_utc >= ? and created_utc < ?"""
queries = {'comment': q_c, 'submission': q_s}

merged_db_path = config.db_dir + db_name + '_MERGED_v2.db'
if os.path.exists(merged_db_path):
    os.remove(merged_db_path)
con_merged = sqlite3.connect(merged_db_path)

for k, v in queries.items():
    print(
        k,
        # '\n',
        # datetime.datetime.fromtimestamp(timestamps[idx]).strftime("%m.%Y")
    )
    for idx in range(len(timestamps)-1):
        # parallelize this line
        dfs = [pd.read_sql_query(v, con=i,params=(int(timestamps[idx]), int(timestamps[idx+1])-1)) for i in con_list]
        input_lens = [len(_) for _ in dfs]
        df_merge = pd.concat(dfs, sort=False, ignore_index=True)
        del dfs
        gc.collect()
        df_merge = df_merge.drop_duplicates(subset=['id'], keep='last')
        print(
            '\t',  input_lens, '\t', len(df_merge), '\n',
            datetime.datetime.fromtimestamp(timestamps[idx+1]).strftime("%d.%m.%Y")
        )
        df_merge.to_sql(con=con_merged, name=k, index=False, if_exists='append')
        del df_merge
        gc.collect()

for c in con_list:
    c.close()

# Check if the new database has duplicates
cur = con_merged.cursor()
queries = {'comment': q_c_duplicated, 'submission': q_s_duplicated}
tester = [len(cur.fetchall())>0 for q in queries.values()]
if any(tester):
    print('!!! WARNING !!!')
else:
    print('all tables are good (no duplicates)')

con_merged.close()
