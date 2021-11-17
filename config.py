#!/user/bin/python3

import os
import pandas as pd
import numpy as np
from datetime import datetime

# DB settings
db_dir = './data/'
db_name = 'sample_data_MERGED.db'
db_path = db_dir + db_name

SUBREDDITS_DF_PATH = './data/subreddits.csv'

survey_years = [2011, 2014, 2015, 2017]

POST_TYPES = ['submission', 'comment']

TIMESTAMPS = np.loadtxt('./data/timestamps.txt')
ELECTION_TIMESTAMPS = np.loadtxt('./data/election_timestamps.txt', dtype=np.int32)

# Plot settings
color_dict = {
    'all': '#212121', 'moderate': '#828282', 'democrat': '#3333FF', 'republican': '#E81B23',
    'homogeneous': '#6800a8', 'heterogeneous': '#828282',
}
line_dict = {'comment': 'dotted', 'submission': 'dashed',}
maker_dict = {'all': '-v', 'moderate': '-s', 'democrat': '-^', 'republican': '-o',}

directories = {
    'graphics': './graphics/',
    'data': './data/'
}

for dir_ in directories.values():
    if not os.path.isdir(dir_):
        os.mkdir(dir_)


def get_subreddits_df() -> pd.DataFrame:
    """
    Returns:
        Collection of subreddits stored in `./data/subreddits.csv`
    """
    _output = pd.read_csv(SUBREDDITS_DF_PATH,)
    return _output

def timestamps2dates(data):
    return [datetime.fromtimestamp(_) for _ in data]
