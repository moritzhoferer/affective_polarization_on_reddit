#! /usr/bin/python3

# Short tutorial: https://medium.com/analytics-vidhya/simplifying-social-media-sentiment-analysis-using-vader-in-python-f9e6ec6fc52f

import re

from nltk.sentiment.vader import SentimentIntensityAnalyzer as nltkImplementation
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer as vaderImplementation

import config


def remove_urls(text: str) -> str:
    # Initial source of inspiration: https://mathiasbynens.be/demo/url-regex
    # Remove all URLs and reamaining markdown brackets
    text = re.sub(r"https?:[a-zA-Z0-9_.+-/#~]+", '', text, flags=re.MULTILINE)
    text = re.sub(r"\[|\]\(\)", "", text, flags=re.MULTILINE)
    # # Remove URLs that are in brackets
    # text = re.sub(r"\]\(https?:\/\/\S*\)", "", text, flags=re.MULTILINE)
    # # Remove left square brackets
    # text = re.sub(r"\[", "", text, flags=re.MULTILINE)
    return text


def remove_html_char_entities(text: str) -> str:
    # Remove characters like: https://dev.w3.org/html5/html-author/charref
    text = re.sub(r"\&[a-zA-Z]+;", "", text, flags=re.MULTILINE)
    return text


def preprocess_post(text: str) -> str:
    # All preprocess functions together
    text = remove_urls(text)
    text = remove_html_char_entities(text)
    return text


vaderAnalyser = vaderImplementation()
nltkAnalyser = nltkImplementation()


def get_sentiment_scores(sentence, source='vader'):
    sentence = preprocess_post(sentence)
    if source == 'vader':
        score = vaderAnalyser.polarity_scores(sentence)
    elif source == 'nltk':
        score = nltkAnalyser.polarity_scores(sentence)
    else:
        print('Reset to default: vader')
        source = vaderAnalyser.polarity_scores(sentence)
    return score


if __name__ == '__main__':
    import pandas as pd
    import sqlite3

    import multiprocessing
    from joblib import Parallel, delayed
    from tqdm import tqdm

    import gc

    num_cores = multiprocessing.cpu_count()
    num_cores = num_cores//2 if num_cores > 4 else num_cores
    print(f"Using {num_cores} cores to calculate the sentiment scores.")


    def foo(entry: tuple) -> dict:
        dict_ = {}
        dict_['id'] = entry[0]
        dict_['created_utc'] = entry[1]
        dict_['link_id'] = entry[2]
        dict_['subreddit'] = entry[3]
        dict_['len'] = len(entry[4])
        dict_['word_count'] = len(entry[4].split(" "))
        dict_.update(get_sentiment_scores(entry[4], source='nltk'))
        return dict_

    
    con = sqlite3.connect(config.db_path)
    cur = con.cursor()

    cur.execute("""
    SELECT count(name) 
    FROM sqlite_master 
    WHERE type='table' AND name='comment_sentiment'
    """)
    #if the count is 1, then table exists
    if cur.fetchone()[0]==1:
        print("Delete old table of sentiment scores.") 
        cur.execute("""DROP TABLE comment_sentiment""")

    # Filter chunck to remove unnecessary comments (deleted/removed).
    query = """
    select id, created_utc, link_id, subreddit, body 
    from comment 
    where body is not '[deleted]' and body is not '[removed]'
    """
    cur.execute(query)
    out = []
    
    chunk_size = int(5e6)
    while (chunk := cur.fetchmany(size=chunk_size)):
        out = Parallel(n_jobs=num_cores)(delayed(foo)(i) for i in tqdm(chunk))
        df = pd.DataFrame.from_records(out, index='id')
        del out
        gc.collect()
        
        df.to_sql(name='comment_sentiment', con=con, if_exists='append', index=True)
        del df
        gc.collect()
