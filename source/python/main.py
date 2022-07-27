import stream_tweets
import tweepy
import yaml
from globals import *

if __name__ == '__main__':
    with open(BEARER_TOKEN_YAML_PATH, 'r') as stream:
        data_loaded = yaml.safe_load(stream)
    token = data_loaded['search_tweets_v2']['bearer_token']
    client = tweepy.Client(token)
    with open(QUERY_FILE,'r') as file:
        query = file.read().rstrip()
    print (query)
    db_name = DB_NAME 
    user_name = DB_USER_NAME
    table_name= TWEETS_TABLE_NAME
    stream_tweets.get_days_tweets(client, query, START_DATE, END_DATE, MAX_RESULTS, None,db_name,user_name,table_name)
