#!/usr/bin/env python
# coding: utf-8



import yaml
import tweepy
import time
import psycopg2
import os
import json
import pandas as pd
from globals import *



# This is the code for a small search project that runs on the local computer.

#deprecated     
def convert_reponse_to_json(tweet):
     a = {'id':tweet.id, 'text':tweet.text, \
       'author_id':tweet.author_id, 'created_at':str(tweet.created_at)}
     return json.dumps(a)

#this function stores the returned tweets to a postgreSQL database table
def handle_results(response, dbname,username,tablename):
    db_conn = psycopg2.connect("dbname="+dbname+" user="+username)
    with db_conn.cursor() as cur:
        for tweet in response.data:
                values = tuple([tweet.id,tweet.conversation_id,tweet.text, tweet.author_id, tweet.created_at])
                #print (table_name)
                cur.execute("INSERT INTO "+tablename+" (id, conversation_id,text,author_id,created_at) VALUES (%s,%s, %s,%s,%s) on conflict(id) do nothing", values)
    db_conn.commit()
    db_conn.close()

##get the tweet converstion ids  from a tweet dataset table
def get_tweet_conversation_id(dbname,username, table_name, client):
    db_conn = psycopg2.connect("dbname="+dbname+" user="+username)
    with db_conn.cursor() as cur:
        id_list ={} 
        cur.execute("select conversation_id from "+table_name)
        curr_row = cur.fetchone()
        while curr_row !=None:
            id_list[curr_row[0]]=id_list.get(curr_row[0],0)+1
            curr_row = cur.fetchone()
    db_conn.close()
    return id_list


#this function use the search API to get tweets and store to the postgreSQL database
def get_tweets(client, query, s_time, e_time, max_r=10, next_token_v=None,dbname=None, username=None,tablename=None):
    #with open('next_token.txt','w') as a, open('../../data/'+str(year)+str(month)+'_tweets.json','w') as b:
    with open('next_token.txt','w') as a:
        if next_token_v!=None:
            response = client.search_all_tweets(query,end_time = e_time, start_time = s_time, max_results=max_r,\
                                     tweet_fields=['author_id','conversation_id','created_at','source'], next_token=next_token_v)
        else:
            response = client.search_all_tweets(query,end_time = e_time, start_time = s_time, max_results=max_r,\
                                     tweet_fields=['author_id','conversation_id','created_at','source'])
        if response.data !=None:
            #print (tablename)
            handle_results(response,dbname,username,tablename)

        while 'next_token' in response.meta.keys():
            time.sleep(4)
            next_t = response.meta['next_token']
            print (next_t)

            response = client.search_all_tweets(query,end_time = e_time, start_time = s_time, max_results=max_r,\
                                     tweet_fields=['author_id','conversation_id','created_at','source'],next_token=next_t)

            if response.data != None:
                handle_results(response,dbname,username,tablename)


        print ('all done')
        
def get_one_month_tweets(client, query, year, month, max_r=10,next_token_v=None,dbname=None,username=None,tablename=None):
    s_time = str(year)+"-"+str(month)+"-01T00:00:01Z"
    if int(month)<=11:
        e_time = str(year)+"-"+str(int(month)+1)+"-01T00:00:01Z"
    else:
        e_time = str(int(year)+1)+"-"+"01-01T00:00:01Z"
    print (s_time)
    print (e_time)
    get_tweets(client, query, s_time, e_time, max_r,next_token_v,dbname,username,tablename)

#date format: 2020-05-01
def get_days_tweets(client, query, s_date, e_date, max_r=10,next_token_v=None,dbname=None,username=None,tablename=None):
    s_time = s_date+"T00:00:01Z"
    e_time = e_date +"T00:00:01Z"  
    #print (tablename)
    get_tweets(client, query, s_time, e_time, max_r,next_token_v,dbname,username,tablename)

    

#this function is for reading streamed tweets from a file
def get_tweet_list(filename, mode=0):
    tweet_list = []
    with open(filename,'r') as a:
        lines = a.readlines()
        
        for line in lines:
            if mode == 0:
                data = json.loads(line)
                for tweet in data:
                    tweet_list.append(tweet)
            else:
                tweet = json.loads(line)
                tweet_list.append(tweet)
    return tweet_list

#this is for storing tweets from file to postgreSQL
def store_to_pg_from_file(path, dbname, dbuser, mode=0):
    conn = psycopg2.connect('dbname='+dbname+' user=' + dbuser)
    cur = conn.cursor()
    
    x = os.listdir(path)
    for f in x:
        print (f)
        tweets = get_tweet_list(path+'/'+f,mode)
        for tweet in tweets:
            values = tuple([tweet['id'],tweet['conversation_id'],tweet['text'],tweet['author_id'],tweet['created_at']])
            cur.execute("INSERT INTO tweets (id,conversation_id, text,author_id,created_at) VALUES (%s, %s, %s,%s,%s) on conflict(id) do nothing", values)
            conn.commit()
    cur.close() 
    conn.close()



