import pandas as pd 
import numpy as np
import os
import sys
import json
import pymysql as mysql
import time

host, login, password, db, path = input('Enter host, login, password, database name and path to dist folder:\n').split()

start_time = time.time()
if not os.path.exists(path):
    os.makedirs(path)
if path[-1] != '/':
    path += '/'

connection = mysql.connect(host, login, password, db)

with connection.cursor() as cur:
    
    print('Games stats_1 collecting...', end='\r')
    cur.execute('SELECT appid, SUM(CASE WHEN games_1.playtime_forever > 0 THEN 1 WHEN games_1.playtime_forever = 0 THEN 0 END) as counter, SUM(playtime_forever)/60 as summary_time FROM games_1 GROUP BY appid;')
    data = cur.fetchall()
    df = pd.DataFrame(data, columns=['appid', 'n_players_non_zero', 'summary_playtime'])
    df.to_csv(path+'playtime_n_players_1.csv', index=False)
    del df
    print('Games statistics_1 collected!')

    print('Games stats_2 collecting...', end='\r')
    cur.execute('SELECT appid, SUM(CASE WHEN games_2.playtime_forever > 0 THEN 1 WHEN games_2.playtime_forever = 0 THEN 0 END) as counter, SUM(playtime_forever)/60 as summary_time FROM games_2 GROUP BY appid;')
    data = cur.fetchall()
    df = pd.DataFrame(data, columns=['appid', 'n_players_non_zero', 'summary_playtime'])
    df.to_csv(path+'playtime_n_players_2.csv', index=False)
    del df
    print('Games statistics_2 collected!')

    print('Tags collecting...', end='\r')
    cur.execute('SELECT * FROM games_tags;')
    data = cur.fetchall()
    df = pd.DataFrame(data, columns=['appid', 'tags'])
    df.to_csv(path+'games_tags.csv', index=False)
    del df
    print('Games tags collected!')

    print('Main info collecting...', end='\r')
    cur.execute('SELECT appid, Title, Price, Release_Date, Rating, Required_Age, Is_Multiplayer FROM app_id_info WHERE type = \'game\';')
    data = cur.fetchall()
    df = pd.DataFrame(data, columns=['appid', 'title', 'price', 'release_date', 'rating', 'required_age', 'is_multiplayer'])
    df.to_csv(path+'games_main_info.csv', index=False)
    del df
    print('Games main info collected!')

    print('Genres collecting...', end='\r')
    cur.execute('SELECT * FROM games_genres;')
    data = cur.fetchall()
    df = pd.DataFrame(data, columns=['appid', 'genre'])
    df.to_csv(path+'games_genres.csv', index=False)
    del df
    print('Games genres collected!')

print('Time:', str(abs(start_time-time.time()))+'s.')