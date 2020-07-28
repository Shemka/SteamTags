import pandas as pd 
import numpy as np
import os
import sys
import json
import pymysql as mysql
import time

host, login, password, db, n_users = input('Enter host, login, password, database name, users_number:\n').split()

start_time = time.time()
connection = mysql.connect(host, login, password, db)

with connection.cursor() as cur:
    cur.execute(f'SELECT UNIQUE steamid FROM games_2 WHERE playtime_forever > 0 LIMIT {n_users};')
    steamids = list(map(lambda x: str(x[0]), cur.fetchall()))
    steamids = '('+', '.join(steamids)+')'
    cur.execute(f'SELECT steamid, appid, playtime_forever FROM games_2 WHERE steamid IN {steamids} AND playtime_forever > 0;')
    data = cur.fetchall()
    print('Dataset contains', len(data), 'rows')
    df = pd.DataFrame(data, columns=['steamid', 'appid', 'playtime_forever'])
    df.to_csv(f'{n_users}_users_slice.csv.gz', index=False, compression='gzip')
    