import requests as r
from bs4 import BeautifulSoup
import json
from tqdm import tqdm
import pymysql as mysql
import time
import pickle as pk
from threading import Thread
import os
clear = lambda: os.system('cls')

host, login, password, db, N_THREADS = input('Enter host, login, password, database name and number of threads:\n').split()
N_THREADS = int(N_THREADS)

print('Scrapping started at', time.ctime())
start_time = time.time()
print('Data preparation...')
connection = mysql.connect(host, login, password, db)
with connection.cursor() as cur:
    cur.execute('SELECT DISTINCT appid, Title FROM app_id_info WHERE type = \'game\';')
    appids_titles = list(map(lambda x: (str(x[0]), x[1]), cur.fetchall()))

response = r.get('https://store.steampowered.com/tag/browse/#global_4667')
soup = BeautifulSoup(response.text, 'lxml')
good_tags = []
cookies = {
    'wants_mature_content': '1',
    'birthtime': '189302401',
    'lastagecheckage': '1-January-1976'
}

print('Popular tags parsing...')
for el in soup.select('.tag_browse_tag'):
    good_tags.append(el.text)

print('Games tags scrapping...')
games = {}
target_size = len(appids_titles)
class ParseThread(Thread):
    def __init__(self, at):
        Thread.__init__(self)
        self.at = at
    
    def run(self):
        for appid, title in self.at:
            try:
                tmp_response = r.get('https://store.steampowered.com/app/'+appid+'/'+title, cookies=cookies)
                tmp_soup = BeautifulSoup(tmp_response.text, 'lxml')
                games[appid] = list(map(lambda x: x.text.replace('\t', '').replace('\n', '').replace('\r', ''), tmp_soup.select('.glance_tags_ctn .popular_tags a.app_tag')))
                games[appid] = list(filter(lambda x: x in good_tags, games[appid]))
            except:
                try:
                    time.sleep(60)
                    tmp_response = r.get('https://store.steampowered.com/app/'+appid+'/'+title, cookies=cookies)
                    tmp_soup = BeautifulSoup(tmp_response, 'lxml')
                    games[appid] = list(filter(lambda x: x.text.replace('\t', '').replace('\n', '').replace('\r', '') in good_tags, tmp_soup.select('.glance_tags_ctn .popular_tags a.app_tag')))
                except:
                    games[appid] = []
            length = len(games)
            print(f'{length:04}/{target_size}', end='\r')

threads = []
for i in range(N_THREADS):
    thread = ParseThread(appids_titles[int((target_size/N_THREADS)*i):int((target_size/N_THREADS)*(i+1))])
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

with open(str(time.time())+'_tags.pickle', 'wb') as f:
    pk.dump(games, f)

print('Tags parsed successfully at', time.ctime())
end_time = time.time()
print('It took', str(abs(end_time-start_time))+'s')