import pickle as pk
import pymysql as mysql
from tqdm import tqdm

host, login, password, db, file_name = input('Enter host, login, password, database name and file name:\n').split()

with open(file_name, 'rb') as f:
    data = pk.load(f)

connection = mysql.connect(host, login, password, db)
with connection.cursor() as cur:
    # Line below for some reasons do not work, but u have to use same SQL-query in MySQL/MariaDB cmd and it works
    #cur.execute('DROP TABLE games_tags;CREATE TABLE games_tags(appid INTEGER NOT NULL UNIQUE, tags VARCHAR(1024) NOT NULL) ENGINE InnoDB;')
    for key in tqdm(data.keys()):
        string = "'"+', '.join(data[key]).replace('\'', '')+"'"
        cur.execute('INSERT INTO games_tags VALUES ({}, {});'.format(key, string))
connection.commit()
connection.close()
