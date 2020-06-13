import json

from utils.mysql_db_connection import MySQLDBConnection

with open('config/config.json') as config:
    config = json.load(config)


class Task(object):
    def __init__(self):
        print('a')

    def __enter__(self):
        print('a')

    def __exit__(self, exc_type, exc_val, exc_tb):
        print('a')

    def extract_data(self):
        with MySQLDBConnection(host=config['databases']['mysql']['host'], user=config['databases']['mysql']['user'],
                               password=config['databases']['mysql']['password'], db=config['databases']['mysql']['db']) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM regions;")
            print(cursor.fetchall())


if __name__ == '__main__':
    t = Task()
    t.extract_data()
