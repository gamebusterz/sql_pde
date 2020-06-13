import json
from tasks.python_task import PythonTask

from utils.mysql_db_connection import MySQLDBConnection

with open('config/config.json') as config:
    config = json.load(config)

with open('task_def/region.json') as task_def:
    task_def = json.load(task_def)

if __name__ == '__main__':
    t = PythonTask()
    t.extract_data(task_def=task_def,host=config['databases']['mysql']['host'], user=config['databases']['mysql']['user'], password=config['databases']['mysql']['password'], db=config['databases']['mysql']['db'])