import json
from tasks.python_task import PythonTask

from utils.mysql_db_connection import MySQLDBConnection

with open('config/config.json') as config:
    config = json.load(config)

with open('task_spec/mock_data.json') as task_spec:
    task_spec = json.load(task_spec)

if __name__ == '__main__':
    t = PythonTask()
    t.extract_data(task_spec=task_spec, timezone=config['timezone'], host=config['databases']['mysql']['host'], user=config['databases']['mysql']['user'], password=config['databases']['mysql']['password'], db=config['databases']['mysql']['db'], batch_threshold=config['batch_threshold'])