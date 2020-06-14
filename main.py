import os
import json
import argparse

from tasks.python_task import PythonTask
from utils.mysql_db_connection import MySQLDBConnection
from utils.json_validator import parse

"""Read config"""
if os.path.exists('config/config.json'):
	config = parse('config/config.json')
else:
	print('config.json does not exist, exiting')
	sys.exit(0)

if __name__ == '__main__':

	"""Command line argument parser"""
	parser = argparse.ArgumentParser(description='Periodic Data Extraction from MySQL')
	parser.add_argument("-s", "--spec", help="Task spec JSON file (rel/abs path)", default='task_spec/mock_data.json', required=False)
	
	args = parser.parse_args()

	"""Read task spec"""
	if args.spec and os.path.exists(args.spec):
		task_spec = parse(args.spec)
	else:
		print('task spec does not exist, exiting')
		sys.exit(0)

	t = PythonTask()
	t.extract_data(task_spec=task_spec, timezone=config['timezone'], host=config['databases']['mysql']['host'], user=config['databases']['mysql']['user'], password=config['databases']['mysql']['password'], db=config['databases']['mysql']['db'], batch_threshold=config['batch_threshold'])