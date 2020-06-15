import os
import json
import argparse
import datetime
import pytz
import coloredlogs,logging

from pytz import timezone
from tasks.python_task import PythonTask
from utils.mysql_db_connection import MySQLDBConnection
from utils.json_validator import parse
from utils.filepath_utils import check_log_path

config_tz = timezone('Asia/Kolkata')
now_utc = datetime.datetime.now(pytz.utc)

check_log_path()
logging.basicConfig(filename='logs/sql_pde_{timestamp}.log'.format(timestamp=format(now_utc.astimezone(config_tz).strftime('%Y%m%d%H%M%S')),level=logging.DEBUG))

logger = logging.getLogger(__name__)
coloredlogs.install(level='DEBUG', logger=logger)


"""Read config"""
if os.path.exists('config/config.json'):
	config = parse('config/config.json')
else:
	logger.critical('config.json does not exist, exiting')
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
		logger.critical('task spec does not exist, exiting')
		sys.exit(0)

	t = PythonTask()
	logger.debug('Calling extract_data')
	t.extract_data(task_spec=task_spec, timezone=config['timezone'], host=config['databases']['mysql']['host'], user=config['databases']['mysql']['user'], password=config['databases']['mysql']['password'], db=config['databases']['mysql']['db'], batch_threshold=config['batch_threshold'],logger=logger)


