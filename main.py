import os
import json
import argparse
import datetime
import pytz
import time
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
	parser.add_argument("-d", "--db", help="db for MySQL source", default=config['databases']['mysql']['db'], required=False)
	parser.add_argument("-H", "--host", help="hostname for db", default=config['databases']['mysql']['host'], required=False)
	parser.add_argument("-P", "--password", help="password for db", default=config['databases']['mysql']['password'], required=False)
	parser.add_argument("-u", "--user", help="user for db", default=config['databases']['mysql']['user'], required=False)
	parser.add_argument("-p", "--port", help="port for db", default=config['databases']['mysql']['port'], required=False)
	parser.add_argument("-b", "--batch", help="Batch threshold in MB (to split tables larger than batch threshold)", default=config['batch_threshold'], required=False)

	args = parser.parse_args()

	"""Read task spec"""
	if args.spec and os.path.exists(args.spec):
		task_spec = parse(args.spec)
	else:
		logger.critical('task spec does not exist, exiting')
		sys.exit(0)

	start = time.time()

	t = PythonTask()
	logger.debug('Calling extract_data')

	if args.db and args.host and args.password and args.user and args.port:
		t.extract_data(task_spec=task_spec, timezone=config['timezone'], host=args.host, user=args.user, password=args.password, db=args.db, batch_threshold=config['batch_threshold'],logger=logger)

	end = time.time()
	logger.info('Time for completion: {time_diff}s'.format(time_diff=round((end - start),3)))

