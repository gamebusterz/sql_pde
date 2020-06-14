import os

LOG_DIR_NAME = 'logs'

def check_log_path():
	if not os.path.exists(LOG_DIR_NAME):
		os.makedirs(LOG_DIR_NAME)