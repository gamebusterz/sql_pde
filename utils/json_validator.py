import json
import logging,coloredlogs

logger = logging.getLogger(__name__)
coloredlogs.install(level='DEBUG', logger=logger)

def parse(filename):
	with open(filename) as f:
		try:
			return json.load(f)
		except (ValueError,json.decoder.JSONDecodeError) as e:
			logger.critical('JSON file: {filename} cannot be parsed: {error}'.format(filename=filename,error=format(e)))
			return False