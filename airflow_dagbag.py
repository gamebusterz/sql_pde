""" Add all DAGs to airflow """

import os

from airflow.models import DagBag
# from airflow_scheduling import af_config

source_dir = 'sql_pde'

# dag_dirs = [os.getcwd().split(source_dir)[0] + source_dir + '/airflow_scheduling']
dag_dirs = '/home/ubuntu/sql_pde/airflow_scheduling'

for dir in dag_dirs:
	dag_bag = DagBag(dir)

	if dag_bag:
		for dag_id, dag in dag_bag.dags.items():
			globals()[dag_id] = dag