import pymysql
from utils.mysql_db_connection import MySQLDBConnection

class PythonTask(object):
    def __init__(self):
        print('a')

    def __enter__(self):
        print('a')

    def __exit__(self, exc_type, exc_val, exc_tb):
        print('a')


    def compose_query(self,task_def):
        if not task_def['query']['custom'] and len(task_def['table']) == 1:
            table_name = task_def['table'][0]['name']
            columns = ', '.join(task_def['table'][0]['columns'])
            
            _filter = []
            for key,val in task_def.get('table',{})[0].get('filters',{}).items():
                filter = '{column} {condition} {metric}'.format(column=key,condition=val.get('condition',''),metric=val.get('metric',''))
                _filter.append(filter)
            _filter = ' AND '.join(_filter)

            _order = ', '.join(['{k} {v}'.format(k=k,v=v) for k,v in task_def['table'][0]['order'].items()])

            query = """SELECT {columns} FROM {table_name} WHERE {_filter} ORDER BY {_order} ;""".format(columns=columns,table_name=table_name,_filter=filter,_order=_order)

        return query


    def extract_data(self,**kwargs):

        query = self.compose_query(kwargs['task_def'])


        with MySQLDBConnection(host=kwargs['host'], user=kwargs['user'], password=kwargs['password'], db=kwargs['db']) as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            print(cursor.fetchall())