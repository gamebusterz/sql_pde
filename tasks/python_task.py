from typing import List

import pymysql
import datetime
import pytz
from pytz import timezone
from utils.mysql_db_connection import MySQLDBConnection
from utils.s3_utils import stream_to_s3
import math
import re


class PythonTask(object):
    def __init__(self):
        print('a')

    def __enter__(self):
        print('a')

    def __exit__(self, exc_type, exc_val, exc_tb):
        print('a')

    def compose_query(self, task_spec, batch_mode):

        if not task_spec['query']['custom'] and len(task_spec['table']) == 1:
            table_name = task_spec['table'][0]['name']
            columns = ', '.join(task_spec['table'][0]['columns'])

            _filter: List[str] = []
            for key, val in task_spec.get('table', {})[0].get('filters', {}).items():
                filter = '{column} {condition} {metric}'.format(column=key, condition=val.get('condition', ''),
                                                                metric=val.get('metric', ''))
                _filter.append(filter)
            _filter = ' AND '.join(_filter)

            _cdc = []
            interval_keywords = {'week', 'day', 'hour', 'minute', 'second'}
            interval_pattern = '\d+ (week|day|hour|minute|second)'
            for key, val in task_spec.get('table', {})[0].get('cdc', {}).items():

                if isinstance(val, str) and any(elem in val for elem in interval_keywords):
                    """If cdc argument is like 12 hours"""
                    interval = re.search(interval_pattern, val).group()
                    cdc = '{column} > DATE_SUB(now(), INTERVAL {interval})'.format(column=key, interval=interval)

                else:
                    """If cdc argument a timestamp or date value"""
                    cdc = "{column} > '{_timestamp}'".format(column=key, _timestamp=val)
                _cdc.append(cdc)
            _cdc = ' AND '.join(_cdc)

            if _cdc != '' and _filter != '':
                where_clause = 'WHERE {_cdc} AND {_filter}'.format(_cdc=_cdc, _filter=_filter)
            elif _cdc == '' and _filter != '':
                where_clause = 'WHERE {_filter}'.format(_filter=_filter)
            elif _cdc != '' and _filter == '':
                where_clause = 'WHERE {_cdc}'.format(_cdc=_cdc)
            else:
                where_clause = ''

            # _order = ', '.join(['{k} {v}'.format(k=k,v=v) for k,v in task_spec['table'][0]['order'].items()])
            _order = task_spec['table'][0]['split_order']

            if not batch_mode:
                query = """SELECT {columns} FROM {table_name} {where_clause} ;""".format(columns=columns,
                                                                                         table_name=table_name,
                                                                                         where_clause=where_clause)
            else:
                query = """SELECT {columns} FROM {table_name} {where_clause} ORDER BY {_order} LIMIT {offset},
                {limit} ;""".format(
                    columns=columns, table_name=table_name, where_clause=where_clause, _order=_order, offset='{offset}',
                    limit='{limit}')

            return query

    def get_batch_size(self, connection, **kwargs):

        """Get approximate table size(faster) instead of calculating exact size"""
        get_table_size_query = """SELECT round(((data_length) / 1024 / 1024), 2) 'size_in_mb'
                                      FROM information_schema.TABLES 
                                      WHERE table_schema = '{dbname}'
                                      AND table_name = '{table}';"""

        get_no_of_rows_query = """SELECT table_rows FROM information_schema.tables WHERE table_schema = '{dbname}' 
        and table_name = '{table}'; """

        dbname = kwargs['dbname']
        tables = kwargs['tables']
        batch_threshold = kwargs['batch_threshold']

        table_sizes = []
        table_row_counts = []

        cursor = connection.cursor()

        for table in tables:
            cursor.execute(get_table_size_query.format(dbname=dbname, table=table))
            table_sizes.append(cursor.fetchone()[0])

            cursor.execute(get_no_of_rows_query.format(dbname=dbname, table=table))
            table_row_counts.append(cursor.fetchone()[0])

        """Get size and no. of rows of largest table"""
        table_size = max(table_sizes)
        table_row_count = max(table_row_counts)

        """Number of batches"""
        if table_size > batch_threshold:
            batch_count = math.ceil(table_size / batch_threshold)

            """ Number of rows in each batch """
            row_batch_size = math.ceil(table_row_count / batch_count)
        else:
            """If table can be exported as single file"""
            row_batch_size = None

        return row_batch_size

    def extract_data(self, **kwargs):

        config_tz = timezone(kwargs['timezone'])
        now_utc = datetime.datetime.now(pytz.utc)
        timestamp_suffix = now_utc.astimezone(config_tz).strftime('%Y%m%d%H%M%S')

        with MySQLDBConnection(host=kwargs['host'], user=kwargs['user'], password=kwargs['password'],
                               db=kwargs['db']) as connection:

            row_batch_size = self.get_batch_size(connection, dbname=kwargs['db'],
                                                 tables=[table['name'] for table in kwargs['task_spec']['table']],
                                                 batch_threshold=kwargs['batch_threshold'])

            batch_mode = True if row_batch_size else False
            query = self.compose_query(kwargs['task_spec'], batch_mode)

            cursor = connection.cursor()

            if not batch_mode:
                cursor.execute(query)
                data = cursor.fetchall()

                header = [desc[0] for desc in cursor.description]

                if data:
                    streamed_file = stream_to_s3(kwargs['task_spec']['task_name'], data, header, timestamp_suffix)
                    print(streamed_file, 'uploaded')

            else:
                offset = 0
                batch_id = 0

                while True:
                    cursor.execute(query.format(offset=offset, limit=row_batch_size))
                    data = cursor.fetchall()
                    header = [desc[0] for desc in cursor.description]
                    row_count = cursor.rowcount
                    if data:
                        streamed_file = stream_to_s3(kwargs['task_spec']['task_name'], data, header, timestamp_suffix,
                                                     batch_id=batch_id)
                        print(streamed_file, 'uploaded')

                    """If last batch"""
                    if row_count < row_batch_size:
                        break

                    batch_id += 1
                    offset += row_batch_size
