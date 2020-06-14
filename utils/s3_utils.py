import smart_open
import io
import csv
import coloredlogs,logging

bucket_name = 'sql-pde-exports'

logger = logging.getLogger(__name__)
coloredlogs.install(level='DEBUG', logger=logger)


def stream_to_s3(task_name,data,header,timestamp_suffix,**kwargs):

    """If file is split in batches"""
    if kwargs:
        file = '_'.join([task_name,timestamp_suffix,str(kwargs['batch_id'])])+'.csv'
    else:
        file = '_'.join([task_name,timestamp_suffix])+'.csv'

    f = io.StringIO()
    with smart_open.open('s3://{bucket_name}/{task_name}/{file}'.format(bucket_name=bucket_name,task_name=task_name,file=file), 'w') as fout:
        logger.info('Streaming file contents to S3')
        _writer = csv.writer(fout)
        _writer.writerow(header)
        fout.write(f.getvalue())
    
        for row in data:
            f.seek(0)
            f.truncate(0)
            _writer.writerow(row)
            fout.write(f.getvalue())
    
    f.close()
    logger.info('Complete')

    return file