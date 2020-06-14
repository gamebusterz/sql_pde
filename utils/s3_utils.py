import smart_open
import io
import csv

def stream_to_s3(task_name,data,header,timestamp_suffix,**kwargs):

    if kwargs:
        file = '_'.join([task_name,timestamp_suffix,str(kwargs['batch_id'])])+'.csv'
    else:
        file = '_'.join([task_name,timestamp_suffix])+'.csv'

    f = io.StringIO()
    with smart_open.open('s3://sql-pde-exports/{task_name}/{file}'.format(task_name=task_name,file=file), 'w') as fout:
        _writer = csv.writer(fout)
        _writer.writerow(header)
        fout.write(f.getvalue())
    
        for row in data:
            f.seek(0)
            f.truncate(0)
            _writer.writerow(row)
            fout.write(f.getvalue())
    
    f.close()

    return file