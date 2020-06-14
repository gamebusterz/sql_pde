import smart_open
import io
import csv
import datetime
import pytz
from pytz import timezone

config_tz = timezone('Asia/Kolkata')

def stream_to_s3(task_name,data,header):

    now_utc = datetime.datetime.now(pytz.utc)
    filename_suffix = now_utc.strftime('%Y%m%d%H%M%S')

    f = io.StringIO()
    with smart_open.smart_open('s3://sql-pde-exports/{task_name}/{file}'.format(task_name=task_name,file=task_name+'_'+filename_suffix+'.csv'), 'w') as fout:
        _writer = csv.writer(fout)
        _writer.writerow(header)
        fout.write(f.getvalue())
    
        for row in data:
            f.seek(0)
            f.truncate(0)
            _writer.writerow(row)
            fout.write(f.getvalue())
    
    f.close()

    return task_name+'_'+filename_suffix+'.csv'