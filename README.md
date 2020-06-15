# SQL Periodic Data Extraction
Generic framework for periodic data extraction from MySQL.

The framework takes a task spec and connects to a MySQL DB to get the set of tables specified in the task_spec and then _streams_ the result to S3 (without storing it locally). If the data is too large to be exported as a single file, it is split and streamed in batches. 

To get the tables size and row count, it uses the system tables to find an approximate number much quicker than doing a `COUNT(*)`. The `batch_threshold` is configurable.

I have also built a real-time ingestion mechanism using Kafka, reading MySQL binlogs for any INSERTs/UPDATEs/DELETEs, which can be converted into a file ready to loaded to a Data Warehouse or be used in a Data Lake. 
However I've not include it here because that would serve a difference purpose, and to make it work for this, we'd have to persist the data in Kafka for days. Let me know if you need to take a look at it. Also, a hybrid approach can probably be built which can read data from either of these two. 

## a. Steps to run
1. Install MySQL. Preferrably v5.6+.
2. Clone this repository and change the config/config.json file to enter MySQL credentials (or specify credentials using commandline arguments as described later).
3. Run `pip3 install -r requirements.txt` to install the dependencies.
4. From the root directory of the project, run `python3 main.py --spec <task_spec_path/task_spec.json>` to submit a task and run the extraction. 
   For example, to run the sample `employee` task spec, run: 
  `python3 main.py --spec task_spec/employees.json`

##### Commandline arguments:
**Note** : All the commandline arguments are optional, the default config from the `config/config.json` are used as credentials and the `task_spec/mock_data.json` is used as the default task.


Run `python3 main.py --help` to see a list of supported commandline arguments:

```
usage: main.py [-h] [-s SPEC] [-d DB] [-H HOST] [-P PASSWORD] [-u USER]
               [-p PORT] [-b BATCH]

Periodic Data Extraction from MySQL

optional arguments:
  -h, --help            show this help message and exit
  -s SPEC, --spec SPEC  Task spec JSON file (rel/abs path)
  -d DB, --db DB        db for MySQL source
  -H HOST, --host HOST  hostname for db
  -P PASSWORD, --password PASSWORD
                        password for db
  -u USER, --user USER  user for db
  -p PORT, --port PORT  port for db
  -b BATCH, --batch BATCH
                        Batch threshold in MB (to split tables larger than
                        batch threshold)
```

For example:  
`--spec abc_task_spec.json --batch 200` arguments will start extraction as specified `abc_task_spec.json` (if it exists) with a batch size of 200 MB, ie, if the table size is larger than 200 MB, it'll be split in batches on 200 MB or less.  

Simply running `python3 main.py` without any arguments will run the default task_spec with the default credentials.

## b. Task Spec 

The task_spec directory consists of the data-extraction tasks, these can also be submitted via commandline arguments. The following is a sample task spec:

`employees.json`
```
{
    "task_name" : "employees",
    "table" : [
    {
      "name" : "employees",
      "columns" : ["employee_id","first_name","last_name","email","phone_number","hire_date","job_id","salary","manager_id","department_id"],
      "filters" : {
        "manager_id" : {
          "condition" : "!=",
          "metric" : 103
        }
      },
        "cdc" : {
            "hire_date" : "1995-05-05"
        },
        "split_order" : "employee_id" 
    }
    ],

    "query" : {
        "custom" : null
    },

    "column_transformation" : {
    },

    "owner" : {
      "name" : "Sailesh Choyal",
      "email" : "saileshchoyal@gmail.com"
    },

    "source" : "mysql",
    "sink" : "s3",
    "output_file_format" : "csv", 
    "output_timezone" : null

}
```
Let's break it down:
1. `task_name`* : An identifier for a task
2. `table`* : Tables to extract and their details:
    * `name`* : table name
    * `columns`* : columns to extract
    * `filters` : where/filter conditions, supports multiple clauses. The above task_spec will apply the `WHERE manager_id != 103` clause.
    * `cdc` : Column for CDC, a condition `cdc_col > cdc_val` will be applied. Supports multiple CDCs with multiple values. Also supports readable user inputs like _12 hours_, _2 weeks_, _30 minutes_ . For example, **modified_at** : **12 hours** will extract all data which has been modified in the last 12 hours from the current/execution time of the task. The above task_spec will apply the `WHERE hire_date > '1995-05-05` clause.
    * `split_order`* : Specifies the order in which the data will be exported while being exported in batches (if table is larger than a specified size)
3. `query` : Can specify a custom query, which can JOIN multiple tables and export them as a single transformed file.
4. `column_transformation` : To extract a column with aa transformation at the extraction step itself. For example, JSON fields can be split into multiple columns while extracting the data itself.
5. `owner` : Can be used to send email alerts 
6. `source, sink` : Although it's currently MySQL, with some additions it can be made to work with other SQL databases such as Postgres or Redshift. Similarly the destination can be something else other than S3 (Google Cloud, Azure) or Data Warehouse such as Redshift. 

**Note** : * implies required fileds

## c. S3 
The files are streamed to an S3 bucket with the `task_name` as the sub-directory and a `timestamp_suffix` appended to the filename. 
Eg. _employees_202015095530.csv_ . 
If the file is exported in batches, a batch_id suffix is also added:  
Eg. _employees_202015095530_0.csv_, _employees_202015095530_1.csv_

**The URL and credentials to view the files in S3 are attached in the email. The `sql-pde-exports` is what you're looking for.**

## d. Misc
#### Logs
The `logs/` directory stores the logs for each execution in a separate file, with details like:
 * number of rows extracted
 * time taken for completion
 * the actual SQL query that was executed to extract the data

#### Compression
 A compress function is added in the `utils/` directory, which can be called to compress the file before exporting to S3. This could only be done if the **stream to s3** option was not used.

#### Sample data
Some of the sample tables and data is generated using:
[Table definition](https://cdn.sqltutorial.org/wp-content/uploads/2020/04/mysql.txt)
[Table data](https://cdn.sqltutorial.org/wp-content/uploads/2020/04/mysql-data.txt)

### Airflow
Having never used Airlfow before (we use a combination of Luigi and Jenkins at my organization), I tried to make it work by writing a DAG for this task and then a DAGBAG so as to not have to move the code to the `dags/` directory. But unfortunately that doesn't progress beyond `Filling up the DagBag` stage. I'm sure this can be worked out, and also a lot of other features of Airlfow can be useful. But in the interest of time, I skipped this.

Hello CRON my old friend !

## e. What would I improve if given more time
1. Handle the scheduling with Airflow
2. Make it deployable via Docker
3. Write tests
4. Added auditing/metrics to push them to something like Cloudwatch or Prometheus,InfluxDB.
5. Data transformation (currently supported via custom query). The following could be the other options:
    * Complete the _column_transformation_ logic (same as custom query but only the whole query need not be written)
    * Row-by-row transformation using a python function
    * Transformation layer using Pandas (optimal when processing a large file as input, instead of row-by-row)
6. Parallelization : Some independent tasks can be parallelized, like extracting batches of rows from MySQL, however the sequence of batches wouln't be maintained in this case. If this is acceptable, the file-streaming to S3 can also be parallelized similarly.
7. I went with a structure where one task_spec deals with one table, and I didn't want to make any last minute changes. It'll only need some minor changes in one file to achieve that. Currently only one table can be exported in the task_spec or multiple tables JOINed together.
