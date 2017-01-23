import MySQLdb
from google.cloud import bigquery
import logging
import os
from MySQLdb.converters import conversions

import pprint

MYSQL_HOST='127.0.0.1'
MYSQL_DATABASE='amazon'
MYSQL_USER='root'
MYSQL_PASSWORD=''

PROJECT_ID = "ceremonial-hold-156112"
DATASET_ID = "youtube"

## set logging
logging.basicConfig(level=logging.ERROR)

## set env key to authenticate application
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "%s/%s" % (os.path.dirname(os.path.abspath(__file__)), 'google_key.json')

bqTypeDict = { 'int' : 'INTEGER',
               'varchar' : 'STRING',
               'double' : 'FLOAT',
               'tinyint' : 'INTEGER',
               'decimal' : 'FLOAT',
               'text' : 'STRING',
               'smallint' : 'INTEGER',
               'char' : 'STRING',
               'bigint' : 'INTEGER',
               'float' : 'FLOAT',
               'longtext' : 'STRING',
               'datetime' : 'TIMESTAMP'
              }

from MySQLdb.times import DateTime2literal

def conv_date_to_timestamp(str_date):
    import time
    import datetime

    date_time = MySQLdb.times.DateTime_or_None(str_date)
    unix_timestamp = (date_time - datetime.datetime(1970,1,1)).total_seconds()

    return unix_timestamp

def Connect():
    ## fix conversion. datetime as str and not datetime object
    conv=conversions.copy()
    conv[12]=conv_date_to_timestamp
    return MySQLdb.connect(host=MYSQL_HOST, db=MYSQL_DATABASE, user=MYSQL_USER, passwd=MYSQL_PASSWORD, conv=conv)


def BuildSchema(table):
    conn = Connect()
    cursor = conn.cursor()
    cursor.execute("DESCRIBE %s;" % table)
    tableDecorator = cursor.fetchall()
    schema = []

    for col in tableDecorator:
        colType = col[1].split("(")[0]
        if colType not in bqTypeDict:
            logging.warning("Unknown type detected, using string: %s", str(col[1]))

        field_mode = "NULLABLE" if col[2] == "YES" else "REQUIRED"
        field = bigquery.SchemaField(col[0], bqTypeDict.get(colType, "STRING"), mode=field_mode)

        schema.append(field)

    return tuple(schema)

def SQLToBQBatch(table, limit=0):
    logging.info("****************************************************")
    logging.info("Starting SQLToBQBatch. Got: Table: %s, Limit: %i" % (table, limit))

    # Instantiates a client
    bigquery_client = bigquery.Client()

    try:
        # Prepares the new dataset
        dataset = bigquery_client.dataset(DATASET_ID)

        # Creates the new dataset
        dataset.create()

        logging.info("Added Dataset")
    except Exception, e:
        logging.info(e)
        if ("Already Exists: " in str(e)):
            logging.info("Dataset already exists")
        else:
            logging.error("Error creating dataset: %s Error", str(e))

    try:
        bq_table = dataset.table(table)
        bq_table.schema = BuildSchema(table)
        bq_table.create()

        logging.info("Added Table")
    except Exception, e:
        logging.info(e)
        if ("Already Exists: " in str(e)):
            logging.info("Table already exists")
        else:
            logging.error("Error creating table: %s Error", str(e))

    conn = Connect()
    cursor = conn.cursor()

    logging.info("Starting load loop")
    count = -1
    cur_pos = 0
    total = 0
    batch_size = 1000

    while count != 0 and (cur_pos < limit or limit == 0):
        count = 0
        if batch_size + cur_pos > limit and limit != 0:
            batch_size = limit - cur_pos
        sqlCommand = "SELECT * FROM %s LIMIT %i, %i" % (table, cur_pos, batch_size)
        logging.info("Running: %s", sqlCommand)
        cursor.execute(sqlCommand)
        data = []
        import pprint

        for _, row in enumerate(cursor.fetchall()):
            data.append(row)
            count += 1

        logging.info("Read complete")

        if count != 0:
            logging.info("Sending request")
            pprint.pprint(data)
            insertResponse = bq_table.insert_data(data)

            for row in insertResponse:
                if 'errors' in row:
                    logging.error('not able to upload data: %s', row['errors'])

            cur_pos += batch_size
            total += count
            logging.info("Done %i, Total: %i", count, total)
            #if "insertErrors" in insertResponse:
            #    logging.error("Error inserting data index: %i", insertResponse["insertErrors"]["index"])
            #    for error in insertResponse["insertErrors"]["errors"]:
            #        logging.error(error)
        else:
            logging.info("No more rows")

SQLToBQBatch('videos')
