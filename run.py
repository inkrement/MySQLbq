#!/usr/bin/env python

import MySQLdb
from google.cloud import bigquery
import logging
import os
from MySQLdb.converters import conversions
import click

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

def conv_date_to_timestamp(str_date):
    import time
    import datetime

    date_time = MySQLdb.times.DateTime_or_None(str_date)
    unix_timestamp = (date_time - datetime.datetime(1970,1,1)).total_seconds()

    return unix_timestamp

def Connect(host, database, user, password):
    ## fix conversion. datetime as str and not datetime object
    conv=conversions.copy()
    conv[12]=conv_date_to_timestamp
    return MySQLdb.connect(host=host, db=database, user=user, passwd=password, conv=conv)


def BuildSchema(host, database, user, password, table):
    conn = Connect(host, database, user, password)
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


@click.command()
@click.option('-h', '--host', default='127.0.0.1', help='MySQL hostname')
@click.option('-d', '--database', required=True, help='MySQL database')
@click.option('-u', '--user', default='root', help='MySQL user')
@click.option('-p', '--password', default='', help='MySQL password')
@click.option('-t', '--table', required=True, help='MySQL table')
@click.option('-i', '--projectid', required=True, help='Google BigQuery Project ID')
@click.option('-n', '--dataset', required=True, help='Google BigQuery Dataset name')
@click.option('-l', '--limit',  default=0, help='max num of rows to load')
@click.option('-s', '--batch_size',  default=1000, help='max num of rows to load')
def SQLToBQBatch(host, database, user, password, table, projectid, dataset, limit, batch_size):
    logging.info("Starting SQLToBQBatch. Got: Table: %s, Limit: %i" % (table, limit))

    # Instantiates a client
    bigquery_client = bigquery.Client()

    try:
        # Prepares the new dataset
        if dataset == '':
            dataset = database

        dataset = bigquery_client.dataset(dataset)

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
        bq_table.schema = BuildSchema(host, database, user, password, table)
        bq_table.create()

        logging.info("Added Table")
    except Exception, e:
        logging.info(e)
        if ("Already Exists: " in str(e)):
            logging.info("Table already exists")
        else:
            logging.error("Error creating table: %s Error", str(e))

    conn = Connect(host, database, user, password)
    cursor = conn.cursor()

    logging.info("Starting load loop")
    count = -1
    cur_pos = 0
    total = 0

    while count != 0 and (cur_pos < limit or limit == 0):
        count = 0
        if batch_size + cur_pos > limit and limit != 0:
            batch_size = limit - cur_pos
        sqlCommand = "SELECT * FROM %s LIMIT %i, %i" % (table, cur_pos, batch_size)
        logging.info("Running: %s", sqlCommand)
        cursor.execute(sqlCommand)
        data = []

        for _, row in enumerate(cursor.fetchall()):
            data.append(row)
            count += 1

        logging.info("Read complete")

        if count != 0:
            logging.info("Sending request")
            insertResponse = bq_table.insert_data(data)

            for row in insertResponse:
                if 'errors' in row:
                    logging.error('not able to upload data: %s', row['errors'])

            cur_pos += batch_size
            total += count
            logging.info("Done %i, Total: %i", count, total)
        else:
            logging.info("No more rows")


if __name__ == '__main__':
    ## set logging
    logging.basicConfig(level=logging.ERROR)

    ## set env key to authenticate application
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "%s/%s" % (os.path.dirname(os.path.abspath(__file__)), 'google_key.json')

    ## run the command
    SQLToBQBatch()
