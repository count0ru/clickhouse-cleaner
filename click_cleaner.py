#!/usr/bin/python
import argparse
import clickhouse_driver

PARSER = argparse.ArgumentParser(description='Deleting database partitions older than date')
PARSER.add_argument('-d', '--database', help='database name', required=True)
PARSER.add_argument('-n', '--max-days', help='remove partitions after max day count', required=True)
PARSER.add_argument('-H', '--host-address', help='server address', default="127.0.0.1")
PARSER.add_argument('-c', '--dry-run', help='only show all queries (not execute)',
                    action='store_true')
ARGS = vars(PARSER.parse_args())

MAX_DAYS = int(ARGS['max_days'])
DBNAME = ARGS['database']
SERVER_ADDR = ARGS['host_address']
IS_DRYRUN = ARGS['dry_run']

try:
    CLIENT = clickhouse_driver.Client(SERVER_ADDR)
except TypeError:
    print "can't connect to server"

try:
    CLIENT.execute('USE ' + DBNAME)
except TypeError:
    print "can't execute query"

# get tables list
TABLES = CLIENT.execute('SHOW TABLES')

for table in TABLES:
    print "Process table:", table[0]
    # select all active partitions for this table
    get_partitions_command = "SELECT DISTINCT partition FROM system.parts WHERE active AND (table = '%s') AND (max_date < toStartOfMonth(toStartOfMonth(now(), 'UTC') - %s)) AND (max_date != '0000-00-00')" % (table[0], MAX_DAYS)
    print get_partitions_command
    partitions = CLIENT.execute(get_partitions_command)
    for partition in partitions:
        # calculate delta between current date and max date in partition
        partition_delete_command = "ALTER TABLE %s.%s DROP PARTITION %s" % (DBNAME, table[0], partition[0])
        if IS_DRYRUN:
            print partition_delete_command
        else:
            print "%s now removed" % partition[0]
            CLIENT.execute(partition_delete_command)
