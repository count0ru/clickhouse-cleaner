#!/usr/bin/python
import argparse
import datetime
import clickhouse_driver
# based on https://fastnetmon.com/data-retention-for-clickhouse-persistent-data-storage script

parser = argparse.ArgumentParser(description='Deleting database partitions older than date')
parser.add_argument('-d', '--database', help='database name', required=True)
parser.add_argument('-n', '--max-days', help='remove partitions after max day count', required=True)
parser.add_argument('-H', '--host-address', help='server address', default="127.0.0.1")
parser.add_argument('-c', '--dry-run', help='only show all queries (not execute)', action='store_true')
args = vars(parser.parse_args())

max_days = int(args['max_days'])
dbname = args['database']
server_addr = args['host_address']
is_dryrun = args['dry_run']

try:
    client = clickhouse_driver.Client(server_addr)
except:
    print "can't connect to server"

try:
    client.execute('USE ' + dbname);
except:
    print "can't execute query"

now = datetime.datetime.now()
tables = client.execute('SHOW TABLES')
 
for table in tables:
    print "Process table:", table[0]
    # Select all active partitions for this table
    partitions = client.execute("SELECT DISTINCT partition, table FROM system.parts where active and table='" + table[0]  +"'")
    for partition in partitions:
        partition_name = partition[0].replace("'", "", -1)
        # Parse date in format '201805'
        partition_date = datetime.datetime.strptime(partition_name, "%Y%m")
        delta = now - partition_date
        if delta.days > max_days:
            partition_delete_command = "ALTER TABLE " + dbname + "." + table[0] + " DROP PARTITION '" + partition_name + "'"
            if is_dryrun:
                print partition_delete_command
            else:
                print partition_name, " will be removed"
                client.execute(partition_delete_command)
                print partition_name, " now removed"
