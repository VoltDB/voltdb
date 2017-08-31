#!/bin/bash

# This script starts a PostgreSQL server. It mostly assumes that you are running
# on one of the Jenkins machines (e.g., by setting the CLASSPATH to a jar file
# in a specific directory that exists on those machines), as USER 'test'; failing
# that, it may not work, so user beware. Typical use would be to run this script
# first (using 'source' or '.', so that variable names are exported to the shell),
# then a set of SqlCoverage tests, and then the stop_postgresql.sh script.

# Code so that this also works on a Mac (OSTYPE darwin*), assuming that PostgreSQL
# is installed, the CLASSPATH has been set to include an appropriate PostgreSQL
# JDBC jar file, and the 'locate' command has been initialized
IGNORE_CASE=
MKTEMP_TEMPLATE=
SHOW_LISTENING_PORTS='sudo netstat -ltnp | grep postgres'
if [[ "$OSTYPE" == darwin* ]]; then
    IGNORE_CASE=-i
    MKTEMP_TEMPLATE=/tmp/tmp.XXXXXXXXXX
    SHOW_LISTENING_PORTS='sudo lsof -n -u postgres | grep LISTEN'
fi

# Check for & kill any postgres processes, in case there is an old one leftover
ps -ef | grep -i postgres
sudo pkill $IGNORE_CASE postgres
ps -ef | grep -i postgres

# Prepare to start the PostgreSQL server, in a new temp dir
export PG_TMP_DIR=$(mktemp -d $MKTEMP_TEMPLATE)
echo  "PG_TMP_DIR:" $PG_TMP_DIR
export PG_PATH=$(locate pg_restore | grep /bin | grep -v /usr/bin | tail -1 | xargs dirname)
echo  "PG_PATH:" $PG_PATH
export PG_PORT=5432
echo  "PG_PORT:" $PG_PORT

# Use the same version of the PostgreSQL JDBC jar, on all platforms
export CLASSPATH=/home/test/jdbc/postgresql-9.4.1207.jar
echo  "CLASSPATH:" $CLASSPATH

# Start the PostgreSQL server, in the new temp directory
# Note: '--lc-collate=C' causes VARCHAR sorting to match VoltDB's
$PG_PATH/initdb -D $PG_TMP_DIR/data --auth=trust --auth-host=trust --auth-local=trust --lc-collate=C
echo -e "unix_socket_directories='.'\nlisten_addresses='*'\nport=$PG_PORT" >> $PG_TMP_DIR/data/postgresql.conf
$PG_PATH/pg_ctl start -w -D $PG_TMP_DIR/data -l $PG_TMP_DIR/postgres.log

# Print info about PostgreSQL processes, to make sure they are working OK
ps -ef | grep -i postgres
eval $SHOW_LISTENING_PORTS
