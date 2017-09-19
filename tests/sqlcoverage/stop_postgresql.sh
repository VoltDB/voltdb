#!/bin/bash

# This script stops a PostgreSQL server. It assumes that you are running on one
# of the Jenkins machines, as USER 'test'; failing that, it may not work, so
# user beware. Typical use would be to run the start_postgresql.sh script first,
# then a set of SqlCoverage tests, and then this script. Note that both PG_PATH
# and PG_TMP_DIR must be set before running this, which they will be, if
# start_postgresql.sh was run first, using 'source' or '.'.

# Stop the PostgreSQL server & delete the temp directory, but save the log file(s)
ps -ef | grep -i postgres
$PG_PATH/pg_ctl stop -w -D $PG_TMP_DIR/data
code1=$?
ls -l $PG_TMP_DIR/postgres.log
cat   $PG_TMP_DIR/postgres.log
mv    $PG_TMP_DIR/postgres.log .
# On some (CentOS?) machines the "real" log is in a different directory, per
# this message in the regular log file:
# ...>LOG:  redirecting log output to logging collector process
# ...>HINT:  Future log output will appear in directory "pg_log".
mv    $PG_TMP_DIR/data/pg_log/postgres*.log .
rm -r $PG_TMP_DIR
code2=$?

code=$(($code1|$code2))
echo "code1 code2:" $code1 $code2
echo "code:" $code
exit $code
