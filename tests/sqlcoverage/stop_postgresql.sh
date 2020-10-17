#!/bin/bash

# This script stops a PostgreSQL server. It assumes that you are running on one
# of the Jenkins machines, as USER 'test'; failing that, it may not work, so
# user beware. Typical use would be to run the start_postgresql.sh script first,
# then a set of SqlCoverage tests, and then this script. Note that both PG_PATH
# and PG_TMP_DIR must be set before running this, which they will be, if
# start_postgresql.sh was run first, using 'source' or '.'.
echo -e "\nRunning ${BASH_SOURCE[0]} ..."

# Stop the PostgreSQL server & delete the temp directory, but save the log file(s)
echo -e "PostgreSQL processes (before shutdown):"
ps -ef | grep -i postgres
echo -e "PostgreSQL shutdown:"
$PG_PATH/pg_ctl stop -w -D $PG_TMP_DIR/data
code1=$?
echo -e "PostgreSQL processes (after shutdown):"
ps -ef | grep -i postgres

echo -e "\nPostgreSQL log file (ls; cat; mv):"
ls -l $PG_TMP_DIR/postgres.log
read NUM_LINES __ <<< `wc $PG_TMP_DIR/postgres.log`
if [[ "$NUM_LINES" -le 50 ]]; then
    cat  $PG_TMP_DIR/postgres.log
else
    head -n 20 $PG_TMP_DIR/postgres.log
    echo "... [long log file truncated] ..."
    tail -n 20 $PG_TMP_DIR/postgres.log
fi
# On some (CentOS?) machines the "real" log is in a different directory, per
# this message in the regular log file:
# ...>LOG:  redirecting log output to logging collector process
# ...>HINT:  Future log output will appear in directory "pg_log".
if ls $PG_TMP_DIR/data/pg_log/postgres*.log 1> /dev/null 2>&1; then
    mv $PG_TMP_DIR/data/pg_log/postgres*.log .
else
    mv $PG_TMP_DIR/postgres.log .
fi

echo -e "\nRemove the TMP directory ($PG_TMP_DIR):"
rm -r $PG_TMP_DIR
code2=$?

code=$(($code1|$code2))
echo "code1 code2:" $code1 $code2
echo "code:" $code
exit $code
