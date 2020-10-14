#!/bin/bash

# This script starts a PostgreSQL server. It mostly assumes that you are running
# on one of the Jenkins machines (e.g., by setting the CLASSPATH to a jar file
# in a specific directory that exists on those machines), as USER 'test'; failing
# that, it may not work, so user beware. Typical use would be to run this script
# first (using 'source' or '.', so that variable names are exported to the shell),
# then a set of SqlCoverage tests, and then the stop_postgresql.sh script.
echo -e "\nRunning ${BASH_SOURCE[0]} ..."

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
echo -e "PostgreSQL processes, if any (before killing them):"
ps -ef | grep -i postgres
sudo pkill $IGNORE_CASE postgres
echo -e "PostgreSQL processes, if any (after killing them):"
ps -ef | grep -i postgres

# Function used to return an error code, if an error occurs
function error-code() {
    # Use a specified exit code, or a default value
    EXIT_CODE="${1:-99}"

    #echo -e "DEBUG: In error-code:"
    #echo -e "DEBUG:     \$1            : $1"
    #echo -e "DEBUG:     EXIT_CODE     : $EXIT_CODE"
    #echo -e "DEBUG:     \$0            : $0"
    #echo -e "DEBUG:     BASH_SOURCE[0]: ${BASH_SOURCE[0]}"

    # If this script was called "normally", then 'exit' with the appropriate
    # error code; but if it was called using 'source' or '.', then simply
    # 'return' with that code, in order to avoid also exiting the shell
    echo -e "\nExiting ${BASH_SOURCE[0]} with error code: $EXIT_CODE"
    if [[ "$0" == "${BASH_SOURCE[0]}" ]]; then
        exit $EXIT_CODE
    fi
    return $EXIT_CODE
}

# Prepare to start the PostgreSQL server, in a new temp dir
export PG_TMP_DIR=$(mktemp -d $MKTEMP_TEMPLATE)
echo -e "\nPG_TMP_DIR:" $PG_TMP_DIR
export PG_PATH=$(locate pg_restore | grep /bin | grep -v /usr/bin | tail -1 | xargs dirname)
if [[ -z "${PG_PATH}" ]]; then
    echo -e "\nERROR: Failed to find PG_PATH:"
    echo "    locate pg_restore         : "`locate pg_restore`
    echo "    ...grep /bin; -v /usr/bin : "`locate pg_restore | grep /bin | grep -v /usr/bin`
    echo "    ...tail -1 | xargs dirname: "`locate pg_restore | grep /bin | grep -v /usr/bin | tail -1 | xargs dirname`
    error-code 11
    return
fi
echo  "PG_PATH:" $PG_PATH
export PG_PORT=5432
echo  "PG_PORT:" $PG_PORT

# Use the same version of the PostgreSQL JDBC jar, on all platforms
export CLASSPATH=/home/test/jdbc/postgresql-9.4.1207.jar
echo  "CLASSPATH:" $CLASSPATH

# Start the PostgreSQL server, in the new temp directory
# Note: '--lc-collate=C' causes VARCHAR sorting to match VoltDB's
echo -e "\nPostgreSQL startup:"
$PG_PATH/initdb -D $PG_TMP_DIR/data --auth=trust --auth-host=trust --auth-local=trust --encoding='UTF-8' --lc-collate=C
echo -e "unix_socket_directories='.'\nlisten_addresses='*'\nport=$PG_PORT" >> $PG_TMP_DIR/data/postgresql.conf
$PG_PATH/pg_ctl start -w -D $PG_TMP_DIR/data -l $PG_TMP_DIR/postgres.log

# Print info about PostgreSQL processes, to make sure they are working OK
echo -e "\nPostgreSQL processes (after startup):"
ps -ef | grep -i postgres
eval $SHOW_LISTENING_PORTS

echo -e "\nEnvironment variables that were set:"
echo "( To echo these in your shell, outside this script, use:"
echo "  echo -e \"PG_TMP_DIR: \$PG_TMP_DIR\nPG_PATH   : \$PG_PATH\nPG_PORT   : \$PG_PORT\nCLASSPATH : \$CLASSPATH\" )"
echo "PG_TMP_DIR: $PG_TMP_DIR"
echo "PG_PATH   : $PG_PATH"
echo "PG_PORT   : $PG_PORT"
echo -e "CLASSPATH : $CLASSPATH\n"

# If this script was not called using 'source' or '.', then warn the user
if [[ "$0" == "${BASH_SOURCE[0]}" ]]; then
    echo -e "WARNING: ${BASH_SOURCE[0]} was not called using 'source' or '.', so the " \
            "environment variables were not set the in enclosing shell; to set them, use:" \
            "\nexport PG_TMP_DIR=$PG_TMP_DIR\nexport PG_PATH=$PG_PATH" \
            "\nexport PG_PORT=$PG_PORT\nexport CLASSPATH=$CLASSPATH"
    error-code 12
    return
fi
