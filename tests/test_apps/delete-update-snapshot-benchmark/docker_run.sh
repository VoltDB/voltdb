#/bin/sh
# this file is used as an entry point inside a docker container
#
# At the least, workload params and sqlcmd params must include --servers=foo
#
# Future: support DR
#
: ${SQLCMD_PARAMS:=""}
: ${WORKLOAD_PARAMS:=""}
: ${DDLONLY:=""}

[ -z "$SQLCMD_PARAMS" ] && echo "ERROR: must set SQLCMD_PARAMS environment variable" >& 2 && exit 1
[ -z "$WORKLOAD_PARAMS" ] && echo "ERROR: must set WORKLOAD_PARAMS environment variable" >& 2 && exit 1

VJAR=$(ls -1t t voltdb/voltdb/voltdbclient-*.jar | head -1)
APPCLASSPATH=$({ \
    \ls -1 voltdb/lib/*.jar; \
    \ls -1 voltdb/lib/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )

# =============================================================================

set -x
voltdb/bin/sqlcmd "$SQLCMD_PARAMS" --query="select ID from DUSB_R1 limit 1"

if [ $? != 0 ]; then
    echo "loading ddl"
    voltdb/bin/sqlcmd "$SQLCMD_PARAMS" < ddl.sql
else
    voltdb/bin/sqlcmd "$SQLCMD_PARAMS" --query="truncate table DUSB_R1; truncate table DUSB_P1; truncate table DUSB_P2; truncate table DUSB_P3"
fi

if [ "$DDLONLY" != "" ]; then
    exit 0
fi

JAVA=`which java`
CLASSPATH="dusbench.jar"
$JAVA -classpath dusbench.jar:${VJAR}:${APPCLASSPATH}  \
      client.benchmark.DUSBenchmark \
      $WORKLOAD_PARAMS
