#!/usr/bin/env bash

export VOLTDB_HOME=../../..


VOLTDBJAR=`ls $VOLTDB_HOME/voltdb/voltdb-2.*.jar | grep -v "doc.jar" | head -1`
if [ -n "${VOLTDBJAR}" ]; then
  CLASSPATH=$VOLTDBJAR
else
  echo "Couldn't find compiled VoltDB jar to run."
  exit 1
fi

TEST_HOME=$VOLTDB_HOME/obj/release/test

# add libs to CLASSPATH
for f in $VOLTDB_HOME/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

java -Xmx512m -classpath $TEST_HOME:${CLASSPATH} org.voltdb.planner.plannerTester "$@"

#exec $CMDVAL
