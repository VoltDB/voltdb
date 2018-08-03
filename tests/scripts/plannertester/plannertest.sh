#!/usr/bin/env bash

# Command line options (from https://wiki.voltdb.com/display/internal/Planner+Tester)
# -h -help
#
# Get the following usage information.
#
# -C='configDir1[,configDir2,...]'
# Specify the path to each config file.
#
# -sp=savePath'
# Specify the path for generated temporary plan files
#
# -r=reportFileDir
# Specify report file path, default will be ./reports, report file name is plannerTester.report.
#
# -i=ignorePattern
# Specify a pattern to ignore, the pattern will not be recorded in the report file.
#
# -s
# Compile queries and save the results as a baseline.
#
# -d
# Do the diff between generated plans and those in the baseline directory
#
# -re
# Report explained plans when there is a diff.
#
# -rs
# Report the sql statements when there is a diff.
#
# -dv
# Same as putting -d -re -rs.
#
# -sv
# Same as putting -s -re -rs.
# For example, run:
# $ ant && ./plannertest.sh -C=ENG-xxxx -sv

export VOLTDB_HOME=../../..

VOLTDBJAR=`ls $VOLTDB_HOME/voltdb/voltdb-[2-9].*.jar | grep -v "doc.jar" | head -1`
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
for f in $VOLTDB_HOME/third_party/java/jars/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

java -Xmx512m -classpath $TEST_HOME:${CLASSPATH} org.voltdb.planner.plannerTester "$@"

