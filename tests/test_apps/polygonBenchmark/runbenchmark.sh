#!/bin/sh

NUM_VERTICES=750
LOG_FILE="log.out"
CSV_FILE="polygonBenchmark.csv"
DURATION=30
./run.sh jars
sqlcmd < ddl.sql

cat /dev/null > "$LOGFILE"
rm -rf "$CSV_FILE"

for function in polygonfromtext validpolygonfromtest; do
    for rf in 0.0 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 1.0; do
        LOG_FILE="$LOG_FILE" NUM_VERTICES="$NUM_VERTICES" REPAIR_FRAC="$rf" DURATION="$DURATION" INSERT_FUNCTION="$function" ./run.sh client
    done
done
