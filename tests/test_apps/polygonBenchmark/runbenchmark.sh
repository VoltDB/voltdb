#!/bin/sh

./run.sh jars
sqlcmd < ddl.sql
CHECKED_OR_NOTCHECKED=unchecked
NUM_VERTICES=750

for rf in 0.0 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 1.0; do
    NUM_VERTICES="$NUM_VERTICES" REPAIR_FRAC="$rf" CHECKED_OR_NOTCHECKED="$CHECKED_OR_NOTCHECKED" ./run.sh client
done
