#!/bin/sh -x
# This is just temporary.  Delete this before merging to master.
# this command should go into the makefile somehow.

BUILD=debug
VERBOSE=

java $VERBOSE -cp obj/$BUILD/prod:obj/$BUILD/test:lib/\*:third_party/java/jars/\* org.voltdb.planner.eegentests.EEPlanTestGenerator "$@"
