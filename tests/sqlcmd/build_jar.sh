#!/usr/bin/env bash
#
# Compiles the InsertEmployee stored procedure, and creates a jar file containing it
# (which is used to test the LOAD CLASSES and REMOVE CLASSES commands).

# compile java source
javac -classpath ../../obj/release/prod procedures/sqlcmdtest/*.java

# build the jar file
jar cf sqlcmdtest-procs.jar -C procedures sqlcmdtest

# removed compiled .class file(s)
rm -rf procedures/sqlcmdtest/*.class
