#!/usr/bin/env bash
#
# Compiles the InsertEmployee stored procedure, and creates a jar file containing it
# (which is used to test the LOAD CLASSES and REMOVE CLASSES commands).
# Also adds other files to the jar file that are used to test handling of
# class loader errors.

# compile java source
javac -classpath ../../obj/release/prod procedures/sqlcmdtest/*.java

# build the jar file
jar cf sqlcmdtest-procs.jar -C procedures sqlcmdtest

# sabotage some dependency classes to test handling of
# secondary class loader errors
rm procedures/sqlcmdtest/*Sabotaged*.class

# build the sabotaged jar file
jar cf sqlcmdtest-sabotaged-procs.jar -C procedures sqlcmdtest

# Further sabotage some dependency classes to test handling of
# immediate class loader errors triggered by static dependencies.
rm procedures/sqlcmdtest/*Killed*.class

# build the sabotaged jar file
jar cf sqlcmdtest-killed-procs.jar -C procedures sqlcmdtest

# removed compiled .class file(s)
rm -rf procedures/sqlcmdtest/*.class
