About <voltdb>/tests/testfuncs/org/voltdb_testfuncs/alternative/testfuncs_alternative.jar
=========================================================================================

The testfuncs_alternative.jar file in this directory is used by sqlcmdtest scripts (in
the <voltdb>/tests/sqlcmd/scripts/udf/ directory) to test using the @UpdateClasses System
Procedure, to modify the definition of UDF's that have already been created, by modifying
the .jar file used to define them. It was created, and can be recreated, as follows:

1. In <voltdb>/tests/testfuncs/org/voltdb_testfuncs/UserDefinedTestFunctions.java, change
the value of USE_ALTERNATIVE_VERSION to true: this significantly changes the behavior of
the "add..." and "concat..." User-Defined Functions. (Don't forget to change it back to
false, when you're done.)

2. Starting from the <voltdb> directory:
ant compile
cd tests/testfuncs/org/voltdb_testfuncs/alternative
jar cvf testfuncs_alternative.jar -C ../../../../../obj/release/testfuncs org/voltdb_testfuncs/UserDefinedTestFunctions.class \
                                  -C ../../../../../obj/release/testfuncs org/voltdb_testfuncs/UserDefinedTestFunctions\$UserDefinedTestException.class \
                                  -C ../../../../../obj/release/testfuncs org/voltdb_testfuncs/UserDefinedTestFunctions\$UDF_TEST.class
