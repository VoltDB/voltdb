--------------------------------------------------------------------------------
VoltDB SQLCommand
--------------------------------------------------------------------------------

VoltDB SQLCommand is a command line tool for connecting to a VoltDB database
and invoking SQL statements and stored procedures interactively. SQLCommand
can be run interactively from the system command line (or shell) or as part
of a script.

The command line for running the SQLCommand utility is the following:

    (linux)               $ ./sqlcmd  [options]  [command]
    (Microsoft Windows)   > sqlcmd.cmd  [options]  [command]

The options let you specify what database to connect to and how the output is
formatted. The allowable options are:

    --help or --usage
    --servers=comma_separated_server_list
    --port=port_number
    --user=user
    --password=password
    --output-format=(fixed|csv|tab)
     --output-skip-metadata


If you specify a command when you invoke the utility, that command is executed
and the utility exits. If you do not specify a command, the utility enters
interactive mode and lets you specify commands at a command line prompt.
Valid commands are:

    {SQL statement}
    EXEC[UTE] {procedure-name} [parameters]
    FILE {file-name}

Command and SQL keywords (such as SELECT) are case insensitive.
Commands can span multiple lines and multiple commands can be given before
execution. Use the semi-colon to complete a set of commands and request
execution.

In addition to database commands, when running the utility interactively (rather
than as part of a script) there are a set of interactive commands that you
can use to get information and control the tool itself. These commands are
executed immediately (they do not require a semi-colon). The interactive
commands are:

    GO
    LIST PROC[EDURES]
    RECALL [command-number]
    QUIT or EXIT

The following sections provide more detail on how to use the SQLCommand utility.


--------------------------------------------------------------------------------
System Requirements
--------------------------------------------------------------------------------
VoltDB SQLCommand is a Java application that requires a Java runtime
environment such as OpenJDK.

As installed, the command script assumes that the VoltDB client library is
located in the relative path: ../../voltdb/*.jar  If you move the script or if
your deployment be different, you must update the command script.

--------------------------------------------------------------------------------
Using VoltDB SQLCommand
--------------------------------------------------------------------------------


Connecting to a VoltDB database ------------------------------------------------

+ By default, the utility connects to a VoltDB database on the localhost, using
  the standard port (21212).  You may override those settings through command
  line arguments, as well as provide specific user credentials to connect with.

+ For more information, run the utility with the --usage or --help command
  line parameter.


Executing SQL Statements -------------------------------------------------------

+ To execute a SQL statement, simply enter any valid VoltDB SQL statement (select,
  insert, update, delete) on the command line.

+ Individual SQL statements can span multiple lines. (A line continuation character
  is not required.)

+ You may enter more than one statement before requesting execution.

+ To request execution, enter a semi-colon at the end of a statement, or enter the
  interactive command GO.


Executing Stored Procedures ------------------------------------------------------

+ You can run any stored procedures that are discovered, as well as the
  following system procedures:

  + @Pause
  + @Quiesce
  + @Resume
  + @Shutdown
  + @SnapshotDelete varchar, varchar
  + @SnapshotRestore varchar, varchar
  + @SnapshotSave varchar, varchar, bit
  + @SnapshotScan varchar
  + @Statistics StatisticsComponent, bit
  + @SystemInformation
  + @UpdateApplicationCatalog varchar, varchar
  + @UpdateLogging varchar

+ 'bit' values may be provided as either {true|yes|1} or {false|no|0}.

+ 'StatisticsComponent' values should be one of the following:
  + INDEX
  + INITIATOR
  + IOSTATS
  + MANAGEMENT
  + MEMORY
  + PROCEDURE
  + TABLE
  + PARTITIONCOUNT
  + STARVATION
  + LIVECLIENTS

+ Specify null values with the word "null" (without quotation marks).

+ The syntax for executing a procedure is:

  exec(ute) <procedure-name> [<param-1> [,<param-2> [,param-n]]]

+ Samples from the "Voter" example application:
  + exec Results;
  + exec Vote 5055555555, 2, 10;


Listing Stored Procedures -------------------------------------------------------

+ You can list available stored procedures, including both system procedures
  as well as any procedure you defined, using the following interactive command:

  list proc(edures)

+ List operations are considered as interactive mode commands only, thus they
  do not require a terminating semi-colon and will execute immediately (even if
  you are in the middle of typing a batch - your batch is left unchanged and the
  command not tracked).

+ List commands are ignored in script files.


Recalling commands -------------------------------------------------------------

+ You can recall past commands using the following syntax:

      recall {line-number}

  for example:

      recall 123

  Where 123 is the line number you wish to recall.

+ The recall operation allows you to reenter the command at a given line number
  as-is (with or without the semi-colon, etc.), allowing you to recall a line of
  script from anywhere into your current command sequence.  Should the line you
  recall contain a semi-colon, it will be executed immediately (as it normally
  would have).

+ Recall operations are considered as interactive mode commands only, thus they
  do not require a terminating semi-colon: the requested command is merely
  recalled and can be edited before submission to the tool.

+ Recall commands are ignored in script files.


Quitting the application -------------------------------------------------------

+ While in interactive mode, type the commands EXIT or QUIT to return to the
  shell prompt.


Running SQLCommand in non-interactive mode ------------------------------------------------

+ You can run the command utility in non-interactive mode by piping in sql
  commands directly to the utility. For example:

  + echo "exec Results" | ./sqlcmd
  + ./sqlcmd < script.sql

+ The utility will run your SQL commands and exit immediately.

+ You can pipe the output to another command or an output file. For example:

  + echo "exec Results" | ./sqlcmd | grep "Edwina Burnam"
  + echo "exec Results" | ./sqlcmd > results.txt

+ Should any error occur during execution, messages are output to Standard
  Error so as not to corrupt the output data, while the application returns
  the environment exit code '-1'.

+ Interactive mode commands such as RECALL, GO, QUIT, EXIT are ignored while
  running in non-interactive mode.


Running script files -----------------------------------------------------------

+ You can run script files stored on your machine using the FILE command:

  file '/tmp/test.sql';

+ Quoting is optional and can be either single or double quotes.

+ In interactive mode, terminating the command with a semi-colon will
  immediately close the current batch and execute all pending statements.

+ FILE statements can be nested and are executed recursively: a script may
  contain one or several FILE statements. If the included file(s) perform a
  logical loop, the command utility will fail.

+ Interactive mode commands such as RECALL, GO, QUIT, EXIT are ignored when
  found in a script file.


Controlling output format ------------------------------------------------------

+ You can choose between 'fixed' (column-aligned), 'csv', and 'tab' formats for
  your result data by using the --output-format command line argument.

+ By default, columns header information, as well as affected row count is
  displayed in the output.  When post-processing the file or piping it to a file,
  you can remove such metadata with the command line argument
  --output-skip-metadata.

+ The following statement shows how to return CSV data, with no header
  information:

     echo "exec Results" | ./sqlcmd --output-format=csv \
    --output-skip-metadata


Important note on standard output messages ------------------------------------------------

+ The VoltDB client library connection writes informational messages to
  standard output.  If you want to remove these extraneous messages from your
  SQLCommand output, you can use grep to filter the output when saving
  data to a file.  The following command shows how you can filter the output:

  + echo "exec Results" | ./sqlcmd --output-format=csv \
    --output-skip-metadata | grep -v "\[Volt Network\]" > VoterResults.csv


--------------------------------------------------------------------------------
Known Issues / Limitations
--------------------------------------------------------------------------------
+ SQLCOmmand executes SQL statements as ad-hoc queries using the @AdHoc system
  procedure. Ad hoc queries are always executed as multi-partition procedures
  and constrain how much data can be selected, aggregated, sorted, and returned.
  If any of these constraints are violated, the query will fail and produce
  error text in the output.

+ For consistency with similar applications on different database engines,
  string output data is NOT quoted.  However, unquoted text can cause parsing
  issues when re-reading the data output in CSV or TAB format.
