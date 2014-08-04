--------------------------------------------------------------------------------
VoltDB sqlcmd
--------------------------------------------------------------------------------

VoltDB sqlcmd is a command line tool for connecting to a VoltDB database
and invoking SQL statements and stored procedures interactively. sqlcmd
can be run interactively from the system command line (or shell) or as part
of a script.

The command line for running the sqlcmd utility is the following:

    $ ./sqlcmd  [options]  [command]

The options let you specify what database to connect to and how the output is
formatted. The allowable options are:

    --help or --usage
    --servers=comma_separated_server_list
    --port=port_number
    --user=user
    --password=password
    --output-format={fixed|csv|tab}
    --output-skip-metadata


If you specify a command when you invoke the utility, that command is executed
and the utility exits. If you do not specify a command, the utility enters
interactive mode and lets you specify commands at a command line prompt.
Valid commands are:

    {SQL statement}
    EXEC[UTE] {procedure-name} [parameters]
    EXPLAIN {sql statement}
    EXPLAINPROC {procedure-name}
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
    HELP
    LIST | SHOW CLASSES
    LIST | SHOW PROC[EDURES]
    LIST | SHOW TABLES
    RECALL [command-number]
    QUIT or EXIT

The following sections provide more detail on how to use the sqlcmd utility.

--------------------------------------------------------------------------------
Using VoltDB sqlcmd
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

+ You can press the Tab key at the start of a command to complete the command name. 
  Note that sqlcmd does not complete terms and database objects in the body of the 
  command.

+ To request execution, enter a semi-colon at the end of a statement, or enter the
  interactive command GO.


Executing Stored Procedures ----------------------------------------------------

+ You can run any stored procedures that are discovered, as well as the
  following system procedures:

  + @Explain varchar
  + @ExplainProc varchar
  + @Pause
  + @Promote
  + @Quiesce
  + @Resume
  + @Shutdown
  + @SnapshotDelete varchar, varchar
  + @SnapshotRestore varchar, varchar
  + @SnapshotSave varchar, varchar, bit
  + @SnapshotSave varchar
  + @SnapshotScan varchar
  + @SnapshotStatus
  + @Statistics StatisticsComponent, bit
  + @SystemInformation
  + @SystemCatalog CatalogComponent
  + @UpdateApplicationCatalog varchar, varchar
  + @UpdateClasses varchar, varchar
  + @UpdateLogging varchar

+ 'bit' values may be provided as either {true|yes|1} or {false|no|0}.

+ 'StatisticsComponent' values should be one of the following:
  + DR
  + INDEX
  + INITIATOR
  + IOSTATS
  + LIVECLIENTS
  + MANAGEMENT
  + MEMORY
  + PARTITIONCOUNT
  + PLANNER
  + PROCEDURE
  + TABLE

+ Specify null values with the word "null" (without quotation marks).

+ The syntax for executing a procedure is:

  exec(ute) <procedure-name> [<param-1> [,<param-2> [,param-n]]]

+ Samples from the "Voter" example application:
  + exec Results;
  + exec Vote 5055555555, 2, 10;

Explaining SQL Queries and VoltDB Stored Procedures -----------------------------

+ You can use the EXPLAIN command to display the execution plan for a
  given SQL statement.  The execution plan describes how VoltDB expects to execute
  the query at runtime, including what indexes are used, the order the tables are
  joined, and so on.  For example:
  
      explain select * from votes;
  
+ You can use the EXPLAINPROC command to return the execution plans for 
  all of the SQL queries within the specified stored procedure. For example:
  
      explainproc Vote;
  

Listing Stored Procedures -------------------------------------------------------

+ You can list available stored procedures, including both system procedures
  as well as any procedure you defined, using the following interactive command:

      list proc[edures]

+ You can list the Java classes in the currently running catalog, showing which
  ones are currently being used as stored procedures, which could potentially
  be used to create a new stored procedure, and all other supporting classes, 
  using the following interactive command:

      list classes

+ List operations are considered as interactive mode commands only, thus they
  do not require a terminating semi-colon and will execute immediately (even if
  you are in the middle of typing a batch - your batch is left unchanged and the
  command not tracked).

+ List commands are ignored in script files.

+ The command 'show' may be used instead of 'list'


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

+ You can also search and find matching entries in the history list by typing
  Control-R. This will search backward in the history for the next entry
  matching the search string typed so far. Hitting Enter will terminate the 
  search and accept the line, thereby executing the command from the history list.  
  Note that the last incremental search string is remembered. If two Control-R's 
  are typed without any intervening characters defining a new search string, the 
  remembered search string is used.

+ Recall commands are ignored in script files.


Quitting the application -------------------------------------------------------

+ While in interactive mode, type the commands EXIT or QUIT to return to the
  shell prompt.  You can also press Control-D on an empty line to return to
  the shell prompt.

+ In general, sqlcmd follows the bash shell emacs-style keyboard interface.


Running sqlcmd in non-interactive mode -----------------------------------------

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


Important note on standard output messages -------------------------------------

+ The VoltDB client library connection writes informational messages to
  standard output.  If you want to remove these extraneous messages from your
  sqlcmd output, you can use grep to filter the output when saving
  data to a file.  The following command shows how you can filter the output:

+ echo "exec Results" | ./sqlcmd --output-format=csv \
    --output-skip-metadata | grep -v "\[Volt Network\]" > VoterResults.csv


--------------------------------------------------------------------------------
Known Issues / Limitations
--------------------------------------------------------------------------------
+ sqlcmd executes SQL statements as ad-hoc queries using the @AdHoc system
  procedure. Ad hoc query execution will be slower than stored procedure execution
  and should not be used for any performance measurement or comparison.
  
+ For consistency with similar applications on different database engines,
  string output data is NOT quoted.  However, unquoted text can cause parsing
  issues when re-reading the data output in CSV or TAB format.
