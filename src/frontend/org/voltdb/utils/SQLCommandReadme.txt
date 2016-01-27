--------------------------------------------------------------------------------
VoltDB sqlcmd
--------------------------------------------------------------------------------

VoltDB sqlcmd is a command line tool for connecting to a VoltDB database
and invoking SQL statements and stored procedures interactively. You can
run sqlcmd from the system command line (or shell) or as part of a script.

The command for running sqlcmd is the following:

    $ sqlcmd  [options]

Options let you specify what database to connect to and how the output is
formatted. The allowable options are:

    --help or --usage
    --servers=comma_separated_server_list
    --port=port_number
    --user=user
    --password=password
    --kerberos=jaas_login_configuration_entry_key
    --output-format={fixed|csv|tab}
    --output-skip-metadata
    --query={query}
    --stop-on-error={true|false}

In addition to SQL statements, sqlcmd lets you enter directives that provide
information, execute stored procedures, or batch process a file of sqlcmd
commands. Valid commands are:

    {SQL statement}
    EXEC[UTE] {procedure-name} [parameters]
    EXPLAIN {sql statement}
    EXPLAINPROC {procedure-name}
    FILE {file-name}
    LIST|SHOW CLASSES
    LIST|SHOW PROC[EDURES]
    LIST|SHOW TABLES
    LOAD CLASSES {jar-file}
    REMOVE CLASSES {class-specification}
    HELP
    QUIT|EXIT

Commands and SQL keywords (such as SELECT) are case insensitive. Each
command must begin on a separate line, although individual SQL queries can span
multiple lines. Use a semi-colon to terminate SQL commands and EXECUTE
directives.

The following sections provide more detail on how to use the sqlcmd utility.

--------------------------------------------------------------------------------
Using VoltDB sqlcmd
--------------------------------------------------------------------------------


Connecting to a VoltDB database ------------------------------------------------

+ By default, the utility connects to a VoltDB database on the localhost, using
  the standard port (21212).  You may override those settings through command
  line arguments, as well as provide specific user credentials to connect with.


Executing SQL Statements -------------------------------------------------------

+ To execute a SQL statement, simply enter any valid VoltDB SQL statement
  (select, insert, update, etc.) on the command line.

+ Individual SQL statements can span multiple lines. (A line continuation
  character is not required.)

+ Use a semi-colon at the end of every SQL statement.


Executing Stored Procedures ----------------------------------------------------

+ You can run stored procedures, including system procedures, using the EXEC
  directive. The syntax for executing a procedure is:

  exec[ute] <procedure-name> [ <parameter> [,...] ];

+ Samples from the "Voter" example application:
  + exec Results;
  + exec Vote 5055555555, 2, 10;

+ Sample system procedure:
  + exec @Statistics "memory" 0;

+ Binary or bit values may be provided as either 1 and 0 or true and false.

+ Specify null values with the word "null" (without quotation marks).


Explaining SQL Queries and VoltDB Stored Procedures ----------------------------

+ You can use the EXPLAIN command to display the execution plan for a given SQL
  statement.  The execution plan describes how VoltDB expects to execute the
  query at runtime, including what indexes are used, the order the tables are
  joined, and so on.  For example:

      explain select * from votes;

+ You can use the EXPLAINPROC command to return the execution plans for
  all of the SQL queries within the specified stored procedure. For example:

      explainproc Vote;


Listing Tables, Views, Stored Procedures, and Classes --------------------------

+ You can list objects in the database schema using the LIST or SHOW command.

+ Use the LIST TABLES command to list tables and views:

      LIST TABLES

+ Use the LIST PROCEDURES command to list available stored procedures, including
  both system procedures and user-defined stored procedures:

      LIST PROC[EDURES]

+ Use the LIST CLASSES command to list the Java classes loaded in the database:

      LIST CLASSES

+ SHOW is an alias for LIST.

Loading and removing classes ---------------------------------------------------

+ Use the LOAD CLASSES command to load classes for stored procedures. For
  example, the following command loads any classes in myapp.jar, adding or
  replacing class definitions as appropriate:

      LOAD CLASSES myapp.jar

  Loaded classes are available for use as stored procedures. For example, if
  myapp.jar has a class myapp.MyProc, the following DDL command will expose
  that class as the MyProc stored procedure:

      create procedure MyProc from class myapp.MyProc;

+ Use the REMOVE CLASSES command to remove any classes that match the specified
  class name string. The class specification can include wildcards. For example,
  the following command removes all classes that were loaded from myapp.jar:

      REMOVE CLASSES myapp.*

Recalling commands -------------------------------------------------------------

+ You can recall previous commands using the up and down arrow keys. Or you can
  recall a specific command by number (the command prompt shows the line number)
  using the RECALL command:

      recall {line-number}

  for example:

      recall 123

  Where 123 is the line number you wish to recall.

+ Once recalled, you can edit the command before reissuing it using typical
  editing keys, such as the left and right arrow keys, backspace and delete.

+ You can also search and find matching entries in the history list by typing
  Control-R. This will search backward in the history for the next entry
  matching the search string typed so far. Hitting Enter will terminate the
  search and accept the line, thereby executing the command from the history
  list. Note that the last incremental search string is remembered. If you type
  two CTRL-R's without any intervening characters defining a new search string,
  the previous search string is used.

+ In general, sqlcmd follows the bash shell emacs-style keyboard interface.


Quitting the application -------------------------------------------------------

+ From the command prompt, type the command EXIT or QUIT to return to the
  shell prompt.  You can also press Control-D to return to the shell prompt.


Running sqlcmd in non-interactive mode -----------------------------------------

+ You can run the command utility in non-interactive mode by piping in sql
  commands directly to the utility. For example:

   $ echo "exec Results" | sqlcmd
   $ sqlcmd < script.sql

+ The utility will run your SQL commands and exit immediately.

+ You can pipe the output to another command or an output file. For example:

  + echo "exec Results" | sqlcmd | grep "Edwina Burnam"
  + echo "exec Results" | sqlcmd > results.txt

+ Should any error occur during execution, messages are output to Standard
  Error so as not to corrupt the output data, while the application returns
  the environment exit code '-1'.

+ Interactive mode commands such as RECALL, QUIT, and EXIT should not be used
  in non-interactive mode.


Running script files -----------------------------------------------------------

+ You can run script files using the FILE command:

  FILE '/tmp/test.sql'

+ Quoting is optional and can be either single or double quotes.

+ FILE statements can be nested and are executed recursively: a script may
  contain one or several FILE statements. If the included file(s) perform a
  logical loop, the command utility will fail.

+ Interactive mode commands such as RECALL, QUIT, and EXIT should not be used
  in a script file.


Controlling output format ------------------------------------------------------

+ You can choose between 'fixed' (column-aligned), 'csv', and 'tab' formats for
  your result data by using the --output-format command line argument.

+ By default, columns header information, as well as affected row count is
  displayed in the output. When post-processing the file or piping it to a file,
  you can remove such metadata with the --output-skip-metadata command line
  argument.

+ For example, the following command returns CSV data with no header
  information:

     $ echo "exec Results" | sqlcmd --output-format=csv --output-skip-metadata


Important note on standard output messages -------------------------------------

+ The sqlcmd utility writes informational messages to standard output. If you
  want to remove these extraneous messages from your sqlcmd output, you can use
  grep to filter the output when saving data to a file. The following command
  shows how you can filter the output:

+ echo "exec Results" | sqlcmd --output-format=csv \
    --output-skip-metadata | grep -v "\[Volt Network\]" > VoterResults.csv


--------------------------------------------------------------------------------
Known Issues / Limitations
--------------------------------------------------------------------------------
+ For consistency with similar applications on different database engines,
  string output data is NOT quoted.  However, unquoted text can cause parsing
  issues when re-reading the data output in CSV or TAB format.
