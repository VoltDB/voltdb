Windowing Example Application
=============================

Overview
--------

App that does four simultaneous things on a single-table schema:

 - Insert random, timestamped tuples at a high rate.
 - Continuously delete tuples that are either too old or over a table size limit.
 - Check for changes in the maximum value stored in the table.
 - Periodically compute an average of values over various time windows.

It does this by creating task-focused classes that implement Runnable.
Each class has a specific job and is scheduled to run periodically in a
threadpool. All inter-task communication is done via the main instance of
this class.

run.sh Actions
--------------

run.sh               : compile all Java clients and stored procedures, build the catalog, and start the server

run.sh srccompile    : compile all Java clients and stored procedures

run.sh server        : start the server

run.sh client        : start the client

run.sh catalog       : build the catalog

run.sh clean         : remove compiled files and artifacts
