=======================================
 Welcome to the VoltDB Velocity Bridge
=======================================

Strap yourself in and prepare for acceleration!

Configuration
-------------

       Source type: ${source_type}
 Connection string: ${connection_string}
 Schema (DDL) file: ${ddl_file}
   Deployment file: ${deployment_file}
      Package name: ${package}
Partitioning table: ${partition_table}
        Run script: ${run_script}

What was generated
------------------

- Schema file (${ddl_file}):
  Provides the DDL commands to initialize the VoltDB database schema.

- Deployment file (${deployment_file}):
  XML specification of the VoltDB database topology and other options.

- Run script (${run_script}):
  Bash script command line interface to running your client and server.

Using the generated files
-------------------------

- Running the server:
  ./run.sh server

- Running the client:
  ./run.sh client
