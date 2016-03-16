# VoltDB Fantasy Sports Example App

Use Case
--------

Fantasy sports can involve many contests with a few participants, as well as some very large contests with hundreds of thousands or even millions of participants.  Updating scores and ranking the contests efficiently as the player stats are changing during the game can be a challenge.  





Code organization
-----------------
The code is divided into projects:

- "db": the database project, which contains the schema, stored procedures and other configurations that are compiled into a catalog and run in a VoltDB database.  
- "client": a java client that generates work

See below for instructions on running these applications.  For any questions, 
please contact bballard@voltdb.com.

Pre-requisites
--------------

Before running these scripts you need to have VoltDB 5.0 or later installed.  If you choose the .tar.gz file distribution, simply untar it to a directory such as your $HOME directory, then add the bin subdirectory to your PATH environment variable.  For example:

    export PATH="$PATH:$HOME/voltdb-ent-5.0.2/bin"

You may choose to add this to your .bashrc file.

If you installed the .deb or .rpm distribution, the binaries should already be in your PATH.  To verify this, the following command should return a version number:

    voltdb --version

Instructions
------------

1. Start the database 

    cd db  
    voltdb create -d deployment-demo.xml  

2. Load the schema

    cd db  
    ./compile_procs.sh  
    sqlcmd < ddl.sql  

3. Run the client

    cd client  
    ./run_client.sh  

    
4. Stop the database

    voltadmin shutdown  
   


 