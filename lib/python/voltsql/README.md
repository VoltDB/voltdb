voltcli
=======

Install Requirements
----------------

- We require Python 2.6+, we don't support Python 3 currently.

- Open a terminal, go to the project folder, and run this command to install all project dependencies:

    ```
    $ pip install -r requirements.txt
    ```
    
- Or if you want to install the dependencies offline, you can use

    ```
    $ pip install -r requirements_offline.txt
    ```
      
Run
---
In project root folder, simply run:
```
$ python voltsql/voltsql.py
```

Or
```
$ ./voltsql.sh
```
Or in the voltdb/bin folder, run:
```
voltsql
```

Test
----
```
$ tox
```

Architecture
------------
```text
+-----------------------------------+
|    _    __      ____  ____  ____  |	
|   | |  / /___  / / /_/ __ \/ __ ) |
|   | | / / __ \/ / __/ / / / __  | |
|   | |/ / /_/ / / /_/ /_/ / /_/ /  |
|   |___/\____/_/\__/_____/_____/   |
+-----------------------------------+
           |         ^
           v         | 
+---------------------------------------------+ 
|        SQLCMD                               |
|     ======	                              |
|   We still use the SQLCMD to execute SQLs,  |
|   and get response from its stdout.         |
|   It's like our "server"                    |
+---------------------------------------------+
           |         ^
           v         |
+---------------------------------------------------------------+
|     VoltSQL                                                   |
|     =======                                                   |
|          - Build interactive command line based on Python     |
|            Prompt Toolkit.                                    |
|          - Get the database information from @SystemCatalog   |
|          - Parse the buffer while typing, so that we can      |
|            generate context-sensitive suggestions based on    |
|            current cursor's postion                           |
|                                                               |
|            +-----------------------------------------------+  |
|            | Completer, SQLParser, refresher, Executor     |  |
|            | =========================================     |  |
|            |                                               |  |
|            +-----------------------------------------------+  |
+---------------------------------------------------------------+
```
