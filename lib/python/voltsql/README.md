voltcli
=======

Install Requirements
----------------

- We require Python 2.6+, we don't support Python 3 currently.

- Install pip.

- Open a terminal, go to the project folder, and run this command to install all project dependencies:

    ```bash
    $ pip install -r requirements.txt
    ```
    
- Or if you want to install the dependencies offline, you can use

    ```bash
    $ pip install -r requirements_offline.txt
    ```
      
Run
---
In project root folder, simply run:
```bash
$ python voltsql/voltsql.py
```

Or
```bash
$ ./voltsql.sh
```
Or in the voltdb/bin folder, run:
```bash
voltsql
```

Test
----
- First you have to install requirements for test

    ```bash
    $ pip install -r requirements_test.txt
    ```
- Then you can running tests using tox

    ```bash
    $ tox
    ```

Options
-----
- smart completion

    If it's on, voltsql will read from voltdb catalog. It will enable voltsql to suggest the table name, column name and udf function.
    
    If it's off, voltsql will only suggest keywords.
    
- multiline

    If it's on, press enter key will create a newline instead of execute the statements. To execute the statements, you have to press Meta+Enter (Or Escape followed by Enter).
    
    If it's off, press enter will execute the statements.
    
- auto refresh

    If it's on, voltsql will fetch the voltdb catalog everytime you execute a statement from voltsql. 
    
    If it's off, voltsql will only fetch catalog one time when you start voltsql.
    
    Despite the option, you can always force an refresh using the command
    
    ```
    update
    ```
    
Commands
-------
`quit`: quit voltsql

`update` force a background catalog refresh


    

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
