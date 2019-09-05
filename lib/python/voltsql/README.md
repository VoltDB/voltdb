voltsql
=======

Installation Requirements
-------------------------

voltsql requires Python 2.6+. It does not currently support Python 3. It also requires additional libraries that must be installed, as listed in the `requirements.txt` file. You can either install the requirements system wide, or you can use virtualenv to create a private python environment.

Before using voltsql, install the necessary libraries as follows:

  1. Install pip.

  2. Optionally, install and activate virtualenv. (See notes below.)

  3. Open a terminal session and install all project dependencies. When installing system-wide, use `sudo`. For example if VoltDB is install in the `voltdb` folder in your home directory:

    ```bash
    $ sudo pip install -r ~/voltdb/lib/python/voltsql/requirements.txt
    ```
    
   Or if you want to install the dependencies offline, you can use:

    ```bash
    $ sudo pip install -r ~/voltdb/lib/python/voltsql/requirements_offline.txt
    ```
    
Note that on Macintosh OS X, an earlier version of the click library is pre-installed and cannot be updated system wide without deleting the current version first. You can use `sudo` to do this, or avoid this issue by installing in a private environment using `virtualenv`.
      
Running voltsql
----------------
Make sure the `bin` folder from the VoltDB installation is in your PATH environment, then you can use the `voltsql` command like all other VoltDB utilities:

    ```bash
    $ voltsql
    ```


Commands
-----
Once voltsql is running, you can enter SQL statements interactively. As you type, voltsql lists available SQL keywords and function names for you. Use the up and down arrow keys to select the desired keyword, then continue typing.

You can also use standard directives from sqlcmd to examine tables and procedures, list classes, and so on. Available commands are:

-  `exit` or `quit`: quit voltsql
-  `examine`: View the execution plan for a statement
-  `exec`: execute a stored procedure
-  `help`: display help text
-  `show` or `list`: List tables, procedures, or classes
-  `refresh` refresh the schema


Options
-----
There are three interactive features that you can turn on and off with function keys:

- **smart completion**

    When on, _smart completion__ lists schema elements such as table and column names where appropriate along with SQL keywords. Press F2 to turn smart completion on and off.
    
- **multiline**

    When on, _multiline__ lets you enter statements on multiples lines before processing the statement or command. Press ESC and then ENTER to execute the statement. When off, each line is processed when you press ENTER. Press F3 to turn multiline on and off.

- **auto refresh**

    When on, -auto refresh_ refreshes the schema after each statement is processed. When off, you must refresh the schema manually with the UPDATE command if the schema changes.
    
    Leaving auto refresh on ensures you have the latest schema available for smart completion. Turning it off can save time if the schema does not change very often.
    
    Press F4 to turn auto refresh on and off.
    
    
Notes
-----
The command history is saved in the file `~/.voltsql_history`, which voltsql uses for recalling previous commands as well as calculating the prioritization of keywords.