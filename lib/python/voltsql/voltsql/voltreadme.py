# This file is part of VoltDB.
# Copyright (C) 2008-2018 VoltDB Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.

# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice, this
#   list of conditions and the following disclaimer in the documentation and/or
#   other materials provided with the distribution.
#
# * Neither the name of the {organization} nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

README = """
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
-  `update` refresh the schema


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
"""
