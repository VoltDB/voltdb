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
voltcli
=======

Install Requirements
----------------

- We require Python 2.6+, we don't support Python 3 currently.

- Install pip.

- Open a terminal, go to the project folder, and run this command to install all project dependencies:

    ```bash
    $ sudo pip install -r requirements.txt
    ```

- Or if you want to install the dependencies offline, you can use

    ```bash
    $ sudo pip install -r requirements_offline.txt
    ```

Run
---
Goto the `voltdb/bin` folder, run:
```bash
voltsql
```

Or you can add `voltdb/bin` to PATH, then you can directly run `voltsql` from terminal.

Test
----
- First you have to install requirements for test

    ```bash
    $ sudo pip install -r requirements_test.txt
    ```
- Then you can running tests using tox

    ```bash
    $ tox
    ```

Options
-----
- **smart completion**

    If it's on, voltsql will read from voltdb catalog. It will enable voltsql to suggest the table name, column name and udf function.

    If it's off, voltsql will only suggest keywords.

- **multiline**

    If it's on, press enter key will create a newline instead of execute the statements. To execute the statements, you have to press Meta+Enter (Or Escape followed by Enter).

    If it's off, press enter will execute the statements.

- **auto refresh**

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

`help` show this readme

Notes
-----
The command history is stored at `~/.voltsql_history`, it is used for retyping the previous commands as well as calculate the prioritization of keywords.
"""
