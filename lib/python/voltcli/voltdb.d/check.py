# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.
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

import multiprocessing
import platform
from voltcli import checkconfig

output = {
            "Hostname" : ["", "Unable to gather information"],
            "OS" : ["", "Unable to gather information"],
            "OS release" : ["", "Unable to gather information"],
            "ThreadCount" : ["", "Unable to gather information"],
            "64 bit" : ["", "Unable to gather information"],
            "Memory" : ["", "Unable to gather information"]
          }

def displayResults():
    fails = 0
    warns = 0
    for key,val in sorted(output.items()):
        if val[0] == "FAIL":
            fails += 1
        elif val[0] == "WARN":
            warns += 1
        print("Status: %-25s %-8s %-9s" % ( key, val[0], val[1] ))
    if fails > 0:
        print("\nCheck FAILED. Please review.")
    elif warns > 0:
        print("\nCheck completed with " + str(warns) + " WARNINGS.")
    else:
        print("\nCheck completed successfully.")

@VOLT.Command(
    description = 'Check system properties.',
    log4j_default = None
)

def check(runner):
    systemCheck()

def systemCheck():
    output['Hostname'] = ["", platform.node()]
    output['ThreadCount'] = ["", str(multiprocessing.cpu_count())]
    checkconfig.test_full_config(output)
    displayResults()
