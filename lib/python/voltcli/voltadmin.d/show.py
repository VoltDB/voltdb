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

from voltcli import checkstats
import datetime

def show_snapshots(runner):
    response = checkstats.get_stats(runner, "SnapshotSummary")
    print(response.table(0).format_table(caption = 'Snapshot Summary'))

def show_license(runner):
    response = runner.call_proc('@SystemInformation',
                                [VOLT.FastSerializer.VOLTTYPE_STRING],
                                ['LICENSE'])
    print(response.table(0).format_table(caption = 'License Information'))
    for tuple in response.table(0).tuples():
        if tuple[0] == 'EXPIRATION':
            expiration = datetime.datetime.strptime(tuple[1], "%a %b %d, %Y")
            daysUntilExpiration = expiration - datetime.datetime.today()
            print("License expires on " + tuple[1] + " (" + str(daysUntilExpiration.days) + " days remaining).")


@VOLT.Multi_Command(
    bundles = VOLT.AdminBundle(),
    description = 'Display information about a live database.',
    modifiers = (VOLT.Modifier('snapshots', show_snapshots, 'Display current snapshot status.'),
                 VOLT.Modifier('license', show_license, 'Display license information.'))
)
def show(runner):
    runner.go()
