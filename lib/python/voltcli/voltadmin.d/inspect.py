# This file is part of VoltDB.
# Copyright (C) 2022 Volt Active Data Inc.
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

import datetime
import re
from voltcli import utility

@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = 'Display licensing and configuration details.',
)

def inspect(runner):
    response = runner.call_proc('@SystemInformation',
                                [VOLT.FastSerializer.VOLTTYPE_STRING],
                                ['LICENSE'])
    print(response.table(0).format_table(caption = 'License Information'))
    for tuple in response.table(0).tuples():
        if tuple[0] == 'EXPIRATION':
            expiration = datetime.datetime.strptime(tuple[1], "%a %b %d, %Y")
            daysUntilExpiration = expiration - datetime.datetime.today()
            print("License expires on " + tuple[1] + " (" + str(daysUntilExpiration.days) + " days remaining).")

    response2 = runner.call_proc('@SystemInformation',
                                 [VOLT.FastSerializer.VOLTTYPE_STRING],
                                 ['DEPLOYMENT'])
    filtered = filter_tuples(response2.table(0), 0, ('hostcount', 'sitesperhost', 'kfactor'))
    print(utility.format_table(filtered,
                               caption = 'Deployment Information',
                               headings = ('PROPERTY', 'VALUE')))

    response3 = runner.call_proc('@SystemInformation',
                                 [VOLT.FastSerializer.VOLTTYPE_STRING],
                                 ['OVERVIEW'])
    filtered = filter_tuples(response3.table(0), 1, ('VERSION', 'UPTIME', 'REPLICATIONROLE',
                                                     'CPUCORES', 'CPUTHREADS', 'MEMORY',
                                                     'KUBERNETES'))
    squashed = squash_same_values(approx_uptime(filtered))
    print(utility.format_table(squashed,
                               caption = 'Configuration Information',
                               headings = ('PROPERTY', 'VALUE', 'HOST ID')))
    print('')

def filter_tuples(table, index, selector):
    selected = []
    for tuple in table.tuples():
        if tuple[index] in selector:
            selected.append(tuple)
    return selected

def approx_uptime(table, propix=1, valix=2):
    for tuple in table:
        if tuple[propix] == 'UPTIME': # ignore millisecs which may somnetimes differ
            tuple[valix] = re.sub(r'\.\d\d\d$', '', tuple[valix])
    return table

def squash_same_values(input, hostix=0, propix=1, valix=2):
    propmap = {}
    for tuple in input:
        prop = tuple[propix]
        if prop not in propmap:
            propmap[prop] = []
        propmap[prop].append((tuple[hostix], tuple[valix]))
    output = []
    for prop in sorted(propmap.keys()):
        pairs = propmap[prop]
        if all([pair[1] == pairs[0][1] for pair in pairs]):
            output.append((prop, pairs[0][1], 'all'))
        else:
            for pair in sorted(pairs, key=lambda p: int(p[0])):
                output.append((prop, pair[1], pair[0]))
    return output
