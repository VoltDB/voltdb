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

from voltcli import utility

def validate_partitioning(runner):
    print('Validating partitioning...')
    columns = [VOLT.FastSerializer.VOLTTYPE_VARBINARY]
    response = runner.call_proc('@ValidatePartitioning', columns, [None])
    mispartitioned_tuples = sum([t[4] for t in response.table(0).tuples()])
    total_hashes = response.table(1).tuple_count()
    mismatched_hashes = total_hashes - sum([t[3] for t in response.table(1).tuples()])
    print('')
    if mispartitioned_tuples == 0 and mismatched_hashes == 0:
        print('Partitioning is correct.')
    if runner.opts.full or mispartitioned_tuples != 0 or mismatched_hashes != 0:
        if mispartitioned_tuples > 0:
            print('Mispartitioned tuples: %d' % mispartitioned_tuples)
        if mismatched_hashes != 0:
            print('Mismatched hashes: %d' % (response.table(1).tuple_count() - mismatched_hashes))
        print(response.table(0).format_table(caption='Partition Validation Results'))
        print(response.table(1).format_table(caption='Hash Validation Results'))

@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = 'Validate the operation of a live database.',
    options = (
        VOLT.BooleanOption('-f', '--full', 'full',
                           'display full results, even when validation passes'),
    ),
)
def validate(runner):
    validate_partitioning(runner)
