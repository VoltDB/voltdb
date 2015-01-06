# This file is part of VoltDB.

# Copyright (C) 2008-2015 VoltDB Inc.
#
# This file contains original code and/or modifications of original code.
# Any modifications made by VoltDB Inc. are licensed under the following
# terms and conditions:
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

from voltcli import utility

def validate_partitioning(runner):
    print 'Validating partitioning...'
    columns = [VOLT.FastSerializer.VOLTTYPE_TINYINT, VOLT.FastSerializer.VOLTTYPE_VARBINARY]
    response = runner.call_proc('@ValidatePartitioning', columns, [1, None])
    mispartitioned_tuples = sum([t[4] for t in response.table(0).tuples()])
    total_hashes = response.table(1).tuple_count()
    mismatched_hashes = total_hashes - sum([t[3] for t in response.table(1).tuples()])
    print ''
    if mispartitioned_tuples == 0 and mismatched_hashes == 0:
        print 'Partitioning is correct.'
    if runner.opts.full or mispartitioned_tuples != 0 or mismatched_hashes != 0:
        if mispartitioned_tuples > 0:
            print 'Mispartitioned tuples: %d' % mispartitioned_tuples
        if mismatched_hashes != 0:
            print 'Mismatched hashes: %d' % (response.table(1).tuple_count() - mismatched_hashes)
        print response.table(0).format_table(caption='Partition Validation Results')
        print response.table(1).format_table(caption='Hash Validation Results')

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
