# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.
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

import os
import urllib

@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = 'Save a VoltDB database snapshot.',
    options = (
        VOLT.BooleanOption('-b', '--blocking', 'blocking',
                           'block transactions and wait until the snapshot completes',
                           default = False),
        VOLT.EnumOption('-f', '--format', 'format',
                        'snapshot format', 'native', 'csv',
                        default = 'native'),
        VOLT.StringListOption(None, '--tables', 'tables',
                              'tables to include in the snapshot',
                              default = None),
        VOLT.StringListOption(None, '--skiptables', 'skip_tables',
                              'tables to skip in the snapshot',
                              default = None)
    ),
    arguments = (
        VOLT.PathArgument('directory', 'the snapshot server directory', absolute = True),
        VOLT.StringArgument('nonce', 'the unique snapshot identifier (nonce)')
    )
)
def save(runner):
    uri = 'file://%s' % urllib.quote(runner.opts.directory)
    nonce = runner.opts.nonce.replace('"', '\\"')
    if runner.opts.blocking:
        blocking = 'true'
    else:
        blocking = 'false'
    raw_json_opts = ['uripath:"%s"' % (uri),
                     'nonce:"%s"' % (nonce),
                     'block:%s' % (blocking),
                     'format:"%s"' % (runner.opts.format)]
    if runner.opts.tables:
        raw_json_opts.append('tables:%s' % (runner.opts.tables))
    if runner.opts.skip_tables:
        raw_json_opts.append('skiptables:%s' % (runner.opts.skip_tables))
    runner.verbose_info('@SnapshotSave "%s"' % raw_json_opts)
    columns = [VOLT.FastSerializer.VOLTTYPE_STRING]
    response = runner.call_proc('@SnapshotSave', columns,
                                ['{%s}' % (','.join(raw_json_opts))])
    print response.table(0).format_table(caption = 'Snapshot Save Results')
