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

import urllib.request, urllib.parse, urllib.error

@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = 'Restore a VoltDB database snapshot.',
    options = (
        # Hidden option to restore the hashinator in addition to the tables.
        VOLT.BooleanOption('-S', '--hashinator', 'hashinator', None, default = False),
        VOLT.StringListOption(None, '--tables', 'tables',
                              'tables to be restored from snapshot.',
                              default = None),
        VOLT.StringListOption(None, '--skiptables', 'skip_tables',
                              'tables to be skipped from snapshot restore.',
                              default = None)
    ),
    arguments = (
        VOLT.PathArgument('directory', 'the snapshot server directory', absolute = True),
        VOLT.StringArgument('nonce', 'the unique snapshot identifier (nonce)')
    )
)
def restore(runner):
    nonce = runner.opts.nonce.replace('"', '\\"')
    if runner.opts.tables and runner.opts.skip_tables:
        print('Cannot specify both --tables and --skiptables.')
        return
    if runner.opts.hashinator:
        hashinator = 'true'
    else:
        hashinator = 'false'
    json_opts = ['{path:"%s",nonce:"%s",hashinator:"%s"}' % (runner.opts.directory, nonce, hashinator)]
    if runner.opts.tables:
        json_opts = ['{path:"%s",nonce:"%s",hashinator:"%s",tables: %s}' % (runner.opts.directory, nonce, hashinator, runner.opts.tables)]
    elif runner.opts.skip_tables:
        json_opts = ['{path:"%s",nonce:"%s",hashinator:"%s",skiptables: %s}' % (runner.opts.directory, nonce, hashinator, runner.opts.skip_tables)]
    runner.verbose_info('@SnapshotRestore "%s"' % json_opts)
    columns = [VOLT.FastSerializer.VOLTTYPE_STRING]
    print('voltadmin: Snapshot restore has been started. Check the server logs for ongoing status of the restore operation.')
    response = runner.call_proc('@SnapshotRestore', columns, json_opts)
    print(response.table(0).format_table(caption = 'Snapshot Restore Results'))
