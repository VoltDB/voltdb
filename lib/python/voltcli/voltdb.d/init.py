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

@VOLT.Command(
    bundles = VOLT.ServerBundle('initialize',
                                needs_catalog=False,
                                supports_live=False,
                                default_host=False,
                                safemode_available=False,
                                supports_daemon=False,
                                supports_multiple_daemons=False,
                                check_environment_config=True,
                                force_voltdb_create=True,
                                supports_paused=False),
    options = (
        VOLT.StringOption('-N', '--name', 'clustername',
                         'specify the the cluster designation name',
                         default = None)
    ),
    description = 'Initializes a new, empty database.'
)
def init(runner):
    if runner.opts.clustername:
        runner.args.extend(['clustername', runner.opts.clustername])
    runner.go()
