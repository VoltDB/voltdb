# This file is part of VoltDB.
# Copyright (C) 2008-2020 VoltDB Inc.
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
    bundles = VOLT.ServerBundle('create',
                                needs_catalog=True,
                                supports_live=False,
                                default_host=True,
                                safemode_available=False,
                                supports_daemon=True,
                                supports_multiple_daemons=True,
                                check_environment_config=True,
                                force_voltdb_create=True,
                                supports_paused=True),
    options = (
        VOLT.BooleanOption('-r', '--replica', 'replica',
                           'start replica cluster (deprecated, please use role="replica" in the deployment file)',
                           default = False),
    ),
    description = 'WARNING: The create is deprecated. Please use INIT and START. Start a new, empty database.',
    hideverb=True
)
def create(runner):
    runner.warning('voltdb create is no longer supported, please use \'init\' to initialize and \'start\' to start the database.')
    runner.go()
