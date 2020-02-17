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
    bundles = VOLT.ServerBundle('add',
                                needs_catalog=False,
                                supports_live=False,
                                default_host=False,
                                safemode_available=False,
                                supports_daemon=True,
                                supports_multiple_daemons=True,
                                check_environment_config=True),
    description = 'WARNING: The add is deprecated. Please use INIT and START. Add the current node to a VoltDB cluster.',
    hideverb = True
)
def add(runner):
    runner.warning('voltdb add is no longer supported, please use \'init\' to initialize and \'start\' to start the database.')
    runner.go()
