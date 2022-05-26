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

import os
from voltcli import utility

@VOLT.Command(
    bundles=VOLT.AdminBundle(),
    description='Update the configuration of a running database.',
    #description2='Either a catalog (extension .jar), a deployment file (extension .xml), or both, must be provided.',
    arguments=(
        VOLT.StringArgument(
            'configuration',
            'database configuration file (extension .xml)',
            min_count=1, max_count=1),
    )
)
def update(runner):
    columns = [VOLT.FastSerializer.VOLTTYPE_NULL, VOLT.FastSerializer.VOLTTYPE_NULL]
    catalog = None
    deployment = None
    configuration = runner.opts.configuration
    deployment = VOLT.utility.File(configuration).read()
    columns[1] = VOLT.FastSerializer.VOLTTYPE_STRING
    params = [catalog, deployment]
    # call_proc() aborts with an error if the update failed.
    runner.call_proc('@UpdateApplicationCatalog', columns, params)
    runner.info('The configuration update succeeded.')
