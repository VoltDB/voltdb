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

import os
from voltcli import utility

@VOLT.Command(
    bundles=VOLT.AdminBundle(),
    description='Update the license of a running database.',
    #description2='A valid VoltDB license file (extension .xml) must be provided.',
    arguments=(
        VOLT.StringArgument(
            'license',
            'VoltDB license file (extension .xml)',
            min_count=1, max_count=1),
    )
)
def license(runner):
    columns = [VOLT.FastSerializer.VOLTTYPE_NULL, VOLT.FastSerializer.VOLTTYPE_NULL]
    licensePath = runner.opts.license
    licenseBytes = VOLT.utility.File(licensePath).read()
    columns[1] = VOLT.FastSerializer.VOLTTYPE_STRING
    params = [licenseBytes]
    # call_proc() aborts with an error if the update failed.
    runner.call_proc('@UpdateLicense', columns, params)
    runner.info('The configuration update succeeded.')
