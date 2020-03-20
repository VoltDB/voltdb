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
    description='Update the license of a running VoltDB database.',
    arguments=(
        VOLT.StringArgument(
            'license',
            'VoltDB_license_file (extension .xml)',
            min_count=1, max_count=1),
    )
)
def license(runner):
    license_file = VOLT.utility.File(runner.opts.license);
    try:
        licenseBytes = license_file.read()
        # call_proc() aborts with an error if the update failed.
        response = runner.call_proc('@UpdateLicense', [VOLT.FastSerializer.VOLTTYPE_STRING], [licenseBytes])
        if response.status() != 1:
            runner.abort('The license update failed with status: %d' % response.response.statusString)
        else:
            runner.info('The license is updated successfully.')
    finally:
        license_file.close();
