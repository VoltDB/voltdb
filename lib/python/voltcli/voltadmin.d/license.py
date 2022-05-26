# This file is part of VoltDB.
# Copyright (C) 2022 Volt Active Data Inc.
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
    ),
)

# get proper error message from the procedure call
def license(runner):
    license_file = VOLT.utility.File(runner.opts.license);
    try:
        licenseBytes = license_file.read_hex().decode()
        # call_proc() aborts with an error if the update failed.
        response = runner.call_proc('@UpdateLicense', [VOLT.FastSerializer.VOLTTYPE_STRING], [licenseBytes])
        if response.status() != 1:
            runner.abort('Failed to update the license. Response status: %d' % response.response.statusString)
        for row in response.table(0).tuples():
            procStatus, errStr = row[0], row[1]
            # 0-Success, 1-Failure
            if procStatus != 0:
                runner.abort("Failed to update the license: %s" % errStr)
        runner.info("The license is updated successfully.")
        # display new license information
        response = runner.call_proc('@SystemInformation', [VOLT.FastSerializer.VOLTTYPE_STRING], ['LICENSE']);
        print(response.table(0).format_table(caption = 'License Information'))
        # exception is handled in utility.File
    finally:
        license_file.close();
