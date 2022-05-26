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

from voltcli import utility

@VOLT.Command(
    bundles=VOLT.AdminBundle(),
    description='Update the Log4J configuration.',
    arguments=(
        VOLT.PathArgument('log4j_xml_path', 'the Log4J configuration file', exists=True),
    )
)
def log4j(runner):
    log4j_file = utility.File(runner.opts.log4j_xml_path)
    try:
        log4j_file.open()
        xml_text = log4j_file.read()
        response = runner.call_proc(
            '@UpdateLogging',
            [VOLT.FastSerializer.VOLTTYPE_STRING],
            [xml_text])
        print(response)
    finally:
        log4j_file.close()

