#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2015 VoltDB Inc.
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

"""
VoltDB LogKey enumeration generator
"""

import sys
import string
import os

gpl_header = \
"""/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.utils;
"""

auto_gen_warning = \
"""/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN voltdb_logstrings.properties GENERATOR */
"""

f = file( "LogKeys.java", 'w' )
f.truncate(0)
f.seek(0)
if not f:
    raise OSError("Can't create file LogKeys.java for writing")

f.write(gpl_header )
f.write(auto_gen_warning)
f.write(
"""/**
 * Keys for internationalized log strings in the voltdb_logstrings resource bundle
 *
 */
public enum LogKeys {
""")
for line in open( "voltdb_logstrings.properties", 'r' ).readlines():
    line = line.strip()
    parts = line.split('=')
    if line.startswith('#'):
        f.write("    //");
        f.write( line.lstrip("#") )
        f.write( "\n" )
    elif len(parts) == 1:
        f.write("\n")
    else:
        f.write( "    " )
        f.write( parts[0].strip() )
        f.write( ",\n" )
f.write ("    NOT_USED;\n}")
