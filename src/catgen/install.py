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

import os

# diff the output directory and only copy if necessary to
# avoid unecessarily dirtying build dependencies when no
# changes are necessary

exit_status = os.system("diff -x .svn -x .fake -x .DS_Store out/cppsrc/ ../ee/catalog/")
if (exit_status != 0):
    os.system("rm ../ee/catalog/*")
    os.system("cp out/cppsrc/* ../ee/catalog/")

exit_status = os.system("diff -x .svn -x gui  -x package.html -x .DS_Store out/javasrc/ ../frontend/org/voltdb/catalog/")
if (exit_status != 0):
    os.system("rm ../frontend/org/voltdb/catalog/*.java")
    os.system("cp out/javasrc/* ../frontend/org/voltdb/catalog/")
