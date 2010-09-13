#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2010 VoltDB Inc.
#
# VoltDB is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# VoltDB is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.

from parser import parse
from strings import *
import os.path

#inspired by code from python cookbook
def ensure_relative_path_exists(newdir):
    if os.path.isdir(newdir):
        pass
    elif os.path.isfile(newdir):
        raise OSError("a file with the same name as the desired " \
                              "dir, '%s', already exists." % newdir)
    else:
        head, tail = os.path.split(newdir)
        print "Head, Tail: %s, %s" % (head, tail)
        if head and not os.path.isdir(head):
            ensure_relative_path_exists(head)
        if tail:
            os.mkdir(newdir)

