#!/usr/bin/env python

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

testspec = """
  class Database {
    /** test comment */
    // more comments
    Partition* partitions; // more comments
    Table*     tables;
    Program*   programs;
    Procedure* procedures;
  }

  /*
  class Garbage {
    Garbage garbage;
  }
  */

  class Partition {
    bool       isActive;
    Range*     ranges;
    Replica*   replicas;
  }

  class Table {
    int         type;
    Table?      buddy1;
    Table?      buddy2;
    Column*     columns;
    Index*      indexes;
    Constraint* constraints;
  }

  class Program {
    Program*   programs;
    Procedure* procedures;
    Table*     tables;
  }
  """

def checkeq( a, b ):
  if a != b:
    raise Exception( 'test failed: %r != %r' % (a,b) )
