#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2015 VoltDB Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

from subprocess import Popen, PIPE
import os
import sys

print sys.path[0]

script = """
# setup commands:
#   begin indexname indextypes schema
# types:
#   bint = big integer
#   int = integer
#   sint = small integer
#   tint = tiny integer
#   float = double (float)
#   dec = decimal
#   str = string
# general commands:
#   is = insert expecting success
#   if = insert expecting failure
#   ls = lookup expecting success
#   lf = lookup expecting failure
#   us = update expecting success
#   uf = update expecting failure

begin TestName MultiIntsTree,MultiGenericTree,MultiIntsHash,MultiGenericHash,UniqueIntsTree,UniqueGenericTree,UniqueIntsHash,UniqueGenericHash bint,bint,bint
is 5,6,7
ls 5,6,7
#us 5,6,7 8,9,10
#uf 5,6,7 8,9,10
#ds 5,6,7
df 8,9,10
exec
begin GenericTest MultiGenericTree,MultiGenericHash,UniqueGenericTree,UniqueGenericHash str4,bint,bint
is foo,6,7
ls foo,6,7
#us foo,6,7 bar,9,10
#uf foo,6,7 bar,9,10
ds foo,6,7
df bar,9,10
exec
done
"""

p = Popen(os.path.join(sys.path[0], "index_scripted_test"),
          shell=False, stdin=PIPE, close_fds=True)
def write(x):
    p.stdin.write(x)
write(script)
retcode = p.wait()
sys.exit(retcode)
