#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2010 VoltDB L.L.C.
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

print "Hooray!!!!"
print sys.path[0]

script = """# setup commands:
#   begin indexname
#   types list-of-types
# types:
#   bi = big integer
#   s = string
# general commands:
#   is = insert success
#   if = insert failure
#   ls = lookup success
#   lf = lookup failure

begin TestName IntsMultiMapIndex/GenericMultiMapIndex bint bint bint
is 5 6 7
ls 5 6 7
exec
done
"""

p = Popen(os.path.join(sys.path[0], "index_scripted_test"),
          shell=False, stdin=PIPE, close_fds=True)
def write(x):
    p.stdin.write(x)
write(script)
retcode = p.wait()
print "retcode:", retcode
sys.exit(retcode)
