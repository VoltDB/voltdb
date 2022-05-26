#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.
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

#
# A simple benchmark for the python client
# You can start VoltDB with the measureoverhead benchmark with `ant benchmark -Dclient=org.volt.benchmark.overhead.OverheadClient -Dsitesperhost=2 -Dclientcount=1 -Dhostcount=1 -Dhost1=localhost -Dclienthost1=localhost -Dduration=999999 -Dtxnrate=1`
# or you can start your own app and substitute your own procedure in ThreadFunc.
# On my i7 920 desktop I was getting 4k invocations with 1 python process and 6 threads and 9.75k invocation with three python processes with 6 threads each
#

from multiprocessing import *
from datetime import *
from fastserializer import *

def ProcessFunc( countQueue, endTime):
    client = FastSerializer("localhost", 21212, "", "")
    proc = VoltProcedure( client, "measureOverhead", [FastSerializer.VOLTTYPE_INTEGER] )
    counter = 0;
    while datetime.datetime.now() < endTime:
        counter += 1
        response = proc.call([counter])
        if response.status != 1:
            print response.statusString
        now = datetime.datetime.now().microsecond / 1000
    countQueue.put(counter)

startTime = datetime.datetime.now()
endTime = startTime + datetime.timedelta( 0, 60)

countQueue = Queue()
procs = []
for x in range(12):
    p = Process( target=ProcessFunc, args=( countQueue, endTime ))
    procs.append(p)
    p.start()

for p in procs:
    p.join()

requestCount = 0;
count = None
while not countQueue.empty():
    requestCount = requestCount + countQueue.get()

duration = datetime.datetime.now() - startTime

print requestCount / duration.seconds


