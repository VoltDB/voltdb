#!/usr/bin/env python2.6
# This file is part of VoltDB.

# Copyright (C) 2008-2022 Volt Active Data Inc.
#
# This file contains original code and/or modifications of original code.
# Any modifications made by Volt Active Data Inc. are licensed under the following
# terms and conditions:
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

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import datetime
import httplib2
import json
import random
import threading
import time

#Set these 

server="volt4b" 
port=8080
sleep_time=5  #Time to sleep between every VMC iteration (VMC delay is 5)
max_err=1 #Max number of errors on any thread before it quits
thr=20    #Concurrent threads of VMC emulation




request_format = "http://%s:%d/api/1.0/?%s"



#These are the requests being made every 5 seconds by VMC in V4.9
#This may change in the future
arguments = [
"Procedure=%40SystemInformation&Parameters=%5B%22OVERVIEW%22%5D&admin=true",
"Procedure=%40Statistics&Parameters=%5B%22MEMORY%22%2C0%5D&admin=true",
"Procedure=%40Statistics&Parameters=%5B%22LATENCY_HISTOGRAM%22%2C0%5D&admin=true",
"Procedure=%40Statistics&Parameters=%5B%22PROCEDUREPROFILE%22%2C0%5D&admin=true",
"Procedure=%40Statistics&Parameters=%5B%22CPU%22%2C0%5D&admin=true",
"Procedure=%40SystemInformation&Parameters=%5B%22DEPLOYMENT%22%5D&admin=true",
"Procedure=%40Statistics&Parameters=%5B%22TABLE%22%2C0%5D&admin=true",
"Procedure=%40Statistics&Parameters=%5B%22MEMORY%22%2C0%5D&admin=true",
"Procedure=%40Statistics&Parameters=%5B%22TABLE%22%2C0%5D&admin=true",
"Procedure=%40Statistics&Parameters=%5B%22PROCEDUREPROFILE%22%2C0%5D&admin=true",
"Procedure=%40SystemCatalog&Parameters=%5B%22TABLES%22%5D&admin=true",
]

HTTP_SUCCESS=200

STATUS_STRINGS = {
"VOLTDB_CONNECTION_LOST": -4,
"VOLTDB_CONNECTION_TIMEOUT": -6,
"VOLTDB_GRACEFUL_FAILURE": -2,
"VOLTDB_OPERATIONAL_FAILURE": -9,
"VOLTDB_RESPONSE_UNKNOWN": -7,
"VOLTDB_SERVER_UNAVAILABLE": -5,
"VOLTDB_SUCCESS": 1,
"VOLTDB_TXN_RESTART": -8,
"VOLTDB_UNEXPECTED_FAILURE": -3,
"VOLTDB_UNINITIALIZED_APP_STATUS_CODE": -128,
"VOLTDB_USER_ABORT": -1,
}

STATUS_CODES = dict((v,k) for k, v in STATUS_STRINGS.iteritems())

pause_on_error=False
def ts():
    return datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S,%f')


def push_it(max_errors=1, sleep_time=0):
    err_ct = 0
    tid = str(threading.current_thread().name)
    print ts() + " Starting thread " + tid
    i = 1
    http = httplib2.Http(cache=None, timeout=15.0)
    #while forever
    while ( i > 0 ) :
        if not i % 100:
            print "%s %s Loop Count: %4d" % (ts(), tid, i)
        for a in arguments:

            url = request_format % (server, port, a)

            try:
                #http = httplib2.Http(cache=None, timeout=15.0)
                response,content = http.request(url, 'GET')
                if response['status'] != str(HTTP_SUCCESS) or json.loads(content)['status'] != STATUS_STRINGS['VOLTDB_SUCCESS']:
                    statusstring = STATUS_CODES.get(json.loads(content)['status'],"Unknown status")
                    print "%s %s Request# %d - Error getting %s\n\thttp_status=%s\tresponse=%s" % (ts(), tid, i, a, response['status'], statusstring)
                    err_ct += 1
            except AttributeError:
                err_ct += 1

            if err_ct >= max_errors:
                if pause_on_error:
                    raw_input("Press any key to continue...")
                else:
                    print "%s: Too many errors - I'm out of here" % tid
                    return (-1)
        time.sleep(sleep_time)
        i += 1

threads = []
for i in range(thr):
    t = threading.Thread(target=push_it, args=(1,sleep_time))
    t.daemon=True
    threads.append(t)

for t in threads:
    #Don't bunch them all up
    time.sleep (random.randint(50,200)/100.0)
    t.start()

for t in threads:
    t.join()

#TODO: This threading is dodgy and doesn't end gracefully
#and doesn't handle ctrl-c gracefully
