#!/usr/bin/env python
# -*- coding: utf-8

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

import sys
# add the path to the volt python client, just based on knowing
# where we are now
sys.path.append('../../lib/python')

import signal
import unittest
import datetime
import decimal
import socket
import threading
import struct
import subprocess
import time
import array

from voltdbclient import *

SERVER_NAME = "EchoServer"
decimal.getcontext().prec = 19

def signalHandler(server, signum, frame):
    server.shutdown()
    server.join()
    raise Exception, "Interrupted by SIGINT."

class EchoServer(threading.Thread):
    def __init__(self, cmd, lock):
        threading.Thread.__init__(self)

        self.__server_cmd = cmd
        self.__lock = threading.Event()
        self.__start = lock

    def run(self):
        server = subprocess.Popen(self.__server_cmd, shell = True)

        time.sleep(1)
        self.__start.set()
        self.__lock.wait()

        # Get the server pid
        jps = subprocess.Popen("jps", stdout = subprocess.PIPE, shell = True)
        (stdout, stderr) = jps.communicate()
        pid = None
        lines = stdout.split("\n")
        for l in lines:
            if SERVER_NAME in l:
                pid = l.split()[0]
        if pid == None:
            return
        # Should kill the server now
        killer = subprocess.Popen("kill -9 %s" % (pid), shell = True)
        killer.communicate()
        if killer.returncode != 0:
            print >> sys.stderr, \
                "Failed to kill the server process %d" % (server.pid)
            return

        server.communicate()

    def shutdown(self):
        self.__lock.set()

class TestFastSerializer(unittest.TestCase):
    byteArray = [None, 1, -21, 127]
    int16Array = [None, 128, -256, 32767]
    int32Array = [None, 0, -32768, 2147483647]
    int64Array = [None, -52423, 2147483647, -9223372036854775807]
    floatArray = [None, float("-inf"), float("nan"), -0.009999999776482582]
    stringArray = [None, u"hello world", u"ça"]
    binArray = [None, array.array('c', ['c', 'f', 'q'])]
    dateArray = [None, datetime.datetime.now(),
                 datetime.datetime.utcfromtimestamp(0),
                 datetime.datetime.utcnow()]
    decimalArray = [None,
                    decimal.Decimal("-837461"),
                    decimal.Decimal("8571391.193847158139"),
                    decimal.Decimal("-1348392.109386749180")]

    ARRAY_BEGIN = 126
    ARRAY_END = 127

    def setUp(self):
        self.fs = FastSerializer('localhost', 21212, None, None)

    def tearDown(self):
        self.fs.socket.close()

    def sendAndCompare(self, type, value):
        self.fs.writeWireType(type, value)
        self.fs.prependLength()
        self.fs.flush()
        self.fs.bufferForRead()
        t = self.fs.readByte()
        self.assertEqual(t, type)
        v = self.fs.read(type)
        self.assertEqual(v, value)

    def sendArrayAndCompare(self, type, value):
        self.fs.writeWireTypeArray(type, value)
        sys.stdout.flush()
        self.fs.prependLength()
        sys.stdout.flush()
        self.fs.flush()
        sys.stdout.flush()
        self.fs.bufferForRead()
        sys.stdout.flush()
        self.assertEqual(self.fs.readByte(), type)
        sys.stdout.flush()
        self.assertEqual(list(self.fs.readArray(type)), value)
        sys.stdout.flush()

    def testByte(self):
        for i in self.byteArray:
            self.sendAndCompare(self.fs.VOLTTYPE_TINYINT, i)

    def testShort(self):
        for i in self.int16Array:
            self.sendAndCompare(self.fs.VOLTTYPE_SMALLINT, i)

    def testInt(self):
        for i in self.int32Array:
            self.sendAndCompare(self.fs.VOLTTYPE_INTEGER, i)

    def testLong(self):
        for i in self.int64Array:
            self.sendAndCompare(self.fs.VOLTTYPE_BIGINT, i)

    def testFloat(self):
        type = self.fs.VOLTTYPE_FLOAT

        for i in self.floatArray:
            self.fs.writeWireType(type, i)
            self.fs.prependLength()
            self.fs.flush()

            self.fs.bufferForRead()
            self.assertEqual(self.fs.readByte(), type)
            result = self.fs.readFloat64()
            if isNaN(i):
                self.assertTrue(isNaN(result))
            else:
                self.assertEqual(result, i)

    def testString(self):
        for i in self.stringArray:
            self.sendAndCompare(self.fs.VOLTTYPE_STRING, i)

    def testDate(self):
        for i in self.dateArray:
            self.sendAndCompare(self.fs.VOLTTYPE_TIMESTAMP, i)

    def testDecimal(self):
        for i in self.decimalArray:
            self.sendAndCompare(self.fs.VOLTTYPE_DECIMAL, i)

    def testArray(self):
        self.fs.writeByte(self.ARRAY_BEGIN)
        self.fs.prependLength()
        self.fs.flush()

        self.sendArrayAndCompare(self.fs.VOLTTYPE_TINYINT, self.byteArray)
        self.sendArrayAndCompare(self.fs.VOLTTYPE_SMALLINT, self.int16Array)
        self.sendArrayAndCompare(self.fs.VOLTTYPE_INTEGER, self.int32Array)
        self.sendArrayAndCompare(self.fs.VOLTTYPE_BIGINT, self.int64Array)
        self.sendArrayAndCompare(self.fs.VOLTTYPE_STRING, self.stringArray)
        self.sendArrayAndCompare(self.fs.VOLTTYPE_TIMESTAMP, self.dateArray)
        self.sendArrayAndCompare(self.fs.VOLTTYPE_DECIMAL, self.decimalArray)

        self.fs.writeByte(self.ARRAY_END)
        self.fs.prependLength()
        self.fs.flush()

    def testTable(self):
        type = FastSerializer.VOLTTYPE_VOLTTABLE

        table = VoltTable(self.fs)
        table.columns.append(VoltColumn(type = FastSerializer.VOLTTYPE_TINYINT,
                                        name = "id"))
        table.columns.append(VoltColumn(type = FastSerializer.VOLTTYPE_BIGINT,
                                        name = "bigint"))
        table.columns.append(VoltColumn(type = FastSerializer.VOLTTYPE_STRING,
                                        name = "name"))
        table.columns.append(VoltColumn(type = FastSerializer.VOLTTYPE_VARBINARY,
                                        name = "bin"))
        table.columns.append(VoltColumn(type = FastSerializer.VOLTTYPE_TIMESTAMP,
                                        name = "date"))
        table.columns.append(VoltColumn(type = FastSerializer.VOLTTYPE_DECIMAL,
                                        name = "money"))
        table.tuples.append([self.byteArray[1], self.int64Array[2],
                             self.stringArray[0], self.binArray[0], self.dateArray[2],
                             self.decimalArray[0]])
        table.tuples.append([self.byteArray[2], self.int64Array[1],
                             self.stringArray[2], self.binArray[1], self.dateArray[1],
                             self.decimalArray[1]])
        #table.tuples.append([self.byteArray[0], self.int64Array[0],
        #                     self.stringArray[1], self.binArray[1], self.dateArray[0],
        #                     self.decimalArray[2]])

        self.fs.writeByte(type)
        table.writeToSerializer()
        self.fs.prependLength()
        self.fs.flush()

        self.fs.bufferForRead()
        self.assertEqual(self.fs.readByte(), type)
        result = VoltTable(self.fs)
        result.readFromSerializer()
        self.assertEqual(result, table)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(-1)

    lock = threading.Event()
    echo = EchoServer(sys.argv[1], lock)
    handler = lambda x, y: signalHandler(echo, x, y)
    signal.signal(signal.SIGINT, handler)

    echo.start()
    lock.wait()
    del sys.argv[1]
    try:
        unittest.main()
    except SystemExit:
        echo.shutdown()
        echo.join()
        raise
