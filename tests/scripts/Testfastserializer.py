#!/usr/bin/env python
# -*- coding: utf-8

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

import signal
import unittest
import datetime
import decimal
import socket
import threading
import struct

from fastserializer import *

decimal.getcontext().prec = 19

def signalHandler(signum, frame):
    raise Exception, "Interrupted by SIGINT."

class EchoServer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        # some struct typing magic
        self.int32Type = lambda length : '%c%di' % ('>', length)
        # setup a socket
        self.client = None
        self.socket = None
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(("localhost", 21213))
        self.socket.listen(1)
        self.running = 1;
        self.start()

    def run(self):
        while (self.running == 1):
            self.client, addr = self.socket.accept()

            # now spin handling normal messages
            while (self.running == 1):
                try:
                    prefix = ""
                    while (len(prefix) < 4):
                        prefix += self.client.recv(4 - len(prefix))
                    responseLength = struct.unpack(self.int32Type(1), prefix)[0]

                    rstr = ""
                    while (len(rstr) < responseLength):
                        rstr += self.client.recv(responseLength - len(rstr))

                    # echo
                    self.client.sendall(prefix)
                    self.client.sendall(rstr)
                except Exception:
                    pass

    def close(self):
        self.running = 0;
        self.socket.close()
        self.client.close()

class TestFastSerializer(unittest.TestCase):
    byteArray = (1, -21, 127)
    int16Array = (128, -256, 32767)
    int32Array = (0, -32768, 2147483647)
    int64Array = (-52423, 2147483647, -9223372036854775807)
    floatArray = (float("-inf"), float("nan"), -0.009999999776482582)
    stringArray = (None, u"hello world", u"ça")
    dateArray = (datetime.datetime.now().replace(microsecond=0),
                 datetime.datetime.fromtimestamp(0),
                 datetime.datetime.utcnow().replace(microsecond=0))
    decimalArray = (None,
                    decimal.Decimal("-837461"),
                    decimal.Decimal("8571391.193847158139"),
                    decimal.Decimal("-1348392.109386749180"))

    ARRAY_BEGIN = 126
    ARRAY_END = 127

    def setUp(self):
        self.echo = EchoServer()
        self.fs = FastSerializer('localhost', 21213, None, None)

    def tearDown(self):
        self.fs.socket.close()
        self.echo.close()
        self.echo.join()

    def sendAndCompare(self, type, value):
        self.fs.writeWireType(type, value)
        self.fs.prependLength()
        self.fs.flush()
        self.fs.bufferForRead()
        self.assertEqual(self.fs.readByte(), type)
        self.assertEqual(self.fs.read(type), value)

    def sendArrayAndCompare(self, type, value):
        self.fs.writeWireTypeArray(type, value)
        self.fs.prependLength()
        self.fs.flush()
        self.fs.bufferForRead()
        self.assertEqual(self.fs.readByte(), type)
        self.assertEqual(self.fs.readArray(type), value)

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

    def testDecimalString(self):
        for i in self.decimalArray:
            self.sendAndCompare(self.fs.VOLTTYPE_DECIMAL_STRING, i)

    def testArray(self):
        # self.fs.writeByte(self.ARRAY_BEGIN)
        # self.fs.prependLength()
        # self.fs.flush()

        self.sendArrayAndCompare(self.fs.VOLTTYPE_TINYINT, self.byteArray)
        self.sendArrayAndCompare(self.fs.VOLTTYPE_SMALLINT, self.int16Array)
        self.sendArrayAndCompare(self.fs.VOLTTYPE_INTEGER, self.int32Array)
        self.sendArrayAndCompare(self.fs.VOLTTYPE_BIGINT, self.int64Array)
        self.sendArrayAndCompare(self.fs.VOLTTYPE_STRING, self.stringArray)
        self.sendArrayAndCompare(self.fs.VOLTTYPE_TIMESTAMP, self.dateArray)
        self.sendArrayAndCompare(self.fs.VOLTTYPE_DECIMAL, self.decimalArray)
        self.sendArrayAndCompare(self.fs.VOLTTYPE_DECIMAL_STRING,
                                 self.decimalArray)

        # self.fs.writeByte(self.ARRAY_END)
        # self.fs.prependLength()
        # self.fs.flush()

    def testTable(self):
        type = FastSerializer.VOLTTYPE_VOLTTABLE

        table = VoltTable(self.fs)
        table.columns.append(VoltColumn(type = FastSerializer.VOLTTYPE_TINYINT,
                                        name = "id"))
        table.columns.append(VoltColumn(type = FastSerializer.VOLTTYPE_BIGINT,
                                        name = "bigint"))
        table.columns.append(VoltColumn(type = FastSerializer.VOLTTYPE_STRING,
                                        name = "name"))
        table.columns.append(VoltColumn(type = FastSerializer.VOLTTYPE_TIMESTAMP,
                                        name = "date"))
        table.columns.append(VoltColumn(type = FastSerializer.VOLTTYPE_DECIMAL,
                                        name = "money"))
        table.tuples.append([self.byteArray[1], self.int64Array[2],
                             self.stringArray[0], self.dateArray[2],
                             self.decimalArray[0]])
        table.tuples.append([self.byteArray[2], self.int64Array[1],
                             self.stringArray[2], self.dateArray[1],
                             self.decimalArray[1]])
        table.tuples.append([self.byteArray[0], self.int64Array[0],
                             self.stringArray[1], self.dateArray[0],
                             self.decimalArray[2]])

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
    signal.signal(signal.SIGINT, signalHandler)
    unittest.main()

