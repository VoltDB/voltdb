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

import sys
if sys.hexversion < 0x02050000:
    raise Exception("Python version 2.5 or greater is required.")
import array
import socket
import struct
import datetime
import decimal
try:
    from hashlib import sha1 as sha
except ImportError:
    from sha import sha

decimal.getcontext().prec = 38

def isNaN(d):
    """Since Python has the weird behavior that a float('nan') is not equal to
    itself, we have to test it by ourselves.
    """

    if d == None:
        return False

    # work-around for Python 2.4
    s = array.array("d", [d])
    return (s.tostring() == "\x00\x00\x00\x00\x00\x00\xf8\x7f" or
            s.tostring() == "\x00\x00\x00\x00\x00\x00\xf8\xff" or
            s.tostring() == "\x00\x00\x00\x00\x00\x00\xf0\x7f")

def if_else(cond, a, b):
    """Work around Python 2.4
    """

    if cond: return a
    else: return b

class ReadBuffer(object):
    """
    Read buffer management class.
    """
    def __init__(self):
        self.clear()

    def clear(self):
        self._buf = ""
        self._off = 0

    def buffer_length(self):
        return len(self._buf)

    def get_buffer(self):
        return self._buf

    def append(self, content):
        self._buf += content

    def shift(self, size):
        self._off += size

    def read(self, size):
        return self._buf[self._off:self._off+size]

    def unpack(self, format, size):
        try:
            values = struct.unpack_from(format, self._buf, self._off)
        except struct.error, e:
            print 'Exception unpacking %d bytes using format "%s": %s' % (size, format, str(e))
        self.shift(size)
        return values

class FastSerializer:
    "Primitive type de/serialization in VoltDB formats"

    LITTLE_ENDIAN = '<'
    BIG_ENDIAN = '>'

    ARRAY = -99

    # VoltType enumerations
    VOLTTYPE_NULL = 1
    VOLTTYPE_TINYINT = 3  # int8
    VOLTTYPE_SMALLINT = 4 # int16
    VOLTTYPE_INTEGER = 5  # int32
    VOLTTYPE_BIGINT = 6   # int64
    VOLTTYPE_FLOAT = 8    # float64
    VOLTTYPE_STRING = 9
    VOLTTYPE_TIMESTAMP = 11 # 8 byte long
    VOLTTYPE_DECIMAL = 22  # fixed precision decimal
    VOLTTYPE_MONEY = 20     # 8 byte long
    VOLTTYPE_VOLTTABLE = 21
    VOLTTYPE_VARBINARY = 25

    # SQL NULL indicator for object type serializations (string, decimal)
    NULL_STRING_INDICATOR = -1
    NULL_DECIMAL_INDICATOR = -170141183460469231731687303715884105728
    NULL_TINYINT_INDICATOR = -128
    NULL_SMALLINT_INDICATOR = -32768
    NULL_INTEGER_INDICATOR = -2147483648
    NULL_BIGINT_INDICATOR = -9223372036854775808
    NULL_FLOAT_INDICATOR = -1.7E308

    # default decimal scale
    DEFAULT_DECIMAL_SCALE = 12

    # procedure call result codes
    PROC_OK = 0

    # there are assumptions here about datatype sizes which are
    # machine dependent. the program exits with an error message
    # if these assumptions are not true. it is further assumed
    # that host order is little endian. See isNaN().

    def __init__(self, host = None, port = 21212, username = "",
                 password = "", dump_file_path = None,
                 connect_timeout = 8,
                 procedure_timeout = None,
                 default_timeout = None):
        """
        :param host: host string for connection or None
        :param port: port for connection or None
        :param username: authentication user name for connection or None
        :param password: authentication password for connection or None
        :param dump_file_path: path to optional dump file or None
        :param connect_timeout: timeout (secs) or None for authentication (default=8)
        :param procedure_timeout: timeout (secs) or None for procedure calls (default=None)
        :param default_timeout: default timeout (secs) or None for all other operations (default=None)
        """
        # connect a socket to host, port and get a file object
        self.wbuf = array.array('c')
        self.host = host
        self.port = port
        if not dump_file_path is None:
            self.dump_file = open(dump_file_path, "wb")
        else:
            self.dump_file = None
        self.default_timeout = default_timeout
        self.procedure_timeout = procedure_timeout

        self.socket = None
        if self.host != None and self.port != None:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.setblocking(1)
            self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
            self.socket.connect((self.host, self.port))

        # input can be big or little endian
        self.inputBOM = self.BIG_ENDIAN  # byte order if input stream
        self.localBOM = self.LITTLE_ENDIAN  # byte order of host

        # Type to reader/writer mappings
        self.READER = {self.VOLTTYPE_NULL: self.readNull,
                       self.VOLTTYPE_TINYINT: self.readByte,
                       self.VOLTTYPE_SMALLINT: self.readInt16,
                       self.VOLTTYPE_INTEGER: self.readInt32,
                       self.VOLTTYPE_BIGINT: self.readInt64,
                       self.VOLTTYPE_FLOAT: self.readFloat64,
                       self.VOLTTYPE_STRING: self.readString,
                       self.VOLTTYPE_VARBINARY: self.readVarbinary,
                       self.VOLTTYPE_TIMESTAMP: self.readDate,
                       self.VOLTTYPE_DECIMAL: self.readDecimal}
        self.WRITER = {self.VOLTTYPE_NULL: self.writeNull,
                       self.VOLTTYPE_TINYINT: self.writeByte,
                       self.VOLTTYPE_SMALLINT: self.writeInt16,
                       self.VOLTTYPE_INTEGER: self.writeInt32,
                       self.VOLTTYPE_BIGINT: self.writeInt64,
                       self.VOLTTYPE_FLOAT: self.writeFloat64,
                       self.VOLTTYPE_STRING: self.writeString,
                       self.VOLTTYPE_VARBINARY: self.writeVarbinary,
                       self.VOLTTYPE_TIMESTAMP: self.writeDate,
                       self.VOLTTYPE_DECIMAL: self.writeDecimal}
        self.ARRAY_READER = {self.VOLTTYPE_TINYINT: self.readByteArray,
                             self.VOLTTYPE_SMALLINT: self.readInt16Array,
                             self.VOLTTYPE_INTEGER: self.readInt32Array,
                             self.VOLTTYPE_BIGINT: self.readInt64Array,
                             self.VOLTTYPE_FLOAT: self.readFloat64Array,
                             self.VOLTTYPE_STRING: self.readStringArray,
                             self.VOLTTYPE_TIMESTAMP: self.readDateArray,
                             self.VOLTTYPE_DECIMAL: self.readDecimalArray}

        self.__compileStructs()

        # Check if the value of a given type is NULL
        self.NULL_DECIMAL_INDICATOR = \
            self.__intToBytes(self.__class__.NULL_DECIMAL_INDICATOR, 0)
        self.NullCheck = {self.VOLTTYPE_NULL:
                              lambda x: None,
                          self.VOLTTYPE_TINYINT:
                              lambda x:
                              if_else(x == self.__class__.NULL_TINYINT_INDICATOR,
                                      None, x),
                          self.VOLTTYPE_SMALLINT:
                              lambda x:
                              if_else(x == self.__class__.NULL_SMALLINT_INDICATOR,
                                      None, x),
                          self.VOLTTYPE_INTEGER:
                              lambda x:
                              if_else(x == self.__class__.NULL_INTEGER_INDICATOR,
                                      None, x),
                          self.VOLTTYPE_BIGINT:
                              lambda x:
                              if_else(x == self.__class__.NULL_BIGINT_INDICATOR,
                                      None, x),
                          self.VOLTTYPE_FLOAT:
                              lambda x:
                              if_else(abs(x - self.__class__.NULL_FLOAT_INDICATOR) < 1e307,
                                      None, x),
                          self.VOLTTYPE_STRING:
                              lambda x:
                              if_else(x == self.__class__.NULL_STRING_INDICATOR,
                                      None, x),
                          self.VOLTTYPE_VARBINARY:
                              lambda x:
                              if_else(x == self.__class__.NULL_STRING_INDICATOR,
                                      None, x),
                          self.VOLTTYPE_DECIMAL:
                              lambda x:
                              if_else(x == self.NULL_DECIMAL_INDICATOR,
                                      None, x)}

        self.read_buffer = ReadBuffer()

        if not username is None and not password is None and not host is None:
            assert not self.socket is None
            self.socket.settimeout(connect_timeout)
            self.authenticate(username, password)

        if self.socket:
            self.socket.settimeout(self.default_timeout)

    def __compileStructs(self):
        # Compiled structs for each type
        self.byteType = lambda length : '%c%db' % (self.inputBOM, length)
        self.ubyteType = lambda length : '%c%dB' % (self.inputBOM, length)
        self.int16Type = lambda length : '%c%dh' % (self.inputBOM, length)
        self.int32Type = lambda length : '%c%di' % (self.inputBOM, length)
        self.int64Type = lambda length : '%c%dq' % (self.inputBOM, length)
        self.uint64Type = lambda length : '%c%dQ' % (self.inputBOM, length)
        self.float64Type = lambda length : '%c%dd' % (self.inputBOM, length)
        self.stringType = lambda length : '%c%ds' % (self.inputBOM, length)
        self.varbinaryType = lambda length : '%c%ds' % (self.inputBOM, length)

    def close(self):
        if self.dump_file != None:
            self.dump_file.close()
        self.socket.close()

    def authenticate(self, username, password):
        # Requires sending a length preceded username and password even if
        # authentication is turned off.

        #protocol version
        self.writeByte(0)

        # service requested
        self.writeString("database")

        if username:
            # utf8 encode supplied username
            self.writeString(username)
        else:
            # no username, just output length of 0
            self.writeString("")

        # password supplied, sha-1 hash it
        m = sha()
        m.update(password)
        pwHash = m.digest()
        self.wbuf.extend(pwHash)

        self.prependLength()
        self.flush()

        # A length, version number, and status code is returned
        try:
            self.bufferForRead()
        except IOError, e:
            print "ERROR: Connection failed. Please check that the host and port are correct."
            raise e
        except socket.timeout:
            raise RuntimeError("Authentication timed out after %d seconds."
                                % self.socket.gettimeout())
        version = self.readByte()
        status = self.readByte()

        if status != 0:
            raise RuntimeError("Authentication failed.")

        self.readInt32()
        self.readInt64()
        self.readInt64()
        self.readInt32()
        for x in range(self.readInt32()):
            self.readByte()

    def setInputByteOrder(self, bom):
        # assuming bom is high bit set?
        if bom == 1:
            self.inputBOM = self.LITTLE_ENDIAN
        else:
            self.inputBOM = self.BIG_ENDIAN

        # recompile the structs
        self.__compileStructs()

    def prependLength(self):
        # write 32 bit array length at offset 0, NOT including the
        # size of this length preceding value. This value is written
        # in the network order.
        ttllen = self.wbuf.buffer_info()[1] * self.wbuf.itemsize
        lenBytes = struct.pack(self.inputBOM + 'i', ttllen)
        map(lambda x: self.wbuf.insert(0, x), lenBytes[::-1])

    def size(self):
        """Returns the size of the write buffer.
        """

        return (self.wbuf.buffer_info()[1] * self.wbuf.itemsize)

    def flush(self):
        if self.socket is None:
            print "ERROR: not connected to server."
            exit(-1)

        if self.dump_file != None:
            self.dump_file.write(self.wbuf)
            self.dump_file.write("\n")
        self.socket.sendall(self.wbuf.tostring())
        self.wbuf = array.array('c')

    def bufferForRead(self):
        if self.socket is None:
            print "ERROR: not connected to server."
            exit(-1)

        # fully buffer a new length preceded message from socket
        # read the length. the read until the buffer is completed.
        responseprefix = ""
        while (len(responseprefix) < 4):
            responseprefix += self.socket.recv(4 - len(responseprefix))
            if responseprefix == "":
                raise IOError("Connection broken")
        if self.dump_file != None:
            self.dump_file.write(responseprefix)
        responseLength = struct.unpack(self.int32Type(1), responseprefix)[0]
        self.read_buffer.clear()
        remaining = responseLength
        while remaining > 0:
            message = self.socket.recv(remaining)
            self.read_buffer.append(message)
            remaining = responseLength - self.read_buffer.buffer_length()
        if not self.dump_file is None:
            self.dump_file.write(self.read_buffer.get_buffer())
            self.dump_file.write("\n")

    def read(self, type):
        if type not in self.READER:
            print "ERROR: can't read wire type(", type, ") yet."
            exit(-2)

        return self.READER[type]()

    def write(self, type, value):
        if type not in self.WRITER:
            print "ERROR: can't write wire type(", type, ") yet."
            exit(-2)

        return self.WRITER[type](value)

    def readWireType(self):
        type = self.readByte()
        return self.read(type)

    def writeWireType(self, type, value):
        if type not in self.WRITER:
            print "ERROR: can't write wire type(", type, ") yet."
            exit(-2)

        self.writeByte(type)
        return self.write(type, value)

    def getRawBytes(self):
        return self.wbuf

    def writeRawBytes(self, value):
        """Appends the given raw bytes to the end of the write buffer.
        """

        self.wbuf.extend(value)

    def __str__(self):
        return repr(self.wbuf)

    def readArray(self, type):
        if type not in self.ARRAY_READER:
            print "ERROR: can't read wire type(", type, ") yet."
            exit(-2)

        return self.ARRAY_READER[type]()

    def readNull(self):
        return None

    def writeNull(self, value):
        return

    def writeArray(self, type, array):
        if (not array) or (len(array) == 0) or (not type):
            return

        if type not in self.ARRAY_READER:
            print "ERROR: Unsupported date type (", type, ")."
            exit(-2)

        # serialize arrays of bytes as larger values to support
        # strings and varbinary input
        if type != FastSerializer.VOLTTYPE_TINYINT:
            self.writeInt16(len(array))
        else:
            self.writeInt32(len(array))

        for i in array:
            self.WRITER[type](i)

    def writeWireTypeArray(self, type, array):
        if type not in self.ARRAY_READER:
            print "ERROR: can't write wire type(", type, ") yet."
            exit(-2)

        self.writeByte(type)
        self.writeArray(type, array)

    # byte
    def readByteArrayContent(self, cnt):
        offset = cnt * struct.calcsize('b')
        return self.read_buffer.unpack(self.byteType(cnt), offset)

    def readByteArray(self):
        length = self.readInt32()
        val = self.readByteArrayContent(length)
        val = map(self.NullCheck[self.VOLTTYPE_TINYINT], val)
        return val

    def readByte(self):
        val = self.readByteArrayContent(1)[0]
        return self.NullCheck[self.VOLTTYPE_TINYINT](val)

    def readByteRaw(self):
        return self.readByteArrayContent(1)[0]

    def writeByte(self, value):
        if value == None:
            val = self.__class__.NULL_TINYINT_INDICATOR
        else:
            val = value
        self.wbuf.extend(struct.pack(self.byteType(1), val))

    # int16
    def readInt16ArrayContent(self, cnt):
        offset = cnt * struct.calcsize('h')
        return self.read_buffer.unpack(self.int16Type(cnt), offset)

    def readInt16Array(self):
        length = self.readInt16()
        val = self.readInt16ArrayContent(length)
        val = map(self.NullCheck[self.VOLTTYPE_SMALLINT], val)
        return val

    def readInt16(self):
        val = self.readInt16ArrayContent(1)[0]
        return self.NullCheck[self.VOLTTYPE_SMALLINT](val)

    def writeInt16(self, value):
        if value == None:
            val = self.__class__.NULL_SMALLINT_INDICATOR
        else:
            val = value
        self.wbuf.extend(struct.pack(self.int16Type(1), val))

    # int32
    def readInt32ArrayContent(self, cnt):
        offset = cnt * struct.calcsize('i')
        return self.read_buffer.unpack(self.int32Type(cnt), offset)

    def readInt32Array(self):
        length = self.readInt16()
        val = self.readInt32ArrayContent(length)
        val = map(self.NullCheck[self.VOLTTYPE_INTEGER], val)
        return val

    def readInt32(self):
        val = self.readInt32ArrayContent(1)[0]
        return self.NullCheck[self.VOLTTYPE_INTEGER](val)

    def writeInt32(self, value):
        if value == None:
            val = self.__class__.NULL_INTEGER_INDICATOR
        else:
            val = value
        self.wbuf.extend(struct.pack(self.int32Type(1), val))

    # int64
    def readInt64ArrayContent(self, cnt):
        offset = cnt * struct.calcsize('q')
        return self.read_buffer.unpack(self.int64Type(cnt), offset)

    def readInt64Array(self):
        length = self.readInt16()
        val = self.readInt64ArrayContent(length)
        val = map(self.NullCheck[self.VOLTTYPE_BIGINT], val)
        return val

    def readInt64(self):
        val = self.readInt64ArrayContent(1)[0]
        return self.NullCheck[self.VOLTTYPE_BIGINT](val)

    def writeInt64(self, value):
        if value == None:
            val = self.__class__.NULL_BIGINT_INDICATOR
        else:
            val = value
        self.wbuf.extend(struct.pack(self.int64Type(1), val))

    # float64
    def readFloat64ArrayContent(self, cnt):
        offset = cnt * struct.calcsize('d')
        return self.read_buffer.unpack(self.float64Type(cnt), offset)

    def readFloat64Array(self):
        length = self.readInt16()
        val = self.readFloat64ArrayContent(length)
        val = map(self.NullCheck[self.VOLTTYPE_FLOAT], val)
        return val

    def readFloat64(self):
        val = self.readFloat64ArrayContent(1)[0]
        return self.NullCheck[self.VOLTTYPE_FLOAT](val)

    def writeFloat64(self, value):
        if value == None:
            val = self.__class__.NULL_FLOAT_INDICATOR
        else:
            val = value
        # work-around for python 2.4
        tmp = array.array("d", [val])
        if self.inputBOM != self.localBOM:
            tmp.byteswap()
        self.wbuf.extend(tmp.tostring())

    # string
    def readStringContent(self, cnt):
        if cnt == 0:
            return ""

        offset = cnt * struct.calcsize('c')
        val = self.read_buffer.unpack(self.stringType(cnt), offset)
        return val[0].decode("utf-8")

    def readString(self):
        # length preceeded (4 byte value) string
        length = self.readInt32()
        if self.NullCheck[self.VOLTTYPE_STRING](length) == None:
            return None
        return self.readStringContent(length)

    def readStringArray(self):
        retval = []
        cnt = self.readInt16()

        for i in xrange(cnt):
            retval.append(self.readString())

        return tuple(retval)

    def writeString(self, value):
        if value is None:
            self.writeInt32(self.NULL_STRING_INDICATOR)
            return

        encoded_value = value.encode("utf-8")
        self.writeInt32(len(encoded_value))
        self.wbuf.extend(encoded_value)

    # varbinary
    def readVarbinaryContent(self, cnt):
        if cnt == 0:
            return array.array('c', [])

        offset = cnt * struct.calcsize('c')
        val = self.read_buffer.unpack(self.varbinaryType(cnt), offset)

        return array.array('c', val[0])

    def readVarbinary(self):
        # length preceeded (4 byte value) string
        length = self.readInt32()
        if self.NullCheck[self.VOLTTYPE_VARBINARY](length) == None:
            return None
        return self.readVarbinaryContent(length)

    def writeVarbinary(self, value):
        if value is None:
            self.writeInt32(self.NULL_STRING_INDICATOR)
            return

        self.writeInt32(len(value))
        self.wbuf.extend(value)

    # date
    # The timestamp we receive from the server is a 64-bit integer representing
    # microseconds since the epoch. It will be converted to a datetime object in
    # the local timezone.
    def readDate(self):
        raw = self.readInt64()
        if raw == None:
            return None
        # microseconds before or after Jan 1, 1970 UTC
        return datetime.datetime.fromtimestamp(raw/1000000.0)

    def readDateArray(self):
        retval = []
        raw = self.readInt64Array()

        for i in raw:
            val = None
            if i != None:
                val = datetime.datetime.fromtimestamp(i/1000000.0)
            retval.append(val)

        return tuple(retval)

    def writeDate(self, value):
        if value is None:
            val = self.__class__.NULL_BIGINT_INDICATOR
        else:
            seconds = int(value.strftime("%s"))
            val = seconds * 1000000 + value.microsecond
        self.wbuf.extend(struct.pack(self.int64Type(1), val))

    def readDecimal(self):
        offset = 16 * struct.calcsize('b')
        if self.NullCheck[self.VOLTTYPE_DECIMAL](self.read_buffer.read(offset)) == None:
            self.read_buffer.shift(offset)
            return None
        val = list(self.read_buffer.unpack(self.ubyteType(16), offset))
        mostSignificantBit = 1 << 7
        isNegative = (val[0] & mostSignificantBit) != 0
        unscaledValue = -(val[0] & mostSignificantBit) << 120
        # Clear the highest bit
        # Unleash the powers of the butterfly
        val[0] &= ~mostSignificantBit
        # Get the 2's complement
        for x in xrange(16):
            unscaledValue += val[x] << ((15 - x) * 8)
        unscaledValue = map(lambda x: int(x), str(abs(unscaledValue)))
        return decimal.Decimal((isNegative, tuple(unscaledValue),
                                -self.__class__.DEFAULT_DECIMAL_SCALE))

    def readDecimalArray(self):
        retval = []
        cnt = self.readInt16()
        for i in xrange(cnt):
            retval.append(self.readDecimal())
        return tuple(retval)

    def __intToBytes(self, value, sign):
        value_bytes = ""
        if sign == 1:
            value = ~value + 1      # 2's complement
        # Turn into byte array
        while value != 0 and value != -1:
            byte = value & 0xff
            # flip the high order bits to 1 only if the number is negative and
            # this is the highest order byte
            if value >> 8 == 0 and sign == 1:
                mask = 1 << 7
                while mask > 0 and (byte & mask) == 0:
                    byte |= mask
                    mask >> 1
            value_bytes = struct.pack(self.ubyteType(1), byte) + value_bytes
            value = value >> 8
        if len(value_bytes) > 16:
            raise ValueError("Precision of this decimal is >38 digits");
        if sign == 1:
            ret = struct.pack(self.ubyteType(1), 0xff)
        else:
            ret = struct.pack(self.ubyteType(1), 0)
        # Pad it
        ret *= 16 - len(value_bytes)
        ret += value_bytes
        return ret

    def writeDecimal(self, num):
        if num is None:
            self.wbuf.extend(self.NULL_DECIMAL_INDICATOR)
            return
        if not isinstance(num, decimal.Decimal):
            raise TypeError("num must be of the type decimal.Decimal")
        (sign, digits, exponent) = num.as_tuple()
        precision = len(digits)
        scale = -exponent
        if (scale > self.__class__.DEFAULT_DECIMAL_SCALE):
            raise ValueError("Scale of this decimal is %d and the max is 12"
                             % (scale))
        rest = precision - scale
        if rest > 26:
            raise ValueError("Precision to the left of the decimal point is %d"
                             " and the max is 26" % (rest))
        scale_factor = self.__class__.DEFAULT_DECIMAL_SCALE - scale
        unscaled_int = int(decimal.Decimal((0, digits, scale_factor)))
        data = self.__intToBytes(unscaled_int, sign)
        self.wbuf.extend(data)

    def writeDecimalString(self, num):
        if num is None:
            self.writeString(None)
            return
        if not isinstance(num, decimal.Decimal):
            raise TypeError("num must be of type decimal.Decimal")
        self.writeString(num.to_eng_string())

    # cash!
    def readMoney(self):
        # money-unit * 10,000
        return self.readInt64()

class VoltColumn:
    "definition of one VoltDB table column"
    def __init__(self, fser = None, type = None, name = None):
        if fser != None:
            self.type = fser.readByte()
        elif type != None and name != None:
            self.type = type
            self.name = name

    def __str__(self):
        # If the name is empty, use the default "modified tuples". Has to do
        # this because HSQLDB doesn't return a column name if the table is
        # empty.
        return "(%s: %d)" % (self.name and self.name or "modified tuples",
                             self.type)

    def __eq__(self, other):
        # For now, if we've been through the query on a column with no name,
        # just assume that there's no way the types are matching up cleanly
        # and there ain't no one for to give us no pain
        if (not self.name or not other.name):
            return True
        return (self.type == other.type and self.name == other.name)

    def readName(self, fser):
        self.name = fser.readString()

    def writeType(self, fser):
        fser.writeByte(self.type)

    def writeName(self, fser):
        fser.writeString(self.name)

class VoltTable:
    "definition and content of one VoltDB table"
    def __init__(self, fser):
        self.fser = fser
        self.columns = []  # column defintions
        self.tuples = []

    def __str__(self):
        result = ""

        result += "column count: %d\n" % (len(self.columns))
        result += "row count: %d\n" % (len(self.tuples))
        result += "cols: "
        result += ", ".join(map(lambda x: str(x), self.columns))
        result += "\n"
        result += "rows -\n"
        result += "\n".join(map(lambda x:
                                    str(map(lambda y: if_else(y == None, "NULL", y),
                                            x)), self.tuples))

        return result

    def __getstate__(self):
        return (self.columns, self.tuples)

    def __setstate__(self, state):
        self.fser = None
        self.columns, self.tuples = state

    def __eq__(self, other):
        if len(self.tuples) > 0:
            return (self.columns == other.columns) and \
                (self.tuples == other.tuples)
        return (self.tuples == other.tuples)

    # The VoltTable is always serialized in big-endian order.
    #
    # How to read a table off the wire.
    # 1. Read the length of the whole table
    # 2. Read the columns
    #    a. read the column header size
    #    a. read the column count
    #    b. read column definitions.
    # 3. Read the tuples count.
    #    a. read the row count
    #    b. read tuples recording string lengths
    def readFromSerializer(self):
        # 1.
        tablesize = self.fser.readInt32()
        # 2.
        headersize = self.fser.readInt32()
        statuscode = self.fser.readByte()
        columncount = self.fser.readInt16()
        for i in xrange(columncount):
            column = VoltColumn(fser = self.fser)
            self.columns.append(column)
        map(lambda x: x.readName(self.fser), self.columns)

        # 3.
        rowcount = self.fser.readInt32()
        for i in xrange(rowcount):
            rowsize = self.fser.readInt32()
            # list comprehension: build list by calling read for each column in
            # row/tuple
            row = [self.fser.read(self.columns[j].type)
                   for j in xrange(columncount)]
            self.tuples.append(row)

        return self

    def writeToSerializer(self):
        table_fser = FastSerializer()

        # We have to pack the header into a buffer first so that we can
        # calculate the size
        header_fser = FastSerializer()

        header_fser.writeByte(0)
        header_fser.writeInt16(len(self.columns))
        map(lambda x: x.writeType(header_fser), self.columns)
        map(lambda x: x.writeName(header_fser), self.columns)

        table_fser.writeInt32(header_fser.size() - 4)
        table_fser.writeRawBytes(header_fser.getRawBytes())

        table_fser.writeInt32(len(self.tuples))
        for i in self.tuples:
            row_fser = FastSerializer()

            map(lambda x: row_fser.write(self.columns[x].type, i[x]),
                xrange(len(i)))

            table_fser.writeInt32(row_fser.size())
            table_fser.writeRawBytes(row_fser.getRawBytes())

        table_fser.prependLength()
        self.fser.writeRawBytes(table_fser.getRawBytes())


class VoltException:
    # Volt SerializableException enumerations
    VOLTEXCEPTION_NONE = 0
    VOLTEXCEPTION_EEEXCEPTION = 1
    VOLTEXCEPTION_SQLEXCEPTION = 2
    VOLTEXCEPTION_CONSTRAINTFAILURE = 3
    VOLTEXCEPTION_GENERIC = 4

    def __init__(self, fser):
        self.type = self.VOLTEXCEPTION_NONE
        self.typestr = "None"
        self.message = ""

        if fser != None:
            self.deserialize(fser)

    def deserialize(self, fser):
        self.length = fser.readInt32()
        if self.length == 0:
            self.type = self.VOLTEXCEPTION_NONE
            return
        self.type = fser.readByte()
        # quick and dirty exception skipping
        if self.type == self.VOLTEXCEPTION_NONE:
            return

        self.message = []
        self.message_len = fser.readInt32()
        for i in xrange(0, self.message_len):
            self.message.append(chr(fser.readByte()))
        self.message = ''.join(self.message)

        if self.type == self.VOLTEXCEPTION_GENERIC:
            self.typestr = "Generic"
        elif self.type == self.VOLTEXCEPTION_EEEXCEPTION:
            self.typestr = "EE Exception"
            # serialized size from EEException.java is 4 bytes
            self.error_code = fser.readInt32()
        elif self.type == self.VOLTEXCEPTION_SQLEXCEPTION or \
                self.type == self.VOLTEXCEPTION_CONSTRAINTFAILURE:
            self.sql_state_bytes = []
            for i in xrange(0, 5):
                self.sql_state_bytes.append(chr(fser.readByte()))
            self.sql_state_bytes = ''.join(self.sql_state_bytes)

            if self.type == self.VOLTEXCEPTION_SQLEXCEPTION:
                self.typestr = "SQL Exception"
            else:
                self.typestr = "Constraint Failure"
                self.constraint_type = fser.readInt32()
                self.table_name = fser.readString()
                self.buffer_size = fser.readInt32()
                self.buffer = []
                for i in xrange(0, self.buffer_size):
                    self.buffer.append(fser.readByte())
        else:
            for i in xrange(0, self.length - 3 - 2 - self.message_len):
                fser.readByte()
            print "Python client deserialized unknown VoltException."

    def __str__(self):
        msgstr = "VoltException: type: %s\n" % self.typestr
        if self.type == self.VOLTEXCEPTION_EEEXCEPTION:
            msgstr += "  Error code: %d\n" % self.error_code
        elif self.type == self.VOLTEXCEPTION_SQLEXCEPTION:
            msgstr += "  SQL code: "
            msgstr += self.sql_state_bytes
        elif self.type == self.VOLTEXCEPTION_SQLEXCEPTION:
            msgstr += "  Constraint violation type: %d\n" + self.constraint_type
            msgstr += "  on table: %s\n" + self.table_name
        return msgstr

class VoltResponse:
    "VoltDB called procedure response (ClientResponse.java)"
    def __init__(self, fser):
        self.fser = fser
        self.version = -1
        self.clientHandle = -1
        self.status = -1
        self.statusString = ""
        self.appStatus = -1
        self.appStatusString = ""
        self.roundtripTime = -1
        self.exception = None
        self.tables = None

        if fser != None:
            self.deserialize(fser)

    def deserialize(self, fser):
        # serialization order: response-length, status, roundtripTime, exception,
        # tables[], info, id.
        fser.bufferForRead()
        self.version = fser.readByte()
        self.clientHandle = fser.readInt64()
        presentFields = fser.readByteRaw();
        self.status = fser.readByte()
        if presentFields & (1 << 5) != 0:
            self.statusString = fser.readString()
        else:
            self.statusString = None
        self.appStatus = fser.readByte()
        if presentFields & (1 << 7) != 0:
            self.appStatusString = fser.readString()
        else:
            self.appStatusString = None
        self.roundtripTime = fser.readInt32()
        if presentFields & (1 << 6) != 0:
            self.exception = VoltException(fser)
        else:
            self.exception = None

        # tables[]
        tablecount = fser.readInt16()
        self.tables = []
        for i in xrange(tablecount):
            table = VoltTable(fser)
            self.tables.append(table.readFromSerializer())

    def __str__(self):
        tablestr=""
        if self.tables != None:
            tablestr = "\n\n".join([str(i) for i in self.tables])
        if self.exception is None:
            return "Status: %d\nInformation: %s\n%s" % (self.status,
                                                        self.statusString,
                                                        tablestr)
        else:
            msgstr = "Status: %d\nInformation: %s\n%s\n" % (self.status,
                                                            self.statusString,
                                                            tablestr)
            msgstr += "Exception: %s" % (self.exception)
            return msgstr

class VoltProcedure:
    "VoltDB called procedure interface"
    def __init__(self, fser, name, paramtypes = []):
        self.fser = fser             # FastSerializer object
        self.name = name             # procedure class name
        self.paramtypes = paramtypes # list of fser.WIRE_* values

    def call(self, params = None, response = True, timeout = None):
        self.fser.writeByte(0)  # version number
        self.fser.writeString(self.name)
        self.fser.writeInt64(1)            # client handle
        self.fser.writeInt16(len(self.paramtypes))
        for i in xrange(len(self.paramtypes)):
            try:
                iter(params[i]) # Test if this is an array
                if isinstance(params[i], basestring): # String is a special case
                    raise TypeError

                self.fser.writeByte(FastSerializer.ARRAY)
                self.fser.writeByte(self.paramtypes[i])
                self.fser.writeArray(self.paramtypes[i], params[i])
            except TypeError:
                self.fser.writeWireType(self.paramtypes[i], params[i])
        self.fser.prependLength() # prepend the total length of the invocation
        self.fser.flush()

        # The timeout in effect for the procedure call is the timeout argument
        # if not None or self.procedure_timeout. Exceeding that time will raise
        # a timeout exception. Restores the original timeout value when done.
        # This default argument usage does not allow overriding with None.
        if timeout is None:
            timeout = self.fser.procedure_timeout
        original_timeout = self.fser.socket.gettimeout()
        self.fser.socket.settimeout(timeout)
        try:
            try:
                res = VoltResponse(self.fser)
            except socket.timeout:
                res = VoltResponse(None)
                res.statusString = "timeout: procedure call took longer than %d seconds" % timeout
            except IOError, err:
                res = VoltResponse(None)
                res.statusString = str(err)
        finally:
            self.fser.socket.settimeout(original_timeout)
        return response and res or None
