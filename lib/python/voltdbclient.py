#!/usr/bin/env python3
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

import sys
if sys.hexversion < 0x03060000:
    raise Exception("Python version 3.6 or greater is required.")
import array
import socket
import base64, textwrap
import struct
import datetime
import decimal
import hashlib
import re
import math
import os
import stat

try:
    import ssl
    ssl_available = True
except ImportError as e:
    ssl_available = False
    ssl_exception = e
try:
    import gssapi
    kerberos_available = True
except ImportError as e:
    kerberos_available = False
    kerberos_exception = e
try:
    import jks
    pyjks_available = True
except ImportError as e:
    pyjks_available = False
    pyjks_exception = e

logger = None

def use_logging():
    import logging
    global logger
    logger = logging.getLogger()

def error(text):
    if logger:
        logger.error(text)
    else:
        print(text)

decimal.getcontext().prec = 38

def int16toBytes(val):
    return [val >>  8 & 0xff,
            val >>  0 & 0xff]

def int32toBytes(val):
    return [val >> 24 & 0xff,
            val >> 16 & 0xff,
            val >>  8 & 0xff,
            val >>  0 & 0xff]

def int64toBytes(val):
    return [val >> 56 & 0xff,
            val >> 48 & 0xff,
            val >> 40 & 0xff,
            val >> 32 & 0xff,
            val >> 24 & 0xff,
            val >> 16 & 0xff,
            val >>  8 & 0xff,
            val >>  0 & 0xff]

def isNaN(d):
    # Per IEEE 754, 'NaN == NaN' must be false,
    # so we cannot check for simple equality
    if d == None:
        return False
    else: # routine misnamed, returns true for 'Inf' too
        return math.isnan(d) or math.isinf(d)

class ReadBuffer(object):
    """
    Read buffer management class.
    """
    def __init__(self):
        self.clear()

    def clear(self):
        self._buf = bytes()
        self._off = 0

    def buffer_length(self):
        return len(self._buf)

    def remaining(self):
        return (len(self._buf) - self._off)

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
        except struct.error as e:
            error('Exception unpacking %d bytes using format "%s": %s' % (size, format, str(e)))
            raise e
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
    VOLTTYPE_GEOGRAPHY_POINT = 26
    VOLTTYPE_GEOGRAPHY = 27

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

    # protocol constants
    AUTH_HANDSHAKE_VERSION = 2
    AUTH_SERVICE_NAME = 4
    AUTH_HANDSHAKE = 5

    # procedure call result codes
    PROC_OK = 0

    # there are assumptions here about datatype sizes which are
    # machine dependent. the program exits with an error message
    # if these assumptions are not true. it is further assumed
    # that host order is little endian. See isNaN().

    # default ssl configuration
    if (ssl_available):
        DEFAULT_SSL_CONFIG = {
        'keyfile': None,
        'certfile': None,
        'cert_reqs': ssl.CERT_NONE,
        'ca_certs': None,
        'do_handshake_on_connect': True
        }
    else:
        DEFAULT_SSL_CONFIG = {}

    def __init__(self, host = None, port = 21212, usessl = False,
                 username = "", password = "",
                 kerberos = False,
                 dump_file_path = None,
                 connect_timeout = 8,
                 procedure_timeout = None,
                 default_timeout = None,
                 ssl_config_file = None,
                 ssl_config = DEFAULT_SSL_CONFIG):
        """
        :param host: host string for connection or None
        :param port: port for connection or None
        :param usessl: switch for use ssl or not
        :param username: authentication user name for connection or None
        :param password: authentication password for connection or None
        :param kerberos: use Kerberos authentication
        :param dump_file_path: path to optional dump file or None
        :param connect_timeout: timeout (secs) or None for authentication (default=8)
        :param procedure_timeout: timeout (secs) or None for procedure calls (default=None)
        :param default_timeout: default timeout (secs) or None for all other operations (default=None)
        :param ssl_config_file: config file that defines java keystore and truststore files
        """
        # connect a socket to host, port and get a file object
        self.wbuf = array.array('B')
        self.host = host
        self.port = port
        self.usessl = usessl
        if kerberos is None:
            self.usekerberos = False
        else:
            self.usekerberos = kerberos
        self.kerberosprinciple = None
        self.ssl_config = ssl_config
        self.ssl_config_file = ssl_config_file
        if not dump_file_path is None:
            self.dump_file = open(dump_file_path, "wb")
        else:
            self.dump_file = None
        self.default_timeout = default_timeout
        self.procedure_timeout = procedure_timeout

        self.socket = None
        if self.host != None and self.port != None:
            ai = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM, socket.IPPROTO_TCP, socket.AI_ADDRCONFIG)[0]
            # ai = (family, socktype, proto, canonname, sockaddr)
            ss = socket.socket(ai[0], ai[1], ai[2])
            if self.usessl:
                if ssl_available:
                    self.socket = self.__wrap_socket(ss)
                else:
                    error("ERROR: To use SSL functionality please install the Python ssl module.")
                    raise ssl_exception
            else:
                self.socket = ss
            self.socket.setblocking(1)
            self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
            try:
                self.socket.connect(ai[4])
            except Exception:
                error("ERROR: Failed to connect to %s port %s" % (ai[4][0], ai[4][1]))
                raise
            #if self.usessl:
            #    print 'Cipher suite: ' + str(self.socket.cipher())

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
                       self.VOLTTYPE_DECIMAL: self.readDecimal,
                       self.VOLTTYPE_GEOGRAPHY_POINT: self.readGeographyPoint,
                       self.VOLTTYPE_GEOGRAPHY: self.readGeography}
        self.WRITER = {self.VOLTTYPE_NULL: self.writeNull,
                       self.VOLTTYPE_TINYINT: self.writeByte,
                       self.VOLTTYPE_SMALLINT: self.writeInt16,
                       self.VOLTTYPE_INTEGER: self.writeInt32,
                       self.VOLTTYPE_BIGINT: self.writeInt64,
                       self.VOLTTYPE_FLOAT: self.writeFloat64,
                       self.VOLTTYPE_STRING: self.writeString,
                       self.VOLTTYPE_VARBINARY: self.writeVarbinary,
                       self.VOLTTYPE_TIMESTAMP: self.writeDate,
                       self.VOLTTYPE_DECIMAL: self.writeDecimal,
                       self.VOLTTYPE_GEOGRAPHY_POINT: self.writeGeographyPoint,
                       self.VOLTTYPE_GEOGRAPHY: self.writeGeography}
        self.ARRAY_READER = {self.VOLTTYPE_TINYINT: self.readByteArray,
                             self.VOLTTYPE_SMALLINT: self.readInt16Array,
                             self.VOLTTYPE_INTEGER: self.readInt32Array,
                             self.VOLTTYPE_BIGINT: self.readInt64Array,
                             self.VOLTTYPE_FLOAT: self.readFloat64Array,
                             self.VOLTTYPE_STRING: self.readStringArray,
                             self.VOLTTYPE_TIMESTAMP: self.readDateArray,
                             self.VOLTTYPE_DECIMAL: self.readDecimalArray,
                             self.VOLTTYPE_GEOGRAPHY_POINT: self.readGeographyPointArray,
                             self.VOLTTYPE_GEOGRAPHY: self.readGeographyArray}

        self.__compileStructs()

        # Check if the value of a given type is NULL
        self.NULL_DECIMAL_INDICATOR = \
            self.__intToBytes(self.__class__.NULL_DECIMAL_INDICATOR, 0)
        self.NullCheck = {self.VOLTTYPE_NULL:
                              lambda x: None,
                          self.VOLTTYPE_TINYINT:
                              lambda x: None if x == self.__class__.NULL_TINYINT_INDICATOR else x,
                          self.VOLTTYPE_SMALLINT:
                              lambda x: None if x == self.__class__.NULL_SMALLINT_INDICATOR else x,
                          self.VOLTTYPE_INTEGER:
                              lambda x: None if x == self.__class__.NULL_INTEGER_INDICATOR else x,
                          self.VOLTTYPE_BIGINT:
                              lambda x: None if x == self.__class__.NULL_BIGINT_INDICATOR else x,
                          self.VOLTTYPE_FLOAT:
                              lambda x: None if abs(x - self.__class__.NULL_FLOAT_INDICATOR) < 1e307 else x,
                          self.VOLTTYPE_STRING:
                              lambda x: None if x == self.__class__.NULL_STRING_INDICATOR else x,
                          self.VOLTTYPE_VARBINARY:
                              lambda x: None if x == self.__class__.NULL_STRING_INDICATOR else x,
                          self.VOLTTYPE_DECIMAL:
                              lambda x: None if x == self.NULL_DECIMAL_INDICATOR else x}

        self.read_buffer = ReadBuffer()

        if self.usekerberos:
            if not kerberos_available:
                raise RuntimeError("Requested Kerberos authentication but unable to import the GSSAPI package.")
            if not self.has_ticket():
                raise RuntimeError("Requested Kerberos authentication but no valid ticket found. Authenticate with Kerberos first.")
            assert not self.socket is None
            self.socket.settimeout(connect_timeout)
            self.authenticate(str(self.kerberosprinciple), "")
        elif not username is None and not password is None and not host is None:
            assert not self.socket is None
            self.socket.settimeout(connect_timeout)
            self.authenticate(username, password)

        if self.socket:
            self.socket.settimeout(self.default_timeout)

    # Front end to SSL socket support.
    #
    # The SSL config file contains a sequence of key=value lines which
    # are used to provide the arguments to the SSL wrap_socket call:
    #   Key                Value                    Provides arguments
    #   ------------------ ------------------------ ------------------
    #   keystore           path to JKS keystore     keyfile, certfile
    #   keystorepassword   password for keystore    --
    #   truststore         path to JKS truststore   ca_certs, cert_reqs
    #   truststorepassword password for truststore  --
    #   cacerts            path to PEM cert chain   ca_certs, cert_reqs
    #   ssl_version        ignored                  --
    #
    # Thus keystore identifies the client (rarely needed), whereas truststore
    # and cacerts identify the server. If truststore and cacerts are both
    # specified, cacerts takes precedence.
    #
    # An empty or missing ssl_config_file results in no certificate checks.

    def __wrap_socket(self, ss):
        parsed_config = {}
        jks_config = {}
        if self.ssl_config_file:
            with open(os.path.expandvars(os.path.expanduser(self.ssl_config_file)), 'r') as f:
                for line in f:
                    try:
                        l = line.strip()
                        if l:
                            k, v = l.split('=', 1)
                            parsed_config[k.lower()] = v
                    except:
                        raise ValueError('Malformed line in SSL config: ' + line)

        if ('keystore' in parsed_config and parsed_config['keystore']) or \
           ('truststore' in parsed_config and parsed_config['truststore']):
            self.__convert_jks_files(ss, parsed_config)

        if 'cacerts' in parsed_config and parsed_config['cacerts']:
            self.ssl_config['ca_certs'] = parsed_config['cacerts']
            self.ssl_config['cert_reqs'] = ssl.CERT_REQUIRED

        protocol = ssl.PROTOCOL_TLS

        return ssl.wrap_socket(ss,
                               keyfile=self.ssl_config['keyfile'],
                               certfile=self.ssl_config['certfile'],
                               server_side=False,
                               cert_reqs=self.ssl_config['cert_reqs'],
                               ssl_version=protocol,
                               ca_certs=self.ssl_config['ca_certs'])

    def __get_name_from_path(self, path):
        x = re.sub('/','-', path)
        y = re.sub('^-', '', x)
        tmpdir = os.getenv('TMPDIR', '/tmp')
        return tmpdir + '/' + y

    def __convert_jks_files(self, ss, jks_config):
        if not pyjks_available:
            error("To use Java KeyStore please install the 'pyjks' module")
            raise pyjks_exception

        def write_pem(der_bytes, type, f):
            f.write("-----BEGIN %s-----\n" % type)
            f.write("\r\n".join(textwrap.wrap(base64.b64encode(der_bytes).decode('ascii'), 64)))
            f.write("\n-----END %s-----\n" % type)

        # extract key and certs
        use_key_cert = False
        if 'keystore' in jks_config and jks_config['keystore'] and \
               'keystorepassword' in jks_config and jks_config['keystorepassword']:
            ks = jks.KeyStore.load(jks_config['keystore'], jks_config['keystorepassword'])
            kfname = self.__get_name_from_path(jks_config['keystore'])
            keyfilename = kfname + '.key.pem'
            keyfile = None
            if not os.path.exists(keyfilename):
                keyfile = self.__create(keyfilename)
            certfilename = kfname + '.cert.pem'
            certfile = None
            if not os.path.exists(certfilename):
                certfile = self.__create(certfilename)
            for alias, pk in list(ks.private_keys.items()):
                # print("Private key: %s" % pk.alias)
                if keyfile is not None:
                    if pk.algorithm_oid == jks.util.RSA_ENCRYPTION_OID:
                        write_pem(pk.pkey, "RSA PRIVATE KEY", keyfile)
                    else:
                        write_pem(pk.pkey_pkcs8, "PRIVATE KEY", keyfile)

                if certfile is not None:
                    for c in pk.cert_chain:
                        write_pem(c[1], "CERTIFICATE", certfile)
                use_key_cert = True
            if keyfile is not None:
                keyfile.close()
            if certfile is not None:
                certfile.close()
            if use_key_cert:
                self.ssl_config['keyfile'] = keyfilename
                self.ssl_config['certfile'] = certfilename

        # extract ca certs
        use_ca_cert = False
        if 'truststore' in jks_config and jks_config['truststore'] and \
               'truststorepassword' in jks_config and jks_config['truststorepassword']:
            ts = jks.KeyStore.load(jks_config['truststore'], jks_config['truststorepassword'])
            tfname = self.__get_name_from_path(jks_config['truststore'])
            cafilename = tfname + '.ca.cert.pem'
            cafile = None
            if not os.path.exists(cafilename):
                cafile = self.__create(cafilename)

            for alias, c in list(ts.certs.items()):
                # print("Certificate: %s" % c.alias)
                if cafile is not None:
                    write_pem(c.cert, "CERTIFICATE", cafile)
                    cafile.close()
                use_ca_cert = True
            if use_ca_cert:
                self.ssl_config['ca_certs'] = cafilename
                self.ssl_config['cert_reqs'] = ssl.CERT_REQUIRED

    def __create(self, filename):
        f = open(filename, 'w')
        os.chmod(filename, stat.S_IRUSR|stat.S_IWUSR)
        return f

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
        self.writeByte(1)
        #sha256
        self.writeByte(1)

        # service requested
        if (self.usekerberos):
            self.writeString("kerberos")
        else:
            self.writeString("database")

        if username:
            # utf8 encode supplied username or kerberos principal name
            self.writeString(username)
        else:
            # no username, just output length of 0
            self.writeString("")

        # password supplied, sha-256 hash it
        m = hashlib.sha256()
        encoded_password = password.encode("utf-8")
        m.update(encoded_password)
        pwHash = bytearray(m.digest())
        self.wbuf.extend(pwHash)

        self.prependLength()
        self.flush()

        # A length, version number, and status code is returned
        try:
            self.bufferForRead()
        except IOError as e:
            error("ERROR: Connection failed. Please check that the host, port, and ssl settings are correct.")
            raise e
        except socket.timeout:
            raise RuntimeError("Authentication timed out after %d seconds."
                                % self.socket.gettimeout())
        version = self.readByte()
        status = self.readByte()

        if (version == self.AUTH_HANDSHAKE_VERSION):
            #service name supplied by VoltDB Server
            service_string = self.readString().encode('ascii','ignore')
            try:
                service_name = gssapi.Name(service_string, name_type=gssapi.NameType.kerberos_principal)
                ctx = gssapi.SecurityContext(name=service_name, mech=gssapi.MechType.kerberos)
                in_token = None
                out_token = ctx.step(in_token)
                while not ctx.complete:
                    self.writeByte(self.AUTH_HANDSHAKE_VERSION)
                    self.writeByte(self.AUTH_HANDSHAKE)
                    self.wbuf.extend(out_token)
                    self.prependLength()
                    self.flush()

                    try:
                        self.bufferForRead()
                    except IOError as e:
                        error("ERROR: Connection failed. Please check that the host, port, and ssl settings are correct.")
                        raise e
                    except socket.timeout:
                        raise RuntimeError("Authentication timed out after %d seconds."
                                            % self.socket.gettimeout())
                    version = self.readByte()
                    status = self.readByte()
                    if version != self.AUTH_HANDSHAKE_VERSION or status != self.AUTH_HANDSHAKE:
                        raise RuntimeError("Authentication failed.")

                    in_token = self.readVarbinaryContent(self.read_buffer.remaining()).tobytes()
                    out_token = ctx.step(in_token)

                try:
                    self.bufferForRead()
                except IOError as e:
                    error("ERROR: Connection failed. Please check that the host, port, and ssl settings are correct.")
                    raise e
                except socket.timeout:
                    raise RuntimeError("Authentication timed out after %d seconds."
                                        % self.socket.gettimeout())
                version = self.readByte()
                status = self.readByte()

            except Exception as e:
                raise RuntimeError("Authentication failed.")


        if status != 0:
            raise RuntimeError("Authentication failed.")

        self.readInt32()
        self.readInt64()
        self.readInt64()
        self.readInt32()
        for x in range(self.readInt32()):
            self.readByte()

    def has_ticket(self):
        '''
        Checks to see if the user has a valid ticket.
        '''
        default_cred = None
        retval = False
        try:
            default_cred = gssapi.creds.Credentials(usage='initiate')
            if default_cred.lifetime > 0:
                self.kerberosprinciple = str(default_cred.name)
                retval = True
            else:
                error("ERROR: Kerberos principal found but login expired.")
        except gssapi.raw.misc.GSSError as e:
            error("ERROR: unable to find default principal from Kerberos cache.")
        return retval

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
        lenBytes = int32toBytes(ttllen)
        #lenBytes = struct.pack(self.inputBOM + 'i', ttllen)
        [self.wbuf.insert(0, x) for x in lenBytes[::-1]]

    def size(self):
        """Returns the size of the write buffer.
        """

        return (self.wbuf.buffer_info()[1] * self.wbuf.itemsize)

    def flush(self):
        if self.socket is None:
            error("ERROR: not connected to server.")
            raise IOError("No Connection")

        if self.dump_file != None:
            self.dump_file.write(self.wbuf)
            self.dump_file.write(b"\n")
        self.socket.sendall(self.wbuf.tobytes())
        self.wbuf = array.array('B')

    def bufferForRead(self):
        if self.socket is None:
            error("ERROR: not connected to server.")
            raise IOError("No Connection")

        # fully buffer a new length preceded message from socket
        # read the length. the read until the buffer is completed.
        responseprefix = bytes()
        while (len(responseprefix) < 4):
            responseprefix += self.socket.recv(4 - len(responseprefix))
            if responseprefix == b'':
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
            self.dump_file.write(b"\n")

    def read(self, type):
        if type not in self.READER:
            error("ERROR: can't read wire type(%d) yet." % (type))
            raise IOError("ERROR: can't read wire type(%d) yet." % (type))

        return self.READER[type]()

    def write(self, type, value):
        if type not in self.WRITER:
            error("ERROR: can't write wire type(%d) yet." % (type))
            raise IOError("ERROR: can't write wire type(%d) yet." % (type))

        return self.WRITER[type](value)

    def readWireType(self):
        type = self.readByte()
        return self.read(type)

    def writeWireType(self, type, value):
        if type not in self.WRITER:
            error("ERROR: can't write wire type(%d) yet." % (type))
            raise IOError("ERROR: can't write wire type(%d) yet." % (type))

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
            error("ERROR: can't read wire type(%d) yet." % (type))
            raise IOError("ERROR: can't write wire type(%d) yet." % (type))

        return self.ARRAY_READER[type]()

    def readNull(self):
        return None

    def writeNull(self, value):
        return

    def writeArray(self, type, array):
        if (not array) or (len(array) == 0) or (not type):
            return

        if type not in self.ARRAY_READER:
            error("ERROR: Unsupported date type (%d)." % (type))
            raise IOError("ERROR: Unsupported date type (%d)." % (type))

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
            error("ERROR: can't write wire type(%d) yet." % (type))
            raise IOError("ERROR: Unsupported date type (%d)." % (type))

        self.writeByte(type)
        self.writeArray(type, array)

    # byte
    def readByteArrayContent(self, cnt):
        offset = cnt * struct.calcsize('b')
        return self.read_buffer.unpack(self.byteType(cnt), offset)

    def readByteArray(self):
        length = self.readInt32()
        val = self.readByteArrayContent(length)
        val = list(map(self.NullCheck[self.VOLTTYPE_TINYINT], val))
        return val

    def readByte(self):
        val = self.readByteArrayContent(1)[0]
        return self.NullCheck[self.VOLTTYPE_TINYINT](val)

    def readByteRaw(self):
        val = self.readByteArrayContent(1)[0]
        if val > 127:
            return val - 256
        else:
            return val

    def writeByte(self, value):
        if value == None:
            value = self.__class__.NULL_TINYINT_INDICATOR
        if value < 0:
            value += 256
        self.wbuf.append(value)

    # int16
    def readInt16ArrayContent(self, cnt):
        offset = cnt * struct.calcsize('h')
        return self.read_buffer.unpack(self.int16Type(cnt), offset)

    def readInt16Array(self):
        length = self.readInt16()
        val = self.readInt16ArrayContent(length)
        val = list(map(self.NullCheck[self.VOLTTYPE_SMALLINT], val))
        return val

    def readInt16(self):
        val = self.readInt16ArrayContent(1)[0]
        return self.NullCheck[self.VOLTTYPE_SMALLINT](val)

    def writeInt16(self, value):
        if value == None:
            val = self.__class__.NULL_SMALLINT_INDICATOR
        else:
            val = value
        self.wbuf.extend(int16toBytes(val))

    # int32
    def readInt32ArrayContent(self, cnt):
        offset = cnt * struct.calcsize('i')
        return self.read_buffer.unpack(self.int32Type(cnt), offset)

    def readInt32Array(self):
        length = self.readInt16()
        val = self.readInt32ArrayContent(length)
        val = list(map(self.NullCheck[self.VOLTTYPE_INTEGER], val))
        return val

    def readInt32(self):
        val = self.readInt32ArrayContent(1)[0]
        return self.NullCheck[self.VOLTTYPE_INTEGER](val)

    def writeInt32(self, value):
        if value == None:
            val = self.__class__.NULL_INTEGER_INDICATOR
        else:
            val = value
        self.wbuf.extend(int32toBytes(val))

    # int64
    def readInt64ArrayContent(self, cnt):
        offset = cnt * struct.calcsize('q')
        return self.read_buffer.unpack(self.int64Type(cnt), offset)

    def readInt64Array(self):
        length = self.readInt16()
        val = self.readInt64ArrayContent(length)
        val = list(map(self.NullCheck[self.VOLTTYPE_BIGINT], val))
        return val

    def readInt64(self):
        val = self.readInt64ArrayContent(1)[0]
        return self.NullCheck[self.VOLTTYPE_BIGINT](val)

    def writeInt64(self, value):
        if value == None:
            val = self.__class__.NULL_BIGINT_INDICATOR
        else:
            val = value
        self.wbuf.extend(int64toBytes(val))

    # float64
    def readFloat64ArrayContent(self, cnt):
        offset = cnt * struct.calcsize('d')
        return self.read_buffer.unpack(self.float64Type(cnt), offset)

    def readFloat64Array(self):
        length = self.readInt16()
        val = self.readFloat64ArrayContent(length)
        val = list(map(self.NullCheck[self.VOLTTYPE_FLOAT], val))
        return val

    def readFloat64(self):
        val = self.readFloat64ArrayContent(1)[0]
        return self.NullCheck[self.VOLTTYPE_FLOAT](val)

    def writeFloat64(self, value):
        if value == None:
            val = self.__class__.NULL_FLOAT_INDICATOR
        else:
            val = float(value)
        ba = bytearray(struct.pack(self.float64Type(1), val))
        self.wbuf.extend(ba)

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

        for i in range(cnt):
            retval.append(self.readString())

        return tuple(retval)

    def writeString(self, value):
        if value is None:
            self.writeInt32(self.NULL_STRING_INDICATOR)
            return

        encoded_value = value.encode("utf-8")
        ba = bytearray(encoded_value)
        self.writeInt32(len(encoded_value))
        self.wbuf.extend(ba)

    # varbinary
    def readVarbinaryContent(self, cnt):
        if cnt == 0:
            return array.array('B', [])

        offset = cnt * struct.calcsize('c')
        val = self.read_buffer.unpack(self.varbinaryType(cnt), offset)

        return array.array('B', val[0])

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
        self.wbuf.extend(int64toBytes(val))

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
        for x in range(16):
            unscaledValue += val[x] << ((15 - x) * 8)
        unscaledValue = [int(x) for x in str(abs(unscaledValue))]
        return decimal.Decimal((isNegative, tuple(unscaledValue),
                                -self.__class__.DEFAULT_DECIMAL_SCALE))

    def readDecimalArray(self):
        retval = []
        cnt = self.readInt16()
        for i in range(cnt):
            retval.append(self.readDecimal())
        return tuple(retval)

    def __intToBytes(self, value, sign):
        value_bytes = bytes()
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

    def readGeographyPoint(self):
        # returns a tuple of a pair of doubles representing lat,long
        lng = self.readFloat64()
        lat = self.readFloat64()
        if (lat == Geography.NULL_COORD) and (lon == Geography.NULL_COORD):
            return None
        return (lng, lat)

    def readGeographyPointArray(self):
        retval = []
        cnt = self.readInt16()
        for i in range(cnt):
            retval.append(self.readGeographyPoint())
        return tuple(retval)

    def writeGeographyPoint(self, point):
        if point is None:
            self.writeFloat64(Geography.NULL_COORD)
            self.writeFloat64(Geography.NULL_COORD)
            return
        if not isinstance(num, tuple):
            raise TypeError("point must be a 2-tuple of floats")
        if len(tuple) != 2:
            raise TypeError("point must be a 2-tuple of floats")
        self.writeFloat64(point[0])
        self.writeFloat64(point[1])

    def readGeography(self):
        return Geography.unflatten(self)

    def readGeographyArray(self):
        retval = []
        cnt = self.readInt16()
        for i in range(cnt):
            retval.append(Geography.unflatten(self))
        return tuple(retval)

    def writeGeography(self, geo):
        if geo is None:
            writeInt32(NULL_STRING_INDICATOR)
        else:
            geo.flatten(self)

class XYZPoint(object):
    """
    Google's S2 geometry library uses (x, y, z) representation of polygon vertices,
    But the interface we expose to users is (lat, lng).  This class is the
    internal representation for vertices.
    """
    def __init__(self, x, y, z):
        self.x = x
        self.y = y
        self.z = z

    @staticmethod
    def fromGeographyPoint(p):
        latRadians = p[0] * (math.pi / 180) # AKA phi
        lngRadians = p[1] * (math.pi / 180) # AKA theta

        cosPhi = math.cos(latRadians)
        x = math.cos(lngRadians) * cosPhi
        y = math.sin(lngRadians) * cosPhi
        z = math.sin(latRadians)

        return XYZPoint(x, y, z)

    def toGeogrpahyPoint(self):
        latRadians = math.atan2(self.z, math.sqrt(self.x * self.x + self.y * self.y))
        lngRadians = math.atan2(self.y, self.x)

        latDegrees = latRadians * (180 / math.pi)
        lngDegrees = lngRadians * (180 / math.pi)
        return (lngDegrees, latDegrees)

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(self, other.__class__):
            return self.__dict__ == other.__dict__
        return False

    def __ne__(self, other):
        """Overrides the default implementation (unnecessary in Python 3)"""
        return not self.__eq__(other)

    def __str__(self):
        p = self.toGeogrpahyPoint()
        return "(%s,%s)" % (p[0], p[1])

class Geography(object):
    """
    S2-esque geography element representing a polygon for now
    """

    EPSILON = 1.0e-12
    NULL_COORD = 360.0

    def __init__(self, loops=[]):
        self.loops = loops

    # Serialization format for polygons.
    #
    # This is the format used by S2 in the EE.  Most of the
    # metadata (especially lat/lng rect bounding boxes) are
    # ignored here in Java.
    #
    # 1 byte       encoding version
    # 1 byte       boolean owns_loops
    # 1 byte       boolean has_holes
    # 4 bytes      number of loops
    #   And then for each loop:
    #     1 byte       encoding version
    #     4 bytes      number of vertices
    #     ((number of vertices) * sizeof(double) * 3) bytes    vertices as XYZPoints
    #     1 byte       boolean origin_inside
    #     4 bytes      depth (nesting level of loop)
    #     33 bytes     bounding box
    # 33 bytes     bounding box
    #
    # We use S2 in the EE for all geometric computation, so polygons sent to
    # the EE will be missing bounding box and other info.  We indicate this
    # by passing INCOMPLETE_ENCODING_FROM_JAVA in the version field.  This
    # tells the EE to compute bounding boxes and other metadata before storing
    # the polygon to memory.

    # for encoding byte + lat min, lat max, lng min, lng max as doubles
    BOUND_LENGTH_IN_BYTES = 33
    POLYGON_OVERHEAD_IN_BYTES = 7 + BOUND_LENGTH_IN_BYTES
    # 1 byte     for encoding version
    # 4 bytes    for number of vertices
    # number of vertices * 8 * 3  bytes  for vertices as XYZPoints
    # 1 byte     for origin_inside_
    # 4 bytes    for depth_
    # length of bound
    LOOP_OVERHEAD_IN_BYTES = 10 + BOUND_LENGTH_IN_BYTES
    VERTEX_SIZE_IN_BYTES = 24

    def serializedSize(self):
        length = POLYGON_OVERHEAD_IN_BYTES
        for loop in self.loops:
            length += loopSerializedSize(loop);
        return length

    @staticmethod
    def loopSerializedSize(loop):
        LOOP_OVERHEAD_IN_BYTES + (len(loop) * VERTEX_SIZE_IN_BYTES)

    @staticmethod
    def unflatten(fs):
        length = fs.readInt32() # size
        if (length == fs.NULL_STRING_INDICATOR):
            return None

        version = fs.readByteRaw() # encoding version
        fs.readByteRaw() # owns loops
        fs.readByteRaw() # has holes
        numLoops = fs.readInt32()
        loops = []
        indexOfOuterRing = 0
        for i in range(numLoops):
            depth, loop = Geography.__unflattenLoop(fs)
            if depth == 0:
                indexOfOuterRing = i
            loops.append(loop)

        Geography.__unflattenBound(fs)

        return Geography(loops);

    @staticmethod
    def __unflattenLoop(fs):
        # 1 byte     for encoding version
        # 4 bytes    for number of vertices
        # number of vertices * 8 * 3  bytes  for vertices as XYZPoints
        # 1 byte     for origin_inside_
        # 4 bytes    for depth_
        # length of bound

        loop = []
        fs.readByteRaw() # encoding version
        numVertices = fs.readInt32()
        for i in range(numVertices):
            x = fs.readFloat64()
            y = fs.readFloat64()
            z = fs.readFloat64()
            loop.append(XYZPoint(x, y, z))

        fs.readByteRaw() # origin_inside_
        depth = fs.readInt32() # depth
        Geography.__unflattenBound(fs);
        return (depth, loop)

    @staticmethod
    def __unflattenBound(fs):
        fs.readByteRaw() # for encoding version
        fs.readFloat64()
        fs.readFloat64()
        fs.readFloat64()
        fs.readFloat64()

    def flatten(self, fs):
        fs.writeInt32(self.serializedSize()) # prepend length

        fs.writeByte(0); # encoding version
        fs.writeByte(1); # owns_loops

        if len(self.loops) > 1: # has_holes
            fs.writeByte(1)
        else:
            fs.writeByte(0)
        fs.writeInt32(len(self.loops))
        depth = 0
        for loop in self.loops:
            Geography.__flattenLoop(loop, depth, fs);
            depth = 1;
        Geography.__flattenEmptyBound(fs);

    @staticmethod
    def __flattenLoop(loop, depth, fs):
        # 1 byte     for encoding version
        # 4 bytes    for number of vertices
        # number of vertices * 8 * 3  bytes  for vertices as XYZPoints
        # 1 byte     for origin_inside_
        # 4 bytes    for depth_
        # length of bound
        fs.writeByte(0);
        fs.writeInt32(len(loop))
        for xyzp in loop:
            fs.writeFloat64(xyzp.x)
            fs.writeFloat64(xyzp.y)
            fs.writeFloat64(xyzp.z)

        fs.writeByte(0);  # origin_inside
        fs.writeInt32(depth); # depth
        Geography.__flattenEmptyBound(fs);

    @staticmethod
    def __flattenEmptyBound(fs):
        fs.writeByte(0); # for encoding version
        fs.writeFloat64(Geography.NULL_COORD)
        fs.writeFloat64(Geography.NULL_COORD)
        fs.writeFloat64(Geography.NULL_COORD)
        fs.writeFloat64(Geography.NULL_COORD)

    @staticmethod
    def formatPoint(point):
        # auto convert XYZ points
        if isinstance(point, XYZPoint):
            point = point.toGeogrpahyPoint()

        fmt = "{}"
        #DecimalFormat df = new DecimalFormat("##0.0###########");

        # Explicitly test for differences less than 1.0e-12 and
        # force them to be zero.  Otherwise you may find a case
        # where two points differ in the less significant bits, but
        # they format as the same number.
        lng = point[0]
        if lng < Geography.EPSILON:
            lng = 0.0
        lat = point[1]
        if lat < Geography.EPSILON:
            lat = 0.0
        return fmt.format(lng) + " " + fmt.format(lat);

    @staticmethod
    def pointToWKT(point):
        # auto convert XYZ points
        if isinstance(point, XYZPoint):
            point = point.toGeogrpahyPoint()

        # This is not GEOGRAPHY_POINT.  This is wkt syntax.
        return "POINT (" + Geography.formatGeographyPoint(point) + ")"


    wktPointMatcher = re.compile(r"^\s*point\s*\(\s*(-?\d+[\.\d*]*)\s+(-?\d+[\.\d*]*)\s*\)", flags=re.IGNORECASE)
    @staticmethod
    def pointFromWKT(wkt):
        if wkt is None:
            raise ValueError("None passed to pointFromWKT")
        match = re.search()
        lngStr = match.group(1)
        latStr = match.group(2)
        if latStr is None or lngStr is None:
            return None
        lng = float(lngStr)
        lat = float(latStr)
        return (lng, lat)

    @staticmethod
    def geographyFromWKT(wkt):
        pass

    def __str__(self):
        # return representation in Well Known Text (WKT)
        wkt = "POLYGON ("

        isFirstLoop = True
        for loop in self.loops:
            if not isFirstLoop:
                wkt += ", "
            wkt += "("

            # iterate backwards
            startIdx = len(loop) - 1
            endIdx = 0
            increment = -1
            # reverse direction for first loop
            if isFirstLoop:
                startIdx = 1
                endIdx = len(loop)
                increment = 1

            wkt += Geography.formatPoint(loop[0]) + ", "
            for idx in range(startIdx, endIdx, increment):
                xyzp = loop[idx]
                wkt += Geography.formatPoint(xyzp) + ", "

            # Repeat the start vertex to close the loop as WKT requires.
            wkt += Geography.formatPoint(loop[0]) + ")"
            isFirstLoop = False

        wkt += ")"
        return wkt

    def __repr__(self):
        return self.__str__()

class VoltColumn:
    "definition of one VoltDB table column"
    def __init__(self, fser = None, type = None, name = None):
        if fser != None:
            self.type = fser.readByte()
            self.name = None
        elif type != None and name != None:
            self.type = type
            self.name = name

    def __str__(self):
        # If the name is empty, use the default "modified tuples". Has to do
        # this because HSQLDB doesn't return a column name if the table is
        # empty.
        return "(%s: %d)" % (self.name or "modified tuples" ,
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
        self.columns = []  # column definitions
        self.tuples = []

    def __str__(self):
        result = ""

        result += "column count: %d\n" % (len(self.columns))
        result += "row count: %d\n" % (len(self.tuples))
        result += "cols: "
        result += ", ".join([str(x) for x in self.columns])
        result += "\n"
        result += "rows -\n"
        result += "\n".join([str(["NULL" if y is None else y for y in x]) for x in self.tuples])

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
        limit_position = self.fser.read_buffer._off + tablesize
        # 2.
        headersize = self.fser.readInt32()
        statuscode = self.fser.readByte()
        columncount = self.fser.readInt16()
        for i in range(columncount):
            column = VoltColumn(fser = self.fser)
            self.columns.append(column)
        list([x.readName(self.fser) for x in self.columns])

        # 3.
        rowcount = self.fser.readInt32()
        for i in range(rowcount):
            rowsize = self.fser.readInt32()
            # list comprehension: build list by calling read for each column in
            # row/tuple
            row = [self.fser.read(self.columns[j].type)
                   for j in range(columncount)]
            self.tuples.append(row)

        # advance offset to end of table-size on read_buffer
        if self.fser.read_buffer._off != limit_position:
            self.fser.read_buffer._off = limit_position

        return self

    def writeToSerializer(self):
        table_fser = FastSerializer()

        # We have to pack the header into a buffer first so that we can
        # calculate the size
        header_fser = FastSerializer()

        header_fser.writeByte(0)
        header_fser.writeInt16(len(self.columns))
        list([x.writeType(header_fser) for x in self.columns])
        list([x.writeName(header_fser) for x in self.columns])

        table_fser.writeInt32(header_fser.size() - 4)
        table_fser.writeRawBytes(header_fser.getRawBytes())

        table_fser.writeInt32(len(self.tuples))
        for i in self.tuples:
            row_fser = FastSerializer()

            list([row_fser.write(self.columns[x].type, i[x]) for x in range(len(i))])

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
        for i in range(0, self.message_len):
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
            for i in range(0, 5):
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
                for i in range(0, self.buffer_size):
                    self.buffer.append(fser.readByte())
        else:
            for i in range(0, self.length - 3 - 2 - self.message_len):
                fser.readByte()
            error("Python client deserialized unknown VoltException.")

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
        for i in range(tablecount):
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
        for i in range(len(self.paramtypes)):
            try:
                iter(params[i]) # Test if this is an array
                if isinstance(params[i], str): # String is a special case
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
            except IOError as err:
                res = VoltResponse(None)
                res.statusString = str(err)
        finally:
            self.fser.socket.settimeout(original_timeout)
        return response and res or None
