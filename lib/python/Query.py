#!/usr/bin/env python
# This file is part of VoltDB.
# Copyright (C) 2008-2015 VoltDB Inc.
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
import cmd
import socket
import os.path
from datetime import datetime
from voltdbclient import *

class VoltQueryClient(cmd.Cmd):
    TYPES = {"byte": FastSerializer.VOLTTYPE_TINYINT,
             "short": FastSerializer.VOLTTYPE_SMALLINT,
             "int": FastSerializer.VOLTTYPE_INTEGER,
             "long": FastSerializer.VOLTTYPE_BIGINT,
             "float": FastSerializer.VOLTTYPE_FLOAT,
             "string": FastSerializer.VOLTTYPE_STRING,
             "varbinary": FastSerializer.VOLTTYPE_STRING,
             "date": FastSerializer.VOLTTYPE_TIMESTAMP}

    TRANSFORMERS = {FastSerializer.VOLTTYPE_TINYINT: eval,
                    FastSerializer.VOLTTYPE_SMALLINT: eval,
                    FastSerializer.VOLTTYPE_INTEGER: eval,
                    FastSerializer.VOLTTYPE_BIGINT: eval,
                    FastSerializer.VOLTTYPE_FLOAT: eval,
                    FastSerializer.VOLTTYPE_STRING: lambda x: x,
                    FastSerializer.VOLTTYPE_STRING: lambda x: x,
                    FastSerializer.VOLTTYPE_TIMESTAMP:
                        lambda x: datetime.fromtimestamp(x)}

    def __init__(self, host, port, username = "", password = "",
                 dump_file = None):
        cmd.Cmd.__init__(self)

        self.__quiet = False
        self.__timeout = None

        self.__initialize(host, port, username, password, dump_file)

    def __initialize(self, host, port, username, password, dump_file):
        self.fs = FastSerializer(host, port, username, password, dump_file)

        self.adhoc = VoltProcedure(self.fs, "@AdHoc",
                                   [FastSerializer.VOLTTYPE_STRING])

        self.stat = VoltProcedure(self.fs, "@Statistics",
                                  [FastSerializer.VOLTTYPE_STRING,
                                   FastSerializer.VOLTTYPE_TINYINT])

        self.snapshotsave = VoltProcedure(self.fs, "@SnapshotSave",
                                          [FastSerializer.VOLTTYPE_STRING,
                                           FastSerializer.VOLTTYPE_STRING,
                                           FastSerializer.VOLTTYPE_TINYINT])
        self.snapshotsavejson = VoltProcedure(self.fs, "@SnapshotSave",
                                          [FastSerializer.VOLTTYPE_STRING])
        self.snapshotscan = VoltProcedure(self.fs, "@SnapshotScan",
                                          [FastSerializer.VOLTTYPE_STRING])
        self.snapshotdelete = VoltProcedure(self.fs, "@SnapshotDelete",
                                            [FastSerializer.VOLTTYPE_STRING,
                                             FastSerializer.VOLTTYPE_STRING])
        self.snapshotrestore = VoltProcedure(self.fs, "@SnapshotRestore",
                                             [FastSerializer.VOLTTYPE_STRING,
                                              FastSerializer.VOLTTYPE_STRING])
        self.snapshotstatus = VoltProcedure(self.fs, "@SnapshotStatus")

        self.systemcatalog = VoltProcedure(self.fs, "@SystemCatalog",
                                               [FastSerializer.VOLTTYPE_STRING])

        self.systeminformation = VoltProcedure(self.fs, "@SystemInformation",
                                               [FastSerializer.VOLTTYPE_STRING])

        self.updatecatalog = VoltProcedure(self.fs, "@UpdateApplicationCatalog",
                                             [FastSerializer.VOLTTYPE_STRING,
                                              FastSerializer.VOLTTYPE_STRING])

        self.quiesce = VoltProcedure(self.fs, "@Quiesce")

        self.pause = VoltProcedure(self.fs, "@Pause")

        self.resume = VoltProcedure(self.fs, "@Resume")

        self.shutdown = VoltProcedure(self.fs, "@Shutdown")

        self.promote = VoltProcedure(self.fs, "@Promote")

        self.response = None

    def __safe_call(self, proc, params = None, response = True, timeout = None):
        if not proc:
            return None

        self.response = None

        try:
            return proc.call(params, response, timeout)
        except IOError, err:
            self.safe_print("Error: %s" % (err))
            if not response:
                raise
            else:
                return None

    def close(self):
        if self.fs != None:
            self.fs.close()

    def execute(self, command):
        self.onecmd(command)
        if self.response != None:
            return self.response
        else:
            raise IOError("Connection down")

    def precmd(self, command):
        if self.fs == None:
            self.safe_print("Not connected to any server, please connect first")
        return command.decode("utf-8")

    def prepare_params(self, procedure, command):
        params = []
        parsed = command.split()

        if len(parsed) != len(procedure.paramtypes):
            raise SyntaxError("Expecting %d parameters, %d given" %
                              (len(procedure.paramtypes), len(parsed)))

        for i in xrange(len(parsed)):
            transformer = self.__class__.TRANSFORMERS[procedure.paramtypes[i]]
            params.append(transformer(parsed[i]))

        return params

    def safe_print(self, *var):
        if not self.__quiet:
            for i in var:
                if i != None:
                    print i,
            print

    def set_quiet(self, quiet):
        self.__quiet = quiet

    def set_timeout(self, timeout):
        self.__timeout = timeout

    def do_connect(self, command):
        if not command:
            return self.help_connect()

        args = command.split()
        if len(args) < 2:
            return self.help_connect()
        host = args[0]
        port = int(args[1])
        username = len(args) >= 3 and args[2] or ""
        password = len(args) >= 4 and args[3] or ""

        self.safe_print("Connecting to server %s on port %d" % (host, port))
        self.__initialize(host, port, username, password)

    def help_connect(self):
        self.safe_print("Connect to a server")
        self.safe_print("\tconnect host port [username] [password]")

    def do_disconnect(self, command):
        self.close()
        self.fs = None

    def help_disconnect(self):
        self.safe_print("Disconnect from the server")

    def do_quit(self, command):
        return True

    def do_exit(self, command):
        return True

    def default(self, command):
        if command == "EOF":
            self.safe_print()
            return True

        self.safe_print("Unknown Command:", command)
        self.do_help(None)

    def do_stat(self, command):
        if self.fs == None:
            return
        if not command:
            return self.help_stat()
        args = command.split()
        if len(args) != 2:
            return self.help_stat()
        self.safe_print("Getting statistics")
        self.response = self.__safe_call(self.stat, [args[0], int(args[1])],
                                         timeout = self.__timeout)
        #self.safe_print(self.response)

    def help_stat(self):
        self.safe_print(
            """
Get the statistics:
\tstat {table|index|procedure|starvation|initiator|partitioncount|iostats|memory|liveclients|management} {0|1}
""")

    def do_snapshotsave(self, command):
        if self.fs == None:
            return
        if not command:
            return self.help_snapshotsave()

        args = command.split()
        if len(args) not in (1,3):
            return self.help_snapshotsave()

        self.safe_print("Taking snapshot")
        if len(args) == 3:
            self.response = self.__safe_call(self.snapshotsave,
                                             [args[0], args[1], int(args[2])],
                                             timeout = self.__timeout)
        else:
            print args
            self.response = self.__safe_call(self.snapshotsavejson,
                                             args,
                                             timeout = self.__timeout)
        self.safe_print(self.response)

    def help_snapshotsave(self):
        self.safe_print("Take a snapshot:")
        self.safe_print("\tsnapshotsave directory nonce blocking")
        self.safe_print("or")
        self.safe_print('\tsnapshotsave {uripath:"file:///tmp",nonce:"mydb",block:true,format:"csv"}')

    def do_snapshotscan(self, command):
        if self.fs == None:
            return
        if not command:
            return self.help_snapshotscan()

        self.safe_print("Scanning snapshots")
        self.response = self.__safe_call(self.snapshotscan, [command],
                                         timeout = self.__timeout)
        self.safe_print(self.response)

    def help_snapshotscan(self):
        self.safe_print("Scan snapshots")
        self.safe_print("\tsnapshotsave directory")

    def do_snapshotdelete(self, command):
        if self.fs == None:
            return
        if not command:
            return self.help_snapshotdelete()

        (paths, nonces) = command.split()
        paths = paths.split(",")
        nonces = nonces.split(",")

        self.safe_print("Deleting snapshots")
        self.response = self.__safe_call(self.snapshotdelete, [paths, nonces],
                                         timeout = self.__timeout)
        self.safe_print(self.response)

    def help_snapshotdelete(self):
        self.safe_print("Delete snapshots")
        self.safe_print("\tsnapshotdelete directory,directory,... "
                        "nonce,nonce,...")

    def do_snapshotrestore(self, command):
        if self.fs == None:
            return
        if not command:
            return self.help_snapshotrestore()

        args = command.split()
        if len(args) != 2:
            return self.help_snapshotrestore()

        self.safe_print("Restoring snapshot")
        self.response = self.__safe_call(self.snapshotrestore,
                                         [args[0], args[1]],
                                         timeout = self.__timeout)
        self.safe_print(self.response)

    def help_snapshotrestore(self):
        self.safe_print("Restore a snapshot:")
        self.safe_print("\tsnapshotrestore directory nonce")

    def do_snapshotstatus(self, command):
        if self.fs == None:
            return

        self.safe_print("Getting snapshot status")
        self.response = self.__safe_call(self.snapshotstatus,
                                         timeout = self.__timeout)
        self.safe_print(self.response)

    def help_snapshotstatus(self):
        self.safe_print("Get snapshot status")
        self.safe_print("\tsnapshotstatus")

    def do_syscatalog(self, command):
        if self.fs == None:
            return
        selector = "TABLES"
        if command:
            selector = command

        self.safe_print("Getting system catalog")
        self.response = self.__safe_call(self.systemcatalog,
                                         [selector],
                                         timeout = self.__timeout)
        self.safe_print(self.response)

    def help_catalog(self):
        self.safe_print("Get system information")
        self.safe_print("\tsyscatalog {TABLES|COLUMNS|INDEXINFO|PRIMARYKEYS|PROCEDURES|PROCEDURECOLUMNS}")

    def do_sysinfo(self, command):
        if self.fs == None:
            return
        selector = "OVERVIEW"
        if command:
            selector = command

        self.safe_print("Getting system information")
        self.response = self.__safe_call(self.systeminformation,
                                         [selector],
                                         timeout = self.__timeout)
        self.safe_print(self.response)

    def help_sysinfo(self):
        self.safe_print("Get system information")
        self.safe_print("\tsysinfo {OVERVIEW|DEPLOYMENT}")

    def do_promote(self, command):
        if self.fs == None:
            return
        self.safe_print("Switching to master")
        self.response = self.__safe_call(self.promote,
                                         timeout = self.__timeout)
        self.safe_print(self.response)

    def help_promote(self):
        self.safe_print("Switch to master")
        self.safe_print("\tpromote")

    def do_updatecatalog(self, command):
        if self.fs == None:
            return
        if not command:
            return self.help_updatecatalog()

        args = command.split()
        if len(args) != 2:
            return self.help_updatecatalog()

        if(not os.path.isfile(args[0]) or not os.path.isfile(args[1])):
            # args[0] is the catalog jar file
            # args[1] is the deployment xml file
            print >> sys.stderr, "Either file '%s' doesnot exist OR file '%s' doesnot exist!!" \
                    (args[0],args[1])
            exit(1)

        xmlf = open(args[1], "r")
        xmlcntnts = xmlf.read()
#       print "xmlcntnts = #%s#" % xmlcntnts
        jarf = open(args[0], "r")
        jarcntnts = jarf.read()
        hexJarcntnts = jarcntnts.encode('hex_codec')
#       print "hexJarcntnts = #%s#" % hexJarcntnts

        self.safe_print("Updating the application catalog")
        self.response = self.__safe_call(self.updatecatalog,
                                 [hexJarcntnts, xmlcntnts],
                                 timeout = self.__timeout)
        self.safe_print(self.response)

    def help_updatecatalog(self):
        self.safe_print("Update the application catalog:")
        self.safe_print("\tupdatecatalog catalogjarfile deploymentfile")

    def do_quiesce(self, command):
        if self.fs == None:
            return
        self.safe_print("Quiesce...")
        self.response = self.__safe_call(self.quiesce, timeout = self.__timeout)
        self.safe_print(self.response)

    def help_quiesce(self):
        self.safe_print("Quiesce the system")
        self.safe_print("\tquiesce")

    def do_pause(self, command):
        if self.fs == None:
            return
        self.safe_print("Entering Admin Mode...")
        self.response = self.__safe_call(self.pause, timeout = self.__timeout)
        self.safe_print(self.response)

    def help_pause(self):
        self.safe_print("Enters cluster Admin Mode.\nYou must be connected to the admin port in order to call this function.")
        self.safe_print("\tpause")

    def do_resume(self, command):
        if self.fs == None:
            return
        self.safe_print("Exiting Admin Mode...")
        self.response = self.__safe_call(self.resume, timeout = self.__timeout)
        self.safe_print(self.response)

    def help_resume(self):
        self.safe_print("Exits cluster Admin Mode.\nYou must be connected to the admin port in order to call this function.")
        self.safe_print("\tresume")

    def do_adhoc(self, command):
        if self.fs == None:
            return
        if not command:
            return self.help_adhoc()

        self.safe_print("Executing adhoc query: %s\n" % (command))
        self.response = self.__safe_call(self.adhoc, [command],
                                         timeout = self.__timeout)
        self.safe_print(self.response)

    def help_adhoc(self):
        self.safe_print("Execute an adhoc query:")
        self.safe_print("\tadhoc SQL_statement")

    def do_shutdown(self, command):
        if self.fs == None:
            return
        self.safe_print("Shutting down the server")
        self.__safe_call(self.shutdown, response = False,
                         timeout = self.__timeout)

    def help_shutdown(self):
        self.safe_print("Shutdown the server")
        self.safe_print("\tshutdown")

    def do_define(self, command):
        if self.fs == None:
            return
        if not command:
            return self.help_define()

        parsed = command.split()
        self.safe_print("Defining stored procedure:", parsed[0])

        if getattr(self.__class__, "do_" + parsed[0], None) != None:
            self.safe_print(parsed[0], "is already defined")

        try:
            method_name = "_".join(["stored", parsed[0]])
            proc_name = "_".join(["procedure", parsed[0]])
            code = """
                def %s(self, command):
                    self.safe_print("Executing stored procedure: %s")
                    try:
                        self.response = self.__safe_call(self.%s, self.prepare_params(self.%s, command), timeout = self.__timeout)
                        self.safe_print(self.response)
                    except SyntaxError, strerr:
                        self.safe_print(strerr)
                   """ % (method_name, parsed[0], proc_name, proc_name)
            tmp = {}
            exec code.strip() in tmp
            setattr(self.__class__, "do_" + parsed[0], tmp[method_name])

            setattr(self.__class__, proc_name,
                    VoltProcedure(self.fs, parsed[0],
                                  [self.__class__.TYPES[i]
                                   for i in parsed[1:]]))
        except KeyError, strerr:
            self.safe_print("Unsupported type", strerr)
            self.help_define()

    def help_define(self):
        self.safe_print("Define a stored procedure")
        self.safe_print("\tdefine stored_procedure_name param_type_1",
                        "param_type_2...")
        self.safe_print()
        self.safe_print("Supported types", self.__class__.TYPES.keys())

def help(program_name):
    print program_name, "hostname port [dump=filename] [command]"

if __name__ == "__main__":
    if len(sys.argv) < 3:
        help(sys.argv[0])
        exit(-1)

    filename = None
    if len(sys.argv) >= 4 and sys.argv[3].startswith("dump="):
        filename = sys.argv[3].split("=")[1]
        del sys.argv[3]

    try:
        command = VoltQueryClient(sys.argv[1], int(sys.argv[2]),
                                  dump_file = filename)
    except socket.error:
        sys.stderr.write("Error connecting to the server %s\n" % (sys.argv[1]))
        exit(-1)

    if len(sys.argv) > 3:
        command.onecmd(" ".join(sys.argv[3:]))
    else:
        command.cmdloop("VoltDB Query Client")
