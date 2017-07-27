#!/usr/bin/env python
# This file is part of VoltDB.
# Copyright (C) 2008-2017 VoltDB Inc.
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
import os
import subprocess
import xml.etree.ElementTree as ET

class XMLFile(object):
    def __init__(self, xmlfile, expecterrors):
        self.xmlfile = xmlfile
        self.expecterrors = expecterrors

    def printStack(self, stack):
        idx = 0
        frames = stack.findall('.//frame')
        nframes = len(frames)
        for frame in frames:
            idx += 1
            fns = frame.findall('.//fn')
            ips = frame.findall('.//ip')
            objs = frame.findall('.//obj')
            dirs = frame.findall('.//dir')
            files = frame.findall('.//file')
            lines = frame.findall('.//line')
            ip  = (ips[0].text if len(ips) > 0 else "<UNKNOWN IP>")
            fn  = (fns[0].text if len(fns) > 0 else "<UNKNOWN FN>")
            obj = (objs[0].text if len(objs) > 0 else "<UNKNOWN OBJ>")
            line = (lines[0].text if len(lines) > 0 else "<UNKNOWN LINE>")
            dir = (dirs[0].text if len(dirs) > 0 else "<UNKNOWN DIR>")
            file = (files[0].text if len(files) > 0 else "<UNKNOWN FILE>")
            print('      %02d/%02d: %s' % (idx, nframes, fn))
            print('             %s/%s:%s' % (dir, file, line))
            print('             %s@%s' % (obj, ip))

    def printError(self, error):
        kind=error.findall('.//what')
        if len(kind) > 0:
            print('  ' + kind[0].text)
        else:
            print('  Unknown kind.')
        stacks = error.findall('.//stack')
        idx = 0
        nstacks = len(stacks)
        for stack in stacks:
            print('    Stack %d/%d' % (idx + 1, nstacks))
            self.printStack(stack)

    def readErrors(self):
        tree = ET.parse(self.xmlfile)
        root = tree.getroot()
        exe=root.findall('.//argv/exe')
        print('Exe: %s' % exe[0].text)
        self.errors = root.findall('.//error')
        self.numErrors = len(self.errors)

    def printErrors(self):
        print(':---------------------------------------------------:')
        print(':----------- Valgrind Failure Report ---------------:')
        print(':---------------------------------------------------:')
        if self.expecterrors != (self.numErrors > 0):
            if self.expecterrors:
                message = "Success"
            else:
                message = "Failure"
            print(':------------- Unexpected %s ------------------:' % message)

        idx = 0
        for error in self.errors:
            idx += 1
            print("Error %d/%d" % (idx + 1, self.numErrors))
            self.printError(error)

if __name__ == '__main__':
    xmlfile = None
    arg = sys.argv[1]
    expectfail=False
    expected_status = 0
    run_valgrind = True
    if arg == '--expect-fail=true':
        expectfail=True
        sys.argv.pop(1)
        expected_status = 1
    elif arg == '--expect-fail=false':
        expectfail=False
        sys.argv.pop(1)
        expected_status = 0
    elif arg == '--just-read-file':
        run_valgrind = False
    for arg in sys.argv:
        if arg.startswith('--xml-file='):
            xmlfile=arg[11:]
            break
    if run_valgrind:
        testReturnStatus = subprocess.call(sys.argv[1:], shell=False)
    else:
        testReturnStatus = 0
    if xmlfile:
        result = XMLFile(xmlfile, expectfail)
        result.readErrors()
        result.printErrors()
        if (testReturnStatus == expected_status):
            os.remove(xmlfile)
    sys.exit(testReturnStatus)
