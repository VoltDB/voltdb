#!/usr/bin/env python
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
import os
import subprocess
import xml.etree.ElementTree as ET
import re

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
            idx += 1
            print('    Stack %d of %d' % (idx, nstacks))
            self.printStack(stack)

    def readErrors(self):
        f = None
        xml = ""
        with open(self.xmlfile, "r") as f:
            xml = f.read()
        # Sometimes valgrind puts extra closing tags
        # in the output.  This is a known bug in versions
        # before 3.12.  Since we cannot install more modern
        # tools we need to try to fix this up by removing all the
        # end tags and adding one at the end.
        xml = re.sub(r"(</valgrindoutput>)", "", xml) + "\n</valgrindoutput>\n"
        root = ET.fromstring(xml)
        exe=root.findall('.//argv/exe')
        print('Exe: %s' % exe[0].text)
        self.errors = root.findall('.//error')
        self.numErrors = len(self.errors)
        result = (self.numErrors > 0)
        return result

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
            print("Error %d of %d" % (idx, self.numErrors))
            self.printError(error)

if __name__ == '__main__':
    xmlfile = None
    expectfail=False
    run_valgrind = True
    debug = False
    done = False
    while not done:
        arg = sys.argv[1]
        if arg == '--expect-fail=true':
            expectfail=True
        elif arg == '--expect-fail=false':
            expectfail=False
        elif arg == '--just-read-file':
            run_valgrind = False
        elif arg == '--debug':
            debug = True
        else:
            done = True
        if not done:
            sys.argv.pop(1)
    for arg in sys.argv:
        if arg.startswith('--xml-file='):
            xmlfile=arg[11:]
            break
    if debug:
        print("Valgrind command: %s" % (" ".join(sys.argv[1:])))
        print("Expect fail: %s" % expectfail)
    if run_valgrind:
        subprocess.call(sys.argv[1:], shell=False)
    if not xmlfile:
	sys.exit(1)
    result = XMLFile(xmlfile, expectfail)
    testfailed = result.readErrors()
    gotexpected = (testfailed == expectfail)
    print("testfailed: %s, expectfail: %s, gotexpected: %s" % (testfailed, expectfail, gotexpected))
    if not gotexpected:
        result.printErrors()
    if gotexpected and run_valgrind:
        os.remove(xmlfile)
    sys.exit(1 if (testfailed) else 0)
