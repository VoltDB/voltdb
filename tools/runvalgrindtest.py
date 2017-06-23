#!/usr/bin/env python
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
            print('      frame %d/%d' % (idx+1, nframes))
            idx += 1
            ip = frame.findall('.//ip')
            obj = frame.findall('.//obj')
            fn = frame.findall('.//fn')
            if len(ip) > 0:
                print('        ip:  ' + ip[0].text)
            if len(obj) > 0:
                print('        obj: ' + obj[0].text)
            if len(fn) > 0:
                print('        fn:  ' + fn[0].text)

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
    if arg == '--expect-fail=true':
        expectfail=True
        sys.argv.pop(1)
        expected_status = 1
    elif arg == '--expect-fail=false':
        expectfail=False
        sys.argv.pop(1)
        expected_status = 0
    for arg in sys.argv:
        if arg.startswith('--xml-file='):
            xmlfile=arg[11:]
            break
    testReturnStatus = subprocess.call(sys.argv[1:], shell=False)
    if xmlfile:
        if (testReturnStatus == expected_status):
            os.remove(xmlfile)
        else:
            result = XMLFile(xmlfile, expectfail)
            result.readErrors()
            result.printErrors()
    sys.exit(testReturnStatus)
