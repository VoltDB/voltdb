#!/usr/bin/env python
import sys
import os
import subprocess
import xml.etree.ElementTree as ET

class XMLFile(object):
    def __init__(self, xmlfile):
        self.xmlfile = xmlfile

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
    def printErrors(self):
        print(':---------------------------------------------------:')
        print(':----------- Valgrind Failure Report ---------------:')
        print(':---------------------------------------------------:')
        tree = ET.parse(self.xmlfile)
        root = tree.getroot()
        exe=root.findall('.//argv/exe')
        print('Exe: %s' % exe[0].text)
        errors = root.findall('.//error')
        idx = 0
        nerrs = len(errors)
        for error in errors:
            print("Error %d/%d" % (idx + 1, nerrs))
            self.printError(error)
        
if __name__ == '__main__':
    xmlfile = None
    for arg in sys.argv:
        if arg.startswith('--xml-file='):
            xmlfile=arg[11:]
            break
    testReturnStatus = subprocess.call(sys.argv[1:], shell=False)
    if xmlfile and testReturnStatus == 0:
        os.remove(xmlfile)
        sys.exit(0)
    else:
        if xmlfile:
            XMLFile(xmlfile).printErrors()
    sys.exit(testReturnStatus)
