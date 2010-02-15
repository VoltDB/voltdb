#!/usr/bin/env python

import os

def cmd_readlines(cmd):
    fd = os.popen(cmd)
    retval = fd.readlines()
    fd.close()
    return retval

darwin = False

lines = cmd_readlines("uname")
for line in lines:
    if line.strip() == "Darwin":
        darwin = True

# if macosx       
if darwin:
    lines = cmd_readlines("ifconfig en0")
    for line in lines:
        parts = line.split()
        if parts[0] == "ether":
            print parts[1]
            
# assume linux
else:
    lines = cmd_readlines("/sbin/ifconfig eth0")
    line = lines[0]
    parts = line.split()
    print parts[4]