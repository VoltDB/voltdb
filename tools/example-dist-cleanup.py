#/usr/bin/env python

import sys

##
## This script does any post processing required when moving an
## example from it's home in /examples to ${build.dir}/dist/examples.
##

# args should be the program name and the path to the example dir
if len(sys.argv) != 2:
    print "examply-dist-cleanup.py expects 2 arguments."
    sys.exit(-1)

def readFile(filename):
    "read a file into a string"
    FH=open(filename, 'r')
    fileString = FH.read()
    FH.close()
    return fileString

def writeFile(filename, content):
    "read a file into a string"
    FH=open(filename, 'w')
    FH.write(content)
    FH.close()

# replace the path to the basebuild.xml in build.xml
pathToBuildXml = sys.argv[1] + "/build.xml"
print pathToBuildXml
content = readFile(pathToBuildXml)
content = content.replace("../includes/basebuild.xml", "basebuild.xml")
writeFile(pathToBuildXml, content)