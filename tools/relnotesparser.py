#!/usr/bin/env python

# Written by Andrew Gent
#
# This program parses the XML source file for the release notes and extracts the text of
# each note, along with the ticket number coded in the <revision> tag.
#
# Normally, this is called as a module by relnotesupdater.py. However, it can be run
# standalone, in which case it writes the release note data as CSV to stdout.
#
# HOW TO USE STANDALONE:
# - in the voltdb-docs repo, make sure you've checked out the latest docs
# - in the voltdb repo:
#       python ./relnotesparser.py ~/workspace/voltdb-doc/userdocs/releasenotes.xml > /tmp/relnotes.csv
#

import sys
import csv
import StringIO

from xml.dom.minidom import parse

relnotes = []

def parsefile(filename):
    global relnotes
    relnotes = []
    tree = parse(filename)
    #root = tree.getroot()
    # Look for all the "revision=" attributes
    findattribute(tree,"revision",0)
    return relnotes

def findattribute(xml,attribute, depth):
    global relnotes
    if depth > 100:
        print "Exiting at depth " + str(depth)
        exit()
    for child in xml.childNodes:
        #print child.nodeName + "..."
        foundattribute = None
        atts = child.attributes
        if (atts):
            #print atts.keys()
            if attribute in atts.keys():
                foundattribute = atts[attribute].value
        if foundattribute:
            row = [foundattribute,smash(innertext(child))]
            relnotes.append(row)
            #relnotes.append([foundattribute,smash(innertext(child))])
            #print foundattribute + "," + quote(smash(innertext(child) ))
        else:
            findattribute(child,attribute,depth+1)

def innertext(node):
        if node.nodeType ==  node.TEXT_NODE:
            return node.data
        else:
            text_string = ""
            for child_node in node.childNodes:
                text_string = text_string + innertext( child_node )
            return text_string

def smash(text):
    # First compress it
    text = ' '.join(text.split())
    return text.encode('ascii','ignore')
def quote(text):
    text = text.replace("\\","\\\\")
    text = text.replace("\"","\\\"")
    #Catch quotation marks
    #text = text.replace('"','""')
    return '"' + text + '"'

if __name__ == "__main__":
    f = sys.argv[1]
    parsed = parsefile(f)
    
    # print the array
    strfile = StringIO.StringIO()
    fid = csv.writer(strfile)
    for r in parsed:
        fid.writerow(r)
        #outputstr = ""
        #c = len(r)
        #for i in range (0,c-1):
        #    outputstr = outputstr + r[i] + ","
        #outputstr = outputstr + quote(r[c-1])
        #print outputstr
    sys.stdout.write( strfile.getvalue() )
    sys.stdout.flush()

