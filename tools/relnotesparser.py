#!/usr/bin/env python

# Written by Andrew Gent
#
# This program parses the XML source file for the release notes and extracts the text of
# each note, along with the ticket number coded in the <revision> tag, and writes them
# out as CSV data. The CSV file can then be compared to the # releasenote field in JIRA 
# using the accompanying relnotesupdater.py script.
#
# HOW TO USE:
# - in the voltdb-docs repo, make sure you've checked out the latest docs
# - in the voltdb repo:
#       python ./relnotesparser.py ~/workspace/voltdb-doc/userdocs/releasenotes.xml > /tmp/relnotes.csv
#

import sys
from xml.dom.minidom import parse


def parsefile(filename):
    tree = parse(filename)
    #root = tree.getroot()
    # Look for all the "revision=" attributes
    findattribute(tree,"revision",0)

def findattribute(xml,attribute, depth):
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
            print foundattribute + "," + quote(smash(innertext(child) ))
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
    parsefile(f)
