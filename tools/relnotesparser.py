#!/usr/bin/env python
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
