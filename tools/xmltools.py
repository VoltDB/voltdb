#!/usr/bin/env python

import sys
from xml.dom.minidom import parse, parseString
from xml.dom import Node

##
## This script does any post processing required when moving an
## example from it's home in /examples to ${build.dir}/dist/examples.
##


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

def pruneTextNodes(aNode):
    ""
    toRemove = []
    for child in aNode.childNodes:
        if child.nodeType == Node.TEXT_NODE:
            toRemove.append(child)
        else:
            pruneTextNodes(child)
    for child in toRemove:
        aNode.removeChild(child)

def getElementId(node):
    assert node.nodeType == Node.ELEMENT_NODE
    if node.hasAttribute("name"):
        return node.tagName + "-" + node.getAttribute("name")
    elif node.hasAttribute("id"):
        return node.tagName + "-" + node.getAttribute("id")
    else:
        return node.tagName

def findNodesWithId(node, id):
    retval = []
    if (id == None) or (getElementId(node) == id):
        retval.append(node)
    for i in range(node.childNodes.length):
        child = node.childNodes.item(i)
        if child.nodeType == Node.ELEMENT_NODE:
            retval += findNodesWithId(child, id);
    return retval

def addBeforeNodesWithId(doc, root, id, text, comment):
    toadd = findNodesWithId(root, id)
    for node in toadd:
        if text != None:
            text = doc.createTextNode(text)
            root.insertBefore(text, node)
        if comment != None:
            comment = doc.createComment(comment)
            root.insertBefore(comment, node)

def addWhitespace(doc, project):
    toadd = []

    for i in range(project.childNodes.length):
        child = project.childNodes.item(i)
        if child.nodeType == Node.ELEMENT_NODE:
            if child.tagName == "property":
                continue
        ws = doc.createTextNode("\n")
        toadd.append((ws,child))
    for pair in toadd:
        project.insertBefore(pair[0], pair[1])

def orderAttributes__(attributes, attributeOrder):
    ordered = []
    if attributeOrder == None:
        attributeOrder = ["name"]
    for attrName in attributeOrder:
        for i in range(attributes.length):
            attr = attributes.item(i)
            if attr.name == attrName:
                ordered.append(attr)

    for i in range(attributes.length):
        attr = attributes.item(i)
        found = False
        for attr2 in ordered:
            if attr2 == attr:
                found = True
        if not found:
            ordered.append(attr)
    return ordered

def prettyXml__(node, indent, attributeOrder):
    if attributeOrder == None:
        attributeOrder = {}

    output = indent

    if node.nodeType == Node.ELEMENT_NODE:
        # start tag
        output = output + "<" + node.tagName
        # print attributes
        order = attributeOrder.get(node.tagName)
        attrs = orderAttributes__(node.attributes, order)
        for attr in attrs:
            output += " " + attr.name + "=\""
            output += node.getAttribute(attr.name)
            output += "\""

        if node.hasChildNodes():
            output += ">\n"
            # print childen
            for child in node.childNodes:
                output += prettyXml__(child, "    " + indent, attributeOrder)
            # close tag
            output += indent + "</" + node.tagName + ">\n"
        else:
            # end tag
            output += "/>\n"

    elif node.nodeType == Node.TEXT_NODE:
        output += node.data
    elif node.nodeType == Node.CDATA_SECTION_NODE:
        print "CDATA_SECTION_NODE"
    elif node.nodeType == Node.ENTITY_NODE:
        print "ENTITY_NODE"
    elif node.nodeType == Node.PROCESSING_INSTRUCTION_NODE:
        print "PROCESSING_INSTRUCTION_NODE"
    elif node.nodeType == Node.COMMENT_NODE:
        output += "<!-- " + node.data + " -->\n"
    elif node.nodeType == Node.DOCUMENT_NODE:
        # print childen
        for child in node.childNodes:
            output += prettyXml__(child, "", attributeOrder)
    elif node.nodeType == Node.DOCUMENT_TYPE_NODE:
        print "DOCUMENT_TYPE_NODE"
    elif node.nodeType == Node.NOTATION_NODE:
        print "NOTATION_NODE"

    return output

def prettyXml(document, attributeOrder):
    output = "<?xml version=\"1.0\" ?>\n"
    output += prettyXml__(document, "", attributeOrder)
    return output

def cloneNode(doc, aNode):
    e = doc.createElement(aNode.tagName)
    if aNode.hasAttributes():
        for i in range(aNode.attributes.length):
            attr = aNode.attributes.item(i)
            e.setAttribute(attr.name, aNode.getAttribute(attr.name))
    if aNode.hasChildNodes():
        for cnode in aNode.childNodes:
            if cnode.nodeType == Node.ELEMENT_NODE:
                cnodeClone = cloneNode(doc, cnode)
                e.appendChild(cnodeClone)
    return e