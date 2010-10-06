#!/usr/bin/env python

import sys
from xml.dom.minidom import parse, parseString
from xml.dom import Node
from xmltools import *

##
## This script does any post processing required when moving an
## example from it's home in /examples to ${build.dir}/dist/examples.
##

def splitElements(project):
    properties = []
    targets = []
    others = []

    for child in project.childNodes:
        if child.nodeType == Node.ELEMENT_NODE:
            if child.tagName == "property":
                properties.append(child)
            elif child.tagName == "target":
                targets.append(child)
            else:
                others.append(child)

    return (properties, targets, others)

def filterElements(elements):
    retval = []
    for e in elements:
        if e.tagName == "import":
            continue
        retval.append(e)
    return retval

def mergeNodes(base, build, getName):
    retval = []
    for x in base:
        found = False
        for y in build:
            if getName(x) == getName(y):
                found = True
        if not found:
            retval.append(x)
    for x in build:
        retval.append(x)
    return retval

def mergeTargets(base, build, doc):
    retval = []

    basemap = {}
    for target in base:
        basemap[target.getAttribute("name")] = target
    buildmap = {}
    for target in build:
        buildmap[target.getAttribute("name")] = target

    for target in build:
        name = target.getAttribute("name")
        depends = target.getAttribute("depends")
        deplist = []
        if depends != None:
            deplist = depends.split(",")
            deplist = [s.strip() for s in deplist]

        if name in basemap:
            basetarget = basemap[name]

            specialDep = "basebuild." + name
            if (specialDep) in deplist:
                # remove the dep from the list of deps
                if len(deplist) == 1:
                    target.removeAttribute("depends")
                else:
                    deplist.remove(specialDep)
                    deplist = deplist.join(", ")
                    target.setAttribute("depends", deplist)

                # insert the deps xml inside this xml
                firstchild = target.childNodes.item(0)
                for i in range(basetarget.childNodes.length):
                    child = basetarget.childNodes.item(i)
                    clone = cloneNode(doc, child)
                    target.insertBefore(clone, firstchild)

            # pick a description from the base if needed
            if not target.hasAttribute("description"):
                if basetarget.hasAttribute("description"):
                    value = basetarget.getAttribute("description")
                    target.setAttribute("description", value)

            # remove the base target from the output targets
            del basemap[name]

    for target in base:
        name = target.getAttribute("name")
        if name in basemap:
            if name == "export":
                #ignore non-overrided export targets
                continue
            retval.append(target)

    for target in build:
        retval.append(target)

    return retval

# args should be
# 1. the program name
# 2. the path to basebuild.xml
# 2. the path to the example dir
if len(sys.argv) != 3:
    print "examply-dist-cleanup.py expects 3 arguments."
    sys.exit(-1)

# replace the path to the basebuild.xml in build.xml
pathToBuildXml = sys.argv[2] + "/build.xml"
pathToBasebuild = sys.argv[1]

basedom = parse(open(pathToBasebuild))
builddom = parse(open(pathToBuildXml))
pruneTextNodes(basedom)
pruneTextNodes(builddom)

baseproject = basedom.getElementsByTagName("project").item(0)
buildproject = builddom.getElementsByTagName("project").item(0)

baseproperties, basetargets, baseothers = splitElements(baseproject)
buildproperties, buildtargets, buildothers = splitElements(buildproject)

properties = mergeNodes(baseproperties, buildproperties, lambda x: x.getAttribute("name"))
for p in properties:
    if p.getAttribute("name") == "voltdb.dir":
        p.setAttribute("location", "../../voltdb")

# use the classpath from the build.xml, not the base.xml (only if there are 2)
for o in buildothers:
    if o.tagName == "path" and o.getAttribute("id") == "project.classpath":
        for o2 in baseothers:
            if o2.tagName == "path" and o2.getAttribute("id") == "project.classpath":
                baseothers.remove(o2)
                break

others = baseothers + buildothers
others = filterElements(others)

output = "<?xml version=\"1.0\" ?><project name='"
output += buildproject.getAttribute("name") + "' default='default'/>"

output = parseString(output)
newproject = output.getElementsByTagName("project").item(0)

for p in properties:
    newproject.appendChild(cloneNode(output, p))

for o in others:
    newproject.appendChild(cloneNode(output, o))

basetargets = [cloneNode(output, t) for t in basetargets]
buildtargets = [cloneNode(output, t) for t in buildtargets]

targets = mergeTargets(basetargets, buildtargets, output)

for t in targets:
    newproject.appendChild(t)

addWhitespace(output, newproject)

addBeforeNodesWithId(output, newproject, "property-voltdb.dir", "\n", None)
addBeforeNodesWithId(output, newproject, "property-voltdb.dir", None,
    "Adjust the location of VoltDB if you move this example.")

addBeforeNodesWithId(output, newproject, "property-build.dir", "\n", None)

attributeOrder = {
    "property": ["name", "value", "location"],
    "target"  : ["name", "depends", "description"],
    "java"    : ["classname"],
    "javac"   : ["srcdir", "destdir"],
    "fileset" : ["dir"],
}

content = prettyXml(output, attributeOrder)
writeFile(pathToBuildXml, content)
#print content
