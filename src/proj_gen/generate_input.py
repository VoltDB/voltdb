#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2011 VoltDB Inc.
#
# VoltDB is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# VoltDB is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.

import sys
import string
import os

def readfile(filename):
    FH=open(filename, 'r')
    fileString = FH.read()
    FH.close()
    return fileString

def writefile(filename, content):
    FH=open(filename, 'w')
    FH.write(content)
    FH.close()

### REPLACED BY SCRIPT ###

# these variables will be replaced when generator_compiler is run
# this is just here for testing
buildFileContent = readfile("build.xml")
clientFileContent = readfile("Client.java")
ddlFileContent = readfile("ddl.sql")
deleteFileContent = readfile("Delete.java")
deploymentFileContent = readfile("deployment.xml")
insertFileContent = readfile("Insert.java")
projectFileContent = readfile("project.xml")
selectFileContent = readfile("Insert.java")

### END REPLACE ###

# this method is pulled from python svn to make this work on 2.5
# trivial mods only
def relpath(path, start=os.curdir):
    """Return a relative version of a path"""

    if not path:
        raise ValueError("no path specified")
    start_list = os.path.abspath(start).split(os.sep)
    path_list = os.path.abspath(path).split(os.sep)
    if start_list[0].lower() != path_list[0].lower():
        unc_path, rest = os.path.splitunc(path)
        unc_start, rest = os.path.splitunc(start)
        if bool(unc_path) ^ bool(unc_start):
            raise ValueError("Cannot mix UNC and non-UNC paths (%s and %s)"
                                                                % (path, start))
        else:
            raise ValueError("path is on drive %s, start on drive %s"
                                                % (path_list[0], start_list[0]))
    # Work out how much of the filepath is shared by start and path.
    for i in range(min(len(start_list), len(path_list))):
        if start_list[i].lower() != path_list[i].lower():
            break
    else:
        i += 1

    rel_list = [os.pardir] * (len(start_list)-i) + path_list[i:]
    if not rel_list:
        return os.curdir
    return os.path.join(*rel_list)

def replace(instr, pattern, value):
    pattern = string.strip(pattern)
    value = string.strip(value)
    return string.replace(instr, pattern, value)

def copyAndSubstituteTemplates(srcString, dest_fn, package_prefix, project_name, voltdb_lib_path):
    """Copy file and replace template parameters with real values"""
    srcString = replace(srcString, "##package_prefix##", package_prefix)
    srcString = replace(srcString, "##project_name##", project_name)
    srcString = replace(srcString, "##upper_project_name##", string.upper(project_name))
    srcString = replace(srcString, "##voltdb_lib_path##", voltdb_lib_path)
    writefile(dest_fn, srcString)

def isDestExist(path):
    return os.path.exists(path)

def isParentDir(candidateParent, candidateChild):
    "determine if one dir contains another"
    rel = relpath(candidateParent, candidateChild)
    for element in rel.split(os.sep):
        if element != "..":
            return False
    return True

if len(sys.argv) != 4:
    print "Usage: generate <name> <package_prefix> <destination>"
    sys.exit(0)

# find the location of the voltdb jar
distpath = os.path.split(sys.path[0])[0]
voltpath = distpath + "/voltdb"

project_name = string.strip(sys.argv[1])
package_prefix = string.strip(sys.argv[2])
package_prefix_wslashies = string.replace(package_prefix, ".", "/")

# destination should be relative to user's original cwd
destination = sys.argv[3]

if isDestExist(destination):
    print "Warning: Destination", destination, "already exists."
    print "Please specify a different location."
    sys.exit(0)

# if the user is putting the new project inside the dist folder,
# use a relative path for voltdb
if isParentDir(distpath, destination):
    voltpath = relpath(distpath + "/voltdb", destination)

# Make the directory for the stored procedures and by extension the root of the project
os.system("mkdir -p %s/src/%s/procedures"%(destination, package_prefix_wslashies))

# Example procedures
for proc in [("Insert.java", insertFileContent),
             ("Delete.java", deleteFileContent),
             ("Select.java", selectFileContent)]:
    dest_fn = "%s/src/%s/procedures/%s"%(destination, package_prefix_wslashies, proc[0])
    copyAndSubstituteTemplates(proc[1], dest_fn, package_prefix, project_name, voltpath)

# Client.java
dest_fn = "%s/src/%s/Client.java"%(destination, package_prefix_wslashies)
copyAndSubstituteTemplates(clientFileContent, dest_fn, package_prefix, project_name, voltpath)

# build.xml
dest_fn = "%s/build.xml"%(destination)
copyAndSubstituteTemplates(buildFileContent, dest_fn, package_prefix, project_name, voltpath)

# project.xml
dest_fn = "%s/project.xml"%(destination)
copyAndSubstituteTemplates(projectFileContent, dest_fn, package_prefix, project_name, voltpath)

# deployment.xml
dest_fn = "%s/deployment.xml"%(destination)
copyAndSubstituteTemplates(deploymentFileContent, dest_fn, package_prefix, project_name, voltpath)

# ddl.sql
dest_fn = "%s/%s-ddl.sql"%(destination, project_name)
copyAndSubstituteTemplates(ddlFileContent, dest_fn, package_prefix, project_name, voltpath)

print "Generated skeleton Volt project in %s" % (destination)
