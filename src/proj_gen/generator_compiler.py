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

# read a file into a string
def readfile(filename):
    FH=open(filename, 'r')
    fileString = FH.read()
    FH.close()
    return fileString

# take a string and make a file from it
def writefile(filename, content):
    FH=open(filename, 'w')
    FH.write(content)
    FH.close()

# these are the documents to embed and the
# variable names to assign them to
names = [("buildFileContent", "build.xml"),
         ("clientFileContent", "Client.java"),
         ("ddlFileContent", "ddl.sql"),
         ("deleteFileContent", "Delete.java"),
         ("deploymentFileContent", "deployment.xml"),
         ("insertFileContent", "Insert.java"),
         ("projectFileContent", "project.xml"),
         ("selectFileContent", "Select.java")]

# load the template
scriptContent = readfile("generate_input.py")
# load the content before the stub code
startContent = scriptContent.split("### REPLACED BY SCRIPT ###")[0]
# load the content after the stub code
endContent = scriptContent.rsplit("### END REPLACE ###")[-1]

# start with the first half of the template
outContent = startContent

# embed all the documents
for namepair in names:
    content = readfile(namepair[1])
    outContent += namepair[0] + " = \"\"\""
    outContent += content + "\"\"\"\n\n"

# wrap up with the second half of the template
outContent += endContent

writefile("generate", outContent)
