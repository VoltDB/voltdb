#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2015 VoltDB Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import sys
from sys import argv
import re

# Parses a sqltest.sql file. See sqltest-example.sql for supported
# syntax. The parsed results are stored as SQLStatement and Metadata
# classes.  There are serializers for these classes to produce VoltDB
# specific output though any serializer could be added. Sqltest files
# are valid sql - all metadata exists in comments

class Metadata:
    def serializeVoltVerify(self, spacer):
        return

    def serializeVoltDescription(self, spacer):
        return


class MetaDesc(Metadata):
    def __init__(self, aDesc):
        self.desc = aDesc

    def serializeVoltDescription(self, spacer):
        print spacer*2 + "/* Description: " + self.desc + " */"


class MetaRows(Metadata):
    def __init__(self, aRowcount):
        self.rowcount = int(aRowcount)

    def serializeVoltVerify(self, spacer):
        print spacer*2 + "RowCount = %s" % self.rowcount


class MetaTable(Metadata):
    def __init__(self, rows):
        self.table = rows

    def serializeVoltVerify(self, spacer):
        print spacer*2 + "TableVerfifier:"
        for x in self.table:
            print x


class SQLStatement:
    global_id = 0

    def isReadOnly(self):
        if (re.compile("(.*?)SELECT(.*)$", re.I).match(self.statement) != None):
            return True
        return False

    def isDDL(self):
        if (re.compile("(.*?)CREATE(.*)$", re.I).match(self.statement) != None):
            return True
        return False

    def isSingleSited(self):
        return True

    def __init__(self, txt):
        SQLStatement.global_id += 1
        self.id = SQLStatement.global_id
        self.statement = txt
        self.metadata = []

    def printSQLStmts(self, spacer):
        if self.isDDL() == False:
            # @stmtAttributes(readOnly = ?, singledSited = ?)
            ro = "readOnly = true" if self.isReadOnly() else "readOnly = false"
            ss = "singleSited = true" if self.isSingleSited() else "singleSited = false"
            print spacer, "@StmtAttributes(", ro, ",", ss, ")"
            # public static final SQLStmt someName = new SQLStmt( TEXT )
            output = "public static final SQLStmt s%d = new SQLStmt\n%s  (%s);\n"%\
                (self.id, spacer, self.statement.strip(";"))
            print spacer, output

    def printDDLStatements(self, spacer):
        if self.isDDL() == True:
            print spacer, self.statement

    def printQueryExecutions(self, spacer):
        # description; hz.queue(); hz.execute(); verification
        if self.isDDL() == False:
            print spacer*2  + "// Query: s%s" % self.id
            for m in self.metadata:
                m.serializeVoltDescription(spacer)
            if self.isReadOnly() == True:
                output = "HZ.queueQuery(s%d);"%self.id
                print spacer*2, output
                output = "result_table = HZ.executeQueries()[0];"
                print spacer*2, output
            else:
                output = "HZ.queueDML(s%d);"%self.id
                print spacer*2, output
                output = "result_long = HZ.executeDML()[0];"
                print spacer*2, output
            for m in self.metadata:
                m.serializeVoltVerify(spacer)


def readMultilineComment(f):
    commentText = ""
    prevChar = None
    char = f.read(1)
    while char != "":
        if prevChar == "*" and char == "/":
            return commentText[:-1] + "\n"
        commentText = commentText + char
        prevChar = char
        char = f.read(1)
    print "Error: EOF processing multi-line comment: " + commentText
    return None

def readSingleLineComment(f):
    commentText = ""
    char = f.read(1)
    while char != "" and char != "\n":
        commentText = commentText + char
        char = f.read(1)
    return commentText + "\n"

def readConsecutiveComments(f):
    comment = ""
    prevChar = None
    mark = f.tell()
    while 1:
        char = f.read(1)
        if char == "":
            f.seek(mark)
            break
        # condense all inititial whitespace to '\n'
        while char.isspace() or char == '\n':
            if not prevChar:
                comment += "\n"
            prevChar = " "
            char = f.read(1)
            continue
        # past whitespace, must start comment
        if char == "-" or char == "/":
            prevChar = char
            char = f.read(1)
        # concat comments or rewind and return
        if prevChar == "-" and char == "-":
            comment += readSingleLineComment(f)
            mark = f.tell()
            prevChar = None
        elif prevChar == "/" and char == "*":
            comment += readMultilineComment(f)
            mark = f.tell()
            prevChar = None
        else:
            f.seek(mark)
            break
    return comment

def getNextStatementText(f):
    "return the next sql statement"
    statement = ""    # accumulated statement text.
    prevChar = None   # previously read character
    char = f.read(1)
    while char != "":
        # detect -- comments
        if prevChar == "-" and char == "-":
            prevChar = None
            statement = statement[:-1]
            readSingleLineComment(f)
            char = f.read(1)
        # detect /* .. */ comments
        elif prevChar == "/" and char == "*":
            prevChar = None
            statement = statement[:-1]
            readMultilineComment(f)
            char = f.read(1)
        # condense all sequential whitespace to " "
        elif char.isspace():
            char = " "
            if prevChar != None and not prevChar.isspace():
                statement = statement + char
            prevChar = char
            char = f.read(1)
        # detect end of statement
        elif char == ";":
            return statement
        # accumulate text
        else:
            statement = statement + char
            prevChar = char
            char = f.read(1)

    return None

def getMetadataList(f):
    "Collect all the metadata items from sequential comments"
    prevChar = None
    mds = []

    # regular expressions for metadata keywords
    desc_re  = re.compile(r"^\s*desc:(.*)$",re.M);
    rows_re  = re.compile(r"^\s*rows:(.*)$",re.M)
    table_re = re.compile(r"^\s*table:(.*)$",re.M)

    # process the comment(s) for metadata
    comment = readConsecutiveComments(f)
    it = iter(comment.split('\n'))
    for line in it:
        desc_m = desc_re.match(line)
        if desc_m:
            md = MetaDesc(desc_m.group(1))
            if md: mds.append(md)

        row_m = rows_re.match(line)
        if row_m:
            md = MetaRows(row_m.group(1))
            if md: mds.append(md)

        table_m = table_re.match(line)
        if table_m:
            rows = []
            try:
                line = it.next()
                while line and line != "" and line != "\n":
                    rows.append(line.split(','))
                    line = it.next()
            except StopIteration:
                pass
            finally:
                md = MetaTable(rows)
                if md: mds.append(md)
    return mds


def getNextStatement(f):
    "build a statement with metadata attached"
    stmtText = getNextStatementText(f)
    if stmtText != None:
        statement = SQLStatement(stmtText)
        mds = getMetadataList(f)
        while mds:
            for m in mds:
                statement.metadata.append(m)
            mds = getMetadataList(f)
        return statement
    return None


def createStoredProcedure(sqlfile, classname):
    # gather cleaned-up statements into a list
    f = open(sqlfile)
    statements = []
    stmt = getNextStatement(f)
    while stmt != None:
        statements.append(stmt)
        stmt = getNextStatement(f)

    # file header stuff
    print "/* This procedure generated by sqltests.py using ", sqlfile, "*/"
    print "package com.volt.sqltest;"
    print "import com.volt.*";

    # let DDL statements print
    print "\n/* DDL SECTION */\n"
    for s in statements:
        s.printDDLStatements("")

    # start procedure
    print "\n/* PROCEDURE SECTION */\n"
    print "@ProcInfo("
    print "\tpartitionInfo = \"T.C1: 0\","
    print "\talwaysSingleSite = true"
    print ")"
    print "public class ", classname, "{"
    print "\tHZTable result_table = null;"
    print "\tlong result_long = 0L;"

    # print query and dml statements
    for s in statements:
        s.printSQLStmts("\t")

    # create run method
    print "\tpublic static HZTable[] run() throws HZ.AbortException {"
    for s in statements:
        s.printQueryExecutions("\t")
        print("")
    print "\t}"
    print "}"


def main(args):
    # sqltests.py create-sp <file.sql>
    if (args[1] == 'create-sp'):
        return(createStoredProcedure(args[2], args[3]))
    else:
        # invalid input, display usage.
        print argv[0] + " create-sp <sqlfile> <output-class>"
        return -1

if __name__ == '__main__':
    exit(main(argv))

