#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2014 VoltDB Inc.
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
# this is deprecated sinve version 2.7
from optparse import OptionParser
import re
import subprocess

java_template = '\
package {%package};\n\
\n\
import org.voltdb.*;\n\
\n\
public class {%classname} extends VoltProcedure {\n\
    public final SQLStmt stmt = new SQLStmt("{%statement}");\n\
\n\
    public VoltTable[] run({%type_para}) {\n\
        voltQueueSQL(stmt{%parameters});\n\
        return voltExecuteSQL();\n\
    }\n\
}'

#TODO: is it better to use a dictionary instead of a list?
#TODO: exception handler
volt_type = [('INVALID', ''),                                    # id = 0
             ('NULL', ''),                                       # id = 1
             ('NUMERIC', ''),                                    # id = 2
             ('TINYINT', 'byte'),                                # id = 3
             ('SMALLINT', 'short'),                              # id = 4
             ('INTEGER', 'int'),                                 # id = 5
             ('BIGINT', 'long'),                                 # id = 6
             ('NOTUSED', 'not used'),
             ('FLOAT', 'double'),                                # id = 8
             ('STRING', 'String'),                               # id = 9
             ('NOTUSED', 'not used'),
             ('TIMESTAMP', 'org.voltdb.types.TimestampType'),    # id = 11
             ('NOTUSED', 'not used'),
             ('NOTUSED', 'not used'),
             ('NOTUSED', 'not used'),
             ('NOTUSED', 'not used'),
             ('NOTUSED', 'not used'),
             ('NOTUSED', 'not used'),
             ('NOTUSED', 'not used'),
             ('NOTUSED', 'not used'),
             ('NOTUSED', 'not used'),
             ('NOTUSED', 'not used'),
             ('VOLTTABLE', 'VoltTable'),                         # id = 21
             ('DECIMAL', 'java.math.BigDecimal'),                # id = 22
             ('NOTUSED', 'not used'),
             ('NOTUSED', 'not used'),
             ('VARBINARY', 'byte[]'),                            # id = 25
            ]

def generate_one_function(func_name, package, statement, para_types, is_array, output_file):
    rv = java_template.replace('{%package}', package)
    rv = rv.replace('{%classname}', func_name)
    rv = rv.replace('{%statement}', statement)

    type_para_list = []
    para_list = []
    for i, (pt, ia) in enumerate(zip(para_types, is_array)):
        if ia:
            pt += "[] "
        else:
            pt += " "
        v = "para" + str(i)
        type_para_list.append(pt + v)
        para_list.append(v)

    rv = rv.replace('{%type_para}', ", ".join(type_para_list))
    tmp = ", ".join(para_list)
    if para_types:
        tmp = ", " + tmp
    rv = rv.replace('{%parameters}', tmp)

    f = open(output_file, "w")
    f.write(rv)
    f.close()

def extract_a_procedure(f):
    line = f.next()
    func_name = line.strip().split()[-1].strip("\"")

    line = f.next() #readonly
    line = f.next()
    # TODO: use it somewhere?
    single_part = True if line.strip().split()[-1] == "true" else False
    while line.startswith('set'):
        line = f.next()

    # sql statement
    line = f.next()
    statement = line.split("sqltext")[-1].strip()
    statement = statement.strip("\"")
    statement = statement.replace("\"", "\\\"")
    statement = ' '.join(statement.split())

    while line.startswith('set'):
        line = f.next()

    # get the type of each parameter
    para_types = []
    is_array = []
    while line.startswith('add /clusters[cluster]/databases[database]/procedures[' + func_name + ']/statements[sql] parameters'):
        f.next()
        line = f.next()
        para_types.append(volt_type[int(line.strip().split()[-1])][1])
        line = f.next().strip().split()[-1] # isarray
        is_array.append(True if line == "true" else False)
        f.next()
        f.next()
        line = f.next()

    # java doesn't permit file name has '.'.
    func_name = func_name.replace('.', '_')
    # because it is hard to go back to the previous line, we need to store the current line
    return func_name, statement, para_types, is_array, line

def find_a_procedure(f, func_name = "", cur_line = ""):
    target = "add /clusters[cluster]/databases[database] procedures " + func_name
    if cur_line.startswith(target):
        return extract_a_procedure(f)

    # start to search target from the next line
    for line in f:
        if line.startswith(target):
            return extract_a_procedure(f)
    return None, None, None, None, None

def process_spec_func(func_name, package, input_file, output_dir):
    f = open(input_file)
    name, statement, para_types, is_array = find_a_procedure(f, func_name, "")
    f.close()

    if name:
        generate_one_function(name, package, statement, para_types, is_array, output_dir + '/' + name + '.java')
    else:
        print "ERROR: couldn't find " + func_name

def process_whole_ddl(package, input_file, output_dir):
    f = open(input_file)
    line = ""
    while True:
        name, statement, para_types, is_array, line = find_a_procedure(f, cur_line = line)
        if not name:
            break
        generate_one_function(name, package, statement, para_types, is_array, output_dir + '/' + name + '.java')
    f.close()

def main():
    opts, args = parse_cmd()
    if len(args) != 1:
        print "ERROR can only handle one ddl"
        sys.exit(-1)

    if args[0].endswith(".jar"):
        subprocess.check_call(("jar xf " + args[0] + " catalog.txt").split())
        input_file = "catalog.txt"
    else:
        input_file = args[0]

    if opts.procedure:
        process_spec_func(opts.procedure, opts.package, input_file, opts.target_dir)
    else:
        process_whole_ddl(opts.package, input_file, opts.target_dir)

def parse_cmd():
    parser = OptionParser()
    parser.add_option("--target_dir", type = "string", action = "store", dest = "target_dir", default = "./")
    parser.add_option("--package", type = "string", action = "store", dest = "package")
    parser.add_option("--procedure", type = "string", action = "store", dest = "procedure")
    return parser.parse_args()

if __name__ == "__main__":
    main()
