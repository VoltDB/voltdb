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
import os
# this is deprecated sinve version 2.7
from optparse import OptionParser
import re
import subprocess
import filecmp

java_template = '''
package {%package};

import org.voltdb.*;

public class {%classname} extends VoltProcedure {
    public final SQLStmt stmt = new SQLStmt("{%statement}");

    public VoltTable[] run({%type_param}) {
        voltQueueSQL(stmt{%parameters});
        return voltExecuteSQL();
    }
}
'''

#TODO: is it better to use a dictionary instead of a list?
#TODO: exception handler
volt_type = ['',                                    # id = 0, INVALID
             '',                                    # id = 1, NULL
             '',                                    # id = 2, NUMERIC
             'byte',                                # id = 3, TINYINT
             'short',                               # id = 4, SMALLINT
             'int',                                 # id = 5, INTEGER
             'long',                                # id = 6, BIGINT
             'not used',
             'double',                              # id = 8, FLOAT
             'String',                              # id = 9, STRING
             'not used',
             'org.voltdb.types.TimestampType',      # id = 11, TIMESTAMP
             'not used',
             'not used',
             'not used',
             'not used',
             'not used',
             'not used',
             'not used',
             'not used',
             'not used',
             'not used',
             'VoltTable',                           # id = 21, VOLTTABLE
             'java.math.BigDecimal',                # id = 22, DECIMAL
             'not used',
             'not used',
             'byte[]',                              # id = 25, VARBINARY
            ]

def generate_one_function(func_name, package, statement, param_types, is_array, output_file):
    rv = java_template.replace('{%package}', package)
    rv = rv.replace('{%classname}', func_name)
    rv = rv.replace('{%statement}', statement)

    type_param_list = []
    param_list = []
    for i, (pt, ia) in enumerate(zip(param_types, is_array)):
        if ia:
            pt += "[] "
        else:
            pt += " "
        v = "param" + str(i)
        type_param_list.append(pt + v)
        param_list.append(v)

    rv = rv.replace('{%type_param}', ", ".join(type_param_list))
    if param_types:
        tmp = ", " + ", ".join(param_list)
    else:
        tmp = ""
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
    param_types = []
    is_array = []
    while line.startswith('add /clusters[cluster/databases[database/procedures[' + func_name + '/statements[sql parameters'):
        f.next()
        line = f.next()
        param_types.append(volt_type[int(line.strip().split()[-1])])
        line = f.next().strip().split()[-1] # isarray
        is_array.append(True if line == "true" else False)
        f.next()
        f.next()
        line = f.next()

    # java doesn't permit file name has '.'.
    func_name = func_name.replace('.', '_')
    # because it is hard to go back to the previous line, we need to store the current line
    return func_name, statement, param_types, is_array, line

def find_a_procedure(f, func_name = "", cur_line = ""):
    target = "add /clusters[cluster/databases[database procedures " + func_name
    if cur_line.startswith(target):
        return extract_a_procedure(f)

    # start to search target from the next line
    for line in f:
        if line.startswith(target):
            return extract_a_procedure(f)
    return None, None, None, None, None

def process_spec_func(func_name, package, input_file, output_dir):
    f = open(input_file)
    name, statement, param_types, is_array, line = find_a_procedure(f, func_name, "")
    f.close()

    if name:
        generate_one_function(name, package, statement, param_types, is_array, output_dir + '/' + name + '.java')
    else:
        print "ERROR: couldn't find " + func_name

def process_whole_ddl(package, input_file, output_dir):
    f = open(input_file)
    line = ""
    while True:
        name, statement, param_types, is_array, line = find_a_procedure(f, cur_line = line)
        if not name:
            break
        generate_one_function(name, package, statement, param_types, is_array, output_dir + '/' + name + '.java')
    f.close()

def self_test():
    ddl = '''
CREATE TABLE P1 ( 
    ID INTEGER DEFAULT '0' NOT NULL, 
    BIG BIGINT,
    RATIO FLOAT,
    TM TIMESTAMP DEFAULT '2014-12-31',
    VAR VARCHAR(300),
    DEC DECIMAL,
    PRIMARY KEY (ID)
);
PARTITION TABLE P1 ON COLUMN ID;
CREATE PROCEDURE Test AS SELECT ID, TM, VAR FROM P1 WHERE TM < ? AND ID > ?; 
'''
    subprocess.check_call("rm -rf /tmp/tempGenJavaSPTool".split())
    subprocess.check_call("mkdir -p /tmp/tempGenJavaSPTool".split())
    # change working directory
    os.chdir("/tmp/tempGenJavaSPTool")
    # compile ddl
    f = open("/tmp/tempGenJavaSPTool/ddl.sql", "w")
    f.write(ddl)
    f.close()
    subprocess.check_call("voltdb compile ddl.sql".split())
    if not os.path.exists("catalog.jar"):
        print "cannot generate catalog.jar"
        sys.exit(-1)
    # generate sp
    subprocess.check_call(("unzip catalog.jar catalog.txt -d /tmp/tempGenJavaSPTool").split())
    process_spec_func("Test", "package", "catalog.txt", "./")
    # compare
    if not os.path.exists("Test.java"):
        print "cannot generate java file"
        sys.exit(-1)

    golden = '''
package package;

import org.voltdb.*;

public class Test extends VoltProcedure {
    public final SQLStmt stmt = new SQLStmt("SELECT ID, TM, VAR FROM P1 WHERE TM < ? AND ID > ?;");

    public VoltTable[] run(org.voltdb.types.TimestampType param0, int param1) {
        voltQueueSQL(stmt, param0, param1);
        return voltExecuteSQL();
    }
}
'''
    f = open("/tmp/tempGenJavaSPTool/golden.java", "w")
    f.write(golden)
    f.close()

    if filecmp.cmp('golden.java', 'Test.java'):
        print "generated our expected java file"
        rv = 0
    else:
        print "generated file is different from our expectation"
        rv = -1

    subprocess.check_call("rm -rf /tmp/tempGenJavaSPTool".split())
    sys.exit(rv)

def main():
    opts, args = parse_cmd()
    if opts.self_test:
        self_test()

    if len(args) != 1:
        print "ERROR can only handle one ddl"
        sys.exit(-1)

    if args[0].endswith(".jar"):
        subprocess.check_call("rm -rf /tmp/tempGenJavaSPTool".split())
        subprocess.check_call("mkdir -p /tmp/tempGenJavaSPTool".split())
        subprocess.check_call(("unzip " + args[0] + " catalog.txt -d /tmp/tempGenJavaSPTool").split())
        input_file = "/tmp/tempGenJavaSPTool/catalog.txt"
    else:
        input_file = args[0]

    try:
        if opts.procedure:
            process_spec_func(opts.procedure, opts.package, input_file, opts.target_dir)
        else:
            process_whole_ddl(opts.package, input_file, opts.target_dir)
    
        if args[0].endswith(".jar"):
            subprocess.check_call("rm -rf /tmp/tempGenJavaSPTool".split())
    except Exception as e:
        subprocess.check_call("rm -rf /tmp/tempGenJavaSPTool".split())
        raise e

def parse_cmd():
    parser = OptionParser()
    parser.add_option("--target_dir", type = "string", action = "store", dest = "target_dir", default = "./")
    parser.add_option("--package", type = "string", action = "store", dest = "package")
    parser.add_option("--procedure", type = "string", action = "store", dest = "procedure")
    parser.add_option("--self-test", action = "store_true", dest = "self_test", default = False)
    return parser.parse_args()

if __name__ == "__main__":
    main()
