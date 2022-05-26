# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.
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

import os
import sys
import string
import random
import shutil
import subprocess
import shlex
from string import Template
from optparse import OptionParser

DEFAULT_CLASSES_DIR = "procedures/uac"
DEFAULT_JARS_DIR = "jars"
JARS_CMD_PRFIX = "./run.sh uacjars "

PAT_CREATE_PROC_STMT = "CREATE PROCEDURE FROM CLASS uac.Proc{0};"
PAT_DROP_PROC_STMT = "DROP PROCEDURE uac.Proc{0};"
PAT_CLS_DEL_STMT = "uac.Proc{0}"

def generate_ddl(tables, ddlname):
    tb = ('create table t{0} (a integer not null, b varchar(10), c integer,'
            'd smallint, e integer, primary key(a, b));'
          'create index t{1}_idx1 on t{2}(a,c);'
          'create index t{3}_idx2 on t{4}(a,d);'
    )

    ddl = "file -inlinebatch END_OF_BATCH\n";
    for i in range(tables):
        ddl += tb.format(i, i, i, i, i, i) + "\n"

    ddl += "END_OF_BATCH\n"

    with open(ddlname, "w") as java_file:
        java_file.write(ddl)
    return

def procedure_stmts(pattern, pfrom, pto, filename):
    stmts = ""
    for i in range(pfrom, pto):
        stmts += pattern.format(i) + "\n"

    if len(stmts.strip(' \t\n\r')) == 0:
        sys.exit("stmts not generated from " + str(pfrom) + " to " + str(pto) + " for file: " + filename)

    with open(filename, "w") as stmts_file:
        stmts_file.write(stmts)
    return


def generate_classes(ithProc, filename):
    random.seed(777)

    with open('classtemplate.txt', 'r') as template_file:
        template = Template(template_file.read())

    stmt_template = Template('    static final SQLStmt stmt$stmtnum = new SQLStmt("SELECT * FROM T$t0, T$t1, T$t2");\n')

    static_data_template = Template('    public static String data$n = "$str";\n')
    some_static_data = ''
    for j in range(0, 30):
        the_str = ''
        for k in range(0, 100):
            the_str += str(random.choice(string.letters))
        some_static_data += static_data_template.substitute(n=j, str=the_str)

    some_stmts = ''
    for j in range(0, 2):
        some_stmts += stmt_template.substitute(
            stmtnum=j,
            t0=random.randint(0, 5),
            t1=random.randint(5, 10),
            t2=random.randint(10, 15))

    java_str = template.substitute(procnum=ithProc, stmts=some_stmts, static_data=some_static_data)
    with open(filename, "w") as java_file:
        java_file.write(java_str)

    return

def generate_base_jar(tablecount, procedurecount):
    for i in range(0, procedurecount):
        filename = "%s/Proc%d.java" % (DEFAULT_CLASSES_DIR, i)
        generate_classes(i, filename)

    # invoke shell script to compile jars
    # TODO: xin
    subprocess.call(shlex.split('./run.sh uacjars uac_base.jar'))

    # generate procedure stmts
    filename = "%s/stmts_base.txt" % (DEFAULT_JARS_DIR)
    procedure_stmts(PAT_CREATE_PROC_STMT, 0, procedurecount, filename)

    return

def add_jars(procedurecount, invocations):
    # add a procedure
    for i in range(0, invocations):
        j = procedurecount + i
        filename = "%s/Proc%d.java" % (DEFAULT_CLASSES_DIR, j)
        generate_classes(j, filename)

        # invoke shell script to compile jars
        cmd = "%s uac_add_%s.jar" % (JARS_CMD_PRFIX, j)
        subprocess.call(shlex.split(cmd))

        # generate procedure stmts
        filename = "%s/stmts_add_%d.txt" % (DEFAULT_JARS_DIR, j)
        procedure_stmts(PAT_CREATE_PROC_STMT, j, j + 1, filename)

def add_batch_jars(procedurecount, invocations, batchsize):
    # add procedures
    for i in range(0, invocations):
        base = procedurecount + i * batchsize
        for j in range(0, batchsize):
            j = base + j
            filename = "%s/Proc%d.java" % (DEFAULT_CLASSES_DIR, j)
            generate_classes(j, filename)

        # invoke shell script to compile jars
        cmd = "%s uac_add_batch_%s.jar" % (JARS_CMD_PRFIX, base)
        subprocess.call(shlex.split(cmd))

        # generate procedure stmts
        filename = "%s/stmts_add_batch_%d.txt" % (DEFAULT_JARS_DIR, base)
        procedure_stmts(PAT_CREATE_PROC_STMT, base, base + batchsize, filename)

def del_jars(procedurecount, invocations):
    # drop a procedure
    for i in range(0, invocations):
        j = procedurecount - i - 1
        # generate procedure stmts
        filename = "%s/stmts_del_%d.txt" % (DEFAULT_JARS_DIR, j)
        procedure_stmts(PAT_DROP_PROC_STMT, j, j + 1, filename)

        filename = "%s/pat_del_%d.txt" % (DEFAULT_JARS_DIR, j)
        procedure_stmts(PAT_CLS_DEL_STMT, j, j + 1, filename)

def del_batch_jars(procedurecount, invocations, batchsize):
    # drop procedures
    for i in range(0, invocations):
        base = procedurecount - (i + 1) * batchsize
        # generate procedure stmts
        filename = "%s/stmts_del_batch_%d.txt" % (DEFAULT_JARS_DIR, base)
        procedure_stmts(PAT_DROP_PROC_STMT, base, base + batchsize, filename)

        filename = "%s/pat_del_batch_%d.txt" % (DEFAULT_JARS_DIR, base)
        procedure_stmts(PAT_CLS_DEL_STMT, base, base + batchsize, filename)


def upd_jars(procedurecount, invocations):
    # drop a procedure
    del_jars(procedurecount, invocations);
    add_jars(procedurecount, invocations);

# cleaning the input dir
def clean_dirpath(dirpath):
    #if os.path.exists(dirpath):
    #    shutil.rmtree(dirpath)
    #os.makedirs(dirpath, 0755)
    return

def clean():
    clean_dirpath(DEFAULT_JARS_DIR)
    clean_dirpath(DEFAULT_CLASSES_DIR)
    clean_dirpath(DEFAULT_CLASSES_DIR)
    return

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-n", "--name", default="None",
                      type='choice', # it is string type
                      choices= ['ADD', 'DEL','UPD', 'ADD_BATCH', 'DEL_BATCH', 'UPD_BATCH', 'None'],
                      help=('The name of benchmark,'
                      'available option are:ADD, DEL, UPD, ADD_BATCH, DEL_BATCH, UPD_BATCH'))
    parser.add_option("-t", "--tablecount", type="int", default=500,
                      help="number of tables")
    parser.add_option("-p", "--procedurecount", type="int", default=1000,
                      help="number of procedures")
    parser.add_option("-i", "--invocations", type="int", default=5,
                      help="number of invocations to run")
    parser.add_option("-b", "--batchsize", type="int", default=1,
                      help="batch size of procedure operations per invocations")

    (options, args) = parser.parse_args()

    if options.name == "UPD" or options.name == "UPD_BATCH":
        sys.exit('Update a procedure without syntax change is similar to add a procedure, '
                 'Otherwise it needs to drop a procedure then add a procedure\n'
                 'Please run other benchmarks: ADD, ADD_BATCH, DROP, DROP_BATCH')

    # generate base jars: 500 tables with 1000 procedures
    generate_ddl(options.tablecount, "ddlbase.sql")

    # clean jar directory
    clean_dirpath(DEFAULT_JARS_DIR)

    # clean procedures
    clean_dirpath(DEFAULT_CLASSES_DIR)
    # generate the benchmark base jar
    generate_base_jar(options.tablecount, options.procedurecount)

    # clean procedures
    clean_dirpath(DEFAULT_CLASSES_DIR)
    if options.name == "None":
        sys.exit("No benchmark generated")

    if options.name == "ADD":
        add_jars(options.procedurecount, options.invocations)
    elif options.name == "ADD_BATCH":
        add_batch_jars(options.procedurecount, options.invocations, options.batchsize)
    elif options.name == "DEL":
        del_jars(options.procedurecount, options.invocations)
    elif options.name == "DEL_BATCH":
        del_batch_jars(options.procedurecount, options.invocations, options.batchsize)
#     elif options.name == "UPD" or options.name == "UPD_BATCH":
#         sys.exit('Update a procedure without syntax change is similar to add a procedure, '
#                  'Otherwise it needs to drop a procedure then add a procedure')
#         upd_jars(options.procedurecount, options.invocations)
#         upd_batch_jars(options.procedurecount, options.invocations, options.batchsize)
