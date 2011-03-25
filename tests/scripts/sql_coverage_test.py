#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2011 VoltDB Inc.
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
import random
import time
import subprocess
import cPickle
import os.path
import imp
from base64 import encodestring
from voltdbclient import *
from optparse import OptionParser
from xml2 import XMLGenerator
from Query import VoltQueryClient
from SQLCoverageReport import generate_html_reports, generate_summary
from SQLGenerator import SQLGenerator

class Config:
    def __init__(self, filename):
        fd = open(filename, "r")
        self.__content = fd.read()
        fd.close()

        self.__config = eval(self.__content.strip())

    def get_configs(self):
        return self.__config.keys()

    def get_config(self, config_name):
        return self.__config[config_name]

def run_once(name, command, statements):
    global normalize
    server = subprocess.Popen(command + " backend=" + name, shell = True)
    client = None

    for i in xrange(10):
        try:
            client = VoltQueryClient("localhost", 21212)
            client.set_quiet(True)
            client.set_timeout(5.0) # 5 seconds
            break
        except socket.error:
            time.sleep(1)

    if client == None:
        return -1

    for statement in statements:
        try:
            client.onecmd("adhoc " + statement["SQL"])
        except:
            print >> sys.stderr, "Error occurred while executing '%s': %s" % \
                (statement["SQL"], sys.exc_info()[1])
            # Should kill the server now
            killer = subprocess.Popen("kill -9 %d" % (server.pid), shell = True)
            killer.communicate()
            if killer.returncode != 0:
                print >> sys.stderr, \
                    "Failed to kill the server process %d" % (server.pid)
            break
        if client.response.tables != None:
            tables = [normalize(t, statement["SQL"]) for t in client.response.tables]
            tablestr = cPickle.dumps(tables, cPickle.HIGHEST_PROTOCOL)
        else:
            tablestr = cPickle.dumps(None, cPickle.HIGHEST_PROTOCOL)
        statement[name] = {"Status": client.response.status,
                           "Info": client.response.statusString,
                           "Result": encodestring(tablestr)}
        if client.response.exception != None:
            statement[name]["Exception"] = str(client.response.exception)

    client.onecmd("shutdown")
    server.communicate()

    return server.returncode

def run_config(config, basedir, output_dir, random_seed, report_all, args):
    for key in config.iterkeys():
        if not os.path.isabs(config[key]):
            config[key] = os.path.abspath(os.path.join(basedir, config[key]))
    report_filename = "report.xml"
    report_filename = os.path.abspath(os.path.join(output_dir, report_filename))
    template = config["template"]

    global normalize
    if "normalizer" in config:
        normalize = imp.load_source("normalizer", config["normalizer"]).normalize
    else:
        normalize = lambda x, y: x

    command = " ".join(args[2:])
    command += " schema=" + os.path.basename(config['ddl'])

    random_state = random.getstate()
    if "template-jni" in config:
        template = config["template-jni"]
    generator = SQLGenerator(config["schema"], template, True)
    statements = []
    counter = 0

    for i in generator.generate():
        statements.append({"id": counter,
                           "SQL": i})
        counter += 1

    if run_once("jni", command, statements) != 0:
        print >> sys.stderr, "Test with the JNI backend had errors."
        exit(1)

    random.seed(random_seed)
    random.setstate(random_state)
    # To get around the timestamp issue. Volt and HSQLDB use different units
    # for timestamp (microsec vs. millisec), so we have to use different
    # template file for regression test, since all the statements are not
    # generated in this case.
    if "template-hsqldb" in config:
        template = config["template-hsqldb"]
    generator = SQLGenerator(config["schema"], template, False)
    counter = 0

    for i in generator.generate():
        statements[counter]["SQL"] = i
        counter += 1

    if run_once("hsqldb", command, statements) != 0:
        print >> sys.stderr, "Test with the HSQLDB backend had errors."
        exit(1)

    report_dict = {"Seed": random_seed, "Statements": statements}
    report = XMLGenerator(report_dict)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    fd = open(report_filename, "w")
    fd.write(report.toXML())
    fd.close()

    success = generate_html_reports(report_dict, output_dir, report_all)
    return success

def usage():
    print sys.argv[0], "config output_dir command"
    print """
config\t\tThe configuration file containing the filenames of the schema,
\t\tthe template, and the normalizer.
output_dir\tThe output directory for the HTML reports.
command\t\tThe command to launch the server.

The schema is merely a Python dictionary which describes the name of the tables
and the column names and types in those tables. The following is an example of a
schema description,

\t{
\t    "T": {
\t        "columns": (("DESC", FastSerializer.VOLTTYPE_STRING),
\t                    ("ID", FastSerializer.VOLTTYPE_INTEGER),
\t                    ("NUM", FastSerializer.VOLTTYPE_INTEGER)),
\t        "partitions": (),
\t        "indexes": ("ID")
\t        }
\t    }

This dictionary describes a table called "T" with three columns "DESC", "ID",
and "NUM".

The template is a .sql file containing SQL statements to run in the test. The
SQL statements are templates with place holders which will be substituted with
real values when the test is run. An example looks like this,

\tSELECT _variable FROM _table WHERE _variable _cmp _variable LIMIT _value[byte];

A possible SQL statement generated from this template based on the table
description above would be

\tSELECT ID FROM T WHERE ID < NUM LIMIT 3;

The following place holders are supported,

\t_variable[type]\tWill be replaced with a column name of the given type,
\t\t\ttype can be int,byte,int16,int32,int64,float,string,
\t\t\tdate. int is a superset of byte,int16,int32,int64.
\t\t\tType can be omitted.
\t_table\t\tWill be replaced with a table name
\t_value[type]\tWill be replaced with a random value of the given type,
\t\t\ttype can be id,byte,int16,int32,int64,float,string,date.
\t\t\tid is unique integer type incremented by 1 each time.
\t\t\tYou can also specify an integer within a range,
\t\t\te.g. _value[int:0,100]

\t_cmp\t\tWill be replaced with a comparison operator
\t_math\t\tWill be replaced with a arithmatic operator
\t_agg\t\tWill be replaced with an aggregation operator
\t_singleton\t\tWill be replaced with an operator like NOT
\t_set\t\tWill be replaced with a set operator
\t_logic\t\tWill be replaced with a logic operator"""

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-s", "--seed", dest="seed",
                      help="seed for random number generator")
    parser.add_option("-c", "--config", dest="config", default=None,
                      help="the name of the config to run")
    parser.add_option("-r", "--report-all", action="store_true",
                      dest="report_all", default=False,
                      help="report all attempted SQL statements rather than mismatches")
    (options, args) = parser.parse_args()

    if options.seed == None:
        seed = random.randint(0, 2**63)
        print "Random seed: %d" % seed
    else:
        seed = int(options.seed)
        print "Using supplied seed: " + str(seed)
    random.seed(seed)

    if len(args) < 3:
        usage()
        sys.exit(3)

    config_filename = args[0]
    output_dir = args[1]
    basedir = os.path.dirname(config_filename)

    config_list = Config(config_filename)
    configs_to_run = []
    if options.config != None:
        if options.config not in config_list.get_configs():
            print >> sys.stderr, \
                "Selected config %s not present in config file" % options.config
            sys.exit(3)
        else:
            configs_to_run.append(options.config)
    else:
        configs_to_run = config_list.get_configs()

    success = True
    statistics = {}
    for config_name in configs_to_run:
        print >> sys.stderr, "SQLCOVERAGE: STARTING ON CONFIG: %s" % config_name
        report_dir = output_dir + '/' + config_name
        config = config_list.get_config(config_name)
        result = run_config(config, basedir, report_dir, seed,
                            options.report_all, args)
        statistics[config_name] = result
        if result[1] != 0:
            success = False

    # Write the summary
    generate_summary(output_dir, statistics)

    if not success:
        print >> sys.stderr, "SQL coverage has errors."
        exit(1)
