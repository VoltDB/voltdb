#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2012 VoltDB Inc.
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
# add the path to the volt python client, just based on knowing
# where we are now
sys.path.append('../../lib/python')

import random
import time
import subprocess
import cPickle
import os.path
import imp
import re
from voltdbclient import *
from optparse import OptionParser
from Query import VoltQueryClient
from SQLCoverageReport import generate_summary
from SQLGenerator import SQLGenerator
from xml.etree import ElementTree
from xml.etree.ElementTree import Element, SubElement
from subprocess import call # invoke unix/linux cmds
from XMLUtils import prettify # To create a human readable xml file

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

def run_once(name, command, statements_path, results_path, testConfigKit):

    print "Running \"run_once\":"
    print "  name: %s" % (name)
    print "  command: %s" % (command)
    print "  statements_path: %s" % (statements_path)
    print "  results_path: %s" % (results_path)
    sys.stdout.flush()

    host = defaultHost
    port = defaultPort
    if(name == "jni"):
        akey = "hostname"
        if akey in testConfigKit:
            host = testConfigKit["hostname"]
            port = testConfigKit["hostport"]

    global normalize
    if(host == defaultHost):
        server = subprocess.Popen(command + " backend=" + name, shell = True)

    client = None
    for i in xrange(10):
        try:
            client = VoltQueryClient(host, port)
            client.set_quiet(True)
            client.set_timeout(5.0) # 5 seconds
            break
        except socket.error:
            time.sleep(1)

    if client == None:
        print >> sys.stderr, "Unable to connect/create client"
        sys.stderr.flush()
        return -1

#    for key in testConfigKits:
#        print "999 Key = '%s', Val = '%s'" % (key, testConfigKits[key])
    if(host != defaultHost):
        # Flush database
        client.onecmd("updatecatalog " + testConfigKit["flushCatalog"] + " " + testConfigKit["deploymentFile"])
        client.onecmd("updatecatalog " + testConfigKit["testCatalog"]  + " " + testConfigKit["deploymentFile"])

    statements_file = open(statements_path, "rb")
    results_file = open(results_path, "wb")
    while True:
        try:
            statement = cPickle.load(statements_file)
        except EOFError:
            break

        try:
            client.onecmd("adhoc " + statement["SQL"])
        except:
            print >> sys.stderr, "Error occurred while executing '%s': %s" % \
                (statement["SQL"], sys.exc_info()[1])
            if(host == defaultHost):
                # Should kill the server now
                killer = subprocess.Popen("kill -9 %d" % (server.pid), shell = True)
                killer.communicate()
                if killer.returncode != 0:
                    print >> sys.stderr, \
                        "Failed to kill the server process %d" % (server.pid)
            break
        tables = None
        if client.response == None:
            print >> sys.stderr, "No error, but an unexpected null client response (server crash?) from executing statement '%s': %s" % \
                (statement["SQL"], sys.exc_info()[1])
            if(host == defaultHost):
                killer = subprocess.Popen("kill -9 %d" % (server.pid), shell = True)
                killer.communicate()
                if killer.returncode != 0:
                    print >> sys.stderr, \
                        "Failed to kill the server process %d" % (server.pid)
            break
        if client.response.tables != None:
            tables = [normalize(t, statement["SQL"]) for t in client.response.tables]
        cPickle.dump({"Status": client.response.status,
                      "Info": client.response.statusString,
                      "Result": tables,
                      "Exception": str(client.response.exception)},
                     results_file)
    results_file.close()
    statements_file.close()

    if(host == defaultHost):
        client.onecmd("shutdown")
        server.communicate()
    else:
        client.onecmd("disconnect")

    sys.stdout.flush()
    sys.stderr.flush()

    if(host == defaultHost):
        return server.returncode
    else:
        return 0

def run_config(suite_name, config, basedir, output_dir, random_seed, report_all, generate_only, args, testConfigKit):
    for key in config.iterkeys():
        print "in run_config key = '%s', config[key] = '%s'" % (key, config[key])
        if not os.path.isabs(config[key]):
            config[key] = os.path.abspath(os.path.join(basedir, config[key]))
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    statements_path = os.path.abspath(os.path.join(output_dir, "statements.data"))
    hsql_path = os.path.abspath(os.path.join(output_dir, "hsql.data"))
    jni_path = os.path.abspath(os.path.join(output_dir, "jni.data"))
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
    counter = 0

    statements_file = open(statements_path, "wb")
    for i in generator.generate():
        cPickle.dump({"id": counter, "SQL": i}, statements_file)
        counter += 1
    statements_file.close()

    if (generate_only):
        # Claim success without running servers.
        return [0,0]

    if run_once("jni", command, statements_path, jni_path, testConfigKit) != 0:
        print >> sys.stderr, "Test with the JNI backend had errors."
        print >> sys.stderr, "  jni_path: %s" % (jni_path)
        sys.stderr.flush()
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

    statements_file = open(statements_path, "wb")
    for i in generator.generate():
        cPickle.dump({"id": counter, "SQL": i}, statements_file)
        counter += 1
    statements_file.close()

    if run_once("hsqldb", command, statements_path, hsql_path, testConfigKit) != 0:
        print >> sys.stderr, "Test with the HSQLDB backend had errors."
        exit(1)

    global compare_results
    compare_results = imp.load_source("normalizer", config["normalizer"]).compare_results
    success = compare_results(suite_name, random_seed, statements_path, hsql_path,
                              jni_path, output_dir, report_all)
    return success

def get_voltcompiler(basedir):
    key = "voltdb"
    (head, tail) = basedir.split(key)
    voltcompiler = head + key + "/bin/voltcompiler"
    if(os.access(voltcompiler, os.X_OK)):
        return voltcompiler
    else:
        return None

def get_hostinfo(options):
    if options.hostname == None:
        hostname = defaultHost
    else:
        hostname = options.hostname
    if options.hostport == None:
        hostport = defaultPort
    else:
        if(options.hostport.isdigit()):
            hostport = int(options.hostport)
        else:
            print "Invalid value for port number: #%s#" % options.hostport
            usage()
            sys.exit(3)
    return (hostname, hostport)

def create_catalogFile(voltcompiler, flush4projectFile, catalogFilename):
    catalogFile = "/tmp/" + catalogFilename + ".jar"
    cmd = voltcompiler + " /tmp " + flush4projectFile + " " + catalogFile
    call(cmd, shell=True)
    if not os.path.exists(catalogFile):
        catalogFile = None
    return catalogFile

def create_projectFile(ddl, projFilename):
    proj = Element('project')
    db = SubElement(proj, 'database')
    schemas = SubElement(db, 'schemas')
    schema = SubElement(schemas, 'schema', {'path':ddl})
    thisProjectFile = "/tmp/" + projFilename + "4projectFile.xml"
    fo = open(thisProjectFile, "wb")
    fo.write(prettify(proj))
    fo.close()
    if not os.path.exists(thisProjectFile):
        thisProjectFile = None
    return thisProjectFile

def create_deploymentFile(options):
    kfactor = options.kfactor
    sitesperhost = options.sitescount
    hostcount = options.hostcount

    deployment = Element('deployment')
    cluster = SubElement(deployment, 'cluster',
            {'kfactor':kfactor,'sitesperhost':sitesperhost,'hostcount':hostcount})
    httpd = SubElement(deployment, 'httpd', {'port':"8080"})
    jsonapi = SubElement(httpd, 'jsonapi', {'enabled':"true"})
    deploymentFile = "/tmp/deploymentFile.xml"

    fo = open(deploymentFile, "wb")
    fo.write(prettify(deployment))
    fo.close()
    if not os.path.exists(deploymentFile):
        deploymentFile = None
    return deploymentFile

# To store all necessary test config info in a dictionary variable
def create_testConfigKits(options, basedir):
    testConfigKits = {}

    flushDDL = basedir + "/" + options.flush
    if not os.path.exists(flushDDL):
        print >> sys.stderr, "Cannot find the flush DDL file: '%s'!" % flushDDL
        sys.exit(3)
#    else:
#        print "flushDDL = #%s#" % flushDDL

    voltcompiler = get_voltcompiler(basedir)
    if voltcompiler == None:
        print >> sys.stderr, "Cannot find the executable voltcompiler!"
        sys.exit(3)
    else:
        testConfigKits["voltcompiler"] = voltcompiler

    flush4projectFile = create_projectFile(flushDDL, 'flush')
    flushCatalog = create_catalogFile(voltcompiler, flush4projectFile, 'flush')
    if flushCatalog == None:
        print >> sys.stderr, "Cannot find the flush catalog jar file!"
        sys.exit(3)
    else:
        testConfigKits["flushCatalog"] = flushCatalog

    deploymentFile = create_deploymentFile(options)
    if deploymentFile == None:
        print >> sys.stderr, "Cannot find the deployment xml file!"
        sys.exit(3)
    else:
        testConfigKits["deploymentFile"] = deploymentFile

    (hostname, hostport) = get_hostinfo(options)
    testConfigKits["hostname"] = hostname
    testConfigKits["hostport"] = hostport
    return testConfigKits

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
\t_maybe\t\tWill be replaced with NOT or simply removed
\t_distinct\t\tWill be replaced with DISTINCT or simply removed
\t_like\t\tWill be replaced with LIKE or NOT LIKE
\t_set\t\tWill be replaced with a set operator
\t_logic\t\tWill be replaced with a logic operator
\t_sortordert\tWill be replaced with ASC, DESC, or 'blank' (implicitly ascending)
"""

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-l", "--leader", dest="hostname",
                      help="the hostname of the leader")
    parser.add_option("-n", "--number", dest="hostcount",
                      help="the number of total hosts used in this test")
    parser.add_option("-k", "--kfactor", dest="kfactor",
                      help="the number of kfactor used in this test")
    parser.add_option("-t", "--sitescount", dest="sitescount",
                      help="the number of partitions used in this test")
    parser.add_option("-p", "--port", dest="hostport",
                      help="the port number of the leader")
    parser.add_option("-f", "--flush", dest="flush",
                      help="the temporary DDL file used to flush databases")
    parser.add_option("-s", "--seed", dest="seed",
                      help="seed for random number generator")
    parser.add_option("-c", "--config", dest="config", default=None,
                      help="the name of the config to run")
    parser.add_option("-r", "--report-all", action="store_true",
                      dest="report_all", default=False,
                      help="report all attempted SQL statements rather than mismatches")
    parser.add_option("-g", "--generate-only", action="store_true",
                      dest="generate_only", default=False,
                      help="only generate and report SQL statements, do not start any database servers")
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
#    print "config_list name = '" + config_list.__class__.__name__ + "'"

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

    testConfigKits = {}
    defaultHost = "localhost"
    defaultPort = 21212
    if(options.hostname != None and options.hostname != defaultHost):
        # To set a dictionary with following 5 keys:
        # testConfigKits["voltcompiler"]
        # testConfigKits["flushCatalog"]
        # testConfigKits["deploymentFile"]
        # testConfigKits["hostname"]
        # testConfigKits["hostport"]
        testConfigKits = create_testConfigKits(options, basedir)

    success = True
    statistics = {}
    for config_name in configs_to_run:
        print >> sys.stderr, "SQLCOVERAGE: STARTING ON CONFIG: %s" % config_name
        report_dir = output_dir + '/' + config_name
        config = config_list.get_config(config_name)
        if(options.hostname != None and options.hostname != defaultHost):
            testDDL = basedir + "/" + config['ddl']
            testProjectFile = create_projectFile(testDDL, 'test')
            testCatalog = create_catalogFile(testConfigKits['voltcompiler'], testProjectFile, 'test')
            # To add one more key
            testConfigKits["testCatalog"] = testCatalog
        result = run_config(config_name, config, basedir, report_dir, seed, options.report_all, \
                            options.generate_only, args, testConfigKits)
        statistics[config_name] = result["keyStats"]
        statistics["seed"] = seed
        if result["mis"] != 0:
            success = False

    # Write the summary
    generate_summary(output_dir, statistics)

    if not success:
        print >> sys.stderr, "SQL coverage has errors."
        exit(1)
