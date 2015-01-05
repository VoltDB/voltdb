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

def print_seconds(seconds=0, message_end="", message_begin="Total   time: ",
                  include_current_time=False):
    """ Prints, and returns, a message containing the specified number of
    seconds, preceded by the 'message_begin' and followed by "seconds, " and
    the 'message_end'; if the number of seconds is greater than or equal to 60,
    it also prints the minutes and seconds in parentheses, e.g., 61.9 seconds
    would be printed as "61.9 seconds (01:02), ". Optionally, if
    'include_current_time' is True, the current time (in seconds since January
    1, 1970) is also printed, in brackets, e.g.,
    "61.9 seconds (1:02) [at 1408645826.68], ", which is useful for debugging
    purposes.
    """

    time_msg = str(seconds) + " seconds"
    if (seconds >= 60):
        time_msg += " (" + re.sub("^0:", "", str(datetime.timedelta(0, round(seconds))), 1) + ")"
    if (include_current_time):
        time_msg += " [at " + str(time.time()) + "]"

    message = message_begin + time_msg + ", " + message_end
    print message
    return message

def print_elapsed_seconds(message_end="", prev_time=-1,
                          message_begin="Elapsed time: "):
    """Computes, returns and prints the difference (in seconds) between the
    current system time and a previous time, which is either the specified
    'prev_time' or, if that is negative (or unspecified), the previous time
    at which this function was called. The printed message is preceded by
    'message_begin' and followed by "seconds, " and 'message_end'; if the
    elapsed time is greater than or equal to 60 seconds, it also includes the
    minutes and seconds in parentheses, e.g., 61.9 seconds would be printed
    as "61.9 seconds (01:02), ".
    """

    now = time.time()
    global save_prev_time
    if (prev_time < 0):
        prev_time = save_prev_time
    save_prev_time = now

    diff_time = now - prev_time
    print_seconds(diff_time, message_end, message_begin)
    return diff_time

def run_once(name, command, statements_path, results_path, submit_verbosely, testConfigKit):

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
    for i in xrange(30):
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
        client.onecmd("updatecatalog " + testConfigKit["testCatalog"]  + " " + testConfigKit["deploymentFile"])

    statements_file = open(statements_path, "rb")
    results_file = open(results_path, "wb")
    while True:
        try:
            statement = cPickle.load(statements_file)
        except EOFError:
            break

        try:
            if submit_verbosely:
                print "Submitting to backend " + name + " adhoc " + statement["SQL"]
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
        table = None
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
        if client.response.tables:
            ### print "DEBUG: got table(s) from ", statement["SQL"] ,"."
            table = normalize(client.response.tables[0], statement["SQL"])
            if len(client.response.tables) > 1:
                print "WARNING: ignoring extra table(s) from result of query ?", statement["SQL"] ,"?"
        # else:
            # print "WARNING: returned no table(s) from ?", statement["SQL"] ,"?"
        cPickle.dump({"Status": client.response.status,
                      "Info": client.response.statusString,
                      "Result": table,
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

def run_config(suite_name, config, basedir, output_dir, random_seed, report_all, generate_only,
    subversion_generation, submit_verbosely, args, testConfigKit):

    # Store the current, initial system time (in seconds since January 1, 1970)
    time0 = time.time()

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
        # print "DEBUG: using normalizer ", config["normalizer"], " for ", template
    else:
        normalize = lambda x, y: x
        # print "DEBUG: using no normalizer for ", template

    command = " ".join(args[2:])
    command += " schema=" + os.path.basename(config['ddl'])

    random_state = random.getstate()
    if "template-jni" in config:
        template = config["template-jni"]
    generator = SQLGenerator(config["schema"], template, subversion_generation)
    counter = 0

    statements_file = open(statements_path, "wb")
    for i in generator.generate():
        cPickle.dump({"id": counter, "SQL": i}, statements_file)
        counter += 1
    statements_file.close()

    if generate_only or submit_verbosely:
        print "Generated %d statements." % counter
    if generate_only:
        # Claim success without running servers.
        return {"keyStats" : None, "mis" : 0}

    # Print the elapsed time, with a message
    global gensql_time
    gensql_time += print_elapsed_seconds("for generating statements (" + suite_name + ")", time0)

    if run_once("jni", command, statements_path, jni_path, submit_verbosely, testConfigKit) != 0:
        print >> sys.stderr, "Test with the JNI backend had errors."
        print >> sys.stderr, "  jni_path: %s" % (jni_path)
        sys.stderr.flush()
        exit(1)

    # Print the elapsed time, with a message
    global voltdb_time
    voltdb_time += print_elapsed_seconds("for running VoltDB (JNI) statements (" + suite_name + ")")

    random.seed(random_seed)
    random.setstate(random_state)

    if run_once("hsqldb", command, statements_path, hsql_path, submit_verbosely, testConfigKit) != 0:
        print >> sys.stderr, "Test with the HSQLDB backend had errors."
        exit(1)

    # Print the elapsed time, with a message
    global hsqldb_time
    hsqldb_time += print_elapsed_seconds("for running HSqlDB statements (" + suite_name + ")")

    global compare_results
    compare_results = imp.load_source("normalizer", config["normalizer"]).compare_results
    success = compare_results(suite_name, random_seed, statements_path, hsql_path,
                              jni_path, output_dir, report_all)

    # Print the elapsed time and total time, with a message
    global compar_time
    compar_time += print_elapsed_seconds("for comparing DB results (" + suite_name + ")")
    print_elapsed_seconds("for run_config of '" + suite_name + "'", time0, "Sub-tot time: ")

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

def create_catalogFile(voltcompiler, projectFile, catalogFilename):
    catalogFile = "/tmp/" + catalogFilename + ".jar"
    cmd = voltcompiler + " /tmp " + projectFile + " " + catalogFile
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

    voltcompiler = get_voltcompiler(basedir)
    if voltcompiler == None:
        print >> sys.stderr, "Cannot find the executable voltcompiler!"
        sys.exit(3)
    else:
        testConfigKits["voltcompiler"] = voltcompiler

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
    #print the whole command line, maybe useful for debugging
    #print " ".join(sys.argv)

    # Print the current, initial system time
    time0 = time.time()
    print "Initial time: " + str(time0) + ", at start (in seconds since January 1, 1970)"
    save_prev_time = time0
    gensql_time = 0.0
    voltdb_time = 0.0
    hsqldb_time = 0.0
    compar_time = 0.0

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
    parser.add_option("-s", "--seed", dest="seed",
                      help="seed for random number generator")
    parser.add_option("-c", "--config", dest="config", default=None,
                      help="the name of the config to run")
    parser.add_option("-S", "--subversion_generation", dest="subversion_generation",
                      action="store_true", default=None,
                      help="enable generation of additional subquery forms for select statements")
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
                "Selected config %s not present in config file %s" % (options.config, config_filename)
            sys.exit(3)
        else:
            configs_to_run.append(options.config)
    else:
        configs_to_run = config_list.get_configs()

    testConfigKits = {}
    defaultHost = "localhost"
    defaultPort = 21212
    if(options.hostname != None and options.hostname != defaultHost):
        # To set a dictionary with following 4 keys:
        # testConfigKits["voltcompiler"]
        # testConfigKits["deploymentFile"]
        # testConfigKits["hostname"]
        # testConfigKits["hostport"]
        testConfigKits = create_testConfigKits(options, basedir)

    success = True
    statistics = {}
    for config_name in configs_to_run:
        print >> sys.stderr, "\nSQLCOVERAGE: STARTING ON CONFIG: %s\n" % config_name
        report_dir = output_dir + '/' + config_name
        config = config_list.get_config(config_name)
        if(options.hostname != None and options.hostname != defaultHost):
            testDDL = basedir + "/" + config['ddl']
            testProjectFile = create_projectFile(testDDL, 'test')
            testCatalog = create_catalogFile(testConfigKits['voltcompiler'], testProjectFile, 'test')
            # To add one more key
            testConfigKits["testCatalog"] = testCatalog
        result = run_config(config_name, config, basedir, report_dir, seed, options.report_all,
                            options.generate_only, options.subversion_generation,
                            options.report_all, args, testConfigKits)
        statistics[config_name] = result["keyStats"]
        statistics["seed"] = seed
        if result["mis"] != 0:
            success = False

    # Write the summary
    time1 = time.time()
    generate_summary(output_dir, statistics)

    # Print the elapsed time, and the current system time
    print_seconds(gensql_time, "for generating ALL statements")
    print_seconds(voltdb_time, "for running ALL VoltDB (JNI) statements")
    print_seconds(hsqldb_time, "for running ALL HSqlDB statements")
    print_seconds(compar_time, "for comparing ALL DB results")
    print_elapsed_seconds("for generating the output report", time1, "Total   time: ")
    print_elapsed_seconds("for the entire run", time0, "Total   time: ")

    if not success:
        print >> sys.stderr, "SQL coverage has errors."
        exit(1)

