#!/usr/bin/env python2

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

import sys
# add the path to the volt python client, just based on knowing
# where we are now
sys.path.append('../../lib/python')
sys.path.append('./normalizer/')

import random
import time
import subprocess
import cPickle
import os.path
import imp
import re
import traceback
from voltdbclientpy2 import *
from optparse import OptionParser
from QueryPy2 import VoltQueryClient
from SQLCoverageReport import generate_summary
from SQLCoverageReport import Reproduce
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

def minutes_colon_seconds(seconds):
    return re.sub("^0:", "", str(datetime.timedelta(0, round(seconds))), 1)

def print_seconds(seconds=0, message_end="", message_begin="Total   time: ",
                  include_current_time=False):
    """ Prints, and returns, a message containing the specified number of
    seconds, first in a minutes:seconds format (e.g. "01:02", or "1:43:48"),
    then just the exact number of seconds in parentheses, e.g.,
    "1:02 (61.9 seconds)", preceded by the 'message_begin' and followed by
    'message_end'. Optionally, if 'include_current_time' is True, the current
    time (in seconds since January 1, 1970) is also printed, in brackets, e.g.,
    "1:02 (61.9 seconds) [at 1408645826.68], ", which is useful for debugging
    purposes.
    """

    time_msg = minutes_colon_seconds(seconds) + " ({0:.6f} seconds)".format(seconds)
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
    'message_begin' and followed by 'message_end'; the elapsed time is printed
    in a minutes:seconds format, with the exact number of seconds in parentheses,
    e.g., 61.9 seconds would be printed as "01:02 (61.9 seconds), ".
    """

    now = time.time()
    global save_prev_time
    if (prev_time < 0):
        prev_time = save_prev_time
    save_prev_time = now

    diff_time = now - prev_time
    print_seconds(diff_time, message_end, message_begin)
    return diff_time

def run_once(name, command, statements_path, results_path,
             submit_verbosely, testConfigKit, precision):

    print "Running \"run_once\":"
    print "  name: %s" % (name)
    print "  command: %s" % (command)
    print "  statements_path: %s" % (statements_path)
    print "  results_path: %s" % (results_path)
    if precision:
        print "  precision: %s" % (precision)
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
        server = subprocess.Popen(command + " backend=" + name, shell=True)

    client = None
    clientException = None
    for i in xrange(30):
        try:
            client = VoltQueryClient(host, port)
            client.set_quiet(True)
            client.set_timeout(5.0) # 5 seconds
            break
        except socket.error as e:
            clientException = e
            time.sleep(1)

    if client == None:
        print >> sys.stderr, "Unable to connect/create client: there may be a problem with the VoltDB server or its ports:"
        print >> sys.stderr, "name:", str(name)
        print >> sys.stderr, "host:", str(host)
        print >> sys.stderr, "port:", str(port)
        print >> sys.stderr, "client (socket.error) exception:", str(clientException)
        sys.stderr.flush()
        return -1

#    for key in testConfigKits:
#        print "999 Key = '%s', Val = '%s'" % (key, testConfigKits[key])
    if(host != defaultHost):
        # Flush database
        client.onecmd("updatecatalog " + testConfigKit["testCatalog"] + " " + testConfigKit["deploymentFile"])

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
                killer = subprocess.Popen("kill -9 %d" % (server.pid), shell=True)
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
                killer = subprocess.Popen("kill -9 %d" % (server.pid), shell=True)
                killer.communicate()
                if killer.returncode != 0:
                    print >> sys.stderr, \
                        "Failed to kill the server process %d" % (server.pid)
            break
        if client.response.tables:
            ### print "DEBUG: got table(s) from ", statement["SQL"] ,"."
            if precision:
                table = normalize(client.response.tables[0], statement["SQL"], precision)
            else:
                table = normalize(client.response.tables[0], statement["SQL"])
            if len(client.response.tables) > 1:
                print "WARNING: ignoring extra table(s) from result of query ?", statement["SQL"] , "?"
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

def get_max_mismatches(comparison_database, suite_name):
    """Returns the maximum number of acceptable mismatches, i.e., the number of
    'known' failures for VoltDB to match the results of the comparison database
    (HSQL or PostgreSQL), which is normally zero; however, there are sometimes
    a few exceptions, e.g., for queries that are not supported by PostgreSQL.
    """
    max_mismatches = 0

    # Kludge to not fail for known issues, when running against PostgreSQL
    # (or the PostGIS extension of PostgreSQL)
    if comparison_database.startswith('Post'):
        # Known failures in the joined-matview-* test suites ...
        # Failures in joined-matview-default-full due to ENG-11086 ("SUM in
        # materialized view reported as zero for column with only nulls")
        if config_name == 'joined-matview-default-full':
            max_mismatches = 4390
        # Failures in joined-matview-int due to ENG-11086
        elif config_name == 'joined-matview-int':
            max_mismatches = 46440
        # Failures in geo-functions due to ENG-19236
        elif config_name == 'geo-functions':
            max_mismatches = 3

    return max_mismatches


def get_config_path(basedir, config_key, config_value):
    """Returns the correct path to a specific (ddl, normalizer, schema, or
    template) file, given its config 'key' and 'value'. The 'key' will be one
    of 'ddl', 'normalizer', 'schema', or 'template', the last of which is the
    more complicated case, requiring us to check the various subdirectories.
    """
    for subdir in os.walk(os.path.join(basedir, config_key)):
        filename = os.path.join(subdir[0], config_value)
        if os.path.isfile(filename):
            return os.path.abspath(filename)
    # If you cannot find the file, leave the value unchanged
    return config_value


def run_config(suite_name, config, basedir, output_dir, random_seed,
               report_invalid, report_all, max_detail_files, reproducer,
               generate_only, subversion_generation, submit_verbosely,
               ascii_only, args, testConfigKit):

    # Store the current, initial system time (in seconds since January 1, 1970)
    time0 = time.time()

    precision = 0
    within_minutes = 0
    for key in config.iterkeys():
        if key == "precision":
            precision = int(config["precision"])
        elif key == "within-minutes":
            within_minutes = int(config["within-minutes"])
        elif not os.path.isabs(config[key]):
            config[key] = get_config_path(basedir, key, config[key])
        print "in run_config key = '%s', config[key] = '%s'" % (key, str(config[key]))
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    global comparison_database
    comparison_database_lower = comparison_database.lower()
    statements_path = os.path.abspath(os.path.join(output_dir, "statements.data"))
    cmpdb_path = os.path.abspath(os.path.join(output_dir, comparison_database_lower + ".data"))
    jni_path = os.path.abspath(os.path.join(output_dir, "jni.data"))
    modified_sql_path = None
    debug_transform_sql_arg = ''
    global debug_transform_sql
    if debug_transform_sql:
        if comparison_database == 'PostgreSQL' or comparison_database == 'PostGIS':
            modified_sql_path = os.path.abspath(os.path.join(output_dir, 'postgresql_transform.out'))
            debug_transform_sql_arg = ' -Dsqlcoverage.transform.sql.file='+modified_sql_path
    template = config["template"]

    global normalize
    if "normalizer" in config:
        normalize = imp.load_source("normalizer", config["normalizer"]).normalize
        # print "DEBUG: using normalizer ", config["normalizer"], " for ", template
        self_check_safecmp = imp.load_source("normalizer", config["normalizer"]).safecmp
        theNow = datetime.datetime.now()
        if self_check_safecmp([theNow], [theNow]) != 0:
             print >> sys.stderr, "safe_cmp fails [datetime] selfcheck"
             exit(2)
        if self_check_safecmp([None], [None]) != 0:
             print >> sys.stderr, "safe_cmp fails [None] selfcheck"
             exit(2)
        if self_check_safecmp([theNow], [None]) <= 0:
             print >> sys.stderr, "safe_cmp fails [datetime], [None] selfcheck"
             exit(2)
        theLater = datetime.datetime.now()
        if self_check_safecmp([None, theNow], [None, theLater]) >= 0:
             print >> sys.stderr, "safe_cmp fails [None, datetime] selfcheck"
             exit(2)

    else:
        normalize = lambda x, y: x
        # print "DEBUG: using no normalizer for ", template

    command = " ".join(args[2:])
    command += " schema=" + os.path.basename(config['ddl'])
    if debug_transform_sql:
        command = command.replace(" -server ", debug_transform_sql_arg+" -server ")

    random_state = random.getstate()
    if "template-jni" in config:
        template = config["template-jni"]
    generator = SQLGenerator(config["schema"], template, subversion_generation, ascii_only)
    counter = 0

    statements_file = open(statements_path, "wb")
    for i in generator.generate(submit_verbosely):
        cPickle.dump({"id": counter, "SQL": i}, statements_file)
        counter += 1
    statements_file.close()

    min_statements_per_pattern = generator.min_statements_per_pattern()
    max_statements_per_pattern = generator.max_statements_per_pattern()
    num_inserts    = generator.num_insert_statements()
    num_patterns   = generator.num_patterns()
    num_unresolved = generator.num_unresolved_statements()

    if generate_only or submit_verbosely:
        print "Generated %d statements." % counter
    if generate_only:
        # Claim success without running servers.
        return {"keyStats" : None, "mis" : 0}

    # Print the elapsed time, with a message
    global total_gensql_time
    gensql_time = print_elapsed_seconds("for generating statements (" + suite_name + ")", time0)
    total_gensql_time += gensql_time

    volt_crashes = 0
    failed = False
    try:
        if run_once("jni", command, statements_path, jni_path,
                    submit_verbosely, testConfigKit, precision) != 0:
            print >> sys.stderr, "Test with the JNI (VoltDB) backend had errors (crash?)."
            failed = True
    except:
        print >> sys.stderr, "JNI (VoltDB) backend crashed!!!"
        traceback.print_exc()
        failed = True
    if (failed):
        print >> sys.stderr, "  jni_path: %s" % (jni_path)
        sys.stderr.flush()
        volt_crashes += 1

    # Print the elapsed time, with a message
    global total_voltdb_time
    voltdb_time = print_elapsed_seconds("for running VoltDB (JNI) statements (" + suite_name + ")")
    total_voltdb_time += voltdb_time

    random.seed(random_seed)
    random.setstate(random_state)

    cmp_crashes  = 0
    diff_crashes = 0
    failed = False
    try:
        if run_once(comparison_database_lower, command, statements_path, cmpdb_path,
                    submit_verbosely, testConfigKit, precision) != 0:
            print >> sys.stderr, "Test with the " + comparison_database + " backend had errors (crash? Connection refused?)."
            failed = True
    except:
        print >> sys.stderr, comparison_database + " backend crashed!!"
        traceback.print_exc()
        failed = True
    if (failed):
        print >> sys.stderr, "  cmpdb_path: %s" % (cmpdb_path)
        sys.stderr.flush()
        cmp_crashes += 1

    # Print the elapsed time, with a message
    global total_cmpdb_time
    cmpdb_time = print_elapsed_seconds("for running " + comparison_database + " statements (" + suite_name + ")")
    total_cmpdb_time += cmpdb_time

    someStats = (get_numerical_html_table_element(min_statements_per_pattern, strong_warn_below=1) +
                 get_numerical_html_table_element(max_statements_per_pattern, strong_warn_below=1, warn_above=100000) +
                 get_numerical_html_table_element(num_inserts,  warn_below=4, strong_warn_below=1, warn_above=1000) +
                 get_numerical_html_table_element(num_patterns, warn_below=4, strong_warn_below=1, warn_above=10000) +
                 get_numerical_html_table_element(num_unresolved, error_above=0) +
                 get_time_html_table_element(gensql_time) +
                 get_time_html_table_element(voltdb_time) +
                 get_time_html_table_element(cmpdb_time) )
    extraStats = (get_numerical_html_table_element(volt_crashes, error_above=0) +
                  get_numerical_html_table_element(cmp_crashes,  error_above=0) +
                  get_numerical_html_table_element(diff_crashes, error_above=0) + someStats )
    max_expected_mismatches = get_max_mismatches(comparison_database, suite_name)

    global compare_results
    try:
        compare_results = imp.load_source("normalizer", config["normalizer"]).compare_results
        success = compare_results(suite_name, random_seed, statements_path, cmpdb_path,
                                  jni_path, output_dir, report_invalid, report_all, extraStats,
                                  comparison_database, modified_sql_path, max_expected_mismatches,
                                  max_detail_files, within_minutes, reproducer,
                                  config.get("ddl"))
    except:
        print >> sys.stderr, "Compare (VoltDB & " + comparison_database + ") results crashed!"
        traceback.print_exc()
        print >> sys.stderr, "  jni_path: %s" % (jni_path)
        print >> sys.stderr, "  cmpdb_path: %s" % (cmpdb_path)
        sys.stderr.flush()
        diff_crashes += 1
        gray_zero_html_table_element = get_numerical_html_table_element(0, use_gray=True)
        errorStats = (gray_zero_html_table_element + gray_zero_html_table_element +
                      gray_zero_html_table_element + gray_zero_html_table_element +
                      gray_zero_html_table_element + gray_zero_html_table_element +
                      gray_zero_html_table_element + gray_zero_html_table_element +
                      gray_zero_html_table_element + gray_zero_html_table_element +
                      get_numerical_html_table_element(volt_crashes, error_above=0) +
                      get_numerical_html_table_element(cmp_crashes,  error_above=0) +
                      get_numerical_html_table_element(diff_crashes, error_above=0) + someStats + '</tr>' )
        success = {"keyStats": errorStats, "mis": -1}

    # Print & save the elapsed time and total time, with a message
    global total_compar_time
    compar_time = print_elapsed_seconds("for comparing DB results (" + suite_name + ")")
    total_compar_time += compar_time
    suite_secs = print_elapsed_seconds("for run_config of '" + suite_name + "'", time0, "Sub-tot time: ")
    sys.stdout.flush()

    # Accumulate the total number of Valid, Invalid, Mismatched & Total statements
    global total_statements
    def next_keyStats_column_value():
        prefix = "<td"
        suffix = "</td>"
        global keyStats_start_index
        start_index  = 0
        end_index    = 0
        next_col_val = "0"
        try:
            start_index  = success["keyStats"].index(prefix, keyStats_start_index) + len(prefix)
            start_index  = success["keyStats"].index('>', start_index) + 1
            end_index    = success["keyStats"].index(suffix, start_index)
            next_col_val = success["keyStats"][start_index: end_index]
            keyStats_start_index = end_index + len(suffix)
        except:
            print "Caught exception:\n", sys.exc_info()[0]
            print "success[keyStats]:\n", success["keyStats"]
            print "keyStats_start_index:", keyStats_start_index
            print "start_index :", start_index
            print "end_index   :", end_index
            print "next_col_val:", next_col_val
        return next_col_val
    global valid_statements
    global invalid_statements
    global mismatched_statements
    global keyStats_start_index
    global total_volt_fatal_excep
    global total_volt_nonfatal_excep
    global total_cmp_excep
    global total_volt_crashes
    global total_cmp_crashes
    global total_diff_crashes
    global total_num_inserts
    global total_num_patterns
    global total_num_unresolved
    global min_all_statements_per_pattern
    global max_all_statements_per_pattern
    keyStats_start_index = 0
    valid_statements      += int(next_keyStats_column_value())
    next_keyStats_column_value()  # ignore Valid %
    invalid_statements    += int(next_keyStats_column_value())
    next_keyStats_column_value()  # ignore Invalid %
    total_statements      += int(next_keyStats_column_value())
    mismatched_statements += int(next_keyStats_column_value())
    next_keyStats_column_value()  # ignore Mismatched %
    total_volt_fatal_excep    += int(next_keyStats_column_value())
    total_volt_nonfatal_excep += int(next_keyStats_column_value())
    total_cmp_excep           += int(next_keyStats_column_value())
    total_volt_crashes    += volt_crashes
    total_cmp_crashes     += cmp_crashes
    total_diff_crashes    += diff_crashes
    total_num_inserts     += num_inserts
    total_num_patterns    += num_patterns
    total_num_unresolved  += num_unresolved
    min_all_statements_per_pattern = min(min_all_statements_per_pattern, min_statements_per_pattern)
    max_all_statements_per_pattern = max(max_all_statements_per_pattern, max_statements_per_pattern)

    finalStats = (get_time_html_table_element(compar_time) +
                  get_time_html_table_element(suite_secs) )

    success["keyStats"] = success["keyStats"].replace('</tr>', finalStats + '</tr>')

    return success

def get_html_table_element_color(value, error_below, strong_warn_below, warn_below,
                                 error_above, strong_warn_above, warn_above, use_gray):
    color = ''
    if (use_gray):
        color = ' bgcolor=#D3D3D3'  # gray
    elif (value < error_below or value > error_above):
        color = ' bgcolor=#FF0000'  # red
    elif (value < strong_warn_below or value > strong_warn_above):
        color = ' bgcolor=#FFA500'  # orange
    elif (value < warn_below or value > warn_above):
        color = ' bgcolor=#FFFF00'  # yellow
    return color

def get_numerical_html_table_element(value, error_below=-1, strong_warn_below=0, warn_below=0,
                                     error_above=1000000000, strong_warn_above=1000000, warn_above=100000,  # 1 billion, 1 million, 100,000
                                     use_gray=False):
    return ('<td align=right%s>%d</td>' %
            (get_html_table_element_color(value, error_below, strong_warn_below, warn_below,
                                          error_above, strong_warn_above, warn_above, use_gray),
             value) )

def get_time_html_table_element(seconds, error_below=0, strong_warn_below=0, warn_below=0,
                                error_above=28800, strong_warn_above=3600, warn_above=600,  # 8 hours, 1 hour, 10 minutes
                                use_gray=False):
    return ('<td align=right%s>%s</td>' %
            (get_html_table_element_color(seconds, error_below, strong_warn_below, warn_below,
                                          error_above, strong_warn_above, warn_above, use_gray),
             minutes_colon_seconds(seconds)) )

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
            {'kfactor':kfactor, 'sitesperhost':sitesperhost, 'hostcount':hostcount})
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
    total_gensql_time = 0.0
    total_voltdb_time = 0.0
    total_cmpdb_time = 0.0
    total_compar_time = 0.0
    keyStats_start_index = 0
    valid_statements = 0
    invalid_statements = 0
    mismatched_statements = 0
    total_statements = 0
    total_volt_fatal_excep = 0
    total_volt_nonfatal_excep = 0
    total_cmp_excep = 0
    total_volt_crashes = 0
    total_cmp_crashes  = 0
    total_diff_crashes = 0
    total_num_inserts  = 0
    total_num_patterns = 0
    total_num_unresolved = 0
    max_all_statements_per_pattern = 0
    min_all_statements_per_pattern = sys.maxint

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
    parser.add_option("-a", "--ascii-only", action="store_true",
                      dest="ascii_only", default=False,
                      help="include only ASCII values in randomly generated string constants")
    parser.add_option("-i", "--report-invalid", action="store_true",
                      dest="report_invalid", default=False,
                      help="report invalid SQL statements, not just mismatches")
    parser.add_option("-r", "--report-all", action="store_true",
                      dest="report_all", default=False,
                      help="report all attempted SQL statements, not just mismatches")
    parser.add_option("-g", "--generate-only", action="store_true",
                      dest="generate_only", default=False,
                      help="only generate and report SQL statements, do not start any database servers")
    parser.add_option("-H", "--hsql", action="store_true",
                      dest="hsql", default=False,
                      help="compare VoltDB results to HSqlDB, rather than PostgreSQL")
    parser.add_option("-P", "--postgresql", action="store_true",
                      dest="postgresql", default=False,
                      help="compare VoltDB results to PostgreSQL, rather than HSqlDB")
    parser.add_option("-G", "--postgis", action="store_true",
                      dest="postgis", default=False,
                      help="compare VoltDB results to PostgreSQL, with the PostGIS extension")
    parser.add_option("-d", "--maxdetailfiles", dest="max_detail_files", default="6",
                      help="maximum number of detail files, per test suite, per failure category "
                         + "(e.g. mismatches vs crashes vs various types of exceptions)")
    parser.add_option("-R", "--reproduce", dest="reproduce", default="DML",
                      help="Provide steps to reproduce failures, up to the specified level "
                         + "(NONE, DDL, DML, CTE, ALL)")
    (options, args) = parser.parse_args()

    if options.seed == None:
        seed = random.randint(0, 2 ** 63)
        print "Random seed: %d" % seed
    else:
        seed = int(options.seed)
        print "Using supplied seed: %d" % seed
    random.seed(seed)

    if len(args) < 3:
        usage()
        sys.exit(3)

    config_filename = args[0]
    output_dir = args[1]
    # Parent directory of the 'config' directory (i.e., this would
    # normally be the 'sqlcoverage' directory)
    basedir = os.path.dirname(os.path.dirname(config_filename))

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

    comparison_database = "PostgreSQL"  # default value (new 11/2018)
    debug_transform_sql = True
    if options.hsql:
        comparison_database = 'HSqlDB'
        debug_transform_sql = False
    if options.postgresql:
        comparison_database = 'PostgreSQL'
        debug_transform_sql = True
    if options.postgis:
        comparison_database = 'PostGIS'
        debug_transform_sql = True

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

    reproducer = Reproduce.NONE  # if unspecified
    if options.reproduce.upper() == "DDL":
        reproducer = Reproduce.DDL
    elif options.reproduce.upper() == "DML":
        reproducer = Reproduce.DML
    elif options.reproduce.upper() == "CTE":
        reproducer = Reproduce.CTE
    elif options.reproduce.upper() == "ALL":
        reproducer = Reproduce.ALL
    print 'Using reproducer: %s (%d)' % (options.reproduce, reproducer)

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
        result = run_config(config_name, config, basedir, report_dir, seed,
                            options.report_invalid, options.report_all,
                            options.max_detail_files, reproducer,
                            options.generate_only, options.subversion_generation,
                            options.report_all, options.ascii_only, args, testConfigKits)
        statistics[config_name] = result["keyStats"]
        statistics["seed"] = seed

        # The maximum number of acceptable mismatches is normally zero, except
        # for certain rare cases involving known errors in PostgreSQL
        if result["mis"] > get_max_mismatches(comparison_database, config_name):
            success = False

    # Write the summary
    time1 = time.time()
    if total_statements > 0:
        valid_percent = '{0:.2f}'.format(100.00 * valid_statements / total_statements)
        invalid_percent = '{0:.2f}'.format(100.00 * invalid_statements / total_statements)
        mismatched_percent = '{0:.2f}'.format(100.00 * mismatched_statements / total_statements)
    else:
        valid_percent = '0.00'
        invalid_percent = '0.00'
        mismatched_percent = '0.00'
    statistics["totals"] = "\n<td align=right>" + str(valid_statements) + "</td>" + \
                           "\n<td align=right>" + valid_percent + "%</td>" + \
                           "\n<td align=right>" + str(invalid_statements) + "</td>" + \
                           "\n<td align=right>" + invalid_percent + "%</td>" + \
                           "\n<td align=right>" + str(total_statements) + "</td>" + \
                           "\n<td align=right>" + str(mismatched_statements) + "</td>" + \
                           "\n<td align=right>" + mismatched_percent + "%</td>" + \
                           "\n<td align=right>" + str(total_volt_fatal_excep) + "</td>" + \
                           "\n<td align=right>" + str(total_volt_nonfatal_excep) + "</td>" + \
                           "\n<td align=right>" + str(total_cmp_excep) + "</td>" + \
                           "\n<td align=right>" + str(total_volt_crashes) + "</td>" + \
                           "\n<td align=right>" + str(total_cmp_crashes) + "</td>" + \
                           "\n<td align=right>" + str(total_diff_crashes) + "</td>" + \
                           "\n<td align=right>" + str(min_all_statements_per_pattern) + "</td>" + \
                           "\n<td align=right>" + str(max_all_statements_per_pattern) + "</td>" + \
                           "\n<td align=right>" + str(total_num_inserts) + "</td>" + \
                           "\n<td align=right>" + str(total_num_patterns) + "</td>" + \
                           "\n<td align=right>" + str(total_num_unresolved) + "</td>" + \
                           "\n<td align=right>" + minutes_colon_seconds(total_gensql_time) + "</td>" + \
                           "\n<td align=right>" + minutes_colon_seconds(total_voltdb_time) + "</td>" + \
                           "\n<td align=right>" + minutes_colon_seconds(total_cmpdb_time) + "</td>" + \
                           "\n<td align=right>" + minutes_colon_seconds(total_compar_time) + "</td>" + \
                           "\n<td align=right>" + minutes_colon_seconds(time1-time0) + "</td></tr>\n"
    generate_summary(output_dir, statistics, comparison_database)

    # output statistics for SQLCoverage auto filer
    with open(os.path.join(output_dir, "stats.txt"), "w") as hack:
        stats = \
            {
            'valid_statements' : valid_statements,
            'valid_percent' : valid_percent,
            'invalid_statements' : invalid_statements,
            'invalid_percent' : invalid_percent,
            'total_statements' : total_statements,
            'mismatched_statements' : mismatched_statements,
            'mismatched_percent' : mismatched_percent,
            'total_volt_fatal_excep' : total_volt_fatal_excep,
            'total_volt_nonfatal_excep' : total_volt_nonfatal_excep,
            'total_cmp_excep' : total_cmp_excep,
            'total_volt_crashes' : total_volt_crashes,
            'total_cmp_crashes' : total_cmp_crashes,
            'total_diff_crashes' : total_diff_crashes,
            'comparison_database' : comparison_database
            }
        hack.write(str(stats))
        hack.close()

    # Print the total time, for each type of activity
    sys.stdout.flush()
    sys.stderr.flush()
    print_seconds(total_gensql_time, "for generating ALL SQL statements")
    print_seconds(total_voltdb_time, "for running ALL VoltDB (JNI) statements")
    print_seconds(total_cmpdb_time,  "for running ALL " + comparison_database + " statements")
    print_seconds(total_compar_time, "for comparing ALL DB results")
    print_elapsed_seconds("for generating the output report", time1, "Total   time: ")
    print_elapsed_seconds("for the entire run", time0, "Total   time: ")
    if total_num_unresolved > 0:
        success = False
        print "Total number of invalid statements with unresolved symbols: %d" % total_num_unresolved
    if total_cmp_excep > 0:
        print "Total number of " + comparison_database + " (all) Exceptions: %d" % total_cmp_excep
    if total_volt_nonfatal_excep > 0:
        print "Total number of VoltDB Minor Exceptions: %d" % total_volt_nonfatal_excep
    if total_volt_fatal_excep > 0:
        success = False
        print "Total number of VoltDB Fatal Exceptions: %d" % total_volt_fatal_excep
    if mismatched_statements > 0:
        print "Total number of mismatched statements (i.e., test failures): %d" % mismatched_statements
    if total_volt_crashes > 0 or total_cmp_crashes > 0 or total_diff_crashes > 0:
        crash_types = []
        if total_volt_crashes > 0:
            crash_types.append("VoltDB ("+str(total_volt_crashes)+")")
        if total_cmp_crashes > 0:
            crash_types.append(comparison_database + " ("+str(total_cmp_crashes)+")")
        if total_diff_crashes > 0:
            crash_types.append("compare results ("+str(total_diff_crashes)+")")
        print "Total number of (" + ", ".join(crash_types) + ") crashes: %d" % (
                total_volt_crashes + total_cmp_crashes + total_diff_crashes)
        success = False

    if not success:
        sys.stdout.flush()
        sys.stderr.flush()
        print >> sys.stderr, "SQL coverage has errors."
        exit(1)
