# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.
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
import random
import re
import subprocess
import sys
import xmlrunner
import unittest
from optparse import OptionParser

random.seed()

# verbs contains verb to version (denote old cli version 1)
volt_support_version = [1, 2]

volt_verbs = {'create': 1,
              'recover': 1,
              'rejoin': 1,
              'add': 1,
              'init': 2,
              'start': 2}

volt_verbs_mapping = {'create': 'create',
                      'recover': 'recover',
                      'rejoin': 'live rejoin',
                      'add': 'add',
                      'init': 'initialize',
                      'start': 'probe'}


# create all the options
class Opt:
    def __init__(self, pyname, javaname, datatype, ver):
        self.pyname = pyname
        self.javaname = javaname
        self.datatype = datatype
        self.ver = ver


# for oldcli
admin = Opt('admin', 'adminport', str, 1)
client = Opt('client', 'port', str, 1)
externalinterface = Opt('externalinterface', 'externalinterface', str, 1)
http = Opt('http', 'httpport', str, 1)
internal = Opt('internal', 'internalport', str, 1)
internalinterface = Opt('internalinterface', 'internalinterface', str, 1)
publicinterface = Opt('publicinterface', 'publicinterface', str, 1)
replication = Opt('replication', 'replicationport', str, 1)
zookeeper = Opt('zookeeper', 'zkport', str, 1)
deployment = Opt('deployment', 'deployment', str, 1)
force = Opt('force', 'force', None, 1)
placementgroup = Opt('placement-group', 'placementgroup', str, 1)
host = Opt('host', 'host', str, 1)
licensefile = Opt('license', 'license', str, 1)
pause = Opt('pause', 'paused', None, 1)
# 'replica' should be immediately after verb
replica = Opt('replica', 'replica', None, 1)
# 'blocking' is only for rejoin, does not have corresponding java optional name, change verb 'live rejoin' to 'rejoin'
blocking = Opt('blocking', '', None, 1)

# for newcli only
mesh = Opt('host', 'mesh', str, 2)
config = Opt('config', 'deployment', str, 2)
voltdbroot = Opt('dir', 'voltdbroot', str, 2)
hostcount = Opt('count', 'hostcount', int, 2)
add = Opt('add', 'enableadd', None, 2)

# negative opt
unknown = Opt('unknown', None, None, 0)

volt_opts = {'create': [admin,
                        client,
                        externalinterface,
                        http,
                        internal,
                        internalinterface,
                        publicinterface,
                        replication,
                        zookeeper,
                        deployment,
                        force,
                        placementgroup,
                        host,
                        licensefile,
                        pause,
                        replica],

             'recover': [admin,
                         client,
                         externalinterface,
                         http,
                         internal,
                         internalinterface,
                         publicinterface,
                         replication,
                         zookeeper,
                         deployment,
                         placementgroup,
                         host,
                         licensefile,
                         pause,
                         replica],

             'rejoin': [admin,
                        client,
                        externalinterface,
                        http,
                        internal,
                        internalinterface,
                        publicinterface,
                        replication,
                        zookeeper,
                        deployment,
                        placementgroup,
                        licensefile],

             'add': [admin,
                     client,
                     externalinterface,
                     http,
                     internal,
                     internalinterface,
                     publicinterface,
                     replication,
                     zookeeper,
                     deployment,
                     placementgroup,
                     licensefile],

             'init': [config,
                      voltdbroot,
                      force],

             'start': [admin,
                       client,
                       externalinterface,
                       http,
                       internal,
                       internalinterface,
                       publicinterface,
                       replication,
                       zookeeper,
                       hostcount,
                       voltdbroot,
                       placementgroup,
                       mesh,
                       licensefile,
                       pause,
                       replica,
                       add]
             }

volt_opts_mandatory = {'create': [],
                       'recover': [],
                       'rejoin': [host],
                       'add': [host],
                       'init': [],
                       'start': []
                       }

volt_opts_negative = [unknown]
# additional output cli
volt_verbs_output = {'create': ' [ CATALOG ]',
                     'recover': '',
                     'rejoin': '',
                     'add': '',
                     'init': '',
                     'start': ''
                     }

# some verbs will generate default opts to java command line
volt_opts_default = {
    'create': {placementgroup.javaname: '0', host.javaname: 'localhost:3021'},
    'recover': {placementgroup.javaname: '0', host.javaname: 'localhost:3021'},
    'rejoin': {placementgroup.javaname: '0'},
    'add': {placementgroup.javaname: '0'},
    'init': {},
    'start': {placementgroup.javaname: '0', mesh.javaname: "\"\""}
}

# regular expression for pre-process the actual output before comparison
ignore = "^(Exec:|Run:) (?P<java_path>.+?)(java) (?P<java_opts>.+?) (-classpath) (?P<classpath>.+?) (org.voltdb.VoltDB)";
ignore_re = re.compile(ignore, re.X | re.M)

# override of environments
volt_override = {'VOLTDB_HEAPMAX': '3072',
                 'VOLTDB_OPTS': '-XX:disableGCHeuristics'}


# regular expression option naming convention
option_name = "--([\-a-z]+)"
option_name_re = re.compile(option_name)
# ignore python only option
# also skip 'blocking'
option_ignore = ['version', 'help', 'verbose', 'background', 'ignore', 'blocking']


class TestsContainer(unittest.TestCase):
    longMessage = True


def make_test_function(haddiff, description):
    def test(self):
        self.assertFalse(haddiff, description)

    return test


def run_unit_test(verb, opts, expected_opts, reportout, expectedOut=None, expectedErr=None):
    stdout, stderr = run_voltcli(verb, opts, reportout)
    haddiff, description = compare_result(stdout, stderr, volt_verbs_mapping[verb], expected_opts, reportout,
                                          expectedOut, expectedErr)
    setattr(TestsContainer, 'test: {0}'.format(verb + " " + " ".join(opts)), make_test_function(haddiff, description))
    return haddiff


# Execute the command.
def run_voltcli(verb, opts, reportout=None, cmd=['../../bin/voltdb'], mode=['--dry-run'], environ=None, cwd=None):
    command = cmd + [verb] + mode + opts
    if reportout:
        reportout.write("Test python cli:\n\t" + " ".join([verb] + opts) + "\n")
    proc = subprocess.Popen(command,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            cwd=cwd,
                            env=environ)
    stdout, stderr = proc.communicate()
    return stdout, stderr


def compare_result(stdout, stderr, verb, opts, reportout, expectedOut=None, expectedErr=None):
    output_str = sanitize(stdout).strip()
    description = "Generate java command line:\n\t" + output_str + "\nTest Passed!\n\n"
    if expectedOut:
        haddiff = False
        if expectedOut != stdout:
            description = "Generate stdout:\n" + stdout + "\n" + "doest not match expected:\n" + expectedOut + + "\nTest Failed!\n\n"
            haddiff = True
        else:
            description = "Generate expected stdout:\n" + stdout + "Test Passed!\n\n"
        reportout.write(description)
        return haddiff, description

    if expectedErr:
        haddiff = False
        if stderr != expectedErr:
            haddiff = True
            description = "Generate stderr:\n" + stderr + "\n" + "doest not match expected:\n" + expectedErr + "\nTest Failed!\n\n"
        else:
            description = "Generate expected stderr:\n" + stderr + "Test Passed!\n\n"
        reportout.write(description)
        return haddiff, description



    # match the verbs
    if output_str.find(verb) != 0:
        description = "Generate java command line:\n\t" + output_str + "\n" + "does not contain expected verb:\n" + verb + "\nTest Failed!\n\n"
        reportout.write(description)
        return True, description

    # match the opts
    output_tokens = output_str.lstrip(verb).split()
    expected_tokens = []
    for k, v in opts.items():
        if v:
            expected_tokens.extend([k, v])
        else:
            expected_tokens.append(k)
    if set(output_tokens) != set(expected_tokens):
        description = "Generate java command line:\n\t" + output_str + "\n" + "does not match expected options:\n" + " ".join(
            expected_tokens) + "\nTest Failed!\n\n"
        reportout.write(description)
        return True, description

    reportout.write(description)
    return False, description


def sanitize_replace(match):
    # If `ignore` pattern does not contain subgroups, remove
    # the whole match.
    if not match.re.groups:
        return ""
    # Otherwise, remove subgroups.
    spans = []
    group_start = match.start()
    for idx in range(match.re.groups):
        start, end = match.span(idx + 1)
        if start < end:
            start -= group_start
            end -= group_start
            spans.append((end, start))
    spans.sort()
    spans.reverse()
    text = match.group()
    last_cut = len(text)
    for end, start in spans:
        end = min(end, last_cut)
        if start >= end:
            continue
        text = text[:start] + text[end:]
        last_cut = start
    return text


def sanitize(text):
    # Remove portion of output matching ignore pattern
    if ignore is True:
        return ""
    if not ignore:
        return text
    text = ignore_re.sub(sanitize_replace, text)
    return text


def gen_config(mandatory_opts, all_ops, count, expected_opts={}):
    opts = []
    i = 1  # pseudo optional value
    for opt in mandatory_opts + random.sample(all_ops, count):
        if not opt.datatype:
            o = '--' + opt.pyname
            expected_opts[opt.javaname] = None
        else:
            o = '--' + opt.pyname + '=' + str(i)
            expected_opts[opt.javaname] = str(i)
        i += 1
        opts.append(o)
    return opts, expected_opts

# Test JAVA HEAP (VOLTDB_HEAPMAX) and Java Runtime Options(VOLTDB_OTPS) can be override
def test_java_opts_override(verb = 'start', reportout = None):
    haddiffs = False
    override_env = dict(os.environ.copy(), **volt_override)
    stdout, _ = run_voltcli(verb, [], environ=override_env)
    matched_java_opts = ignore_re.match(stdout).group('java_opts')
    reportout.write("Given: " + " ".join([k + '=' + v for k, v in volt_override.items()]) + "\n" +
                    "Got JVM Options: " + matched_java_opts + "\n")
    if 'VOLTDB_HEAPMAX' in volt_override:
        if '-Xmx{}m'.format(volt_override['VOLTDB_HEAPMAX']) in matched_java_opts:
            reportout.write("VOLTDB_HEAPMAX override sucessfully!\n")
        else:
            reportout.write("VOLTDB_HEAPMAX override failed!\n")
            haddiffs = True
    if 'VOLTDB_OPTS' in volt_override:
        if volt_override['VOLTDB_OPTS'] in matched_java_opts:
            reportout.write("VOLTDB_OPTS override sucessfully!\n\n")
        else:
            reportout.write("VOLTDB_OPTS override failed!\n\n")
            haddiffs = True
    return haddiffs

def do_main():
    parser = OptionParser()
    parser.add_option("-o", "--report_file", dest="report_file",
                      default="./voltverbstest.report",
                      help="report output file")
    (options, args) = parser.parse_args()

    # generate output report: plain text
    reportout = open(options.report_file, 'w+')

    # test override of environment
    haddiffs = test_java_opts_override(reportout=reportout)

    try:
        for verb, version in volt_verbs.items():
            if not (version in volt_support_version):
                continue

            # test verb coverage
            stdout, stderr = run_voltcli(verb, [], mode=['--help'])
            # parse_help_message(stdout)
            available_opts = option_name_re.findall(stdout)
            covered_opts = [opt.pyname for opt in volt_opts_mandatory[verb] + volt_opts[verb]]
            untested_opts = set(available_opts) - set(option_ignore) - set(covered_opts)
            if untested_opts:
                description = "Uncovered option(s) for " + verb + " : [" + " ".join(untested_opts) + "]\n"
                reportout.write(description)
                haddiffs = True

            # generate the test cases
            ## generate minimal config
            opts, expected_opts = gen_config(volt_opts_mandatory[verb], volt_opts[verb], 0,
                                             volt_opts_default[verb].copy())
            haddiffs = run_unit_test(verb, opts, expected_opts, reportout) or haddiffs

            ## generate config that contain a single opt
            for opt in volt_opts[verb]:
                opts, expected_opts = gen_config(volt_opts_mandatory[verb] + [opt], [], 0,
                                                 volt_opts_default[verb].copy())
                haddiffs = run_unit_test(verb, opts, expected_opts, reportout) or haddiffs

            ## generate config contain random opts
            opts, expected_opts = gen_config(volt_opts_mandatory[verb], volt_opts[verb],
                                             random.randint(0, len(volt_opts[verb])), volt_opts_default[verb].copy())
            haddiffs = run_unit_test(verb, opts, expected_opts, reportout) or haddiffs

            ## generate config contain negative opts
            opts, expected_opts = gen_config(volt_opts_mandatory[verb], volt_opts_negative, 1)
            haddiffs = run_unit_test(verb, opts, expected_opts, reportout,
                                     expectedOut="""Usage: voltdb {} [ OPTIONS ... ]{}\n\nvoltdb: error: no such option: --{}\n""".
                                     format(verb, volt_verbs_output[verb], volt_opts_negative[0].pyname)) or haddiffs

        unittest.main(testRunner=xmlrunner.XMLTestRunner(output='test-reports'))
        # unittest.main(verbosity=2)
    finally:
        print "Summary report written to file://" + os.path.abspath(options.report_file)
        if haddiffs:
            sys.exit("One or more voltverbstest script failures or errors was detected.")
        else:
            print "All verb test covered and passed!"


if __name__ == "__main__":
    do_main()
