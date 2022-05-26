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

# How are the tests generated for the Python unittest framework?
#  See http://blog.kevinastone.com/generate-your-tests.html for
#  a detailed walkthrough the process of dynamic unittest generation

import sys

if sys.hexversion < 0x03060000:
    raise Exception("Python version 3.6 or greater is required.")

import os
import random
import re
import subprocess
import xmlrunner
import unittest
from optparse import OptionParser

random.seed()

# verbs contains verb to version map.
# version 2 is the 'new' cli (which is about 9 years old)
# version 1 was the 'legacy' cli (totally removed in v11.0)
volt_support_version = [2]

# regular verbs only here. more-or-less, if the python
# 'voltdb' commands does not run the actual voltdb server,
# it won't work in this list. see volt_irregular_verbs.
volt_verbs = {'init': 2,
              'start': 2}

volt_verbs_mapping = {'check': 'check',
                      'collect': 'collect',
                      'get': 'get',
                      'init': 'initialize',
                      'mask': 'mask',
                      'start': 'probe' }

# create all the options
class Opt:
    def __init__(self, pyname, javaname, datatype, ver):
        self.pyname = pyname
        self.javaname = javaname
        self.datatype = datatype
        self.ver = ver

# alphabetical order please

add = Opt('add', 'enableadd', None, 2)
admin = Opt('admin', 'adminport', str, 2)
classes = Opt('classes', 'classes', str, 2)
client = Opt('client', 'port', str, 2)
config = Opt('config', 'deployment', str, 2)
days = Opt('days', 'days', int, 2)
drpublic = Opt('drpublic', 'drpublic', str, 2)
dryrun = Opt('dry-run', 'dry-run', None, 2)
externalinterface = Opt('externalinterface', 'externalinterface', str, 2)
force = Opt('force', 'force', None, 2)
hostcount = Opt('count', 'hostcount', int, 2)
http = Opt('http', 'httpport', str, 2)
internalinterface = Opt('internalinterface', 'internalinterface', str, 2)
internal = Opt('internal', 'internalport', str, 2)
licensefile = Opt('license', 'license', str, 2)
mesh = Opt('host', 'mesh', str, 2)
missing = Opt('missing', 'missing', str, 2)
output = Opt('output', 'file', str, 2)
pause = Opt('pause', 'paused', None, 2)
placementgroup = Opt('placement-group', 'placementgroup', str, 2)
prefix = Opt('prefix', 'prefix', str, 2)
publicinterface = Opt('publicinterface', 'publicinterface', str, 2)
replication = Opt('replication', 'replicationport', str, 2)
retain = Opt('retain', 'retain', int, 2)
schema = Opt('schema', 'schema', str, 2)
skipheapdump = Opt('skip-heap-dump', 'skip-heap-dump', None, 2)
topicsport = Opt('topicsport', 'topicsHostPort', str, 2)
topicspublic = Opt('topicspublic', 'topicspublic', str, 2)
verbose = Opt('verbose', 'verbose', None, 2)
voltdbroot = Opt('dir', 'voltdbroot', str, 2)
zookeeper = Opt('zookeeper', 'zkport', str, 2)

# negative opt
unknown = Opt('unknown', None, None, 0)

volt_opts = {'check': [],
             'collect': [prefix,
                         output,
                         dryrun,
                         skipheapdump,
                         days,
                         voltdbroot,
                         force],
             'get': [voltdbroot,
                     force,
                     output],
             'init': [config,
                      voltdbroot,
                      force,
                      retain,
                      classes,
                      licensefile,
                      schema],
             'mask': [],
             'start': [admin,
                       client,
                       drpublic,
                       externalinterface,
                       http,
                       internal,
                       internalinterface,
                       publicinterface,
                       replication,
                       topicsport,
                       zookeeper,
                       add,
                       hostcount,
                       voltdbroot,
                       placementgroup,
                       mesh,
                       licensefile,
                       missing,
                       pause,
                       topicsport,
                       topicspublic],
             }

volt_opts_mandatory = {'check': [],
                       'collect': [],
                       'get': [],
                       'init': [],
                       'mask': [],
                       'start': []}

volt_opts_negative = [unknown]

# additional output cli

volt_verbs_output = {'check': '',
                     'collect': ' VOLTDBROOT',
                     'get': ' RESOURCE',
                     'init': '',
                     'mask': ' DEPLOYMENTFILE ...',
                     'start': ''}

# some verbs will generate default opts to java command line

volt_opts_default = {'check': {},
                     'collect': {},
                     'get': {},
                     'init': {},
                     'mask': {},
                     'start': {placementgroup.javaname: '0', mesh.javaname: "\"\""}}

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

# voltdb get and others use positional arguments so their
# tests can't use the same pattern as "voltdb start|init|..."
# model data for the irregular (voltdb get) family
volt_irregular_verbs = { "get": 2, }
get = Opt("get", "get", str, 2)
mask = Opt("mask", "mask", str, 2)
irr_deployment = Opt('deployment', 'deployment', str, 2)
irr_schema = Opt("schema", "schema", str, 2)
irr_classes = Opt("classes", "classes", str, 2)
irr_license = Opt("license", "license", str, 2)
voltdbrootdir = Opt("--dir somedir", "getvoltdbroot somedir", str, 2)
otheroot = Opt("otheroot", "getvoltdbroot otheroot", str, 2)
defaultroot = Opt("", "getvoltdbroot voltdbroot",  str, 2)
# noverb = Opt("", "no verb: put some help text here", str, 2)
# noobj = Opt("", "no obj: put some help text here", str, 2)
# noparms = Opt("", "no parms", str, 2)
# noout = Opt("", "file \"\"", str, 2)
somefile = "somefile"
out = Opt("--output "+somefile, "file"+" "+somefile, str, 2)
verbs = [ get, ]
objects = [ irr_deployment, irr_schema, irr_classes, irr_license ]  # required
options = [ voltdbrootdir, out, "none", ]

# TODO: add other irregular verbs
# check, collect, mask
# above structures do not seem sufficiently general

class TestsContainer(unittest.TestCase):
    longMessage = True

def make_test_function(haddiffs, description):
    def test(self):
        self.assertFalse(haddiffs, description)
    return test


def run_unit_test(verb, opts, expected_opts, reportout, expectedOut=None, expectedErr=None):
    stdout, stderr = run_voltcli(verb, opts, reportout)
    haddiffs, description = compare_result(stdout, stderr, volt_verbs_mapping[verb], expected_opts, reportout,
                                          expectedOut, expectedErr)
    setattr(TestsContainer, 'test: {0}'.format(verb + " " + " ".join(opts)), make_test_function(haddiffs, description))
    return haddiffs


# Execute the command.
def run_voltcli(verb, opts, reportout=None, cmd=['voltdb'], mode=['--dry-run'], environ=None, cwd=None):
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
    if stdout:
        stdout = stdout.decode("utf-8")
    if stderr:
        stderr = stderr.decode("utf-8")
    return stdout, stderr


def compare_result(stdout, stderr, verb, opts, reportout, expectedOut=None, expectedErr=None):
    output_str = sanitize(stdout).strip()
    description = "Generated java command line:\n\t" + output_str + "\nTest Passed!\n\n"
    if expectedOut:
        haddiffs = False
        if expectedOut != stdout:
            description = "Generated stdout:\n" + stdout + "\n" + "does not match expected:\n" + expectedOut + "\nTest Failed!\n\n"
            haddiffs = True
        else:
            description = "Generated expected stdout:\n" + stdout + "Test Passed!\n\n"
        reportout.write(description)
        return haddiffs, description

    if expectedErr:
        haddiffs = False
        if stderr != expectedErr:
            haddiffs = True
            description = "Generated stderr:\n" + stderr + "\n" + "doest not match expected:\n" + expectedErr + "\nTest Failed!\n\n"
        else:
            description = "Generated expected stderr:\n" + stderr + "Test Passed!\n\n"
        reportout.write(description)
        return haddiffs, description



    # match the verbs
    # if output_str.find(verb) == -1:
    if output_str.lower().find(verb) == -1:
        description = "Generated java command line:\n\t" + output_str + "\n" + "does not contain expected verb:\n" + verb + "\nTest Failed!\n\n"
        reportout.write(description)
        return True, description

    # match the opts
    output_tokens = output_str.lstrip(verb).split()
    expected_tokens = []
    for k, v in list(opts.items()):
        if v:
            expected_tokens.extend([k, v])
        else:
            expected_tokens.append(k)
    if set(output_tokens) != set(expected_tokens):
        description = "Generated java command line:\n\t" + output_str + "\n" + "does not match expected options:\n" + " ".join(
            expected_tokens) + "\nTest Failed!\n\n"

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

# Test JAVA HEAP (VOLTDB_HEAPMAX) and Java Runtime Options(VOLTDB_OTPS) can be overridden
def test_java_opts_override(verb = 'start', reportout = None):
    haddiffs = False
    override_env = dict(os.environ.copy(), **volt_override)
    stdout, _ = run_voltcli(verb, [], environ=override_env)
    m = ignore_re.match(stdout)
    if m is None:
        raise RuntimeError("No matches found in: '%s'" % stdout)
    matched_java_opts = m.group('java_opts')
    reportout.write("Given: " + " ".join([k + '=' + v for k, v in list(volt_override.items())]) + "\n" +
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

# Test the irregular verbs -- the ones that have positional args
def test_irregular_verbs(reportout = None):
    haddiffs = False

    for v in volt_irregular_verbs:
        for obj in objects:
            for pos in options:
                if pos == "none":
                    clean_args = [obj.pyname]
                    expected_opts = [volt_verbs_mapping[v], obj.javaname,]
                else:
                    expected_opts = [volt_verbs_mapping[v], obj.javaname, pos.javaname]
                    clean_args = []
                    for a in [obj.pyname, pos.pyname]:
                        if len(a) > 0:
                            for b in str(a).split():
                                clean_args.append(b)
                stdout, stderr = run_voltcli(v, clean_args, reportout)

                javaout = sanitize(stdout)
                haddiffs, description = compare_irregular(javaout, expected_opts, reportout)

                if pos == "none":
                    setattr(TestsContainer, 'test: {0}'.format(v+ " " + " ".join([ obj.pyname, ])),
                            make_test_function(haddiffs, description))
                else:
                    setattr(TestsContainer, 'test: {0}'.format(v+ " " + " ".join([ obj.pyname, pos.pyname, ])),
                            make_test_function(haddiffs, description))

def compare_irregular(actual, expected, reportout):
    """
        Follows the model already in place:
        False means "no differences", aka match
        True means "differences", aka doesn't match
    """
    description = "Generated java command line:\n\t" + actual
    if actual.strip() == " ".join(expected).strip():
        description += "\nTest Passed!\n\n"
        haddiffs = False
    else:
        description += "\nTest Failed!\n\n"
        haddiffs = True
    reportout.write(description)
    return haddiffs, description

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
    # test irregulars...
    haddiffs = test_irregular_verbs(reportout=reportout)

    try:
        for verb, version in list(volt_verbs.items()):
            if not (version in volt_support_version):
                continue

            # test verb coverage
            stdout, stderr = run_voltcli(verb, [], mode=['--help'])
            # parse_help_message(stdout)
            available_opts = option_name_re.findall(stdout)
            covered_opts = [opt.pyname for opt in volt_opts_mandatory[verb] + volt_opts[verb]]
            untested_opts = set(available_opts) - set(option_ignore) - set(covered_opts)
            if untested_opts:
                description = "Uncovered option(s) for " + verb + " : [" + " ".join(untested_opts) + "]\nCoverage Failed!\n\n"
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
        print("Summary report written to file://" + os.path.abspath(options.report_file))
        if haddiffs:
            sys.exit("One or more voltverbstest script failures or errors was detected.")
        else:
            print("All verb tests passed, all verb options covered!")


if __name__ == "__main__":
    do_main()
