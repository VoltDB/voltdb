import os
import random
import re
import subprocess
import sys
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
replica = Opt('replica', 'replica', None, 1)  # should be immediately after verb

blocking = Opt('blocking', '', None, 1)  # hidden option, only for rejoin, change verb 'live rejoin' to 'rejoin'

# for newcli only
mesh = Opt('host', 'mesh', str, 2)
config = Opt('config', 'deployment', str, 2)
voltdbroot = Opt('dir', 'voltdbroot', str, 2)
hostcount = Opt('count', 'hostcount', int, 2)

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
                         force,
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
                       force,
                       placementgroup,
                       mesh,
                       licensefile,
                       pause,
                       replica]
             }

volt_opts_mandatory = {'create': [],
                       'recover': [],
                       'rejoin': [host],
                       'add': [host],
                       'init': [],
                       'start': []
                       }

volt_opts_negative = [unknown]

# some verbs will generate default opts to java command line
volt_opst_default = {
    'create': {placementgroup.javaname: '0', host.javaname: 'localhost:3021'},
    'recover': {placementgroup.javaname: '0', host.javaname: 'localhost:3021'},
    'rejoin': {placementgroup.javaname: '0'},
    'add': {placementgroup.javaname: '0'},
    'init': {},
    'start': {placementgroup.javaname: '0', mesh.javaname: "\"\"", hostcount.javaname: '1'}
}

# regular expression for pre-process the actual output before comparison
ignore = "^(Exec:|Run:) (?P<java_path>.+?)(java) (?P<java_opts>.+?) (-classpath) (?P<classpath>.+?) (org.voltdb.VoltDB)";
ignore_re = re.compile(ignore, re.X | re.M)


def do_main():
    parser = OptionParser()
    parser.add_option("-o", "--report_file", dest="report_file",
                      default="./verbtest.report",
                      help="report output file")
    (options, args) = parser.parse_args()

    # generate output report: plain text
    reportout = open(options.report_file, 'w+')

    # test override of environment

    haddiffs = False
    try:
        for verb, version in volt_verbs.items():
            if not (version in volt_support_version):
                continue

            # generate the test cases
            ## generate simplest config
            opts, expected_opts = gen_config(volt_opts_mandatory[verb], volt_opts[verb], 0,
                                             volt_opst_default[verb].copy())
            stdout, stderr = run_voltcli(verb, opts)
            haddiffs = compare_result(stdout, stderr, volt_verbs_mapping[verb], expected_opts, reportout) or haddiffs

            ## generate config that contain a single opt
            for opt in volt_opts[verb]:
                opts, expected_opts = gen_config(volt_opts_mandatory[verb] + [opt], [], 0,
                                                 volt_opst_default[verb].copy())
                stdout, stderr = run_voltcli(verb, opts)
                haddiffs = compare_result(stdout, stderr, volt_verbs_mapping[verb], expected_opts,
                                          reportout) or haddiffs

            ## generate config contain random opts
            opts, expected_opts = gen_config(volt_opts_mandatory[verb], volt_opts[verb],
                                             random.randint(0, len(volt_opts[verb])), volt_opst_default[verb].copy())
            stdout, stderr = run_voltcli(verb, opts)
            haddiffs = compare_result(stdout, stderr, volt_verbs_mapping[verb], expected_opts, reportout) or haddiffs

            ## generate config contain negative opts
            opts, _ = gen_config(volt_opts_mandatory[verb], volt_opts_negative, 1)
            stdout, stderr = run_voltcli(verb, opts)
            haddiffs = compare_result(stdout, stderr, volt_verbs_mapping[verb], expected_opts, reportout,
                                    expectedOut="""Usage: voltdb {} [ OPTIONS ... ]

                                                   voltdb: error: no such option: --{}""".
                                      format(verb, volt_opts_negative[0].pyname)) or haddiffs



    finally:
        print "Summary report written to file://" + os.path.abspath(options.report_file)
        if haddiffs:
            sys.exit("One or more voltverbstest script failures or errors was detected.")


# Execute the command.
def run_voltcli(verb, opts, cmd=['../../bin/voltdb'], mode=['--dry-run'], environ=None, cwd=None):
    command = cmd + [verb] + mode + opts
    proc = subprocess.Popen(command,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            cwd=cwd,
                            env=environ)
    stdout, stderr = proc.communicate()
    return stdout, stderr


def compare_result(stdout, stderr, verb, opts, repotout, expectedOut=None, expectedErr=None):
    output_str = sanitize(stdout).strip()
    if expectedOut or expectedErr:
        haddiff = False
        if stdout != expectedOut:
            haddiff = True
            repotout.write("Generate stdout:\n" + stdout + "\n")
            repotout.write("Doest not match expected:\n" + expectedOut + "\n")
        if stderr != expectedErr:
            haddiff = True
            repotout.write("Generate stderr:\n" + stderr + "\n")
            repotout.write("Doest not match expected:\n" + expectedErr + "\n")
        return haddiff

    # match the verbs
    if output_str.find(verb) != 0:
        repotout.write("Generate java command line:\n" + output_str + "\n")
        repotout.write("Does not contain expected verb:\n" + verb + "\n")
        return True

    # match the opts
    output_tokens = output_str.lstrip(verb).split()
    expected_tokens = []
    [expected_tokens.extend([k, v]) for k, v in opts.items()]
    if set(output_tokens) != set(expected_tokens):
        repotout.write("Generate java command line:\n" + output_str + "\n")
        repotout.write("Does not match expected options:\n" + " ".join(expected_tokens) + "\n")
        return True

    return False


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
    i = 0  # pseudo optional value
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


if __name__ == "__main__":
    do_main()
