#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2018 VoltDB Inc.

# Author: Phil Rosegay

# VOLTDB K8S node startup adapter
#
# Uses DNS SRV records to discover nodes and edit the VoltDB configuration when nodes startup.
# Initialize a new VoltDB Database on first run.
#
# args are voltdb ... (ie. voltdb start command)
# Invoke from the container ENTRYPOINT, or k8s command
#
#   voltdbk8s.py voltdb start <parms>

#
# voltdb start cmd with -H will be ignored, -H will be set to the pod name.
#

from __future__ import print_function
import sys, os
import socket
import subprocess
import httplib2
import shlex
from time import sleep
from subprocess import Popen, STDOUT


# mount point for persistent volume voltdbroot
PV_VOLTDBROOT = os.getenv('VOLTDB_K8S_ADAPTER_PVVOLTDBROOT', "/voltdbroot")


# TODO: need to implement internal-interface option (voltdb command) support
VOLTDB_INTERNAL_INTERFACE = int(os.getenv('VOLTDB_K8S_ADAPTER_INTERNAL_PORT', 3021))
VOLTDB_HTTP_PORT = int(os.getenv('VOLTDB_K8S_ADAPTER_HTTP_PORT', 8080))


# These VoltDB start args may be comma separated lists, the value passed to voltdb will be selected
# using the pod-pod_ordinal as index to the list of values. If there is only a single value, that value
# will be used for all pods.
MAYBE_ORDINAL_START_ARGS = ['--externalinterface',
                        '--internalinterface',
                        '--publicinterface',
                        '--admin',
                        '--client',
                        '--http',
                        '--internal',
                        '--replication',
                        '--zookeeper',
                        '--drpublic',
                     ]

def printerr(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def printdbg(*args, **kwargs):
    if __debug__:
        print(*args, file=sys.stdout, **kwargs)

def query_dns_srv(query):
    m_list = []
    try:
        # SRV gives us records for each node in the cluster like ...
        # voltdb stateful set pods are registered on startup not on readiness.
        # _service._proto.name.  TTL   class SRV priority weight port target.
        #nginx.default.svc.cluster.local    service = 10 100 0 voltdb-0.nginx.default.svc.cluster.local.
        # the fqdn is structured as ... headless-service-name.namespace.svc.cluster.local
        # this is similar to etcd
        cmd = "nslookup -type=SRV %s | awk '/%s/ {print $NF}'" % ((query,)*2)
        #cmd = "host -t SRV %s | awk '{print $NF}'" % query
        answers = subprocess.check_output(cmd, shell=True)
    except Exception as e:
        printerr(str(e))
        return m_list
    for rdata in answers.split('\n'):
        if len(rdata):
            m_list.append(rdata.split(' ')[-1][:-1])  # drop the trailing '.'
    printdbg(m_list)
    # return a list of fq hostnames of pods in the service domain
    return sorted(m_list)


def try_to_connect(host, port):
    s = socket.socket()
    try:
        s.connect((host, port))
        return True
    except Exception as e:
        printdbg(str(e))
        return False
    finally:
        s.close()


def http_get(url, host, port):
    # TODO: need to support clusters with externssl
    proto = "http"
    admin = "true"
    urlp = "%s://%s:%d/api/1.0/?%s&admin=%s" % (proto, host, port, url, admin)
    printdbg(urlp)
    h = httplib2.Http(".cache")
    return h.request(urlp, "GET")


def get_system_information(host, port, section='OVERVIEW'):
    try:
        resource = "Procedure=@SystemInformation&Parameters=[\"" + section + "\"]"
        resp_headers, content = http_get(resource, host, port)
        return resp_headers, content
    except:
        raise


def find_arg_index(args, arg):
    _args = arg.split(',')
    for a in _args:
        for i in range(0, len(args)):
            if args[i] == a:
                return i+1
    return None


def add_or_replace_arg(args, option, value, action=None):
    # option is comma separated list of option formats to be treated equally, ie. "-L,--license"
    # we'll reasonably assume that only one of the option formats is present
    options = option.split(",")
    for o in options:
        ix = find_arg_index(args, o)
        if ix is not None:
            break
    if ix is None:
        if action == 'replace':
            return
        args.append(o)
        args.append(value)
        return
    args[ix] = value
    return


def str_to_arg_list(text):
    # strip any leading and trailing quotes
    al = []
    text = text.strip("'\"")
    # returns a list of shell-like arguments
    for a in shlex.split(text):
        if '=' in a:
            al.extend(a.split('='))
        else:
            al.append(a)
    return al

#def init_voltdbroot(working_voltdbroot, assets_dir):


def get_hostname_tuple(fqdn):
    hostname, domain = fqdn.split('.', 1)
    ssp = hostname.split('-')
    hn = ('-'.join(ssp[0:-1]), ssp[-1], hostname, domain)  # statefulset hostnames are podname-pod_ordinal
    return hn   # returns a tuple (ss-name, pod_ordinal, hostname, domain)


if __name__ == "__main__":

    printdbg(sys.argv)
    printdbg(os.environ)

    # check that our args look like a voltdb start command line and only that
    if not (sys.argv[1] == 'voltdb' and sys.argv[2] == 'start'):
        printerr("WARNING: expected voltdb start command but found '%s'" % sys.argv[1])
        ###sys.exit(-1)

    fqhostname = os.getenv('VOLTDB_K8S_ADAPTER_FQHOSTNAME', socket.getfqdn())

    printdbg(fqhostname)

    # use the domain of the leader address to find other pods in our cluster
    hn = get_hostname_tuple(fqhostname)
    printdbg(hn)

    ssname, pod_ordinal, my_hostname, domain = hn

    if len(hn) != 4 or not pod_ordinal.isdigit():
        # we don't know what do with this, just fork
        #fork_voltdb(ssname+"-0."+domain)
        printerr("ERROR: Hostname parse error, is this a statefulset pod?, cannot continue")
        printerr(fqhostname)
        sys.exit(-1)

    pod_ordinal = int(pod_ordinal)

    # For maintenance mode, don't bring up the database just sleep indefinitely
    # nb. in maintenance mode, liveness checks will timeout if enabled
    if '--k8s-maintenance' in sys.argv:
        while True:
            sleep(10000)
        #os.execv('tail' '-f', '/dev/null')

    # TODO: -D not tested
    din = find_arg_index(sys.argv, '-D,--dir')
    if din:
        voltdbroot = '.'.join(my_hostname, sys.argv[din])
    else:
        voltdbroot = fqhostname

    # discover pods in our cluster, if any
    # nb. voltdb pods are registered when then are started, and therefore may not appear ready to k8s
    cluster_pods = query_dns_srv(domain)

    # find nodes which are actually up and
    # nodes may be "published before they are ready to receive traffic"
    cluster_pods_up = []
    for host in cluster_pods:
        print("Connecting to '%s'" % host)
        if try_to_connect(host, VOLTDB_INTERNAL_INTERFACE):
            # we may have found a running node, get voltdb SYSTEMINFORMATION
            if try_to_connect(host, VOLTDB_HTTP_PORT):
                sys_info = None
                try:
                    sys_info = get_system_information(host, VOLTDB_HTTP_PORT)
                    cluster_pods_up.append(host)
                except:
                    pass
    # in the event that no pods are up, direct fork to pod-0
    if len(cluster_pods_up) == 0:
        cluster_pods_up = [ssname+"-0."+domain]

    printdbg(cluster_pods_up)

    # before we fork over, see if /voltdbroot (persistent storage mount) is empty
    # if it is, initialize a new database there from our assets
    if not os.path.exists(PV_VOLTDBROOT):
        printerr("ERROR: '%s' is not mounted!!!!" % PV_VOLTDBROOT)
        sys.exit(-1)
    assets_dir = os.path.join(os.getenv('VOLTDB_INIT_VOLUME', '/etc/voltdb'))
    working_voltdbroot = os.path.join(PV_VOLTDBROOT, voltdbroot)

    try:
        pv = os.listdir(working_voltdbroot)
    except OSError:
        pv = []
    printdbg(pv)

    # initialize the voltdbroot if necessary
    if len(pv) == 0:
        print("Initializing a new voltdb database at '%s'" % working_voltdbroot)
        try:
            os.mkdir(working_voltdbroot)
        except OSError:
            pass
        olddir = os.getcwd()
        os.chdir(working_voltdbroot)
        cmd = [sys.argv[1], 'init']
        """
        These asset files are mounted from a configmap
        !!!Updating the configmap will have NO EFFECT on a running or restarted instance!!!
        The command to create the configmap is typically:

        kubectl create configmap --from-file=deployment=mydeployment.xml [--from-file=classes=myclasses.jar] [--from-file=schema=myschema.sql]

        The deployment file node count is ignored, node count is specified in the runtime environment variable $NODECOUNT
        """
        if os.path.isdir(assets_dir):
            deployment_file = os.path.join(assets_dir, 'deployment')
            if os.path.isfile(deployment_file):
                cmd.extend(['--config', deployment_file])
            classes_file = os.path.join(assets_dir, 'classes')
            if os.path.isfile(classes_file):
                cmd.extend(['--classes', classes_file])
            schema_file = os.path.join(assets_dir, 'schema')
            if os.path.isfile(schema_file):
                cmd.extend(['--schema', schema_file])
        extra_init_args = os.getenv('VOLTDB_INIT_ARGS')
        if extra_init_args:
            cmd.extend(str_to_arg_list(extra_init_args))
        print("Init command: " + str(cmd))
        sp = Popen(cmd, shell=False, stderr=STDOUT)
        sp.wait()
        if sp.returncode != 0:
            print("ERROR failed Initializing voltdb database at '%s' (did you forget --force?)" %
                  working_voltdbroot)
            sys.exit(-1)
        print("Initialize new voltdb succeeded!!!")
        os.chdir(olddir)
        os.system("rm -f " + ssname)
        os.system("ln -sf " + voltdbroot + " " + ssname)

    # check that we have the correct/consistent PV for this node
    try:
        pv = os.listdir(PV_VOLTDBROOT)
    except OSError:
        pv = []
    if voltdbroot not in pv:
        printerr("WARNING voltdbroot expected: '%s' actual: '%s'" % (voltdbroot, pv))
        ###sys.exit(1)  # not enforcing this but beware pv's might go to any pod on reuse?

    os.chdir(working_voltdbroot)
    args = sys.argv[:]  # copy

    # normalize args
    # some of our "args" might be environment strings of args, if so break them up for the shell
    nargs = []
    for a in args:
        if ' ' in a:
            nargs.extend(str_to_arg_list(a))
        else:
            nargs.append(a)
    args = nargs

    # filter pod-pod_ordinal args: ex: '--foo a,b,c,d' when run time podname is 'podname-2' results in the
    # value '--foo c' rendered to the commandline
    for oa in MAYBE_ORDINAL_START_ARGS:
        ix = find_arg_index(args, oa)
        if ix is not None:
            printdbg(args[ix])
            if ',' in args[ix]:
                li = args[ix].split(',')
                printdbg(li)
                printdbg(pod_ordinal)
                printdbg(len(li))
                if len(li) < pod_ordinal:
                    printerr("ERROR treating '%s' as a pod_ordinal comma separated list but there appear to be"
                             " insufficient entries for host '%s'" % (oa, pod_ordinal))
                    sys.exit(-1)
                args[ix] = li[pod_ordinal]

    # build the voltdb start command line
    printdbg(args)
    args = shlex.split(' '.join(args))

    if os.path.isdir(assets_dir):
        license_file = os.path.join(assets_dir, 'license')
        if os.path.isfile(license_file):
            add_or_replace_arg(args, "-l,--license", license_file)
    add_or_replace_arg(args, "-D,--dir", working_voltdbroot, action="replace")
    add_or_replace_arg(args, "-H,--host", ','.join(cluster_pods_up))

    print("VoltDB cmd is '%s'" % args[1:])
    print("Starting VoltDB...")

    # flush so we see our output in k8s logs
    sys.stdout.flush()
    sys.stderr.flush()

    # fork voltdb
    d = os.path.dirname(args[0])
    os.execv(os.path.join(d, args[1]), args[1:])
    sys.exit(0)
