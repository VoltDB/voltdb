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

import sys, os
import socket
import subprocess
import httplib2
import shlex
from time import time, sleep, strftime, gmtime, localtime, timezone
from subprocess import Popen, STDOUT
import logging
import fileinput
import re
from traceback import format_exc
from tempfile import mkstemp
import random


# mount point for persistent volume voltdbroot
PV_VOLTDBROOT = os.getenv('VOLTDB_K8S_ADAPTER_PVVOLTDBROOT', "/voltdbroot")


# TODO: need to implement internal-interface option (voltdb command) support
VOLTDB_INTERNAL_INTERFACE = int(os.getenv('VOLTDB_K8S_ADAPTER_INTERNAL_PORT', 3021))
VOLTDB_HTTP_PORT = int(os.getenv('VOLTDB_K8S_ADAPTER_ADMIN_PORT', 8080))


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

def query_dns_srv(query):
    m_list = []
    try:
        # SRV gives us records for each node in the cluster like ...
        # voltdb stateful set pods are registered on startup not on readiness.
        # _service._proto.name.  TTL   class SRV priority weight port target.
        #nginx.default.svc.cluster.local    service = 10 100 0 voltdb-0.nginx.default.svc.cluster.local.
        # the fqdn is structured as ... headless-service-name.namespace.svc.cluster.local
        # this is similar to etcd
        logging.debug(query)
        cmd = "nslookup -type=SRV %s | awk '/^%s/ {print $NF}'" % ((query,)*2)
        #cmd = "host -t SRV %s | awk '{print $NF}'" % query
        answers = subprocess.check_output(cmd, shell=True)
        logging.debug(answers)
    except Exception as e:
        logging.error(str(e))
        return m_list
    for rdata in answers.split('\n'):
        if len(rdata):
            m_list.append(rdata.split(' ')[-1][:-1])  # drop the trailing '.'
    logging.debug(m_list)
    # return a list of fq hostnames of pods in the service domain
    return sorted(m_list)


def try_to_connect(host, port):
    s = socket.socket()
    try:
        logging.debug("try_to_connect to '"+host+":"+str(port)+"'")
        s.connect((host, port))
        logging.debug("connected!")
        return True
    except Exception as e:
        logging.debug(str(e))
        return False
    finally:
        s.close()


def http_get(url, host, port):
    # TODO: need to support clusters with externssl
    proto = "http"
    admin = "true"
    urlp = "%s://%s:%d/api/1.0/?%s&admin=%s" % (proto, host, port, url, admin)
    logging.debug(urlp)
    h = httplib2.Http(".cache", timeout=15)
    return h.request(urlp, "GET")


def get_system_information(host, port, section='OVERVIEW'):
    try:
        resource = "Procedure=@SystemInformation&Parameters=[\"" + section + "\"]"
        resp_headers, content = http_get(resource, host, port)
        logging.debug(resp_headers, content)
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


def get_fqhostname():
    # get the pod's hostname, typically cluster-name-ORDINAL.domain
    return os.getenv('VOLTDB_K8S_ADAPTER_FQHOSTNAME', socket.getfqdn())


def get_hostname_tuple(fqdn):
    try:
        hostname, domain = fqdn.split('.', 1)
        ssp = hostname.split('-')
        hn = ('-'.join(ssp[0:-1]), ssp[-1], hostname, domain)  # statefulset hostnames are podname-pod_ordinal
    except:
        return None
    return hn   # returns a tuple (ss-name, pod_ordinal, hostname, domain)


def setup_logging():
    log_format = '%(asctime)s %(levelname)-8s %(filename)14s:%(lineno)-6d %(message)s'
    loglevel = logging.DEBUG
    console_loglevel = logging.DEBUG
    # got our voltdbroot?
    ssname, pod_ordinal, my_hostname, domain = get_hostname_tuple(get_fqhostname())
    # TODO: there could be multiple roots, for which this won't work
    plogfile = os.path.join(PV_VOLTDBROOT, "voltdbk8s.log")
    merge_logfile = ""
    try:
        merge_logfile = subprocess.check_output(shlex.split("find -L "+os.path.join(PV_VOLTDBROOT, ssname, 'voltdbroot')
                                                                           +" -type f -name 'volt.log'")).strip()
    except subprocess.CalledProcessError as e:
        merge_logfile = ""
    logger = logging.getLogger()
    logger.setLevel(logging.NOTSET)
    logger.propogate = True
    file = logging.FileHandler(plogfile, 'a')
    console = logging.StreamHandler()
    file.setLevel(loglevel)
    console.setLevel(console_loglevel)
    formatter = logging.Formatter(log_format)
    file.setFormatter(formatter)
    console.setFormatter(formatter)
    logging.getLogger('').handlers = []
    logging.getLogger('').addHandler(console)
    logging.getLogger('').addHandler(file)
    if merge_logfile:
        voltdblog = logging.FileHandler(merge_logfile, 'a')
        voltdblog.setFormatter(formatter)
        logging.getLogger('').addHandler(voltdblog)

    # print banner
    logging.info("VoltDB K8S CONTROLLER")
    logging.info("GMT is: " + strftime("%Y-%m-%d %H:%M:%S", gmtime()) +
                 " LOCALTIME is: " + strftime("%Y-%m-%d %H:%M:%S", localtime()) +
                 " TIMEZONE OFFSET is: " + str(timezone))
    logging.info("logging to: '%s'" % plogfile)
    if merge_logfile:
        logging.info("logging to: '%s'" % merge_logfile)
    logging.info("command line: %s" % ' '.join(sys.argv))
    for k, v in os.environ.items():
        logging.debug("environment: " + k + "=" + v)
    logging.debug(os.getcwd())


def hang():
    # launch as a subprocess
    sp = Popen(['sleep', '999999'], stderr=STDOUT)
    logging.info("maintenance mode sleep ... pid " + str(sp.pid))
    sys.stdout.flush()
    sys.stderr.flush()
    sp.wait()


def get_files_list(dir):
    # skip files starting with .., such as ..data, that k8s puts in configmaps
    files = [f for f in os.listdir(dir) if not f.startswith('..')]
    if len(files) > 1:
        plf = os.path.join(dir, '.loadorder')
        if os.path.exists(plf):
            with open(plf, 'r') as f:
                fl = f.readline().strip().split(',')
            fqpl = map(lambda x: os.path.join(dir, x), fl)
        else:
            fqpl = [ dir + "/*", ]
        return fqpl
    elif len(files) == 1:
        return [ os.path.join(dir, files[0]) ]
    return None


def main():
    # See if /voltdbroot (persistent storage mount) is exists
    if not os.path.exists(PV_VOLTDBROOT):
        logging.error("Persistent volume '%s' is not mounted!!!!" % PV_VOLTDBROOT)
        sys.exit(1)

    # setup loggers
    # log to the console and to a file on the PV
    setup_logging()

    # check that our args look like a voltdb start command line and only that
    if not sys.argv[2] == 'start':
        logging.error("WARNING: expected voltdb start command but found '%s'" % sys.argv[1])
        sys.exit(1)

    fqhostname = get_fqhostname()
    logging.info("HOSTNAME: " +fqhostname)

    # use the domain of the leader address to find other pods in our cluster
    hn = get_hostname_tuple(fqhostname)
    logging.debug(hn)

    ssname, pod_ordinal, my_hostname, domain = hn

    if len(hn) != 4 or not pod_ordinal.isdigit():
        logging.error("Hostname parse error, is this a statefulset pod?, cannot continue")
        sys.exit(1)
    pod_ordinal = int(pod_ordinal)

    din = find_arg_index(sys.argv, '-D,--dir')
    if din:
        logging.error("-D is not supported in the init or start command in k8s")
        sys.exit(1)

    # assets: mounted by config maps
    assets_dir = os.path.join(os.getenv('VOLTDB_INIT_VOLUME', '/etc/voltdb'))

    # we expect there to be a symlink to the voltdbroot parent directory with the statefulset name
    # in the PV root directory. The symlink is the cluster name and the same on every PV claim,
    # so there is a common dir name and a unique dir name on each PV claim.
    # Here we use the dir name prefix VDBRtttttt where t is the unixtime*16^7. You may use any valid dir name chars
    # ending in ".domain". The cluster-name symlink should point to the voltdb root dir.
    # In voltdbroot/cluster-name/voltdbroot/config there is path.properties
    # paths in this file are coerced to the simlink reference prior to forking voltdb.

    # vdb root dir symlink name
    working_voltdbroot = os.path.join(PV_VOLTDBROOT, ssname)

    # initialize the voltdbroot if necessary
    if not os.path.exists(os.path.join(working_voltdbroot, 'voltdbroot', '.initialized')):
        logging.info("Initializing a new voltdb database at '%s'" % working_voltdbroot)
        olddir = os.getcwd()
        os.chdir(PV_VOLTDBROOT)
        try:
            os.unlink(ssname)
        except:
            pass
        # generate a unique name for the voltdbroot
        #voltdbroot = fqhostname  # old way
        voltdbroot = "VDBR-" + str(pod_ordinal) + "-" + str(int(time()*1e6)) + "." + domain
        os.mkdir(voltdbroot)
        os.system("ls -l")
        os.symlink(voltdbroot, ssname)
        os.chdir(voltdbroot)
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
            classes_dir = os.path.join(assets_dir, "classes")
            l = get_files_list(classes_dir)
            logging.debug(l)
            if l is not None:
                cmd.append('--classes')
                cmd.append(','.join(l))
            schema_dir = os.path.join(assets_dir, "schema")
            l = get_files_list(schema_dir)
            logging.debug(l)
            if l is not None:
                cmd.append('--schema')
                cmd.append(','.join(l))
        extra_init_args = os.getenv('VOLTDB_INIT_ARGS')
        if extra_init_args:
            cmd.extend(str_to_arg_list(extra_init_args))
        logging.info("Init command: " + str(cmd))
        os.system("echo $PWD")
        os.system("ls -la")
        sp = Popen(cmd, shell=False) ###a, stderr=STDOUT)
        sp.wait()
        if sp.returncode != 0:
            logging.error("failed Initializing voltdb database at '%s' (did you forget --force?)" %
                  working_voltdbroot)
            sys.exit(1)
        logging.info("Initialize new voltdb succeeded!!!")
        # setup logging again, pointing to the new logfile in the voltdbroot
        setup_logging()
        os.chdir(olddir)

    os.system("find "+PV_VOLTDBROOT+" -ls")
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
            if ',' in args[ix]:
                li = args[ix].split(',')
                if len(li) < pod_ordinal:
                    logging.error("Treating '%s' as a pod_ordinal comma separated list but there appear to be"
                             " insufficient entries for host '%s'" % (oa, pod_ordinal))
                    sys.exit(1)
                args[ix] = li[pod_ordinal]

    # DNS discover pods in our cluster
    # nb. voltdb pods are registered when then are started, and therefore may not appear ready to k8s

    # find nodes which: 1) have the mesh port open; 2) respond to an admin query
    # nodes may be "published before they are ready to receive traffic"

    while True:
        cluster_pods = query_dns_srv(domain)
        if fqhostname in cluster_pods:
            # remove ourself
            cluster_pods.remove(fqhostname)
        cluster_pods_responding_mesh = []
        cluster_pods_up = []
        connect_hosts = [ssname + "-0." + domain]

        for host in cluster_pods:
            logging.info("Connecting to '%s'" % host)
            if try_to_connect(host, VOLTDB_INTERNAL_INTERFACE):
                cluster_pods_responding_mesh.append(host)
                # we may have found a running node, get voltdb SYSTEMINFORMATION
                if try_to_connect(host, VOLTDB_HTTP_PORT):
                    cluster_pods_up.append(host)
                    # sys_info = None
                    # try:
                    #     sys_info = get_system_information(host, VOLTDB_ADMIN_PORT)
                    # except:
                    #     pass
        logging.debug("database nodes up: %s" % cluster_pods_up)
        logging.debug("mesh ports responding: %s" % cluster_pods_responding_mesh)

        # if the database is up use all that are available
        if len(cluster_pods_up) > 0:
            connect_hosts = cluster_pods_up
            break

        # if the database is down
        # forming initial mesh we direct the connection request to host0
        # bring up pods in an orderly fashion, one at a time
        if len(cluster_pods_responding_mesh) >= pod_ordinal:
            break
        sleep(1)

    if os.path.isdir(assets_dir):
        license_file = os.path.join(assets_dir, 'license')
        if os.path.isfile(license_file):
            add_or_replace_arg(args, "-l,--license", license_file)
    add_or_replace_arg(args, "-D,--dir", working_voltdbroot, action="replace")
    add_or_replace_arg(args, "-H,--host", random.choice(connect_hosts))

    # fix path.properties voltdbroot path
    # ensure that all fq paths to voltdbroot use the ssname symlink
    res = "=(.*)/.+?\." + domain.replace('.','\.') +"/"
    cre = re.compile(res, flags=re.MULTILINE)
    with open('voltdbroot/config/path.properties', 'r') as f:
        lines = f.read()
        if len(lines) == 0:
            logging.error("path.properties is empty")
            os.system("find . -ls")
            #sleep(9999999)
            sys.exit(1)
        lines = cre.sub("=\g<1>/"+ssname+"/", lines)
        tfd, tmpfilepath = mkstemp(dir="voltdbroot/config")
        with os.fdopen(tfd, 'w') as f:
            f.write(lines)
    # need this to be atomic
    os.rename(tmpfilepath, "voltdbroot/config/path.properties")
    with open('voltdbroot/config/path.properties', 'r') as f:
        lines = f.read()

    # For maintenance mode, don't bring up the database just sleep indefinitely
    # nb. in maintenance mode, liveness checks will timeout if enabled
    if '--k8s-maintenance' in args:
        hang()
        #os.execv('tail' '-f', '/dev/null')

    # build the voltdb start command line
    logging.debug(os.getcwd())
    args = shlex.split(' '.join(args))

    logging.info("VoltDB cmd is '%s'" % ' '.join(args[1:]))
    logging.info("Starting VoltDB...")

    # flush so we see our output in k8s logs
    sys.stdout.flush()
    sys.stderr.flush()

    # fork voltdb
    d = os.path.dirname(args[0])
    os.execv(os.path.join(d, args[1]), args[1:])
    sys.exit(0)

if __name__ == "__main__":
    try:
        main()
    except:
        logging.error(format_exc())
        raise
