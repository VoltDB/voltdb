#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2018 VoltDB Inc.

# Author: Phil Rosegay

# VOLTDB K8S node startup controller
#
# Uses DNS SRV records to discover nodes and edit the VoltDB configuration when nodes startup.
# Copy "pristine" voltdbroot from docker image to persistent storage on first run.
#
# args are voltdb ... (ie. voltdb start command)
# Invoke from the container ENTRYPOINT ex.
#
#   voltdbk8s.py voltdb start <parms>

#
# voltdb start cmd with -H will be ignored, -H will be set appropriately.
#


import sys, os
import socket
import subprocess
import httplib2
from shutil import copytree


# mount point for persistent volume voltdbroot
PV_VOLTDBROOT = "/voltdbroot"


# TODO: need to implement internal-interface option (voltdb command) support
VOLTDB_INTERNAL_INTERFACE=3021
VOLTDB_HTTP_PORT = 8080


def query_dns_srv(query):
    m_list = []
    try:
        # SRV gives us records for each node in the cluster like ...
        # _service._proto.name.  TTL   class SRV priority weight port target.
        #nginx.default.svc.cluster.local    service = 10 100 0 voltdb-0.nginx.default.svc.cluster.local.
        # the fqdn is structured as ... headless-service-name.namespace.svc.cluster.local
        # this is similar to etcd
        answers = subprocess.check_output(("nslookup -type=SRV %s" % query).split(' ')).split('\n')[2:]
    except:
        return m_list
    for rdata in answers:
        if len(rdata):
            m_list.append(rdata.split(' ')[-1][:-1])  # drop the trailing '.'
            print rdata
    print m_list
    # return a list of fq hostnames of pods in the service domain
    return sorted(m_list)


def try_to_connect(host, port):
    s = socket.socket()
    try:
        s.connect((host, port))
        return True
    except Exception as e:
        print str(e)
        return False
    finally:
        s.close()


def http_get(url, host, port):
    # TODO: need to support clusters with externssl
    proto = "http"
    admin = "true"
    urlp = "%s://%s:%d/api/1.0/?%s&admin=%s" % (proto, host, port, url, admin)
    print urlp
    h = httplib2.Http(".cache")
    return h.request(urlp, "GET")


def get_system_information(host, port, section='OVERVIEW'):
    try:
        resource = "Procedure=@SystemInformation&Parameters=[\"" + section + "\"]"
        print resource
        resp_headers, content = http_get(resource, host, port)
        return resp_headers, content
    except:
        raise


def find_arg_index(args, arg):
    for i in range(0, len(args)):
        if args[i] == arg:
            return i+1
    return None


def fork_voltdb(host, voltdbroot):
    # before we fork over, see if /voltdbroot (persistent storage mount) is empty
    # if it is, copy our pristine voltdbroot from the image
    if not os.path.exists(PV_VOLTDBROOT):
        print "ERROR: '%s' is not mounted!!!!" % PV_VOLTDBROOT
        sys.exit(-1)
    working_voltdbroot = os.path.join(PV_VOLTDBROOT, voltdbroot)
    print os.listdir(PV_VOLTDBROOT)
    print working_voltdbroot
    if voltdbroot not in os.listdir(PV_VOLTDBROOT):
        # our docker image has pwd pointing to the pristine voltdbroot
        # TODO: this is ok for now (maybe) but when catalogs change we need to persist back to our image?
        pristine_root = os.getcwd()
        print "Copying pristine voltdbroot to '%s'" % working_voltdbroot
        copytree(pristine_root, working_voltdbroot, symlinks=True, ignore=None)
        os.chdir(PV_VOLTDBROOT)
        os.system("rm -f " + ssname)
        os.system("ln -sf " + voltdbroot + " " + ssname)
    os.chdir(working_voltdbroot)
    args = sys.argv[:]  # copy
    for i in range(0, len(args)):
        if args[i] == "-D" or args[i] == '--directory':
            args[i+1] = working_voltdbroot
    # run voltdb replacing current process
    hni = find_arg_index(args, '-H')
    if hni == 0:
        hni = len(args)
        args[hni] = "-H"
        hni += 1
    args[hni] = host
    print "VoltDB cmd is '%s'" % args[1:]
    # flush so we see our output in k8s logs
    sys.stdout.flush()
    sys.stderr.flush()
    d = os.path.dirname(args[0])
    os.execv(os.path.join(d, args[1]), args[1:])
    sys.exit(0)


def get_hostname_tuple(fqdn):
    hostname, domain = fqdn.split('.', 1)
    ssp = hostname.split('-')
    hn = ('-'.join(ssp[0:-1]), ssp[-1], hostname, domain)  # statefulset hostnames are podname-ordinal
    return hn   # returns a tuple (ss-name, ordinal, hostname, domain)


if __name__ == "__main__":

    print sys.argv
    print os.environ

    # check that our args look like a voltdb start command line and only that
    if not (sys.argv[1] == 'voltdb' and sys.argv[2] == 'start'):
        print "ERROR: expected voltdb start command but found '%s'" % sys.argv[1]
        sys.exit(-1)

    fqhostname = socket.getfqdn()
    print fqhostname

    # for maintenance mode, don't bring up the database just hang
    # nb. in maintenance mode, liveness checks will probably timeout if enabled
    if '--k8s-maintenance' in sys.argv:
        while True:
            from time import sleep
            sleep(10000)
        #os.execv('tail' '-f', '/dev/null')

    # use the domain of the leader address to find other pods in our cluster
    hn = get_hostname_tuple(fqhostname)
    print hn

    ssname, ordinal, my_hostname, domain = hn

    if len(hn) != 4 or not ordinal.isdigit():
        # TODO: need a better check for running with k8s statfulset???
        # we don't know what do with this, just fork
        fork_voltdb(ssname+"-0."+domain, None)

    # if there are some pods up in our cluster, connect to the first one we find
    # if we fail to form/rejoin/join a cluster, k8s will likely just restart the pod

    # get a list of fq hostnames of pods in the domain from DNS SRV records
    my_cluster_members = query_dns_srv(domain)

    din = find_arg_index(sys.argv, '-D')
    if din:
        voltdbroot = '.'.join(my_hostname, sys.argv[din])
    else:
        voltdbroot = fqhostname

    # nodes may be "published before they are ready to receive traffic"
    for host in my_cluster_members:
        print "Connecting to '%s'" % host
        if try_to_connect(host, VOLTDB_INTERNAL_INTERFACE):
            # we may have found a running node, get voltdb SYSTEMINFORMATION
            if try_to_connect(host, VOLTDB_HTTP_PORT):
                sys_info = None
                try:
                    sys_info = get_system_information(host, VOLTDB_HTTP_PORT)
                except:
                    raise
                print "sysinfo: " + str(sys_info)
            # try to connect to mesh
            fork_voltdb(host, voltdbroot)

    fork_voltdb(ssname+"-0."+domain, voltdbroot)
