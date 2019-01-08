#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2018 VoltDB Inc.

# Author: Phil Rosegay

# Basic readiness check for voltdb in k8s
# contact support

import sys
import os
import subprocess


def do_cmd(cmd):
    r = subprocess.check_output(cmd, shell=True).strip().replace('null', 'None')
    R = eval(r)
    return R


# subprocess.check_output was introduced in 2.7, let's be sure.
if sys.hexversion < 0x02070000:
    raise Exception("Python version 2.7 or greater is required.")

HOSTNAME = os.environ['HOSTNAME'].split('.')[0]

URL = "curl -sg http://localhost:8080/api/2.0/"

RC = 0

# Check PingPartitions verifies that transactions are being processed
# If the database is Paused, it will appear not-ready to k8s
cmd = URL+"""?Procedure=@PingPartitions\&Parameters=\[0\] \
        | jq -c '[.status,.statusstring]' """
r = do_cmd(cmd)
if r[0] != 1:
        print "Database is not ready or not processing transactions, '%s'" % r[1]
        RC = 1

# Database replication checks:

# !!!WARNING!!! !!!WARNING!!! !!!WARNING!!!
# These checks will report the pod as not ready when Database Replication XDCR is in an error state.
# Consider the effects of not-ready on your applications' upstream micro-services, such as load balancers.
# DR will buffer replication to disk if a temporary connection failure occurs, and will resume where it
# off when the connection(s) are re-established under otherwise normal conditions.

# We have found that it is often not appropriate to ready-check DR, DR status should be monitored and
# alerted independently of k8s.

# Delete or comment the following line to enable DR readiness checks
sys.exit(RC)


# Check DRROLE for this host, STATE=='STOPPED' fail readiness
# returns one row for the local cluster
cmd = URL+"""?Procedure=@Statistics\&Parameters=["DRROLE"] | jq -c '[.results."0"[] | .ROLE,.STATE]'"""
r = do_cmd(cmd)
DR_ROLE = r[0]

# if DR is not configured, skip the remaining replication checks
if DR_ROLE == "NONE":
        sys.exit(RC)

# if DRROLE reports STOPPED, this means that a DR exception has occured
if r[1] == "STOPPED":
        print "Database replication has failed DRROLE: %s  STATUS: %s" % (r[0],r[1])  # print error for k8s
        RC = 1

# Check DRCONSUMER IS_COVERED for this host, any false responses fail readiness
cmd = URL+"""?Procedure=@Statistics\&Parameters=\["DRCONSUMER",0\] \
         | jq -c '[[.results][]."1"[] | select(.HOSTNAME | startswith("%s")) | select(.IS_COVERED == "false") | .REMOTE_CLUSTER_ID] | unique'""" % HOSTNAME
r = do_cmd(cmd)
if len(r) > 0:
        print "DB Replication Consumer has failed, some remote cluster partitions are not covered: '%s'" % r
        RC = 1

# Check DRPRODUCER MODE for this host's consumer connections, if any "NOT_CONNECTED" for this host, fail readiness
cmd = URL+"""?Procedure=@Statistics\&Parameters=\["DRPRODUCER",0\] \
        | jq -c '[[.results][]."0"[] | select(.HOSTNAME | startswith("%s")) | select(.MODE == "NOT_CONNECTED") | .REMOTE_CLUSTER_ID] | unique'""" % HOSTNAME
r = do_cmd(cmd)
if len(r) > 0:
        print "DB Replication Producer connection(s) to cluster-id(s) '%s' has failed." % r
        RC = 1
sys.exit(RC)
