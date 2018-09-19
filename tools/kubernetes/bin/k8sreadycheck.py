#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2018 VoltDB Inc.

# Author: Phil Rosegay

# Basic readiness check for voltdb in k8s
# contact support

import subprocess


def do_cmd(cmd):
    r = subprocess.check_output(cmd, shell=True).strip().replace('null', 'None')
    R = eval(r)
    return R


# subprocess.check_output was introduced in 2.7, let's be sure.
if sys.hexversion < 0x02070000:
    raise Exception("Python version 2.7 or greater is required.")

HOSTNAME = os.environ['HOSTNAME']

URL = "curl -sg http://localhost:8080/api/2.0/"

# Check PingPartitions verifies that transactions are being processed
# If the database is Paused, it will appear not-ready to k8s
cmd = URL+"""?Procedure=@PingPartitions\&Parameters=\[0\] \
        | jq -c '[.status,.statusstring]' """
r = do_cmd(cmd)
if r[0] != 1:
        print "Database is not processing transactions '%s'" % r[1]
        sys.exit(1)

# Check DRROLE for this host, STATE=='STOPPED' fail readiness
cmd = URL+"""?Procedure=@Statistics\&Parameters=["DRROLE"] | jq -c '[.results."0"[] | .ROLE,.STATE]'"""
r = do_cmd(cmd)
DR_ROLE = r[0]

# if Database Replication (DR) is not configured, skip the remaining DR checks
if DR_ROLE == "NONE":
        sys.exit(0)

# if DRROLE reports anything but ACTIVE, a DR exception has most likely occurred
if r[1] != "ACTIVE":
        print "Database replication has failed, DRROLE is '%s'" % r  # print error for k8s
        sys.exit(1)

# Check DRCONSUMER IS_COVERED for this host, any false responses fail readiness
cmd = URL+"""?Procedure=@Statistics\&Parameters=\["DRCONSUMER",0\] \
         | jq -c '[[.results][]."1"[] | select(.HOSTNAME | startswith("%s")) | select(.IS_COVERED == "false") | .REMOTE_CLUSTER_ID] | unique'""" % HOSTNAME
r = do_cmd(cmd)
if len(r) > 0:
        print "Database Replication Consumer has failed, some remote cluster partitions are not covered: '%s'" % r
        sys.exit(1)

# Check DRPRODUCER MODE for this host's consumer connections, if any "NOT_CONNECTED" for this host, fail readiness
cmd = URL+"""?Procedure=@Statistics\&Parameters=\["DRPRODUCER",0\] \
        | jq -c '[[.results][]."0"[] | select(.HOSTNAME | startswith("%s")) | select(.MODE == "NOT_CONNECTED") | .REMOTE_CLUSTER_ID] | unique'""" % HOSTNAME
r = do_cmd(cmd)
if len(r) > 0:
        print "Database Replication Producer connection to cluster-id(s) '%s' has failed." % r
        sys.exit(1)
