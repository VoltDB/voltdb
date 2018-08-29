#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2018 VoltDB Inc.

# Author: Phil Rosegay

# Basic readiness check for voltdb in k8s
# contact support

import subprocess
import sys, os
HOST = os.environ["HOSTNAME"]

# PingPartitions will transactionally check all partitions for availibility, it returns 0 on success.
# nb. the cluster will appear not-ready if the database is paused.
cmd = "curl -sg http://localhost:8080/api/1.0/?Procedure=@PingPartitions\&Parameters=\[0\] | jq '.status,.statusstring' | xargs"
r = subprocess.check_output(cmd, shell=True).strip()
if r != "1":
        print r
        sys.exit(1)
#sys.exit(0)

# DR PRODUCER ACTIVE: returns null (DR not configured), true (DR is ok), false (DR is down)
#cmd = """curl -sg http://localhost:8080/api/1.0/?Procedure=@Statistics\&Parameters=\['DRPRODUCER'\] | jq '.results[].data[] | [.[%s],.[%s]] | select(.[0] == "%s") | .[1]' """ % (HOSTNAME, ISSYNCED, HOST)
cmd = """curl -sg http://localhost:8080/api/1.0/?Procedure=@Statistics\&Parameters=\['DRPRODUCER'\] | jq '.results[].data[] | select(length == 10 and .[2] == "%s") | .[5]' """ % (HOST)
r = subprocess.check_output(cmd, shell=True).strip()
if r != '"ACTIVE"':
        print "Database replication is NOT syncing (%s)" % r
        sys.exit(1)

# everything is ok
sys.exit(0)
