#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2018 VoltDB Inc.

# Author: Phil Rosegay

# Basic readiness check for voltdb in k8s
# contact support

import sys
import os
import subprocess

cmd = """curl -sg http://localhost:8080/api/1.0/?Procedure=@PingPartitions\&Parameters=\[0\] \
        | jq '.status,.statusstring' \
        | xargs"""
r = subprocess.check_output(cmd, shell=True).strip()
if r[0] != "1":
        print r
        sys.exit(1)

sys.exit(0)
