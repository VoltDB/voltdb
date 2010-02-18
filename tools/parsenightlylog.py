#!/usr/bin/env python

import os, re, sys

# Parse output from ant benchmarkcluster and output a java properties file.

bestsingle = 0.0
bestdouble = 0.0
bestcluster = 0.0
bestdozen = 0.0
clustersize = 0

logstring = "\d+ \[main\] INFO COMPILER - "
hostinfopattern = re.compile(logstring +
    "Catalog leader: volt.. hosts, sites (\d+), (\d+)")
benchmarkpattern = re.compile(logstring +
    "Path to catalog file:.*/([^/]+)\-ddl\.sql")
resultpattern = re.compile("Transactions per second: (\d+\.\d*)")
clientpattern = re.compile("               volt(.*):(.*):")

print "# benchmarkcluster results"
print "# benchmark-hosts-sites-clients-processes = transactions per second"

while True:
    line = sys.stdin.readline()
    if line == '':
        if bestsingle != 0.0:
            fd = os.open("bestsingle.properties", os.O_CREAT | os.O_WRONLY)
            os.write(fd, "YVALUE = " + str(bestsingle) + "\n")
            os.close(fd)
        if bestdouble != 0.0:
            fd = os.open("bestdouble.properties", os.O_CREAT | os.O_WRONLY)
            os.write(fd, "YVALUE = " + str(bestdouble) + "\n")
            os.close(fd)
        if bestcluster != 0.0:
            fd = os.open("bestcluster.properties", os.O_CREAT | os.O_WRONLY)
            os.write(fd, "YVALUE = " + str(bestcluster) + "\n")
            os.close(fd)
        if bestcluster != 0.0:
            fd = os.open("clustersize.properties", os.O_CREAT | os.O_WRONLY)
            os.write(fd, "YVALUE = " + str(clustersize) + "\n")
            os.close(fd)
        if bestdozen != 0.0:
            fd = os.open("bestdozen.properties", os.O_CREAT | os.O_WRONLY)
            os.write(fd, "YVALUE = " + str(bestdozen) + "\n")
            os.close(fd)
        exit(0)

    match = hostinfopattern.match(line)
    if match:
        hosts = int(match.group(1))
        sites = int(match.group(2))

    match = benchmarkpattern.match(line)
    if match:
        benchmark = match.group(1)

    match = resultpattern.match(line)
    if match:
        result = float(match.group(1))

    match = clientpattern.match(line)
    if match:
        clients = set()
        processes = set()
        # read the rest of the contiguous clients
        while match:
            clients.add(match.group(1))
            processes.add(match.group(2))
            line = sys.stdin.readline()
            if line == '': break # whoops, truncated file!  try to recover
            match = clientpattern.match(line)
        if (benchmark == "tpcc"):
            if (hosts == 1) and (result > bestsingle):
                bestsingle = result
            if (hosts == 2) and (result > bestdouble):
                bestdouble = result
            if (hosts >= 5) and (hosts <= 6) and (result > bestcluster):
                clustersize = hosts
                bestcluster = result
            if (hosts == 12) and (result > bestdozen):
                bestdozen = result
        # output properties
        print benchmark + "-" + str(hosts) + "h-" + str(sites) + "s-" + \
            str(len(clients)) + "c-" + str(len(processes)) + "p = " + \
            str(result)
        # prepare to parse next test run
        hosts = ""
        sites = ""
        benchmark = ""
        result = ""
