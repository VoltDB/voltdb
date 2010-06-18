#!/usr/bin/env python

# This is a visualizer which pulls microbenchmark results from the MySQL
# databases and visualizes them. Four graphs will be generated per workload,
# latency graphs on single node and multiple nodes, and throughput graphs
# on single node and multiple nodes.
#
# Run it without any arguments to see what arguments are needed.

import sys
import os
import time
import datetime
import MySQLdb
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

def COLORS(k):
    return (((k ** 3) % 255) / 255.0,
            ((k * 100) % 255) / 255.0,
            ((k * k) % 255) / 255.0)

class Stat:
    def __init__(self, hostname, username, password, database):
        self.conn = MySQLdb.connect(host = hostname,
                                    user = username,
                                    passwd = password,
                                    db = database)
        self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

    def close(self):
        self.cursor.close()
        self.conn.close()

class LatencyStat(Stat):
    LATENCIES = """
SELECT startTime AS time, numHosts AS hosts, AVG(latencies) AS latency
FROM ma_instances AS runs
    JOIN ma_clientInstances AS clients ON clusterStartTime = startTime
    JOIN (SELECT instanceId, AVG(clusterRoundtripAvg) AS latencies
          FROM ma_clientProcedureStats
          GROUP BY instanceId) AS stats ON stats.instanceId = clientInstanceId
WHERE runs.startTime >= '%s'
    AND clients.applicationName = "Microbenchmark"
    AND clients.subApplicationName = "%s"
GROUP BY startTime
LIMIT %u
"""

    def get_latencies(self, workload_name, start_time, count):
        res = []
        latencies = {}

        self.cursor.execute(self.LATENCIES % (start_time, workload_name, count))
        res = list(self.cursor.fetchall())

        for i in res:
            i["time"] = datetime.date.fromtimestamp(i["time"] / 1000.0)

            key = (i["time"], i["hosts"])
            if i["latency"] == None:
                continue
            if key not in latencies \
                    or i["latency"] < latencies[key]["latency"]:
                latencies[key] = i

        return latencies.values()

class ThroughputStat(Stat):
    THROUGHPUT = """
SELECT resultid as id,
       hostcount as hosts,
       date(time) as time,
       avg(txnpersecond) as tps
FROM results
WHERE time >= '%s'
      AND benchmarkname = 'org.voltdb.benchmark.workloads.Generator'
      AND benchmarkoptions LIKE '%%workload=%s%%'
GROUP BY hostcount, date(time)
ORDER BY time DESC
LIMIT %u
"""

    def get_throughputs(self, workload_name, time, count):
        throughput_map = {}

        self.cursor.execute(self.THROUGHPUT % (time, workload_name, count))
        return list(self.cursor.fetchall())

class WorkloadNames(Stat):
    NAMES = """
SELECT DISTINCT subApplicationName as names
FROM ma_clientInstances
WHERE applicationName = 'Microbenchmark' AND subApplicationName != 'FirstWorkload'
"""

    def get_names(self):
        name_map = {}

        self.cursor.execute(self.NAMES)
        return list(self.cursor.fetchall())

class Plot:
    DPI = 100.0

    def __init__(self, title, xlabel, ylabel, filename, w, h):
        self.filename = filename
        self.legends = {}
        w = w == None and 800 or w
        h = h == None and 300 or h
        fig = plt.figure(figsize=(w / self.DPI, h / self.DPI),
                         dpi=self.DPI)
        self.ax = fig.add_subplot(111)
        self.ax.set_title(title)
        plt.ylabel(ylabel)
        plt.xlabel(xlabel)
        fig.autofmt_xdate()

    def plot(self, x, y, color, legend):
        self.ax.plot(x, y, linestyle="-", label=str(legend), marker="^",
                     markerfacecolor=color, markersize=10)

    def close(self):
        formatter = matplotlib.dates.DateFormatter("%b %d")
        self.ax.xaxis.set_major_formatter(formatter)
        plt.legend(loc=0)
        plt.savefig(self.filename, format="png", transparent=False,
                    bbox_inches="tight", pad_inches=0.2)

def parse_credentials(filename):
    credentials = {}
    fd = open(filename, "r")
    for i in fd:
        line = i.strip().split("?")
        credentials["hostname"] = line[0].split("/")[-2]
        db = line[0].split("/")[-1]
        pair = line[1].split("&")
        user = pair[0].strip("\\").split("=")
        password = pair[1].strip("\\").split("=")
        if user[1].startswith("monitor"):
            credentials["latency"] = {user[0]: user[1],
                                      password[0]: password[1],
                                      "database": db}
        else:
            credentials["throughput"] = {user[0]: user[1],
                                         password[0]: password[1],
                                         "database": db}
    fd.close()

    return credentials

def usage():
    print "Usage:"
    print "\t", sys.argv[0], "credential_file output_dir filename_base" \
        " [numDays] [width] [height] "
    print
    print "\t", "number of past days to take into account"
    print "\t", "width in pixels"
    print "\t", "height in pixels"

def main():
    if len(sys.argv) < 4:
        usage()
        exit(-1)

    if not os.path.exists(sys.argv[2]):
        print sys.argv[2], "does not exist"
        exit(-1)

    credentials = parse_credentials(sys.argv[1])
    path = os.path.join(sys.argv[2], sys.argv[3])
    numDays = 30
    width = None
    height = None
    if len(sys.argv) >= 5:
        numDays = int(sys.argv[4])
    if len(sys.argv) >= 6:
        width = int(sys.argv[5])
    if len(sys.argv) >= 7:
        height = int(sys.argv[6])

    workload_names = WorkloadNames(credentials["hostname"],
                                   credentials["latency"]["user"],
                                   credentials["latency"]["password"],
                                   credentials["latency"]["database"])
    latency_stat = LatencyStat(credentials["hostname"],
                               credentials["latency"]["user"],
                               credentials["latency"]["password"],
                               credentials["latency"]["database"])
    volt_stat = ThroughputStat(credentials["hostname"],
                               credentials["throughput"]["user"],
                               credentials["throughput"]["password"],
                               credentials["throughput"]["database"])

    timedelta = datetime.timedelta(days=numDays)
    starttime = datetime.datetime.now() - timedelta
    timestamp = time.mktime(starttime.timetuple()) * 1000.0

    names = workload_names.get_names()
    for n in names:
        name = n["names"]

        latencies = latency_stat.get_latencies(name, timestamp, 900)
        throughput = volt_stat.get_throughputs(name, starttime, 900)

        latency_map = {}
        latencies.sort(key=lambda x: x["time"])
        for v in latencies:
            if v["time"] == None or v["latency"] == None:
                continue
            if v["hosts"] not in latency_map:
                latency_map[v["hosts"]] = {"time": [], "latency": []}
            datenum = matplotlib.dates.date2num(v["time"])
            latency_map[v["hosts"]]["time"].append(datenum)
            latency_map[v["hosts"]]["latency"].append(v["latency"])

        if 1 in latency_map:
            pl = Plot("Average Latency on Single Node for Workload: " + name, "Time", "Latency (ms)",
                      path + "-latency-single-" + name + ".png",
                      width, height)
            v = latency_map.pop(1)
            pl.plot(v["time"], v["latency"], COLORS(1), 1)
            pl.close()

        if len(latency_map) > 0:
            pl = Plot("Average Latency for Workload: " + name, "Time", "Latency (ms)",
                      path + "-latency-" + name + ".png", width, height)
            for k in latency_map.iterkeys():
                v = latency_map[k]
                pl.plot(v["time"], v["latency"], COLORS(k), k)
            pl.close()

        throughput_map = {}
        throughput.sort(key=lambda x: x["id"])
        for v in throughput:
            if v["hosts"] not in throughput_map:
                throughput_map[v["hosts"]] = {"time": [], "tps": []}
            datenum = matplotlib.dates.date2num(v["time"])
            throughput_map[v["hosts"]]["time"].append(datenum)
            throughput_map[v["hosts"]]["tps"].append(v["tps"])

        if 1 in throughput_map:
            pl = Plot("Performance on Single Node for Workload: " + name, "Time", "Throughput (txns/sec)",
                      path + "-throughput-single-" + name + ".png",
                      width, height)
            v = throughput_map.pop(1)
            pl.plot(v["time"], v["tps"], COLORS(1), 1)
            pl.close()

        if len(throughput_map) > 0:
            pl = Plot("Performance for Workload: " + name, "Time", "Throughput (txns/sec)",
                      path + "-throughput-" + name + ".png", width, height)
            for k in throughput_map.iterkeys():
                v = throughput_map[k]
                pl.plot(v["time"], v["tps"], COLORS(k), k)
            pl.close()

    latency_stat.close()
    volt_stat.close()

if __name__ == "__main__":
    main()
