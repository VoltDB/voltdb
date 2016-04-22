#!/usr/bin/env python

# This is a visualizer which pulls TPC-C benchmark results from the MySQL
# databases and visualizes them. Four graphs will be generated, latency graph on
# sinigle node and multiple nodes, and throughput graph on single node and
# multiple nodes.
#
# Run it without any arguments to see what arguments are needed.

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))) +
                os.sep + 'tests/scripts/')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from voltdbclient import *
from operator import itemgetter, attrgetter
import numpy as np
import time
import re

STATS_SERVER = 'volt2'

COLORS = ['b','g','c','m','k']

MARKERS = ['+', '*', '<', '>', '^', '_',
           'D', 'H', 'd', 'h', 'o', 'p']

mc = {}

def get_stats(hostname, port) :
    """Get most recent run statistics of all apps within the last 'days'
    """

    conn = FastSerializer(hostname, port)
    proc = VoltProcedure(conn, "@AdHoc",[FastSerializer.VOLTTYPE_STRING])


    resp = proc.call(["select appname,nodes,duration,date,branch,throughput as tps,kit_build_tag as build,lat95,lat99 from app_stats where appname like 'YCSB-Anticache%' order by date desc,appname limit 1000"])

    conn.close()

    # keyed on app name, value is a list of runs sorted chronologically
    maxdate = datetime.datetime(1970,1,1,0,0,0)
    mindate = datetime.datetime(2038,1,19,0,0,0)
    stats = dict()
    run_stat_keys = ['app', 'nodes','duration','date','branch','tps','build']

    app_stats = [];
    for row in resp.tables[0].tuples:
        maxdate = max(maxdate, row[3])
        mindate = min(mindate, row[3])

        run_stats = dict(zip(run_stat_keys, row))
        app_stats.append(run_stats)

    return (app_stats, mindate, maxdate)


def usage():
    print "Usage:"
    print "\t", sys.argv[0], "output_dir [build-tag]" \
        " [width] [height]"
    print
    print "\t", "width in pixels"
    print "\t", "height in pixels"

def main():
    build_tag = None;
    width = None;
    height = None;

    if len(sys.argv) < 1:
        usage()
        exit(-1)

    path = sys.argv[1];
    if not os.path.exists(path):
        print path, "does not exist"
        exit(-1)

    if len(sys.argv) >= 3:
        build_tag = str(sys.argv[2])
    print(sys.argv);

    if len(sys.argv) >= 4:
        width = int(sys.argv[3])

    if len(sys.argv) >= 5:
        height = int(sys.argv[4])

    # show all the history
    (stats, mindate, maxdate) = get_stats(STATS_SERVER, 21212)

    root_path = path
    filenames = []              # (appname, latency, throughput)
    iorder = 0
    buckets = {};
    for data in stats:
        app = data["app"];
        app = app.replace('/','')
        # parse the appname so we can group them together: YCSB-Anticache-A-Z0.7-1:1
        workload_group= re.search(r"YCSB-Anticache-([A-Z]+)-Z(\d+\.\d+)-(\d:\d)",app);
        workload = workload_group.group(1);
        zipfian = workload_group.group(2);
        ratio = workload_group.group(3);
        tps = data["tps"];
        branch = data["branch"];
        date = data["date"];
        build = data["build"];

        # put it in buckets
        stats = [zipfian,tps];
        statslist = [];
        if build in buckets :
            if ratio in buckets[build] :
                if workload in buckets[build][ratio] :
                    statslist = buckets[build][ratio][workload]
                else :
                    buckets[build][ratio][workload] = {}
            else :
                buckets[build][ratio] = {};
                buckets[build][ratio][workload] = {}
        else :
            buckets[build] = {};
            buckets[build][ratio] = {};
            buckets[build][ratio][workload] = {};

        statslist.append(stats);

        buckets[build][ratio][workload] = statslist;


    # build the graphs
    for build in buckets :
        location = 0;
        subplotlocations = {}
        sblocation = 0;
        # create a subplot for each ratio
        numrows = len(buckets[build]);
        fig = plt.figure(figsize=(70,20))
        fig.suptitle('Zipfian Distribution',fontsize=28,fontweight='bold');
        # sort the ratio's first before we use them,

        for ratio in sorted(buckets[build]) :

            if ratio in subplotlocations :
                sblocation = subplotlocations[ratio]
            else :
                location = location + 1;
                subplotlocations[ratio] = location

            #print("Adding new subplot: ratio: %s build: %s at subplotlocations[ratio]" % (ratio,build));
            sb = plt.subplot(1,numrows,subplotlocations[ratio]);
            sb.invert_xaxis()
            # these need to be applied AFTER the subplot is created.
            plt.xlabel("Zipfian",fontsize=20);
            plt.ylabel("TPS",fontsize=20);

            sb.set_title("workload distribution: "+ratio);
            sb.grid(True);

            # stack a plot for each workload
            legendlist = []
            # sort the workloads:
            for workload in sorted(buckets[build][ratio]) :
                legendlist.append(workload);
                statsraw = buckets[build][ratio][workload];
                # sort them
                stats = np.array(statsraw);
                #statslist= stats[stats[:,0].argsort()[::-1]]
                statslist =  stats[stats[:,0].argsort()]

                x = statslist[:,0]
                #xsmooth = np.linspace(x.min(),x.max(),300)
                y = statslist[:,1]
                #ysmooth = spline(x,300,xnew

                sb.plot(x,y, "-", linewidth=10,label=workload,solid_capstyle='round',solid_joinstyle='round',aa=True)

            plt.legend(legendlist, loc='best')
        fig.savefig(build+".png");
        fig.clear();
        plt.close(fig);

    plt.close("all");

if __name__ == "__main__":
    main()
