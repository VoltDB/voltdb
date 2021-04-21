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
from voltdbclientpy2 import *
from operator import itemgetter, attrgetter
import numpy as np
import time
import re

STATS_SERVER = 'perfstatsdb.voltdb.lan'

COLORS = ['b','g','c','m','k']

MARKERS = ['+', '*', '<', '>', '^', '_',
           'D', 'H', 'd', 'h', 'o', 'p']

mc = {}

def getLatestKitBuildTag(conn,kit_pattern,appname_pattern) :
    if (kit_pattern == "latest" or kit_pattern == None) :
        sql="select kit_build_tag from app_stats where appname like '"+appname_pattern+"' order by date desc,appname limit 1"
    else :
        sql="select kit_build_tag from app_stats where kit_build_tag like '"+kit_pattern+"' and appname like '"+appname_pattern+"' order by date desc,appname limit 1";

    proc = VoltProcedure(conn, "@AdHoc",[FastSerializer.VOLTTYPE_STRING])
    resp = proc.call([sql])
    res=resp.tables[0].tuples[0][0]

    return res;


def get_stats(conn,build_tag) :
    """Get most recent run statistics of all apps within the last 'days'
    """

    proc = VoltProcedure(conn, "@AdHoc",[FastSerializer.VOLTTYPE_STRING])

    if ( build_tag == None ) :
        print("No build tag provided");
        return;
    else :
        sql="select appname,nodes,duration,date,branch,throughput as tps,kit_build_tag as build,lat95,lat99 from app_stats where kit_build_tag = '"+build_tag+"' order by date desc,appname"

    #print("sql:"+sql);
    resp = proc.call([sql])


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


def plotByWorkload(buckets,path,combograph=False,title='') :
   # build the graphs
    for build in buckets :
        location = 0;
        subplotlocations = {}
        sblocation = 0;
        # create a subplot for each ratio
        numcols = 1
        if ( combograph ):
            fig = plt.figure(figsize=(70,20))
            fig.suptitle(title,fontsize=10,fontweight='bold');
            numcols = len(buckets[build]);

        for workload in sorted(buckets[build]) :
            if ( not combograph) :
                fig = plt.figure(figsize=(9,7))
                fig.suptitle(title,fontsize=10);
                subplotlocations[workload] = 1;
            else :
                if workload in subplotlocations :
                    sblocation = subplotlocations[workload]
                else :
                    location = location + 1;
                    subplotlocations[workload] = location

            #print("Adding new subplot: ratio: %s build: %s at subplotlocations[ratio]" % (ratio,build));
            sb = plt.subplot(1,numcols,subplotlocations[workload]);
            #sb.set_color_cycle(COLORS);
            # these need to be applied AFTER the subplot is created.
            plt.xlabel("Zipfian Value",fontsize=20,fontweight='bold');
            plt.ylabel("Txns/Sec",fontsize=20,fontweight='bold');

            sb.set_title("Workload "+workload+": Throughput",fontsize=20,fontweight='bold');
            sb.grid(True);

            # stack a plot for each workload
            legendlist = []
            # sort the workloads:
            for ratio in sorted(buckets[build][workload]) :
                legendlist.append(ratio);
                statsraw = buckets[build][workload][ratio];
                stats = np.array(statsraw);
                statslist =  stats[stats[:,0].argsort()]

                x = statslist[:,0]
                y = statslist[:,1]
                if  len(y) <= 2 :
                    print("WARNING ratio %s: only has %s datapoints x/y: %s/%s" % (ratio,len(y),x,y));

                sb.plot(x,y, "-^", linewidth=3,label=ratio,solid_capstyle='round',solid_joinstyle='round',aa=True)
                # this will force it to use the specific x values, and not autoscale.
                sb.set_xticks([float(i) for i in x]);

            plt.legend(legendlist, loc='best')
            # Highest zipfian number should go first.
            sb.invert_xaxis();
            if ( not combograph) :
                savepath=path+"/"+build+"-workload-"+workload+".png"
                print("writing:"+savepath);
                fig.savefig(savepath);
                fig.clear();

        if ( combograph ) :
            savepath=path+"/"+build+"-workload.png"
            print("writing:"+savepath);
            fig.savefig(savepath);
            fig.clear();

        plt.close(fig);

    plt.close("all");

def usage():
    print "Usage:"
    print "\t", sys.argv[0], "output_dir [build-tag|latest] [master-tag|latest]"
    print "\t example: ./ycsb-graphs.py /tmp/test jenkins-kit-performance-rambranch-build-73 latest"
    print "\t example: ./ycsb-graphs.py /tmp/test latest latest"
    print "\t example: ./ycsb-graphs.py /tmp/test latest jenkins-kit-performance-ycsb-zipfian-build-4"
    print "if no master-tag is given, master stat's will not be included"
    print "if latest is used, it will grab the latest version from the stats database"

def main():
    build_tag = None;
    master_tag = None;

    if len(sys.argv) < 2:
        usage()
        exit(-1)

    path = sys.argv[1];
    if not os.path.exists(path):
        print path, " save path does not exist"
        exit(-1)

    if len(sys.argv) >= 3:
        build_tag = str(sys.argv[2])

    if len(sys.argv) >= 4:
        master_tag = str(sys.argv[3])

    conn = FastSerializer(STATS_SERVER,21212)

    latestAnticache=getLatestKitBuildTag(conn,build_tag,'YCSB-Anticache-%')
    print("workload kit:"+latestAnticache);
    (stats, mindate, maxdate) = get_stats(conn,latestAnticache)
    workBuckets = getBucketsByWorkload(stats,mindate,maxdate);

    if master_tag != None :
        latestMaster=getLatestKitBuildTag(conn,master_tag,'YCSB-%-master')
        print("master kit:"+latestMaster);
        (masterstats,mastermindate,mastermaxdate) = get_stats(conn,latestMaster)
        masterWorkBuckets = getBucketsByWorkload(masterstats,mindate,maxdate)


    conn.close();

    # we need to merge the workload and master buckets, add the master values to the workload bucket
    if master_tag != None :
        for build in workBuckets :
            for workload in workBuckets[build] :
                for mbuild in masterWorkBuckets :
                    if ( workload in masterWorkBuckets[mbuild] ) :
                        workBuckets[build][workload]["master"] = masterWorkBuckets[mbuild][workload]["master"]


        plotByWorkload(workBuckets,path,title=latestMaster+" vs "+ latestAnticache )
    else :
        plotByWorkload(workBuckets,path,title=latestAnticache )

def getBucketsByWorkload(stats,mindate,maxdate) :
    #root_path = path
    filenames = []              # (appname, latency, throughput)
    iorder = 0
    buckets = {};
    for data in stats:
        app = data["app"];
        app = app.replace('/','')
        # parse the appname so we can group them together: YCSB-Anticache-A-Z0.7-1:1
        workload_group= re.search(r"YCSB-Anticache-([A-Z]+)-Z(\d+\.\d+)-(\d:\d)",app);
        if ( workload_group == None) :
            #YCSB-A-Z0.7-master
            workload_group = re.search(r"YCSB-([A-Z]+)-Z(\d+\.\d+)",app);

        workload = workload_group.group(1);
        zipfian = workload_group.group(2);
        if ( len(workload_group.groups()) >= 3  ) :
            ratio = workload_group.group(3);
        else :
            ratio = "master"

        tps = data["tps"];
        branch = data["branch"];
        date = data["date"];
        build = data["build"];

        # put it in buckets
        stats = [zipfian,tps];
        statslist = [];
        # structure will be buckets[build][workload][ratio]
        if build in buckets :

            if workload in buckets[build] :
                if ratio in buckets[build][workload] :
                    statslist = buckets[build][workload][ratio]
                else :
                    buckets[build][workload][ratio] = {}
            else :
                buckets[build][workload] = {};
                buckets[build][workload][ratio] = {}
        else :
            buckets[build] = {};
            buckets[build][workload] = {};
            buckets[build][workload][ratio] = {};

        statslist.append(stats);

        buckets[build][workload][ratio] = statslist;

    return buckets;

if __name__ == "__main__":
    main()
