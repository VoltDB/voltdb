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
import csv

STATS_SERVER = 'volt2'

#COLORS = ['b','g','c','m','k']

# These are the "Tableau 20" colors as RGB.
COLORS = [(31, 119, 180), (174, 199, 232), (255, 127, 14), (255, 187, 120),
             (44, 160, 44), (152, 223, 138), (214, 39, 40), (255, 152, 150),
             (148, 103, 189), (197, 176, 213), (140, 86, 75), (196, 156, 148),
             (227, 119, 194), (247, 182, 210), (127, 127, 127), (199, 199, 199),
             (188, 189, 34), (219, 219, 141), (23, 190, 207), (158, 218, 229)]
# Scale the RGB values to the [0, 1] range, which is the format matplotlib accepts.
for i in range(len(COLORS)):
    r, g, b = COLORS[i]
    COLORS[i] = (r / 255., g / 255., b / 255.)

MARKERS = ['+', '*', '<', '>', '^', '_',
           'D', 'H', 'd', 'h', 'o', 'p']

WIDTH=1600
HEIGHT=840
APX=80
APY=10

mc = {}

def get_stats(hostname, port, days):
    """Get most recent run statistics of all apps within the last 'days'
    """

    conn = FastSerializer(hostname, port)
    proc = VoltProcedure(conn, 'AverageOfPeriod',
                         [FastSerializer.VOLTTYPE_SMALLINT])
    resp = proc.call([days])
    conn.close()

    # keyed on app name, value is a list of runs sorted chronologically
    maxdate = datetime.datetime(1970,1,1,0,0,0)
    mindate = datetime.datetime(2038,1,19,0,0,0)
    stats = dict()
    run_stat_keys = ['app', 'nodes', 'branch', 'date', 'tps', 'lat95', 'lat99']
    #print resp
    for row in resp.tables[0].tuples:
        group = (row[0],row[1])
        app_stats = []
        maxdate = max(maxdate, row[3])
        mindate = min(mindate, row[3])
        if group not in stats:
            stats[group] = app_stats
        else:
            app_stats = stats[group]
        run_stats = dict(zip(run_stat_keys, row))
        app_stats.append(run_stats)

    return (stats, mindate, maxdate)

class Plot:
    DPI = 100.0

    def __init__(self, title, xlabel, ylabel, filename, w, h, xmin, xmax, series):
        self.filename = filename
        self.legends = {}
        w = w == None and WIDTH or w
        h = h == None and HEIGHT or h
        self.xmax = xmax
        self.xmin = xmin
        self.series = series
        self.title = title

        self.fig = plt.figure(figsize=(w / self.DPI, h / self.DPI),
                         dpi=self.DPI)
        self.ax = self.fig.add_subplot(111)
        self.ax.set_title(title)
        plt.tick_params(axis='x', which='major', labelsize=16)
        plt.tick_params(axis='y', labelright=True, labelleft=False, labelsize=16)
        plt.Locator.MAXTICKS=2000
        plt.grid(True, color='black', alpha=0.5)
        self.fig.autofmt_xdate()
        plt.ylabel(ylabel)
        plt.xlabel(xlabel)

    def plot(self, x, y, color, marker_shape, legend, linestyle):
        self.ax.plot(x, y, linestyle, label=legend, color=color,
                     marker=marker_shape, markerfacecolor=color, markersize=8)

    def close(self):
        plt.axvline(x=datetime.datetime(2016,1,11,12,00,0),color='black')
        x_formatter = matplotlib.dates.DateFormatter("%b %d %y")
        self.ax.xaxis.set_major_formatter(x_formatter)
        xmin, xmax = plt.xlim()
        if (self.xmax - self.xmin).days >= 365:
            l = 13
            loc = matplotlib.dates.WeekdayLocator(byweekday=matplotlib.dates.MO, interval=13)
            minloc = None
        else:
            l = 7
            loc = matplotlib.dates.WeekdayLocator(byweekday=matplotlib.dates.MO, interval=1)
            minloc = matplotlib.ticker.AutoMinorLocator(n=l)
        if loc:
            self.ax.xaxis.set_major_locator(loc)
        if minloc:
            self.ax.xaxis.set_minor_locator(minloc)
        y_formatter = matplotlib.ticker.ScalarFormatter(useOffset=False)
        self.ax.yaxis.set_major_formatter(y_formatter)
        ymin, ymax = plt.ylim()
        plt.xlim((self.xmin.toordinal(), (self.xmax+datetime.timedelta(1)).replace(minute=0, hour=0, second=0, microsecond=0).toordinal()))
        if self.series.startswith('lat'):
            lloc = 2
        else:
            lloc = 3
        plt.legend(prop={'size': 10}, loc=lloc)
        plt.savefig(self.filename, format="png", transparent=False, bbox_inches="tight") #, pad_inches=0.2)
        plt.close('all')

def plot(title, xlabel, ylabel, filename, width, height, app, data, series, mindate, maxdate, polarity, analyze):
    global mc
    plot_data = dict()

    for run in data:
        if run['branch'] not in plot_data:
            plot_data[run['branch']] = {series: []}

        if series == 'tppn':
            value = run['tps']/run['nodes']
        else:
            value = run[series]

        if value != 0.0:
            datenum = matplotlib.dates.date2num(run['date'])
            plot_data[run['branch']][series].append((datenum,value))

    if len(plot_data) == 0:
        return

    runs = 0
    for run in plot_data.itervalues():
        runs += len(run.values()[0])

    if runs == 0:
        pl = Plot(title, xlabel, ylabel, filename, width, height, mindate, maxdate, series)
        pl.ax.annotate("Intentionally blank", xy=(.5, .5), xycoords='axes fraction',
                        horizontalalignment='center', verticalalignment='center')
        pl.close()
        return

    # increase the figure size to allow for annotations
    branches_sort = sorted(plot_data.keys())
    height = (height or HEIGHT) + APY * len(branches_sort)

    pl = Plot(title, xlabel, ylabel, filename, width, height, mindate, maxdate, series)

    flag = dict()
    try:
        # may not have a master branch for this chart
        branches_sort.remove('master')
    except:
        print "WARN: has no master: %s" % title
        return
    branches_master_first = ['master'] + branches_sort
    with open("vis_stats.txt", "wb") as f:
        writer = csv.writer(f)
        z = dict()
        bn = 0
        for b in branches_master_first:
            bd = plot_data[b]
            bn += 1
            for k,v in bd.items():
                if k not in flag.keys():
                    flag[k] = []
                v = sorted(v, key=lambda x: x[0])
                u = zip(*v)
                if b not in mc:
                    mc[b] = (COLORS[len(mc.keys())%len(COLORS)], MARKERS[len(mc.keys())%len(MARKERS)])
                if not u:
                    continue
                pl.plot(u[0], u[1], mc[b][0], mc[b][1], b, '-')

                iref = None
                ma = [None]
                if b == 'master':
                    # find the index in u for our reference measurement
                    # on on master referenced from end of list
                    iref = -1
                    """
                    for i in range(-1, 0-len(v),-1):
                        date,measurement = v[i]
                        # C5 last run on volt3's 735973.874306 2016-01-10 20:59:00+00:00
                        #print type(date), date, measurement
                        if str(date) == "735973.874306":
                            iref = i
                            #print matplotlib.dates.num2date(date),date,measurement,i
                    """
                    if iref is None:
                        print 'reference not found for '+ title
                    # compute and saved master's average, median, std on raw data
                    master = dict(avg=np.average(u[1]), med=np.median(u[1]), std=np.std(u[1]))
                    # start building a tuple with reference : testname, lastval, ma[iref], mstd[iref]
                    # moving avg components only present if we have enough data items to compute the series
                    if iref:
                        analyze.append([b, title, u[1][iref], None, None])
                    else:
                        analyze.append([b, title, None, None, None])
                    if len(u[0]) >= 10:
                        (ma,mstd) = moving_average(u[1], 10)
                        if iref and ma[iref]:
                            # add ma[iref] to the reference tuple
                            analyze[-1][3] = ma[iref]
                            analyze[-1][4] = mstd[iref]
                        pl.plot(u[0], ma, mc[b][0], None, None, ":")
                        median = np.median(u[1]) # was (ma)
                        failed = 0
                        if polarity==1:
                            # increasing is bad
                            cv = np.nanmin(ma)
                            rp = (u[0][np.nanargmin(ma)], cv)
                            if b == 'master' and ma[-1] > median * 1.05:
                                failed = 1
                        else:
                            # decreasing is bad
                            cv = np.nanmax(ma)
                            rp = (u[0][np.nanargmax(ma)], cv)
                            if b == 'master' and ma[-1] < median * 0.95:
                                failed = 1

                        twosigma = np.sum([np.convolve(mstd, polarity*2), ma], axis=0)
                        pl.plot(u[0], twosigma, mc[b][0], None, None, '-.')
                        pl.ax.annotate(r"$2\sigma$", xy=(u[0][-1], twosigma[-1]), xycoords='data', xytext=(20,0), textcoords='offset points', ha='right', color=mc[b][0], alpha=0.5)

                        twntypercent = np.sum([np.convolve(ma, polarity*0.2), ma], axis=0)
                        pl.plot(u[0], twntypercent, mc[b][0], None, None, '-.')
                        pl.ax.annotate(r"20%", xy=(u[0][-1], twntypercent[-1]), xycoords='data', xytext=(20,0), textcoords='offset points', ha='right', color=mc[b][0], alpha=0.5)

                        p = (ma[-1]-rp[1])/rp[1]*100.    #pct diff min/max
                        q = (ma[-1]-median)/median*100.  #pct diff median

                        if failed != 0:
                            if abs(p) < 10:
                                color = 'yellow'
                            else:
                                color = 'red'
                            flag[k].append((b, p))
                            for pos in ['top', 'bottom', 'right', 'left']:
                                pl.ax.spines[pos].set_edgecolor(color)
                            pl.ax.set_axis_bgcolor(color)
                            pl.ax.patch.set_alpha(0.1)

                        # annotate value of the best point on master aka "reference point"
                        pl.ax.annotate("%.2f" % cv, xy=rp, xycoords='data', xytext=(0,-10*polarity),
                            textcoords='offset points', ha='center', color=mc[b][0], alpha=0.5)
                        # annotate value and percent vs reference point of most recent moving average on master
                        pl.ax.annotate("%.2f" % ma[-1], xy=(u[0][-1],ma[-1]), xycoords='data', xytext=(5,+5),
                            textcoords='offset points', ha='left', alpha=0.5)
                        pl.ax.annotate("(%+.2f%%)" % p, xy=(u[0][-1],ma[-1]), xycoords='data', xytext=(5,-5),
                            textcoords='offset points', ha='left', alpha=0.5)

                        master.update(ma=ma[-1], median=median, pctmed=q, cv=cv, pctmm=p, mstd=mstd[-1])
                        pl.ax.annotate(mc[b][1] + ' master: n: %d avg: %.2f med: %.2f sdev: %.2f (%.2f%%) ma: %.2f ma-med: %.2f (%+.2f%%) ma-best: %.2f (%+.2f%%) ma-std: %.2f' % (
                                            len(u[1]), master["avg"], master["med"], master["std"], master["std"]/master["avg"]*100., master["ma"], median, q, cv, p, mstd[-1]),
                                            xy=(APX,APY*bn), xycoords='figure points', horizontalalignment='left', verticalalignment='top', color=mc[b][0], fontsize=10, alpha=1.0)
                    else:
                        master.update(ma=None, median=None, pctmed=None, cv=None, pctmm=None, mstd=None)
                        pl.ax.annotate(mc[b][1] + ' master: n: %d avg: %.2f med: %.2f sdev: %.2f (%.2f%%)' % (
                            len(u[1]), master["avg"], master["med"], master["std"], master["std"]/master["avg"]*100.),
                            xy=(APX,APY*bn), xycoords='figure points', horizontalalignment='left', verticalalignment='top', color=mc[b][0], fontsize=10, alpha=1.0)
                    visstats = master
                    """"
                    # analyze master data
                    d = analyze[-1]
                    if d[1] == title:
                        if d[3]: # ma values are present
                            # compute number of stdev's its off by
                            #nb polarity==1 increasing is bad
                            # diff of mvavg[iref] and latest as nstdev
                            d.extend([u[1][-1], (d[2]-u[1][-1])/d[3]*polarity])
                        else:
                            d.extend([u[1][-1], None])
                    else:
                        d.extend([title, None, None, None, u[1][-1], None])
                    """
                else:
                    if len(u[0]) >= 10:
                        (ma,mstd) = moving_average(u[1], 10)
                        pl.plot(u[0], ma, mc[b][0], None, None, ":")
                    if len(analyze[-1]) > 5:
                        # copy master stats for this branch
                        analyze.append(analyze[-1][0:5])
                    # set the branch name
                    analyze[-1][0] = b
                    branch = [np.average(u[1]), np.median(u[1]), np.std(u[1]), None, None, None, None, None]
                    nstdv = float("NaN")
                    if master["ma"]:
                        nstdv = (master["ma"] - branch[1]) / master["mstd"] * polarity
                    pl.ax.annotate(mc[b][1] + ' %s: n: %d avg: %.2f med: %.2f sdev: %.2f (%.2f%%) no-std-master-ma: %.2f' % (b, len(u[1]), branch[0], branch[1], branch[2], branch[2]/branch[0]*100., nstdv),
                        xy=(APX,APY*bn), xycoords='figure points', horizontalalignment='left', verticalalignment='top', color=mc[b][0], fontsize=10, alpha=1.0)

                    # analyze branch data
                    #if b == 'pr160117_perf_check_before_geo_merge_notest':
                    if True or b == 'pr160123_perf_volt16s_notest':
                        d = analyze[-1]
                        if d[1] == title:
                            if d[4] >= 0:
                                # compute number of stdev's its off by
                                #nb polarity==1 increasing is bad
                                # d[0] is branch name
                                # d[1] is chart title
                                # d[2] is master at ref
                                # d[3] is master-ma at ref
                                # d[4] is master ma-std at ref
                                # d[5] is branch last
                                # d[6] is branch[last]- d[3]  / d[4] ie. no. of std deviations relative to master-ma at ref
                                # d[7] is branch[last] - d[3] / d[3] ie. pct relative to master-ma at ref
                                d.extend([u[1][-1], (u[1][-1]-d[3])/d[4]*polarity, (u[1][-1]-d[3])/d[3]*100.*polarity])
                            else:
                                d.extend([u[1][-1], None, None])
                        else:
                            d.extend([title, None, None, None, u[1][-1], None, None])


                """
                #pl.ax.annotate(b, xy=(u[0][-1],u[1][-1]), xycoords='data',
                #        xytext=(0, 0), textcoords='offset points') #, arrowprops=dict(arrowstyle="->"))
                x = u[0][-1]
                y = u[1][-1]
                pl.ax.annotate(str(y), xy=(x,y), xycoords='data', xytext=(5,0),
                    textcoords='offset points', ha='left')
                xmin, ymin = [(u[0][i],y) for i,y in enumerate(u[1]) if y == min(u[1])][-1]
                xmax, ymax= [(u[0][i],y) for i,y in enumerate(u[1]) if y == max(u[1])][-1]
                if ymax != ymin:
                    if xmax != x:
                        pl.ax.annotate(str(ymax), xy=(xmax,ymax),
                            textcoords='offset points', ha='center', va='bottom', xytext=(0,5))
                    if xmin != x:
                        pl.ax.annotate(str(ymin), xy=(xmin,ymin),
                            textcoords='offset points', ha='center', va='top', xytext=(0,-5))
                """

        pl.ax.annotate(datetime.datetime.today().strftime("%Y/%m/%d %H:%M:%S"), xy=(.08, .95),
                xycoords='figure fraction', horizontalalignment='left', verticalalignment='top',
                fontsize=8)

    pl.close()
    return flag

def generate_index_file(filenames):
    row = """
      <tr>
        <td><a href="%s"><img src="%s" width="400" height="200"/></a></td>
        <td><a href="%s"><img src="%s" width="400" height="200"/></a></td>
        <td><a href="%s"><img src="%s" width="400" height="200"/></a></td>
      </tr>
"""

    sep = """
     </table>
     <table frame="box">
     <tr>
         <th colspan="3"><a name="%s">%s</a></th>
     </tr>
"""

    full_content = """
<html>
  <head>
    <title>Performance Graphs</title>
  </head>
  <body>
    Generated on %s
    <table frame="box">
%s
    </table>
  </body>
</html>
"""

    hrow = """
    <tr>
        <td %s><a href=#%s>%s</a></td>
        <td %s><a href=#%s>%s</a></td>
        <td %s><a href=#%s>%s</a></td>
        <td %s><a href=#%s>%s</a></td>
    </tr>
"""
    #h = map(lambda x:(x[0].replace(' ','%20'), x[0]), filenames)
    h = []
    for x in filenames:
        tdattr = "<span></span>" #"bgcolor=green"
        tdnote = ""
        M = 0.0
        if len(x) == 6:
            for v in x[5].values():
                if len(v) > 0:
                    M = max(M, abs(v[0][1]))
        if M > 0.0:
            tdattr = '<span style="color:yellow">&#9658;</span>'
            if M > 10.0:
                tdattr = '<span style="color:red">&#9658;</span>'
            tdnote = " (by %.2f%%)" % M
        h.append(("", x[0].replace(' ','%20'), tdattr + x[0] + tdnote))
    n = 4
    z = n-len(h)%n
    while z > 0 and z < n:
        h.append(('','',''))
        z -= 1

    rows = []
    t = ()
    for i in range(1, len(h)+1):
        t += tuple(h[i-1])
        if i%n == 0:
            rows.append(hrow % t)
            t = ()

    last_app = None
    for i in filenames:
        if i[0] != last_app:
            rows.append(sep % (i[0], i[0]))
            last_app = i[0]
        rows.append(row % (i[1], i[1], i[2], i[2], i[3], i[3]))

    return full_content % (time.strftime("%Y/%m/%d %H:%M:%S"), ''.join(rows))

def moving_average(x, n, type='simple'):
    """
    compute an n period moving average.

    type is 'simple' | 'exponential'

    """
    x = np.asarray(x)
    if type=='simple':
        weights = np.ones(n)
    else:
        weights = np.exp(np.linspace(-1., 0., n))

    weights /= weights.sum()

    a =  np.convolve(x, weights, mode='full')[:len(x)]
    a[:n-1] = None

    s = [float('NaN')]*(n-1)
    for d in range(n, len(x)+1):
        s.append(np.std(x[d-n:d]))
    return (a,s)


def usage():
    print "Usage:"
    print "\t", sys.argv[0], "output_dir filename_base [ndays]" \
        " [width] [height]"
    print
    print "\t", "width in pixels"
    print "\t", "height in pixels"

def main():
    if len(sys.argv) < 3:
        usage()
        exit(-1)

    if not os.path.exists(sys.argv[1]):
        print sys.argv[1], "does not exist"
        exit(-1)

    prefix = sys.argv[2]
    path = os.path.join(sys.argv[1], sys.argv[2])
    ndays = 2000
    if len(sys.argv) >=4:
        ndays = int(sys.argv[3])
    width = None
    height = None
    if len(sys.argv) >= 5:
        width = int(sys.argv[4])
    if len(sys.argv) >= 6:
        height = int(sys.argv[5])

    # show all the history
    (stats, mindate, maxdate) = get_stats(STATS_SERVER, 21212, ndays)
    mindate = (mindate).replace(hour=0, minute=0, second=0, microsecond=0)
    maxdate = (maxdate + datetime.timedelta(days=1)).replace(minute=0, hour=0, second=0, microsecond=0)

    root_path = path
    filenames = []              # (appname, latency, throughput)
    iorder = 0

    analyze = []

    for group, data in stats.iteritems():
        (app,nodes) = group
        app = app.replace('/','')
        #print app
        #if app.startswith("YCSB-Anticache-"):
        #    continue

        conn = FastSerializer(STATS_SERVER, 21212)
        proc = VoltProcedure(conn, "@AdHoc", [FastSerializer.VOLTTYPE_STRING])
        resp = proc.call(["select chart_order, series, chart_heading, x_label, y_label, polarity from charts where appname = '%s' order by chart_order" % app])
        conn.close()

        app = app +" %d %s" % (nodes, ["node","nodes"][nodes>1])

        #chart polarity: -1 for tps (decreasing is bad), 1 for latencies (increasing is bad)

        legend = { 1 : dict(series="lat95", heading="95tile latency",            xlabel="Time",      ylabel="Latency (ms)",      polarity=1),
                   2 : dict(series="lat99", heading="99tile latency",            xlabel="Time",      ylabel="Latency (ms)",      polarity=1),
                   3 : dict(series="tppn",  heading="avg throughput per node",    xlabel="Time",      ylabel="ops/sec per node", polarity=-1)
                 }

        for r in resp.tables[0].tuples:
            legend[r[0]] = dict(series=r[1], heading=r[2], xlabel=r[3], ylabel=r[4], polarity=r[5])

        fns = [app]
        flags = dict()
        for r in legend.itervalues():
            title = app + " " + r['heading']
            #print title
            fn = "_" + title.replace(" ","_") + ".png"
            fns.append(prefix + fn)
            z = len(analyze)
            f = plot(title, r['xlabel'], r['ylabel'], path + fn, width, height, app, data, r['series'], mindate, maxdate, r['polarity'], analyze)
            try:
                if len(analyze[-1]) < 7:
                    analyze[-1].extend([None]*(7-len(analyze[-1])))
            except (IndexError):
                pass
            #print analyze[z : -1]  # print rows added for last chart
            if f:
                flags.update(f)

        fns.append(iorder)
        fns.append(flags)
        filenames.append(tuple(fns))

    fa = open(root_path + "_analyze.csv", "wb")
    writer = csv.writer(fa)
    header = "branch,chart,master-value-at-ref,master-ma-at-ref,master-mstd-at-ref,branch-value-last,no-stdev-vs-master(neg=worse),pct-vs-master(neg=worse)"
    writer.writerows(sorted(analyze, key=lambda x: (x[6] or 99999999.)))
    fa.close()

    #filenames.append(("KVBenchmark-five9s-latency", "", "", "http://ci/job/performance-nextrelease-5nines/lastSuccessfulBuild/artifact/pro/tests/apptests/savedlogs/5nines-histograms.png", iorder))
    #filenames.append(("KVBenchmark-five9s-nofail-latency", "", "", "http://ci/job/performance-nextrelease-5nines-nofail/lastSuccessfulBuild/artifact/pro/tests/apptests/savedlogs/5nines-histograms.png", iorder))
    filenames.append(("KVBenchmark-five9s-nofail-nocl-latency", "", "", "http://ci/job/performance-nextrelease-5nines-nofail-nocl/lastSuccessfulBuild/artifact/pro/tests/apptests/savedlogs/5nines-histograms.png", iorder))
    filenames.append(("KVBenchmark-five9s-nofail-nocl-kvm-latency", "", "", "http://ci/job/performance-nextrelease-5nines-nofail-nocl-kvm/lastSuccessfulBuild/artifact/pro/tests/apptests/savedlogs/5nines-histograms.png", iorder))
    filenames.append(("Openet-Shocker-three9s-latency", "", "", "http://ci/job/performance-nextrelease-shocker/lastSuccessfulBuild/artifact/pro/tests/apptests/savedlogs/3nines-histograms.png", iorder))

    # generate index file
    index_file = open(root_path + '-index.html', 'w')
    sorted_filenames = sorted(filenames, key=lambda f: f[0].lower()+str(f[1]))
    index_file.write(generate_index_file(sorted_filenames))
    index_file.close()

if __name__ == "__main__":
    main()
