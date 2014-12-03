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

import time
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from voltdbclient import *
from operator import itemgetter, attrgetter
import numpy as np

STATS_SERVER = 'volt2'

COLORS = ['b','g','c','m','k']

MARKERS = ['+', '*', '<', '>', '^', '_',
           'D', 'H', 'd', 'h', 'o', 'p']

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
        w = w == None and 2000 or w
        h = h == None and 1000 or h
        self.xmax = xmax
        self.xmin = xmin
        self.series = series

        self.fig = plt.figure(figsize=(w / self.DPI, h / self.DPI),
                         dpi=self.DPI)
        self.ax = self.fig.add_subplot(111)
        self.ax.set_title(title)
        plt.tick_params(axis='x', which='major', labelsize=16)
        plt.tick_params(axis='y', labelright=True, labelleft=False, labelsize=16)
        plt.grid(True)
        self.fig.autofmt_xdate()
        plt.ylabel(ylabel)
        plt.xlabel(xlabel)

    def plot(self, x, y, color, marker_shape, legend, linestyle):
        self.ax.plot(x, y, linestyle, label=legend, color=color,
                     marker=marker_shape, markerfacecolor=color, markersize=8)

    def close(self):
        x_formatter = matplotlib.dates.DateFormatter("%b %d %y")
        self.ax.xaxis.set_major_formatter(x_formatter)
        loc = matplotlib.dates.WeekdayLocator(byweekday=matplotlib.dates.MO, interval=1)
        self.ax.xaxis.set_major_locator(loc)
        self.ax.xaxis.set_minor_locator(matplotlib.ticker.AutoMinorLocator(n=7))
        y_formatter = matplotlib.ticker.ScalarFormatter(useOffset=False)
        self.ax.yaxis.set_major_formatter(y_formatter)
        ymin, ymax = plt.ylim()
        plt.xlim((self.xmin.toordinal(), (self.xmax+datetime.timedelta(1)).replace(minute=0, hour=0, second=0, microsecond=0).toordinal()))
        if self.series.startswith('lat'):
            lloc = 2
        else:
            lloc = 3
        plt.legend(prop={'size': 12}, loc=lloc)
        plt.savefig(self.filename, format="png", transparent=False,
                    bbox_inches="tight", pad_inches=0.2)
        plt.close('all')

def plot(title, xlabel, ylabel, filename, width, height, app, data, series, mindate, maxdate):
    global mc
    plot_data = dict()
    for run in data:
        if run['branch'] not in plot_data:
            plot_data[run['branch']] = {series: []}

        if series == 'tppn':
            value = run['tps']/run['nodes']
        else:
            value = run[series]

        datenum = matplotlib.dates.date2num(run['date'])
        plot_data[run['branch']][series].append((datenum,value))

    if len(plot_data) == 0:
        return

    pl = Plot(title, xlabel, ylabel, filename, width, height, mindate, maxdate, series)

    flag = dict()
    for b,bd in plot_data.items():
        for k,v in bd.items():
            if k not in flag.keys():
                flag[k] = []
            v = sorted(v, key=lambda x: x[0])
            u = zip(*v)
            if b not in mc:
                mc[b] = (COLORS[len(mc.keys())%len(COLORS)], MARKERS[len(mc.keys())%len(MARKERS)])
            pl.plot(u[0], u[1], mc[b][0], mc[b][1], b, '-')

            ma = [None]
            if b == 'master' and len(u[0]) >= 10:
                (ma,mstd) = moving_average(u[1], 10)
                pl.plot(u[0], ma, mc[b][0], None, None, ":")
                failed = 0
                if k.startswith('lat'):
                    polarity = 1
                    cv = np.nanmin(ma)
                    rp = (u[0][np.nanargmin(ma)], cv)
                    if b == 'master' and ma[-1] > cv * 1.05:
                        failed = 1
                else:
                    polarity = -1
                    cv = np.nanmax(ma)
                    rp = (u[0][np.nanargmax(ma)], cv)
                    if b == 'master' and ma[-1] < cv * 0.95:
                        failed = 1

                twosigma = np.sum([np.convolve(mstd, polarity*2), ma], axis=0)
                pl.plot(u[0], twosigma, mc[b][0], None, None, '-.')
                pl.ax.annotate(r"$2\sigma$", xy=(u[0][-1], twosigma[-1]), xycoords='data', xytext=(20,0), textcoords='offset points', ha='right')

                twntypercent = np.sum([np.convolve(ma, polarity*0.2), ma], axis=0)
                pl.plot(u[0], twntypercent, mc[b][0], None, None, '-.')
                pl.ax.annotate(r"20%", xy=(u[0][-1], twntypercent[-1]), xycoords='data', xytext=(20,0), textcoords='offset points', ha='right')

                p = (ma[-1]-rp[1])/rp[1]*100.

                if failed != 0:
                    if p<10:
                        color = 'yellow'
                    else:
                        color = 'red'
                    flag[k].append((b, p))
                    for pos in ['top', 'bottom', 'right', 'left']:
                        pl.ax.spines[pos].set_edgecolor(color)
                    pl.ax.set_axis_bgcolor(color)
                    pl.ax.set_alpha(0.2)

                pl.ax.annotate("%.2f" % cv, xy=rp, xycoords='data', xytext=(0,-10*polarity),
                    textcoords='offset points', ha='center')
                pl.ax.annotate("%.2f" % ma[-1], xy=(u[0][-1],ma[-1]), xycoords='data', xytext=(5,+5),
                    textcoords='offset points', ha='left')
                pl.ax.annotate("(%+.2f%%)" % p, xy=(u[0][-1],ma[-1]), xycoords='data', xytext=(5,-5),
                    textcoords='offset points', ha='left')

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
    for group, data in stats.iteritems():
        (app,nodes) = group
        app = app.replace('/','')

        conn = FastSerializer(STATS_SERVER, 21212)
        proc = VoltProcedure(conn, "@AdHoc", [FastSerializer.VOLTTYPE_STRING])
        resp = proc.call(["select chart_order, series, chart_heading, x_label, y_label from charts where appname = '%s' order by chart_order" % app])
        conn.close()

        app = app +" %d %s" % (nodes, ["node","nodes"][nodes>1])

        legend = { 1 : dict(series="lat95", heading="95tile latency",            xlabel="Time",      ylabel="Latency (ms)"),
                   2 : dict(series="lat99", heading="99tile latency",            xlabel="Time",      ylabel="Latency (ms)"),
                   3 : dict(series="tppn",  heading="avg throughput per node",    xlabel="Time",      ylabel="ops/sec per node")
                 }

        for r in resp.tables[0].tuples:
            legend[r[0]] = dict(series=r[1], heading=r[2], xlabel=r[3], ylabel=r[4])

        fns = [app]
        flags = dict()
        for r in legend.itervalues():
            title = app + " " + r['heading']
            fn = "_" + title.replace(" ","_") + ".png"
            fns.append(prefix + fn)
            f = plot(title, r['xlabel'], r['ylabel'], path + fn, width, height, app, data, r['series'], mindate, maxdate)
            flags.update(f)

        fns.append(iorder)
        fns.append(flags)
        filenames.append(tuple(fns))

    filenames.append(("KVBenchmark-five9s-latency", "", "", "http://ci/job/performance-nextrelease-5nines/lastSuccessfulBuild/artifact/pro/tests/apptests/savedlogs/5nines-histograms.png", iorder))
    filenames.append(("KVBenchmark-five9s-nofail-latency", "", "", "http://ci/job/performance-nextrelease-5nines-nofail/lastSuccessfulBuild/artifact/pro/tests/apptests/savedlogs/5nines-histograms.png", iorder))
    filenames.append(("KVBenchmark-five9s-nofail-nocl-latency", "", "", "http://ci/job/performance-nextrelease-5nines-nofail-nocl/lastSuccessfulBuild/artifact/pro/tests/apptests/savedlogs/5nines-histograms.png", iorder))

    # generate index file
    index_file = open(root_path + '-index.html', 'w')
    sorted_filenames = sorted(filenames, key=lambda f: f[0].lower()+str(f[1]))
    index_file.write(generate_index_file(sorted_filenames))
    index_file.close()

if __name__ == "__main__":
    main()
