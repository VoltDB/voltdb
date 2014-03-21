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
import numpy

STATS_SERVER = 'volt2'

def COLORS(k):
    return (((k ** 3) % 255) / 255.0,
            ((k * 100) % 255) / 255.0,
            ((k * k) % 255) / 255.0)

#COLORS = plt.cm.Spectral(numpy.linspace(0, 1, 10)).tolist()
COLORS = ['b','g','r','c','m','y','k']
#print COLORS

MARKERS = ['+', '*', '<', '>', '^', '_',
           'D', 'H', 'd', 'h', 'o', 'p']

mc = {}

def get_stats(hostname, port, days):
    """Get most recent run statistics of all apps within the last 'days'
    """

    conn = FastSerializer(hostname, port)
    proc = VoltProcedure(conn, 'CenterAverageOfPeriod',
                         [FastSerializer.VOLTTYPE_SMALLINT])
    resp = proc.call([days])
    conn.close()

    # keyed on app name, value is a list of runs sorted chronologically
    stats = dict()
    run_stat_keys = ['app', 'nodes', 'branch', 'date', 'tps', 'lat95', 'lat99']
    for row in resp.tables[0].tuples:
        group = (row[0],row[1])
        app_stats = []
        if group not in stats:
            stats[group] = app_stats
        else:
            app_stats = stats[group]
        run_stats = dict(zip(run_stat_keys, row))
        app_stats.append(run_stats)

    return stats

class Plot:
    DPI = 100.0

    def __init__(self, title, xlabel, ylabel, filename, w, h, ndays):
        self.filename = filename
        self.ndays = ndays
        self.legends = {}
        w = w == None and 1200 or w
        h = h == None and 400 or h
        self.fig = plt.figure(figsize=(w / self.DPI, h / self.DPI),
                         dpi=self.DPI)
        self.ax = self.fig.add_subplot(111)
        self.ax.set_title(title)
        plt.xticks(fontsize=10)
        plt.yticks(fontsize=10)
        plt.tick_params(axis='y', labelleft=True, labelright=True)
        plt.ylabel(ylabel, fontsize=10)
        plt.xlabel(xlabel, fontsize=10)
        self.fig.autofmt_xdate()

    def plot(self, x, y, color, marker_shape, legend):
        self.ax.plot(x, y, linestyle="-", label=str(legend), color=color,
                     marker=marker_shape, markerfacecolor=color, markersize=4)

    def close(self):
        x_formatter = matplotlib.dates.DateFormatter("%b %d %y")
        self.ax.xaxis.set_major_formatter(x_formatter)
        y_formatter = matplotlib.ticker.ScalarFormatter(useOffset=False)
        self.ax.yaxis.set_major_formatter(y_formatter)
        ymin, ymax = plt.ylim()
        plt.ylim((ymin-(ymax-ymin)*0.1, ymax+(ymax-ymin)*0.1))
        #xmax = datetime.datetime.today().toordinal()
        #plt.xlim((xmax-self.ndays, xmax))
        plt.legend(prop={'size': 10}, loc=2)
        plt.savefig(self.filename, format="png", transparent=False,
                    bbox_inches="tight", pad_inches=0.2)
        plt.close('all')

def plot(title, xlabel, ylabel, filename, width, height, app, data, series):
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

    pl = Plot(title, xlabel, ylabel, filename, width, height, 1)
    for b,bd in plot_data.items():
        for k,v in bd.items():
            v = sorted(v, key=lambda x: x[0])
            u = zip(*v)
            if b not in mc:
                mc[b] = (COLORS[len(mc.keys())%len(COLORS)], MARKERS[len(mc.keys())%len(MARKERS)])
            pl.plot(u[0], u[1], mc[b][0], mc[b][1], b)
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
        <td><a href=#%s>%s</a></td>
        <td><a href=#%s>%s</a></td>
        <td><a href=#%s>%s</a></td>
        <td><a href=#%s>%s</a></td>
    </tr>
"""

    h = map(lambda x:(x[0].replace(' ','%20'), x[0]), filenames)
    n = 4
    z = n-len(h)%n
    while z > 0 and z < n:
        h.append(('',''))
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
    stats = get_stats(STATS_SERVER, 21212, ndays)

    root_path = path
    filenames = []              # (appname, latency, throughput)
    iorder = 0
    for group, data in stats.iteritems():
        (app,nodes) = group

        conn = FastSerializer(STATS_SERVER, 21212)
        proc = VoltProcedure(conn, "@AdHoc", [FastSerializer.VOLTTYPE_STRING])
        resp = proc.call(["select series, chart_heading, x_label, y_label from charts where appname = '%s' order by chart_order" % app])
        conn.close()

        app = app +" %d %s" % (nodes, ["node","nodes"][nodes>1])

        if len(resp.tables[0].tuples) > 0:
            legend = resp.tables[0].tuples
        else:
            legend = [ ('lat95',    "avg latency95",             "Time",     "Latency (ms)"),
                       ('lat99',    "avg latency99",             "Time",     "latency (ms)"),
                       ('tppn',     "avg throughput per node",   "Time",     "TPS per node"),
                     ]

        app_filename = app.replace(' ', '_')
        fns = [app]
        for r in legend:
            title = app + " " + r[1]
            fn = "_" + title.replace(" ","_") + ".png"
            fns.append(prefix + fn)
            plot(title, r[2], r[3], path + fn, width, height, app, data, r[0])

        fns.append(iorder)
        filenames.append(tuple(fns))

    # generate index file
    index_file = open(root_path + '-index.html', 'w')
    sorted_filenames = sorted(filenames, key=lambda f: f[0].lower()+str(f[1]))
    index_file.write(generate_index_file(sorted_filenames))
    index_file.close()

if __name__ == "__main__":
    main()
