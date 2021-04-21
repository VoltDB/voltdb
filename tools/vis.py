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
from voltdbclientpy2 import *

STATS_SERVER = 'perfstatsdb.voltdb.lan'

def COLORS(k):
    return (((k ** 3) % 255) / 255.0,
            ((k * 100) % 255) / 255.0,
            ((k * k) % 255) / 255.0)

MARKERS = ['+', '*', '<', '>', '^', '_',
           'D', 'H', 'd', 'h', 'o', 'p']

def get_branches(hostname, port, days):

    mydate = datetime.datetime.today()-datetime.timedelta(days=days)

    query = "select branch, count(*) from app_stats where date >= '%s' group by branch order by 1 asc" % \
                    mydate.strftime('%Y-%m-%d 00:00:00')

    conn = FastSerializer(hostname, port)
    proc = VoltProcedure(conn, '@AdHoc',
                         [FastSerializer.VOLTTYPE_STRING])
    resp = proc.call([query])
    conn.close()

    branches = []
    for row in resp.tables[0].tuples:
        branches.append(str(row[0]))

    return branches

def get_min_date(hostname, port):

    query = "select min(date) from app_stats"

    conn = FastSerializer(hostname, port)
    proc = VoltProcedure(conn, '@AdHoc',
                         [FastSerializer.VOLTTYPE_STRING])
    resp = proc.call([query])
    conn.close()

    ndays = datetime.datetime.today()-resp.tables[0].tuples[0][0]
    return ndays.days+1

def get_stats(hostname, port, days, branch):
    """Get statistics of all runs

    Example return value:
    { u'VoltKV': [ { 'lat95': 21,
                 'lat99': 35,
                 'nodes': 1,
                 'throughput': 104805,
                 'date': datetime object}],
      u'Voter': [ { 'lat95': 20,
                    'lat99': 47,
                    'nodes': 1,
                    'throughput': 66287,
                    'date': datetime object}]}
    """

    conn = FastSerializer(hostname, port)
    proc = VoltProcedure(conn, 'BestOfPeriod',
                         [FastSerializer.VOLTTYPE_SMALLINT,
                         FastSerializer.VOLTTYPE_STRING])
    resp = proc.call([days, branch])
    conn.close()

    # keyed on app name, value is a list of runs sorted chronologically
    stats = dict()
    run_stat_keys = ['nodes', 'date', 'tps', 'lat95', 'lat99']
    for row in resp.tables[0].tuples:
        app_stats = []
        if row[0] not in stats:
            stats[row[0]] = app_stats
        else:
            app_stats = stats[row[0]]
        run_stats = dict(zip(run_stat_keys, row[1:]))
        app_stats.append(run_stats)

    # sort each one
    for app_stats in stats.itervalues():
        app_stats.sort(key=lambda x: x['date'])

    return stats

class Plot:
    DPI = 100.0

    def __init__(self, title, xlabel, ylabel, filename, w, h, ndays):
        self.filename = filename
        self.ndays = ndays
        self.legends = {}
        w = w == None and 1200 or w
        h = h == None and 400 or h
        fig = plt.figure(figsize=(w / self.DPI, h / self.DPI),
                         dpi=self.DPI)
        self.ax = fig.add_subplot(111)
        self.ax.set_title(title)
        plt.xticks(fontsize=10)
        plt.yticks(fontsize=10)
        plt.ylabel(ylabel, fontsize=8)
        plt.xlabel(xlabel, fontsize=8)
        fig.autofmt_xdate()

    def plot(self, x, y, color, marker_shape, legend):
        self.ax.plot(x, y, linestyle="-", label=str(legend),
                     marker=marker_shape, markerfacecolor=color, markersize=4)

    def close(self):
        formatter = matplotlib.dates.DateFormatter("%b %d %y")
        self.ax.xaxis.set_major_formatter(formatter)
        ymin, ymax = plt.ylim()
        plt.ylim((ymin-(ymax-ymin)*0.1, ymax+(ymax-ymin)*0.1))
        xmax = datetime.datetime.today().toordinal()
        plt.xlim((xmax-self.ndays, xmax))
        plt.legend(prop={'size': 10}, loc=2)
        plt.savefig(self.filename, format="png", transparent=False,
                    bbox_inches="tight", pad_inches=0.2)
        plt.close('all')


def plot(title, xlabel, ylabel, filename, width, height, app, data, data_type, ndays):
    plot_data = dict()
    for run in data:
        if run['nodes'] not in plot_data:
            plot_data[run['nodes']] = {'time': [], data_type: []}

        datenum = matplotlib.dates.date2num(run['date'])
        plot_data[run['nodes']]['time'].append(datenum)

        if data_type == 'tps':
            value = run['tps']/run['nodes']
        else:
            value = run[data_type]
        plot_data[run['nodes']][data_type].append(value)

    if len(plot_data) == 0:
        return

    i = 0
    pl = Plot(title, xlabel, ylabel, filename, width, height, ndays)
    sorted_data = sorted(plot_data.items(), key=lambda x: x[0])
    for k, v in sorted_data:
        pl.plot(v['time'], v[data_type], COLORS(i), MARKERS[i], k)
        i += 3

    for k, v in sorted_data:
        x = v['time'][-1]
        y = v[data_type][-1]
        pl.ax.annotate(str(y), xy=(x,y), xycoords='data', xytext=(5,-5),
            textcoords='offset points', ha='left')
        xmin, ymin = [(v['time'][i],y) for i,y in enumerate(v[data_type]) if y == min(v[data_type])][-1]
        xmax, ymax= [(v['time'][i],y) for i,y in enumerate(v[data_type]) if y == max(v[data_type])][-1]
        if ymax != ymin:
            if xmax != x:
                pl.ax.annotate(str(ymax), xy=(xmax,ymax),
                    textcoords='offset points', ha='center', va='bottom', xytext=(0,5))
            if xmin != x:
                pl.ax.annotate(str(ymin), xy=(xmin,ymin),
                    textcoords='offset points', ha='center', va='top', xytext=(0,-5))

    pl.close()

def generate_index_file(filenames):
    row = """
      <tr>
        <td width="100">%s</td>
        <td><a href="%s"><img src="%s" width="400" height="200"/></a></td>
        <td><a href="%s"><img src="%s" width="400" height="200"/></a></td>
        <td><a href="%s"><img src="%s" width="400" height="200"/></a></td>
      </tr>
"""

    sep = """
     </table>
     <table frame="box">
     <tr>
         <th colspan="4"><a name="%s">%s</a></th>
     </tr>
"""

    full_content = """
<html>
  <head>
    <title>Performance Graphs</title>
  </head>
  <body>
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
    toc = sorted(list(set([x[0] for x in filenames])))
    h = map(lambda x:(x.replace(' ','%20'), x), toc)
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
        rows.append(row % (i[4], i[1], i[1], i[2], i[2], i[3], i[3]))

    return full_content % ''.join(rows)

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
    if len(sys.argv) >=4:
        ndays = int(sys.argv[3])
    else:
        ndays = get_min_date(STATS_SERVER, 21212)
    width = None
    height = None
    if len(sys.argv) >= 5:
        width = int(sys.argv[4])
    if len(sys.argv) >= 6:
        height = int(sys.argv[7])

    # show all the history
    branches = get_branches(STATS_SERVER, 21212, ndays)
    branches.sort
    i=0
    for p in ['master', 'release-']:
        for b in branches:
            if b.startswith(p):
                x=branches.pop(branches.index(b))
                branches.insert(i, x)
                i+=1
    root_path = path
    filenames = []              # (appname, latency, throughput)
    iorder = 0
    for branch in branches:

        iorder += 1

        stats = get_stats(STATS_SERVER, 21212, ndays, branch)

        prefix = sys.argv[2] + "-" + branch
        path = root_path + "-" + branch

        # Plot single node stats for all apps
        for app, data in stats.iteritems():
            app_filename = app.replace(' ', '_')
            latency95_filename = '%s-latency95-%s.png' % (prefix, app_filename)
            latency99_filename = '%s-latency99-%s.png' % (prefix, app_filename)
            throughput_filename = '%s-throughput-%s.png' % (prefix, app_filename)
            filenames.append((app, latency95_filename, latency99_filename, throughput_filename, branch, iorder))

            plot(app + " latency95 on " + branch, "Time", "Latency (ms)",
                 path + "-latency95-" + app_filename + ".png", width, height, app,
                 data, 'lat95', ndays)

            plot(app + " latency99 on " + branch, "Time", "Latency (ms)",
                 path + "-latency99-" + app_filename + ".png", width, height, app,
                 data, 'lat99', ndays)

            plot(app + " throughput(best) on " + branch, "Time", "Throughput (txns/sec)",
                 path + "-throughput-" + app_filename + ".png", width, height, app,
                 data, 'tps', ndays)

    # generate index file
    index_file = open(root_path + '-index.html', 'w')
    sorted_filenames = sorted(filenames, key=lambda f: f[0].lower()+str(f[5]))
    index_file.write(generate_index_file(sorted_filenames))
    index_file.close()

if __name__ == "__main__":
    main()
