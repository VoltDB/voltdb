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
from operator import itemgetter, attrgetter
import numpy
from mpl_toolkits.axes_grid.anchored_artists import AnchoredText

STATS_SERVER = 'perfstatsdb.voltdb.lan'

def COLORS(k):
    return (((k ** 3) % 255) / 255.0,
            ((k * 100) % 255) / 255.0,
            ((k * k) % 255) / 255.0)

#COLORS = plt.cm.Spectral(numpy.linspace(0, 1, 10)).tolist()
COLORS = ['b','g','r','c','m','y','k']
#print COLORS

MARKERS = ['o', '*', '<', '>', '^', '_',
           'D', 'H', 'd', 'h', '+', 'p']

mc = {}

def get_branches(hostname, port, days):

    mydate = datetime.datetime.today()-datetime.timedelta(days=days)

    query = "select branch, max(date), count(*) from app_stats where date >= '%s' group by branch order by 3 desc" % \
                    mydate.strftime('%Y-%m-%d 00:00:00')

    conn = FastSerializer(hostname, port)
    proc = VoltProcedure(conn, '@AdHoc',
                         [FastSerializer.VOLTTYPE_STRING])
    resp = proc.call([query])
    conn.close()

    branches = []
    keys=['branch','sampledate','count']
    for row in resp.tables[0].tuples:
        branches.append(dict(zip(keys,row)))

    return branches

def get_stats(hostname, port, days):
    """Get most recent run statistics of all apps within the last 'days'
    """

    conn = FastSerializer(hostname, port)
    proc = VoltProcedure(conn, 'BestOfPeriod_mr',
                         [FastSerializer.VOLTTYPE_SMALLINT])
    resp = proc.call([days])
    conn.close()

    # keyed on app name, value is a list of runs sorted chronologically
    stats = dict()
    run_stat_keys = ['app', 'branch', 'nodes', 'date', 'tps', 'lat95', 'lat99', 'count']
    for row in resp.tables[0].tuples:
        group = (row[1],row[0],row[2])
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
        h = h == None and 1200 or h
        self.fig = plt.figure(figsize=(w / self.DPI, h / self.DPI),
                         dpi=self.DPI)
        self.ax = self.fig.add_subplot(111)
        self.ax.set_title(title)
        plt.xticks(fontsize=10)
        plt.yticks(fontsize=10)
        plt.tick_params(axis='y', labelleft=True, labelright=True)
        plt.ylabel(ylabel, fontsize=8)
        plt.xlabel(xlabel, fontsize=8)
        self.ax.set_aspect(1)
        self.ax.set_yscale('log')
        self.ax.set_xscale('log')
        self.cm = plt.get_cmap("PiYG")

    def plot(self, x, y, color, marker_shape, legend):
        self.ax.scatter(x, y, label=str(legend), c=color,
                     marker=marker_shape) #, markerfacecolor=color)
                     #float(x)/float(y), cmap=self.cm, # color=color,

    def close(self):
        ymin, ymax = plt.ylim()
        plt.ylim((ymin-(ymax-ymin)*0.2, ymax+(ymax-ymin)*0.2))
        xmin, xmax = plt.xlim()
        plt.xlim((xmin-(xmax-xmin)*0.2, xmax+(xmax-xmin)*0.2))
        plt.legend(prop={'size': 10}, loc=2)
        plt.savefig(self.filename, format="png", transparent=False)
        plt.close('all')

def plot(title, xbranch, ybranch, filename, width, height, data, root_path):
    global mc

    xlabel = "%s Thpt tx/sec" % xbranch
    ylabel = "%s Thpt tx/sec" % ybranch

    pl = Plot(title, xlabel, ylabel, filename, width, height, 1)

    seq = []

    if len(data) > 0:
        for k,v in data.iteritems():
            if v["y"]['tps'] == 0.0:
                continue
            diff = (float(v["y"]['tps'])-float(v["x"]['tps']))/float(v["y"]['tps']) * 100.
            acolor = ['g','r'][diff<0]
            pl.plot(v["x"]['tps'], v["y"]['tps'], acolor, MARKERS[0], "")
            test_case = "%s %d %s" % (k[2],k[3],["node","nodes"][k[3]>1])
            seq.append([test_case, round(diff,2), round(v["y"]['tps'],2), v["y"]['count'], round(v["x"]['tps'], 2), v["x"]["count"],  acolor])
            if abs(diff) > 5.:
                atxt = "%s (%.2f%%)" % (test_case, diff)
                pl.ax.annotate(atxt, xy=(v["x"]['tps'], v["y"]['tps']), xycoords='data',
                        xytext=(10*[1,-1][diff>0], -5), textcoords='offset points', ha=["left","right"][diff>0],
                        size=10, color=acolor) #, arrowprops=dict(arrowstyle="->"))

    #if len(info) > 1:
    #    _at = AnchoredText("\n".join(info), loc=2, prop=dict(size=10))
    #    pl.ax.add_artist(_at)

    pl.close()

    seq = sorted(seq, key=lambda t: t[1])
    seq.insert(0, ['test #nodes','% diff', ybranch, '#samples', xbranch, '#samples', 'flag'])
    with open(filename.replace('png','html'), "w+") as f:
        f.write(get_html_tbl(reduce(lambda x,y: x+y, seq, []), 7))

def get_html_tbl(seq, col_count):
    if len(seq) % col_count:
        seq.extend([''] * (col_count - len(seq) % col_count))
    tbl_template = '<table>%s</table>' % ('<tr>%s</tr>' % ('<td>%s</td>' * col_count) * (len(seq)/col_count))
    return tbl_template % tuple(seq)

def generate_index_file(root, filenames):
    row = """
      <tr>
        <td><a href="%s"><img src="%s" width="400" height="400"/></a></td>
      </tr4
"""
    table = """
%s
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
        rows.append(row % (i[1], i[1]))
        try:
            with open(sys.argv[1]+"/"+str(i[1].replace('png','html')), 'r') as f:
                rows.append(table % f.read())
        except:
            pass

    return  full_content % ''.join(rows)

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

    branches = get_branches(STATS_SERVER, 21212, ndays)

    # show all the history
    stats = get_stats(STATS_SERVER, 21212, ndays)

    root_path = path
    filenames = []              # (appname, latency, throughput)
    iorder = 0

    bc = [(branches[i],branches[j]) for i in range(len(branches)) for j in range(len(branches)) if i != j]

    for bg in bc:
        merged = {}
        #missing = ['Cases missing from %s:' % bg[1]['branch']]
        for group,data in stats.iteritems():
            if group[1].startswith('Security'):
                continue
            if bg[0]['branch'] == group[0]:
                k = (bg[1]['branch'],group[1],group[2])
                if k in stats:
                    m = stats[k]
                    merged[(bg[0]['branch'],bg[1]['branch'])+(group[1],group[2])] = {"y": data[0], "x": stats[k][0]}
                #else:
                #    missing.append("%s %d %s" % (group[1],group[2],["node","nodes"][group[2]>1]))

        app = "%s vs %s" % (bg[0]['branch'], bg[1]['branch'])
        title = "%s as of %s vs %s as of %s" % (bg[0]['branch'],bg[0]['sampledate'],bg[1]['branch'],bg[1]['sampledate'])
        app_filename = app.replace(' ', '_')
        """
        latency95_filename = '%s-latency95-%s.png' % (prefix, app_filename)
        latency99_filename = '%s-latency99-%s.png' % (prefix, app_filename)
        throughput_filename = '%s-throughput-%s.png' % (prefix, app_filename)
        filenames.append((app, latency95_filename, latency99_filename, throughput_filename, iorder))
        """
        throughput_filename = '%s-throughput-%s.png' % (prefix, app_filename)
        filenames.append((app, throughput_filename, iorder))

        """
        plot(app + " latency95", "Time", "Latency (ms)",
             path + "-latency95-" + app_filename + ".png", width, height, app,
             data, 'lat95')

        plot(app + " latency99", "Time", "Latency (ms)",
             path + "-latency99-" + app_filename + ".png", width, height, app,
             data, 'lat99')
        """

        plot(title+" throughput", bg[1]['branch'], bg[0]['branch'],
                    path + "-throughput-" + app_filename + ".png", width, height, merged, root_path)

    # generate index file
    index_file = open(root_path + '-index.html', 'w')
    #sorted_filenames = sorted(filenames, key=lambda f: f[0].lower()+str(f[1]))
    index_file.write(generate_index_file(root_path, filenames))
    index_file.close()

if __name__ == "__main__":
    main()
