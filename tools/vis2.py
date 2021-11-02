#!/usr/local/bin/python2.7

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
import numpy as np
import csv
import time
import datetime

STATS_SERVER = 'perfstatsdb.voltdb.lan'
NaN = float("nan")
REFERENCE_BRANCH = 'master'  # this is global but can be modified by the 4th parameter if any

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

WIDTH = 1700
HEIGHT = 850
APX = 80
APY = 10
last = -1

DATA_HEADER = "branch|chart|ref-last|ref-ma-last|ref-stdev|branch-last|#-of-stdev-vs-ref(neg=worse)|pct-vs-ref(neg=worse)".split(
    "|")

branch_colors = {}


def get_stats(hostname, port, days):
    """Get most recent run statistics of all apps within the last 'days'
    """

    conn = FastSerializer(hostname, port)
    proc = VoltProcedure(conn, 'AverageOfPeriod',
                         [FastSerializer.VOLTTYPE_SMALLINT])
    resp = proc.call([days])
    conn.close()

    # keyed on app name, value is a list of runs sorted chronologically
    maxdate = datetime.datetime(1970, 1, 1, 0, 0, 0)
    mindate = datetime.datetime(2038, 1, 19, 0, 0, 0)
    stats = dict()
    run_stat_keys = ['app', 'nodes', 'branch', 'date', 'tps', 'lat95', 'lat99']
    # print resp
    for row in resp.tables[0].tuples:
        group = (row[0], row[1])
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
        plt.Locator.MAXTICKS = 2000
        plt.grid(True, color='black', alpha=0.5)
        self.fig.autofmt_xdate()
        plt.autoscale(enable=True, axis='x', tight=None)
        plt.ylabel(ylabel)
        plt.xlabel(xlabel)

    def plot(self, x, y, color, marker_shape, legend, linestyle):
        self.ax.plot(x, y, linestyle, label=legend, color=color,
                     marker=marker_shape, markerfacecolor=color, markersize=8)

    def close(self):
        plt.axvline(x=datetime.datetime(2016, 1, 11, 12, 00, 0), color='black')
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
        plt.xlim((self.xmin.toordinal(),
                  (self.xmax + datetime.timedelta(1)).replace(minute=0, hour=0, second=0, microsecond=0).toordinal()))
        if self.series.startswith('lat'):
            lloc = 2
        else:
            lloc = 3
        plt.legend(prop={'size': 10}, loc=lloc)
        plt.savefig(self.filename, format="png", transparent=False, bbox_inches="tight", pad_inches=0.2)
        plt.close('all')


class Bdata(dict):
    def __init__(self, *args, **kwargs):
        dict.update(self, *args, **kwargs)

    def __getitem__(self, key):
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            return None

    def __setitem__(self, key, value):
        return dict.__setitem__(self, key, value)

    def __delitem__(self, key):
        return dict.__delitem__(self, key)

    def __contains__(self, key):
        return dict.__contains__(self, key)

    def update(self, *args, **kwargs):
        dict.update(self, *args, **kwargs)

    def __getattribute__(self, *args, **kwargs):
        if dict.__contains__(self, args[0]):
            return self.__getitem__(args[0])
        return dict.__getattribute__(self, args[0])

    def __setattr__(self, key, value):
        return dict.__setitem__(self, key, value)


def plot(title, xlabel, ylabel, filename, width, height, app, data, series, mindate, maxdate, polarity, analyze):
    global branch_colors
    plot_data = dict()

    for run in data:
        if run['branch'] not in plot_data:
            plot_data[run['branch']] = {series: []}

        if series == 'tppn':
            value = run['tps'] / run['nodes']
        else:
            value = run[series]

        if value != 0.0:
            datenum = matplotlib.dates.date2num(run['date'])
            plot_data[run['branch']][series].append((datenum, value))

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

    toc = dict()

    if REFERENCE_BRANCH in branches_sort:
        branches_sort.remove(REFERENCE_BRANCH)
        branches_reference_first = [REFERENCE_BRANCH] + branches_sort
    else:
        branches_reference_first = branches_sort
        print "WARN: has no reference: %s" % title

    # loop thru branches, 'reference' branch is first
    bn = 0
    for b in branches_reference_first:
        bd = plot_data[b]
        bn += 1
        # k is the chart type like lat95, lat99, thpt, etc
        # v is the chart's data list[tuples(float,float),]
        for k, v in bd.items():
            if len(v) == 0:
                print "branch %s, chart %s has no data points, skipping..." % (b, k)
                continue
            if k not in toc.keys():
                toc[k] = []
            v = sorted(v, key=lambda x: x[0])
            # u is the chart data as [(y values,...), (x values,...)]
            u = zip(*v)

            # get the colors and markers
            if b not in branch_colors:
                branch_colors[b] = (
                COLORS[len(branch_colors.keys()) % len(COLORS)], MARKERS[len(branch_colors.keys()) % len(MARKERS)])

            # compute and saved reference's average, median, std on raw (all) data
            bdata = Bdata(branch=b, title=title, chart=k, color=None, seriescolor=branch_colors[b][0],
                          seriesmarker=branch_colors[b][1], xdata=u[0], ydata=u[1],
                          last=u[1][-1], avg=np.average(u[1]), median=np.median(u[1]), stdev=np.std(u[1]), ma=[NaN],
                          ama=NaN, mstd=[NaN], mastd=NaN, pctmadiff=NaN, mnstddiff=NaN, failed=None, bgcolor=None,
                          pctmaxdev=NaN, mnstdmadiff=NaN)

            analyze.append(bdata)

            # plot the series
            pl.plot(u[0], u[1], bdata.seriescolor, bdata.seriesmarker, bdata.branch, '-')

            # reference is processed first, but set this up in case there is no reference data for a chart
            reference = analyze[0]  # remember reference

            MOVING_AVERAGE_DAYS = 10

            (ma_series, ma_avg, ma_std_series, ma_std) = moving_average(bdata.ydata, MOVING_AVERAGE_DAYS)

            # plot the moving average
            if len(ma_series) > MOVING_AVERAGE_DAYS:
                pl.plot(bdata.xdata, ma_series, bdata.seriescolor, None, None, ":")

            # the reference branch
            if b == REFERENCE_BRANCH:
                # if we have enough data compute moving average and moving std dev
                # std is std of data corresponding to each ma_series window
                # nb. std is used only in the charts for the 2-sigma line
                # ma_avg is mean of moving average population
                # ma_std_series is std of moving average all values

                if len(ma_series) >= MOVING_AVERAGE_DAYS:

                    bdata.update(ma=ma_series, ama=ma_avg, mstd=ma_std_series, mastd=ma_std)

                    # see if the series should be flagged out of spec
                    failed = 0
                    if polarity == 1:
                        # increasing is bad
                        bestmapoint = np.nanmin(ma_series)
                        localminormax = (bdata.xdata[np.nanargmin(ma_series)], bestmapoint)
                        # if b == REFERENCE_BRANCH and bdata.ma[last] > bdata.median * 1.05:
                        #     failed = 1
                    else:
                        # decreasing is bad
                        bestmapoint = np.nanmax(ma_series)
                        localminormax = (bdata.xdata[np.nanargmax(ma_series)], bestmapoint)
                        # if b == REFERENCE_BRANCH and bdata.ma[last] < bdata.median * 0.95:
                        #     failed = 1

                    # plot the 2-sigma line on the reference series
                    twosigma = np.sum([np.convolve(bdata.mstd, polarity * 2), bdata.ma], axis=0)
                    pl.plot(bdata.xdata, twosigma, bdata.seriescolor, None, None, '-.')
                    pl.ax.annotate(r"$2\sigma$", xy=(bdata.xdata[last], twosigma[last]), xycoords='data',
                                   xytext=(20, 0),
                                   textcoords='offset points', ha='right', color=bdata.seriescolor, alpha=0.5)

                    # plot the 20% line on the reference series
                    twntypercent = np.sum([np.convolve(bdata.ma, polarity * 0.2), bdata.ma], axis=0)
                    pl.plot(bdata.xdata, twntypercent, bdata.seriescolor, None, None, '-.')
                    pl.ax.annotate(r"20%", xy=(bdata.xdata[last], twntypercent[last]), xycoords='data', xytext=(20, 0),
                                   textcoords='offset points', ha='right', color=bdata.seriescolor, alpha=0.5)

                    pctmaxdev = (bestmapoint - reference.ma[last]) / bestmapoint * 100. * polarity  # pct diff min/max
                    # pctmedian = (reference.ma_series[last] - bdata.median) / reference.median * 100.  #pct diff median
                    pctmadiff = (reference.ma[last] - bdata.ydata[last]) / reference.ma[last] * 100. * polarity  # pct diff last vs ma_series mean
                    mnstddiff = (reference.ma[last] - bdata.ydata[last]) / reference.stdev * polarity  # no std diff last vs ma_series
                    mnstdmadiff = (reference.ma[last] - bdata.ydata[last]) / reference.mstd[last] * polarity  # no std diff std of most recent window

                    bdata.update(pctmadiff=pctmadiff, mnstddiff=mnstddiff)

                    # when do we flag a chart? standard deviation is easy to use for an estimator but since it relies
                    # on squares it will tend to give poor results if the deviations are large. Also, we'll just assume
                    # that our distribution is Normal, so 95% of data points should lie withing 2 stddev of the mean

                    # set background color of chart if there's a data point outside 2sigma or if the moving
                    # average has negatively deviated from its mean by more than 5 or 10%

                    # yellow if > 5%
                    # red if >= 10%
                    # negative values are worse
                    failed = False
                    color = None
                    # moving average has degraded by 5+ (yellow) or 10+ (red) pct
                    # last has degraded from ma_series (mean) by 5+ or 10+ pcs AND last >= 1.5 stdev off the last mean
                    if pctmaxdev <= -10.0 or \
                            pctmadiff <= -10.0 and mnstddiff <= -1.5:
                        color = 'red'
                    elif pctmaxdev <= -5.0 or \
                            pctmadiff > -10.0 and pctmadiff <= -5.0 and mnstddiff <= -1.5:
                        color = 'yellow'

                    if color:
                        failed = True
                    print title, b, k, pctmaxdev, pctmadiff, mnstddiff, str(color)

                    toc[k].append((b, color))
                    if failed:
                        for pos in ['top', 'bottom', 'right', 'left']:
                            pl.ax.spines[pos].set_edgecolor(color)
                        pl.ax.set_facecolor(color)
                    pl.ax.patch.set_alpha(0.1)

                    bdata.update(failed=failed, bgcolor=color)

                    # annotate value of the best point aka localminormax
                    pl.ax.annotate("%.2f" % bestmapoint, xy=localminormax, xycoords='data', xytext=(0, -10 * polarity),
                                   textcoords='offset points', ha='center', color=bdata.seriescolor, alpha=0.5)

                    # annotate value and percent vs reference point of most recent moving average on reference
                    pl.ax.annotate("%.2f" % bdata.ma[last], xy=(bdata.xdata[last], bdata.ma[last]), xycoords='data',
                                   xytext=(5, +5), textcoords='offset points', ha='left', alpha=0.5)

                    pl.ax.annotate("(%+.2f%%)" % pctmaxdev, xy=(bdata.xdata[last], bdata.ma[last]), xycoords='data',
                                   xytext=(5, -5),
                                   textcoords='offset points', ha='left', alpha=0.5)

                    # annotation with moving average values
                    # bdata.update(pctmedian=pctmedian, bestmapoint=bestmapoint, pctmaxdev=pctmaxdev)

                    # raw data to the chart
                    pl.ax.annotate('%s %s: %s n: %d last: %.2f avg: %.2f sdev: %.2f (%.2f%% avg)  (%.2f%% ma_series) ma_series: %.2f'
                                   ' (%+.2f%% of bestma) (%+.2f%% of lastma) (%+.2f #stdev) (%.2f  #ma_std_series) avg(ma_series):'
                                   ' %.2f std(ma_series): %.2f' % (
                                       bdata.seriesmarker, bdata.branch, bdata.bgcolor, len(bdata.ydata),
                                       bdata.ydata[last],
                                       bdata.avg, bdata.stdev, bdata.stdev / bdata.avg * 100., bdata.stdev / bdata.ma[last] * 100.,
                                       bdata.ma[last], pctmaxdev, bdata.pctmadiff, bdata.mnstddiff, mnstdmadiff, bdata.ama, bdata.mastd),
                                   xy=(APX, APY * bn),
                                   xycoords='figure points', horizontalalignment='left', verticalalignment='top',
                                   color=bdata.seriescolor, fontsize=10, alpha=1.0)

            else:
                # branches comparing to reference
                if reference.ama is not NaN:
                    pctmadiff = (reference.ama - bdata.ydata[last]) / reference.ama * 100. * polarity  # pct diff last vs ma_series mean
                    mnstddiff = (reference.ama - bdata.ydata[last]) / reference.stdev * polarity  # no std diff last vs ma_series

                    color = None
                    if pctmadiff > -10.0 and pctmadiff <= -5.0:
                        color = 'yellow'
                    elif pctmadiff <= -10.0:
                        color = 'red'
                    if mnstddiff >= 2.0:
                        color = 'red'

                    bdata.update(ma=ma_series, ama=ma_avg, mstd=ma_std_series, mastd=ma_std)

                    bdata.update(bgcolor=color, pctmadiff=pctmadiff, mnstddiff=mnstddiff)

                pl.ax.annotate('%s %s: %s n: %d last: %.2f avg: %.2f sdev: %.2f (%.2f%% avg)  (%.2f%% ma_series) ma_series: %.2f'
                               ' (%+.2f%% of bestma) (%+.2f%% of lastma) (%+.2f #stdev) (%.2f  #ma_std_series) avg(ma_series):'
                               ' %.2f std(ma_series): %.2f' % (
                                   bdata.seriesmarker, bdata.branch, bdata.bgcolor, len(bdata.ydata),
                                   bdata.ydata[last],
                                   bdata.avg, bdata.stdev, bdata.stdev / bdata.avg * 100.,
                                   bdata.stdev / bdata.ma[last] * 100.,
                                   bdata.ma[last], bdata.pctmaxdev, bdata.pctmadiff, bdata.mnstddiff, bdata.mnstdmadiff, bdata.ama,
                                   bdata.mastd),
                               xy=(APX, APY * bn),
                               xycoords='figure points', horizontalalignment='left', verticalalignment='top',
                               color=bdata.seriescolor, fontsize=10, alpha=1.0)

        if len(analyze) == 1:
            pl.ax.annotate(datetime.datetime.today().strftime("%Y/%m/%d %H:%M:%S"), xy=(.20, .95),
                       xycoords='figure fraction', horizontalalignment='left', verticalalignment='top',
                       fontsize=8)

    pl.close()
    return toc

def generate_index_file(filenames, branches):
    row = """
      <tr>
        <td><a href="%s"><img src="%s" width="100%%" height="35%%"/></a></td>
        <td><a href="%s"><img src="%s" width="100%%" height="35%%"/></a></td>
        <td><a href="%s"><img src="%s" width="100%%" height="35%%"/></a></td>
      </tr>
    """

    sep = """
     </table>
     <table frame="box" width="100%%">
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
    Generated on %s<br>
    Reference branch is '%s'<br>
    %s
    <table frame="box" width="100%%">
%s
    </table>
  </body>
</html>
"""
    branch_studies = """
        <a href=%s>%s</a>
    """

    hrow = """
    <tr>
        <td %s><a href=#%s>%s</a></td>
        <td %s><a href=#%s>%s</a></td>
        <td %s><a href=#%s>%s</a></td>
        <td %s><a href=#%s>%s</a></td>
    </tr>
    """

    toc = []
    for x in filenames:
        tdattr = "<span></span>"
        if len(x) == 6:
            color = None
            # rollup worse case color flag condition
            for type in x[5].values():
                for branch in type:
                    if branch[1] == 'red':
                        color = 'red'
                        break
                    elif color is None and branch[1] == 'yellow':
                        color = 'yellow'
                if color == 'red':
                    break
            if color:
                tdattr = '<span style="color:%s">&#9658;</span>' % color
        toc.append(("", x[0].replace(' ', '%20'), tdattr + x[0]))

    n = 4
    z = n - len(toc) % n
    while z > 0 and z < n:
        toc.append(('', '', ''))
        z -= 1

    rows = []
    t = ()
    for i in range(1, len(toc) + 1):
        t += tuple(toc[i - 1])
        if i % n == 0:
            rows.append(hrow % t)
            t = ()

    last_app = None
    for i in filenames:
        if i[0] != last_app:
            rows.append(sep % (i[0], i[0]))
            last_app = i[0]
        rows.append(row % (i[1], i[1], i[2], i[2], i[3], i[3]))

    return full_content % (time.strftime("%Y/%m/%d %H:%M:%S"), REFERENCE_BRANCH, branches, ''.join(rows))


def generate_data_file(data, branches, prefix):
    row = """
          <tr>
            <td align="center"><span style="color:%s">&#9658;</span></td>
            <td align="left" width="40%%"><a href="%s">%s</a></td>
            <td align="right">%s</td>
            <td align="right">%s</td>
            <td align="right">%s</td>
            <td align="right">%s</td>
            <td align="right">%s</td>
            <td align="right">%s</td>
          </tr>
    """

    sep = """
         </table>
         <table frame="box" width="100%%">
         <tr>
             <th colspan="8"><a name="%s">%s (vs: %s)</a></th>
         </tr>
            %s
    """

    full_content = """
    <html>
      <head>
        <title>Performance Raw Data</title>
      </head>
      <body>
        Generated on %s<br>
        Reference is branch %s<br>
            %s
        <table frame="box" width="100%%">
    %s
        </table>
      </body>
    </html>
    """

    hrow = """
        <tr>
            <th>Flag</th>
            <th>%s</th>
            <th>%s</th>
            <th>%s</th>
            <th>%s</th>
            <th>%s</th>
            <th>%s</th>
            <th>%s</th>
        </tr>
    """

    rows = []
    last_app = None
    bgcolors = {'black': 'white', 'None': 'white'}
    # data is an numpy ndarray = trouble
    # i looks like [(branch, flag-color, chart-name, analyze-data...), ...] ie. the analyze tuples
    for d in range(len(data)):
        i = tuple(data[d])
        if i[0] != last_app:
            rows.append(sep % (i[0], i[0], REFERENCE_BRANCH, hrow % tuple(DATA_HEADER[1:])))
            last_app = i[0]
        # use the char'ts pass/failed state from its background color to render a flag
        # the bgcolor can be yellow, red or None.
        bgcolor = bgcolors.get(i[1], i[1] or 'white')
        rows.append(row % ((bgcolor, png_filename(i[2], prefix), i[2]) + tuple([round(x, 3) for x in i[3:]])))

    return full_content % (time.strftime("%Y/%m/%d %H:%M:%S"), REFERENCE_BRANCH, branches, ''.join(rows))


def png_filename(filename, prefix):
    return prefix + "_" + filename.replace(" ", "_") + ".png"


def moving_average(series, window_size, type='simple'):
    """
    compute an window_size period moving average.

    type is 'simple' | 'exponential'

    """

    if len(series) < window_size:
        return [NaN], NaN, [NaN], NaN

    series = np.asarray(series)
    if type == 'simple':
        weights = np.ones(window_size)
    else:
        weights = np.exp(np.linspace(-1., 0., window_size))

    weights /= weights.sum()

    ma_series = [NaN] * (window_size - 1) + list(np.convolve(series, weights, mode='valid'))

    # compute the mean of the moving average population
    # over the most recent ma group
    ma_avg = np.average(ma_series[len(ma_series) - window_size:])

    # compute the standard deviation of the set of data
    # corresponding to each moving average window
    ma_std_series = [NaN]*(window_size - 1)
    for d in range(0, len(series) - window_size + 1):
        ma_std_series.append(np.std(series[d: d + window_size]))

    # also compute the standard deviation of the moving avg
    # all available data points. scalar result
    ma_std = np.std(ma_series[window_size - 1:])

    return (ma_series, ma_avg, ma_std_series, ma_std)


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
        print (os.getcwd())
        print sys.argv[1], "does not exist"
        os.mkdir(sys.argv[1])
        #exit(-1)

    prefix = sys.argv[2]
    path = os.path.join(sys.argv[1], sys.argv[2])
    ndays = 2000
    if len(sys.argv) >= 4:
        ndays = int(sys.argv[3])
    if len(sys.argv) >= 5:
        global REFERENCE_BRANCH
        REFERENCE_BRANCH = str(sys.argv[4])
    width = WIDTH
    height = HEIGHT
    if len(sys.argv) >= 6:
        width = int(sys.argv[5])
    if len(sys.argv) >= 7:
        height = int(sys.argv[6])

    # show all the history
    (stats, mindate, maxdate) = get_stats(STATS_SERVER, 21212, ndays)
    mindate = (mindate).replace(hour=0, minute=0, second=0, microsecond=0)
    maxdate = (maxdate + datetime.timedelta(days=1)).replace(minute=0, hour=0, second=0, microsecond=0)

    root_path = path
    filenames = []  # (appname, latency, throughput)
    iorder = 0

    analyze = []

    for group, data in stats.iteritems():

        (study, nodes) = group
        study = study.replace('/', '')

        # if you just want to test one study, put it's name here and uncomment...
        #if study != 'CSV-narrow-ix':
        #    continue

        conn = FastSerializer(STATS_SERVER, 21212)
        proc = VoltProcedure(conn, "@AdHoc", [FastSerializer.VOLTTYPE_STRING])
        resp = proc.call(["select chart_order, series, chart_heading, x_label, y_label, polarity from charts where appname = '%s' order by chart_order" % study])
        conn.close()

        app = study + " %d %s" % (nodes, ["node", "nodes"][nodes > 1])

        # chart polarity: -1 for tps (decreasing is bad), 1 for latencies (increasing is bad)

        legend = {1: dict(series="lat95", heading="95tile latency", xlabel="Time", ylabel="Latency (ms)", polarity=1),
                  2: dict(series="lat99", heading="99tile latency", xlabel="Time", ylabel="Latency (ms)", polarity=1),
                  3: dict(series="tppn", heading="avg throughput per node", xlabel="Time", ylabel="ops/sec per node",
                          polarity=-1)
                  }

        for r in resp.tables[0].tuples:
            legend[r[0]] = dict(series=r[1], heading=r[2], xlabel=r[3], ylabel=r[4], polarity=r[5])

        fns = [app]
        tocs = dict()
        for r in legend.itervalues():
            aanalyze = []
            title = app + " " + r['heading']
            fn = "_" + title.replace(" ", "_") + ".png"
            fns.append(prefix + fn)
            toc = plot(title, r['xlabel'], r['ylabel'], path + fn, width, height, app, data, r['series'], mindate,
                       maxdate, r['polarity'], aanalyze)
            reference = Bdata()
            if len(aanalyze):
                reference = aanalyze[0]
            for branch in aanalyze:
                analyze.append(tuple(
                    [branch['branch'], branch['bgcolor'], branch['title'], reference['last'], reference['ma'][last], reference['stdev'], branch['last'],
                     branch['mnstddiff'], branch['pctmadiff']]))

            if toc:
                tocs.update(toc)
        # there's an analyze tuple for each chart
        # if len(analyze)/3 >= 6:
        #     break

        fns.append(iorder)
        fns.append(tocs)
        filenames.append(tuple(fns))

    filenames.append(("KVBenchmark-five9s-nofail-latency", "", "",
                      "http://ci/job/performance-nextrelease-5nines-nofail/lastSuccessfulBuild/artifact/pro/tests/apptests/savedlogs/5nines-histograms.png",
                      iorder))
    filenames.append(("KVBenchmark-five9s-nofail-nocl-latency", "", "",
                      "http://ci/job/performance-nextrelease-5nines-nofail-nocl/lastSuccessfulBuild/artifact/pro/tests/apptests/savedlogs/5nines-histograms.png",
                      iorder))
    filenames.append(("KVBenchmark-five9s-nofail-nocl-kvm-latency", "", "",
                      "http://ci/job/performance-nextrelease-5nines-nofail-nocl-kvm/lastSuccessfulBuild/artifact/pro/tests/apptests/savedlogs/5nines-histograms.png",
                      iorder))
    filenames.append(("Openet-Shocker-three9s-latency", "", "",
                      "http://ci/job/performance-nextrelease-shocker/lastSuccessfulBuild/artifact/pro/tests/apptests/savedlogs/3nines-histograms.png",
                      iorder))
    filenames.append(("Openet-Shocker-three9s-4x2-latency", "", "",
                      "http://ci/job/performance-nextrelease-shocker-4x2/lastSuccessfulBuild/artifact/pro/tests/apptests/savedlogs/3nines-histograms.png",
                      iorder))

    # sort and save the raw data analyze file
    with open(root_path + "-analyze.csv", "wb") as f:
        writer = csv.writer(f, delimiter='|')
        writer.writerows(DATA_HEADER)
        aa = np.array(analyze,
                      dtype=[('branch-name', 'S99'), ('bgcolor', 'S99'), ('file', 'S99'), ('reference', float),
                             ('ma', float), ('std', float), ('branch-last', float), ('nstd', float), ('pct', float)])
        branches = []
        sanalyze = np.sort(aa, order=['branch-name', 'nstd'])
        for r in sanalyze:
            if r[0] not in branches:
                branches.append(r[0])
        writer.writerows(sanalyze)

    # make convenient hlinks to the branch studies tables
    branches = '\n'.join(
        ["<a href=" + prefix + "-analyze.html#" + b + ">" + b + " (vs. " + REFERENCE_BRANCH + ")</a><br>"
         for b in sorted(np.unique(branches))])

    # generate branch study html page
    with open(root_path + '-analyze.html', 'w') as data_file:
        data_file.write(generate_data_file(sanalyze, branches, prefix))

    # generate index file
    index_file = open(root_path + '-index.html', 'w')
    sorted_filenames = sorted(filenames, key=lambda f: f[0].lower() + str(f[1]))
    index_file.write(generate_index_file(sorted_filenames, branches))
    index_file.close()


if __name__ == "__main__":
    main()
