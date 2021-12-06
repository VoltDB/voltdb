#!/usr/bin/env python2

# This is a visualizer which pulls TPC-C benchmark results from the MySQL
# databases and visualizes them. Four graphs will be generated, latency graph on
# sinigle node and multiple nodes, and throughput graph on single node and
# multiple nodes.
#
# Run it without any arguments to see what arguments are needed.

import sys
import os
from pickle import TRUE, FALSE

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))) +
                os.sep + 'tests/scripts/')
sys.path.append("../lib/python/")
# check for python2
if sys.hexversion < 0x03000000:
    from voltdbclientpy2 import *
else:
    from voltdbclient import *

import numpy as np
import csv
import time
import datetime

STATS_SERVER = 'perfstatsdb.voltdb.lan'
NaN = float("nan")
REFERENCE_BRANCH = 'master'  # this is global but can be modified by the 4th parameter if any
last = -1

branch_colors = {}


def get_branches(hostname, port, days):
    conn = FastSerializer(hostname, port)
    proc = VoltProcedure(conn, '@AdHoc',
                         [FastSerializer.VOLTTYPE_STRING])
    resp = proc.call(["select distinct BRANCH from app_stats where DATE > DATEADD(DAY, -%s, NOW())" % days ])
    conn.close()
    branches = []
    for x in resp.tables[0].tuples:
        branches.append(x[0])
    return branches


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
        run_stats = dict(list(zip(run_stat_keys, row)))
        app_stats.append(run_stats)

    return (stats, mindate, maxdate)

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


# chart_db_info.polarity
# chart_db_info.title
def gather_plot_bdata(group_bdata, app, data, appgroup, series, chart_db_info):
    # each run contains a dictionary like so:
    # dict: {'app': 'DelUpdBench-P-IO-block10-asc', 'nodes': 1, 'branch': 'ENG-21619-dusb-random', 'date': datetime.datetime(2021, 11, 17, 7, 33, 53), 'tps': 866.2208516683413, 'lat95': 3702.78, 'lat99': 3784.7}
    # convert it to a series
    plot_data = {}
    for run in data:
        branch = run['branch']
        if not branch in plot_data.keys():
            plot_data[branch] = {series: []}

        # series is the chart type i.e. lat99, lat95 etc
        if series == 'tppn':
            value = run['tps'] / run['nodes']
        else:
            value = run[series]

        if value != 0.0:
            datenum = int(run['date'].strftime('%s'))
            plot_data[branch][series].append((datenum, value))

    plot_branches = []
    all_branches = list(plot_data.keys())
    if REFERENCE_BRANCH in all_branches:
        plot_branches = [REFERENCE_BRANCH]
        all_branches.remove(REFERENCE_BRANCH)
    else:
        print("WARNING NO REFERENCE BRANCH %s to compare with %s for series %s, chart_info %s" % (REFERENCE_BRANCH, branch, series, chart_db_info))

    plot_branches.extend(all_branches)
    references = dict()

    # do the reference branch first
    for branch in plot_branches:
        for series in plot_data[branch].keys():
            if len(plot_data[branch][series]) == 0:
                continue

            chart = series

            values = plot_data[branch][series]
            values = sorted(values, key=lambda x: x[0])
            # u is the chart data as [(y values,...), (x values,...)]
            u = list(zip(*values))
            b = branch
            failurereasons = []

            # TODO, fix me
            branch_colors[branch] = ['red', 'green']
            # compute and saved reference's average, median, std on raw (all) data
            bdata = Bdata(branch=b, title=chart_db_info['title'], chart=chart, color=None, seriescolor=branch_colors[b][0],
                  seriesmarker=branch_colors[b][1], xdata=u[0], ydata=u[1],
                  last=u[1][-1], avg=np.average(u[1]), median=np.median(u[1]), stdev=np.std(u[1]), ma=[NaN],
                  ama=NaN, mstd=[NaN], mastd=NaN, pctmadiff=NaN, mnstddiff=NaN, failed=None, bgcolor=None,
                  pctmaxdev=NaN, mnstdmadiff=NaN, scatterseriesdata=values, app=app, appgroup=appgroup,
                  result="ok", failurereasons=failurereasons, xlabel=chart_db_info['xlabel'],
                  ylabel=chart_db_info['ylabel'], heading=chart_db_info['heading'], ref_ma=NaN, ref_ama=NaN, ref_mstd=NaN, ref_mastd=NaN)

            MOVING_AVERAGE_DAYS = 10

            # if len(bdata.ydata) < 10:
            #    if len(bdata.ydata) >= 1:
            #        print("WARNING, branch %s ,%s ,series %s not enough data to compare moving average with reference %s" % (branch, chart_db_info['title'], series, REFERENCE_BRANCH))
            #    else:
            #        print("WARNING, NO DATA branch %s ,%s ,series %s not enough data to compare moving average with reference %s" % (branch, chart_db_info['title'], series, REFERENCE_BRANCH))
            #        continue
            if len(bdata.ydata) == 0:
                print("WARNING, NO DATA branch %s ,%s ,series %s not enough data to compare moving average with reference %s" % (branch, chart_db_info['title'], series, REFERENCE_BRANCH))

            (ma_series, ma_avg, ma_std_series, ma_std) = moving_average(bdata.ydata, MOVING_AVERAGE_DAYS)
            # if ma_avg == "nan" or ma_avg == NaN or ma_avg == float(NaN):
            #    print("stop")
            bdata.update(ma=ma_series, ama=ma_avg, mstd=ma_std_series, mastd=ma_std)

            if chart_db_info['polarity'] == 1:
                # increasing is bad
                bestmapoint = np.nanmin(ma_series)
                # localminormax = (bdata.xdata[np.nanargmin(ma_series)], bestmapoint)
                # if b == REFERENCE_BRANCH and bdata.ma[last] > bdata.median * 1.05:
                #     result = "failed"
            else:
                # decreasing is bad
                bestmapoint = np.nanmax(ma_series)
                # localminormax = (bdata.xdata[np.nanargmax(ma_series)], bestmapoint)
                # if b == REFERENCE_BRANCH and bdata.ma[last] < bdata.median * 0.95:
                #     result = "failed"

            # determine any failures or warnings
            result = "ok"

            # if this is the first time creating a test on a new branch
            # their may not be a reference branch

            # COMPARE AGAINST THE REFERENCE BRANCH
            if REFERENCE_BRANCH in references.keys() and branch != REFERENCE_BRANCH:
                reference = references[REFERENCE_BRANCH][series]
                bdata.update(ref_ma=reference.ma, ref_ama=reference.ama, ref_mstd=reference.mstd, ref_mastd=reference.mastd)
                pctmadiff = (reference.ama - bdata.ama) / reference.ama * 100. * chart_db_info['polarity']  # pct ma diff last vs ma_series mean
                mnstddiff = (reference.stdev - bdata.stdev) / reference.stdev * chart_db_info['polarity']  # no std diff last vs ma_series
                pctlastdiff = (reference.ama - bdata.ydata[last]) / reference.ama * 100. * chart_db_info['polarity'] # last value vs the the moving average of the reference

                pctmaxdev = (bestmapoint - reference.ma[last]) / bestmapoint * 100. * chart_db_info['polarity']  # pct diff min/max
                bdata.update(pctmadiff=pctmadiff, mnstddiff=mnstddiff)

                if pctmadiff > -10.0 and pctmadiff <= -5.0:
                    result = "warning"
                    failurereasons.append("Warning, branch '%s' %s 10 day moving avg is worse then 5%% (%s) from reference branch %s" % (b, bdata.chart, pctmadiff, REFERENCE_BRANCH))
                elif pctmadiff <= -10.0 and pctlastdiff <= -10.0 :
                    result = "failed"
                    failurereasons.append("branch '%s' %s 10 day moving avg and last run are worse then 10%% (%s and %s respectively) from reference branch %s" % (b, bdata.chart, pctmadiff, pctlastdiff, REFERENCE_BRANCH))
                elif pctmadiff <= -5.0 :
                    result = "warning"
                    failurereasons.append("Warning, branch '%s' %s 10 day moving avg is worse then 5%% (%s) from reference branch %s" % (b, bdata.chart, pctmadiff, REFERENCE_BRANCH))
                elif  pctlastdiff <= -10.0:
                    result = "warning"
                    failurereasons.append("Warning, branch '%s' %s last run is worse then 10%% (%s) from reference branch %s moving avg" % (b, bdata.chart, pctlastdiff, REFERENCE_BRANCH))
                if mnstddiff >= 2.0:
                    result = "failed"
                    failurereasons.append("branch '%s' %s 10 day moving stdev is more then 2.0 deviations (%s) from reference branch %s" % (b, bdata.chart, mnstddiff, REFERENCE_BRANCH))

                if result != "ok":
                    bdata.update(failurereasons=failurereasons)
                    if result == "failed":
                        bdata.update(bgcolor='#eb9db3')  # light red
                    elif result == "warning":
                        bdata.update(bgcolor='#f0eec7')  # light yellow
            else:
                # THIS BRANCH IS THE REFERENCE BRANCH COMPARE AGAINST SELF
                reference = bdata
                bdata.update(ref_ma=reference.ma, ref_ama=reference.ama, ref_mstd=reference.mstd, ref_mastd=reference.mastd)

                pctmadiff = (reference.ama - bdata.ydata[last]) / reference.ama * 100. * chart_db_info['polarity']  # pct diff last vs ma_series mean
                refdiff = reference.ama - bdata.ydata[last]
                mnstddiff = refdiff / reference.stdev * chart_db_info['polarity']  # no std diff last vs ma_series

                pctmaxdev = (bestmapoint - reference.ma[last]) / bestmapoint * 100. * chart_db_info['polarity']  # pct diff min/max
                # pctmedian = (reference.ma_series[last] - bdata.median) / reference.median * 100.  #pct diff median

                bdata.update(pctmadiff=pctmadiff, mnstddiff=mnstddiff)

                # no reference given, compare against self
                if branch not in references.keys():
                    references[branch] = dict()
                    if series not in references[branch].keys():
                        references[branch][series] = ""

                if pctmadiff <= -10.0:
                    result = "failed"
                    failurereasons.append("Current branch %s %s last run (%s) is worse then 10%% (%s) of the moving average (%s)" % (b, series, bdata.ydata[last], pctmadiff, reference.ama))
                elif mnstddiff <= -1.5:
                    result = "failed"
                    failurereasons.append("Current branch %s %s last run (%s) is worse then 1.5 deviations (%s)" % (b, series, bdata.ydata[last], mnstddiff))
                elif  pctmadiff > -10.0 and pctmadiff <= -5.0 and mnstddiff <= -1.5:
                    failurereasons.append("Warning, Current branch %s %s last run (%s) is worse then 5% (%s) of the moving average (%s_" % (b, series, bdata.ydata[last], pctmadiff, reference.ama))
                    result = "warning"

                if result != "ok":
                    bdata.update(failurereasons=failurereasons)
                    if result == "failed":
                        bdata.update(bgcolor='#eb9db3')  # light red
                    elif result == "warning":
                        bdata.update(bgcolor='#f0eec7')  # light yellow

                references[branch][series] = bdata

            bdata.update(result=result)
            if result != "ok":
                bdata.update(failed=result)

            group_bdata.append(bdata)


def generate_gplot_file(data, appgroup, srcbranch, referencebranch, failures, compareall=False):
    full_content = """
<html>
  <head>
    <title>Performance Graphs</title>
    <!--
    <meta http-equiv="content-security-policy"
  content="default-src 'none'; script-src 'self';
  connect-src 'self'; img-src 'self';
  style-src 'self' 'sha256-ReCElG+2hRA+QFh2U9nTT5bBBpEub2xMKJoVxytumOU=';" />
    -->
    <style>
    table, th, td {
      border-collapse: collapse;
    }
    th, td {
      padding-top: 1px;
      padding-bottom: 1px;
      padding-left: 1px;
      padding-right: 1px;
    }
    </style>
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script type="text/javascript">
    google.charts.load('current', {'packages':['line','corechart','controls']});


    google.charts.setOnLoadCallback(drawCharts);
    var srcbranch="%s"
    var appgroup="%s"
    function drawCharts() {
        var dashboard = new google.visualization.Dashboard(document.getElementById('dashboard_div'));
        var rangeSlider = new google.visualization.ChartWrapper({
            'controlType': 'NumberRangeFilter',
            'containerId': 'daterange_div',
            'options': {
              'filterColumnLabel': 'date',
            }
        });

        let plotdata = new Map()
        let optiondata = new Map()

        %s
        charts = []
        i=0;
        for (let [app,v] of plotdata) {
            for (let [chartname,v] of plotdata.get(app)) {

                // we have to use visualization.LineChart instead of charts.line because materialized charts.line
                // doesn't support interpolateNulls, so we get broken lines
                // we won't have graphs for some branches
                var elem = document.getElementById(app+"-"+chartname)
                if ( elem != null ) {
                    i += 0;
                    var chart = new google.visualization.LineChart(elem);
                    chart.draw(plotdata.get(app).get(chartname).get('data'), optiondata.get(app).get(chartname).get('options'));
                    charts.push(chart)
                    google.visualization.events.addListener(chart, 'select', function() {
                        // javascript tracks these by value, so we keep the values intack
                        c_chartname = chartname;
                        c_app = app;
                        // how to handle event data
                        // https://developers.google.com/chart/interactive/docs/events?csw=1
                        c_data = plotdata.get(c_app).get(c_chartname).get('data');
                        sel = charts[i].getSelection();
                        item = sel[0]
                        // we clicked on a legend
                        if (item.row == null && item.column != null ) {
                             var c = c_data.getColumnLabel(item.column);
                             window.location = c+"-"+appgroup+"-gplot.html";
                        }
                        // we clicked on a datapoint
                        if (item.row != null && item.column != null) {
                            var str = c_data.getFormattedValue(item.row, item.column);
                            var c = c_data.getColumnLabel(item.column);
                            //alert(str + "branch:" + c)
                            window.location = "https://github.com/VoltDB/internal/commits/" + c;
                        }
                    });
                }
            }
        }
        // bind the rangeSlider to all the charts
        //dashboard.bind(rangeSlider, charts);
    }
    </script>
  </head>

  <body>
    <center><h2>Branch %s Performance Group %s vs %s </h2></center>
    Generated on %s<br>
    <a href='https://wiki.voltdb.com/pages/viewpage.action?pageId=48824375'>Wiki Help</a><br/>
    <a href='./index.html#%s'>Back to Home</a><br/>
    <a href='./master-vs-world-%s-gplot.html'>Back to All Branch Overview</a><br/>
    <div id="dashboard_div"/></br>
    <div id="daterange_div"/><br/>
    <table frame="box" width="100%%">
%s
    </table>
  </body>
</html>
"""

    js = ""
    chartbody = ""
    htmlbody = ""
    for app in data.keys():
        my_appgroup = data[app]['appgroup']
        if appgroup != my_appgroup:
            continue

        chartrow = ""
        chartrow += "<tr><th align='left' colspan='3'><a name='%s'>%s</a></th></tr>\n" % (app, app)
        chartrow += "<tr>"
        for chart in data[app].keys():
            if chart == "appgroup":
                continue

            for branch in data[app][chart].keys():

                if data[app][chart][branch].appgroup != appgroup:
                    continue
                # if the current working branch is also the reference branch, show
                # all the lines for each branch
                if branch != srcbranch:
                    continue
                d = data[app][chart][branch]
                if len(d.failurereasons) > 0:
                    chartrow += "<td align='left' valign='top'>%s %s %s self/reference= ama:%.2f/%.2f ma.std:%.2f/%.2f<br/><b>%s</b></td>" % (app, chart, branch, d.ama, d.ref_ama, d.mastd, d.ref_mastd, d.failurereasons)
                else:
                    chartrow += "<td align='left' valign='top'>%s %s %s self/reference= ama:%.2f/%.2f ma.std:%.2f/%.2f</td>" % (app, chart, branch, d.ama, d.ref_ama, d.mastd, d.ref_mastd)

        chartrow += "</tr>\n"
        js += "optiondata.set('%s',new Map());\n" % app
        js += "plotdata.set('%s',new Map());\n" % app
        has_charts = False
        for chart in data[app].keys():

            if chart == "appgroup":
                continue

            if srcbranch not in data[app][chart].keys():
                continue

            chartrow += "<td border=1 align='left' ><div id='%s-%s'/></td>" % (app, chart)

            js += "plotdata.get('%s').set('%s',new Map());\n" % (app, chart)
            js += "optiondata.get('%s').set('%s',new Map());\n" % (app, chart)
            branch_list = []

            # if we need to show more then two lines i.e. master vs all the branches
            # do it here.
            for branch in data[app][chart].keys():
                if data[app][chart][branch].appgroup != appgroup:
                    continue

                # if the current working branch is also the reference branch, show
                # all the lines for each branch
                if not (branch == srcbranch or branch == referencebranch):
                    if not compareall:
                        continue

                #if compareall:
                #    print("adding compareall branch %s %s , app %s plot vs %s" % (branch, chart, app , srcbranch))

                # and branch != srcbranch and branch != referencebranch and srcbranch != referencebranch:
                #    continue
                # if not

                has_charts = True
                branch_list.append(branch)
                js += "// srcbranch: %s chart:%s app:%s branch: %s\n" % (srcbranch, chart, app , branch)
                js += "plotdata.get('%s').get('%s').set('%s',new Map());\n" % (app, chart, branch)
                js += " data = new google.visualization.DataTable();\n"
                js += " data.addColumn('date', 'Date');\n"
                js += " data.addColumn('number', '%s');\n" % branch

                js += " data.addRows([\n"
                xdata = data[app][chart][branch].xdata
                ydata = data[app][chart][branch].ydata
                c = 0
                for x in xdata:
                    y = ydata[c]
                    c += 1
                    js += "[new Date(%s*1000),%s],\n" % (x, y)

                js += "]);\n"
                js += "plotdata.get('%s').get('%s').get('%s').set('data',data);\n" % (app, chart, branch)

            js += "var joinedData = '';\n"
            if len(branch_list) > 0:
                js += "joinedData = plotdata.get('%s').get('%s').get('%s').get('data');\n" % (app, chart, branch_list[0])
            join_idx = []
            for b in range(1, len(branch_list)):
                join_idx.append(b)
                js += "moreData =   plotdata.get('%s').get('%s').get('%s').get('data');\n" % (app, chart, branch_list[b])
                js += "joinedData = google.visualization.data.join(joinedData, moreData, 'full', [[0, 0]], %s, [1]);\n" % join_idx

            # if srcbranch not in data[app][chart].keys():
            #    continue

            if  data[app][chart][srcbranch].bgcolor:
                bgcolor = data[app][chart][srcbranch].bgcolor
            else:
                bgcolor = '#ffffff'

            js += """
                var options = {
                title: "%s",
                chartArea: {'width': '75%%', 'height': '80%%', right:0},
                legend: {'position': 'bottom'},
                width: 600,
                height: 400,
                hAxis: {
                    format: 'M/d/yy',
                },
                vAxis: {
                    title: "%s"
                },

                interpolateNulls: true,
                backgroundColor: {
                    fill: '%s',
                    opacity: 5
                 },
            };\n
            """ % (data[app][chart][srcbranch].title, data[app][chart][srcbranch].ylabel, bgcolor)

            js += "\toptiondata.get('%s').get('%s').set('options',options);\n" % (app, chart)
            js += "\tplotdata.get('%s').get('%s').set('data',joinedData);\n" % (app, chart)

        if has_charts:
            chartrow += "</tr>\n"
            chartbody += chartrow

    htmlbody += "<table><tr><th colspan=2>Anomalies:</th></tr>"
    for app in failures.keys():

        if len(failures[app]) > 0:
            for f in failures[app]:
                htmlbody += "<tr><td><a href='#%s'>%s</a></td><td>%s</td></tr>" % (app, app, f)

    htmlbody += "</table>"
    htmlbody += "<table frame='box' width='100%%' border=1>"
    htmlbody += chartbody

    htmlbody += "</table>\n"

    return full_content % (srcbranch, appgroup, js, srcbranch, appgroup, REFERENCE_BRANCH, time.strftime("%Y/%m/%d %H:%M:%S"), srcbranch, appgroup, htmlbody)


def moving_average(iseries, window_size):
    """
    compute an window_size period moving average.

    type is 'simple' | 'exponential'

    """
    series = iseries[:]
    series = np.asarray(series)
    slength = len(series)

    if slength < window_size / 3:
        return [NaN], NaN, [NaN], NaN
    elif slength > window_size / 3 and slength < window_size:
        window_size = slength

    # make sure we have an array the same size as window_size
    if slength > window_size:
        ma_series = series[-window_size:]
    else:
        ma_series = series
    # compute the mean of the moving average population
    # over the most recent ma group
    ma_avg = np.average(ma_series)

    # compute the standard deviation of the set of data
    # corresponding to each moving average window

    # no idea why it's done this way, its the same as doing a np.std
    ma_std_series = [NaN] * (window_size - 1)
    for d in range(0, len(ma_series) - window_size + 1):
        ma_std_series.append(np.std(ma_series[d: d + window_size]))

    # also compute the standard deviation of the moving avg
    # all available data points. scalar result
    ma_std = np.std(series)

    if len(ma_series) != len(ma_std_series):
        assert("AAHH OOH, size doesn't match")
    return (ma_series, ma_avg, ma_std_series, ma_std)


def usage():
    print("Usage:")
    print("\t", sys.argv[0], "output_dir filename_base [ndays]" \
                             " [width] [height]")
    print()
    print("\t", "width in pixels")
    print("\t", "height in pixels")


def main():
    if len(sys.argv) < 3:
        usage()
        exit(-1)

    if not os.path.exists(sys.argv[1]):
        print(os.getcwd())
        print(sys.argv[1], "does not exist")
        os.mkdir(sys.argv[1])
        # exit(-1)

    # NOT USED, but need for compatibility with older jenkins jobs

    ROOT_DIR=sys.argv[1]+"/"
    ndays = 2000
    if len(sys.argv) >= 3:
        ndays = int(sys.argv[2])
    if len(sys.argv) >= 4:
        global REFERENCE_BRANCH
        REFERENCE_BRANCH = str(sys.argv[3])
    width = 600
    height = 400
    if len(sys.argv) >= 5:
        width = int(sys.argv[4])
    if len(sys.argv) >= 6:
        height = int(sys.argv[5])

    # show all the history
    branch_list = get_branches(STATS_SERVER, 21212, ndays)
    # print(branches)
    (stats, mindate, maxdate) = get_stats(STATS_SERVER, 21212, ndays)
    mindate = (mindate).replace(hour=0, minute=0, second=0, microsecond=0)
    maxdate = (maxdate + datetime.timedelta(days=1)).replace(minute=0, hour=0, second=0, microsecond=0)

    filenames = []  # (appname, latency, throughput)
    iorder = 0

    analyze = []

    # Group is the APP_NAME and number of nodes, data is the raw data from the database
    group_bdata = []
    # Groups contain all the data fr
    for group, data in stats.items():
        print("group: %s" % list(group))
        (study, nodes) = group
        study = study.replace('/', '')
        # if you just want to test one study, put it's name here and uncomment...
        # if "Voter" not in study:
        #    continue

        conn = FastSerializer(STATS_SERVER, 21212)
        proc = VoltProcedure(conn, "@AdHoc", [FastSerializer.VOLTTYPE_STRING])
        resp = proc.call(["select chart_order, series, chart_heading, x_label, y_label, polarity, appgroup from charts where appname = '%s' order by chart_order" % study])
        conn.close()

        app = study + " %d %s" % (nodes, ["node", "nodes"][nodes > 1])

        # chart polarity: -1 for tps (decreasing is bad), 1 for latencies (increasing is bad)
        appgroup = study.split("-")[0]

        # Default chart titles
        legend = {1: dict(series="lat95", heading="95tile latency", xlabel="Time", ylabel="Latency (ms)", polarity=1, appgroup=appgroup),
                  2: dict(series="lat99", heading="99tile latency", xlabel="Time", ylabel="Latency (ms)", polarity=1, appgroup=appgroup),
                  3: dict(series="tppn", heading="avg throughput per node", xlabel="Time", ylabel="ops/sec per node",
                          polarity=-1, appgroup=appgroup)
                  }

        # Get legend overrides from the DB
        for r in resp.tables[0].tuples:
            if r[6] != "OTHER":
                appgroup = r[6]

            legend[r[0]] = dict(series=r[1], heading=r[2], xlabel=r[3], ylabel=r[4], polarity=r[5], appgroup=appgroup)

        for r in list(legend.values()):
            title = app + " " + r['heading']
            r['title'] = title

            bdata = gather_plot_bdata(group_bdata, app, data, appgroup, r['series'], r)
            # group_bdata.extend(aanalyze)

    # filenames.append(("KVBenchmark-five9s-nofail-latency", "", "",
    #                  "http://ci/job/performance-nextrelease-5nines-nofail/lastSuccessfulBuild/artifact/pro/tests/apptests/savedlogs/5nines-histograms.png",
    #                  iorder))
    # filenames.append(("KVBenchmark-five9s-nofail-nocl-latency", "", "",
    #                  "http://ci/job/performance-nextrelease-5nines-nofail-nocl/lastSuccessfulBuild/artifact/pro/tests/apptests/savedlogs/5nines-histograms.png",
    #                  iorder))
    # filenames.append(("KVBenchmark-five9s-nofail-nocl-kvm-latency", "", "",
    #                  "http://ci/job/performance-nextrelease-5nines-nofail-nocl-kvm/lastSuccessfulBuild/artifact/pro/tests/apptests/savedlogs/5nines-histograms.png",
    #                  iorder))
    # filenames.append(("Openet-Shocker-three9s-latency", "", "",
    #                  "http://ci/job/performance-nextrelease-shocker/lastSuccessfulBuild/artifact/pro/tests/apptests/savedlogs/3nines-histograms.png",
    #                  iorder))
    # filenames.append(("Openet-Shocker-three9s-4x2-latency", "", "",
    #                  "http://ci/job/performance-nextrelease-shocker-4x2/lastSuccessfulBuild/artifact/pro/tests/apptests/savedlogs/3nines-histograms.png",
    #                  iorder))

    print("rootdir:%s" % ROOT_DIR)

    # for the gplot graphs, we need to organize them by
    # [app][chart][branch]
    sorted_bdata = dict()
    appgroups = set()
    for bdata in group_bdata:
        appgroups.add(bdata.appgroup)
        if not isinstance(bdata, Bdata):
            continue
        if bdata.app not in sorted_bdata.keys():
            sorted_bdata[bdata.app] = dict()
        if bdata.chart not in sorted_bdata[bdata.app]:
            sorted_bdata[bdata.app][bdata.chart] = dict()

        if bdata.branch not in sorted_bdata[bdata.app][bdata.chart].keys():
            sorted_bdata[bdata.app][bdata.chart][bdata.branch] = bdata
        # create a shortcut so we don't have drill down to the branch to get this info
        sorted_bdata[bdata.app]['appgroup'] = bdata.appgroup

    rollupgraph = "master-vs-world"
    branch_list.insert(0, rollupgraph)
    with open(ROOT_DIR + "index.html", 'w') as sorted_file:
        sorted_file.write("<html><head><title>Performance Graphs</title></head><body>")
        sorted_file.write("<center><h2>Performance Graphs , referenced against %s</h2></center>" % REFERENCE_BRANCH)
        sorted_file.write("<p>generated on: %s</p>" %  time.strftime("%Y/%m/%d %H:%M:%S"))
        sorted_file.write("<h4><a href='https://wiki.voltdb.com/pages/viewpage.action?pageId=48824375'>Wiki Help</a></h4>")
        # write out the Table of Contents
        c = 1
        sorted_file.write("<table>")
        for b in branch_list:
            if c % 5 == 1:
                sorted_file.write("<tr>")
            else:
                sorted_file.write("<td><a href='#%s'>%s</a></td>" % (b, b))

            if c % 5 == 0:
                    sorted_file.write("</tr>\n")
            c += 1
        sorted_file.write("</table>\n")
        sorted_file.write("<table>")

        do_rollupgraph = False
        for b in branch_list:

            sorted_file.write("<tr><th colspan=3><a name='%s'>branch: %s</a></th></tr>\n" % (b, b))
            print("branch %s" % b)
            if b == rollupgraph:
                do_rollupgraph = True
                b = "master"
            else:
                do_rollupgraph = False

            c = 1
            # track failures per appgroup and branch
            appgroup_failures = dict()
            appgroup_results = dict()
            for appgroup in appgroups:
                # did any tests fail in this appgroup?
                results = 0
                failures = 0
                warnings = 0
                appgroup_failures[appgroup] = dict()
                for app in sorted_bdata.keys():

                    for chart in sorted_bdata[app].keys():
                        if chart == "appgroup":
                            continue

                        for branch in sorted_bdata[app][chart].keys():
                            if b != branch:
                                continue
                            if b not in appgroup_failures[appgroup].keys():
                                appgroup_failures[appgroup][b] = dict()
                            if app not in appgroup_failures[appgroup][b].keys():
                                appgroup_failures[appgroup][b][app] = []
                            if sorted_bdata[app][chart][branch].appgroup == appgroup:
                                results += 1
                                if sorted_bdata[app][chart][branch].result == "failed":
                                    failures += 1
                                    appgroup_failures[appgroup][branch][app].append(sorted_bdata[app][chart][branch].failurereasons)
                                elif sorted_bdata[app][chart][branch].result == "warning":
                                    warnings += 1
                                    appgroup_failures[appgroup][branch][app].append(sorted_bdata[app][chart][branch].failurereasons)
                        # appgroup_failures.extend(sorted_bdata[app][chart][branch].failurereasons)

                # print("app %s chart %s branch %s" % (app, chart, branch))

                # make a 3 column table with failure data and links to the app group page
                plotgroup = b
                if do_rollupgraph:
                    plotgroup = rollupgraph
                if c % 3 == 1:
                    sorted_file.write("<tr>")
                if results == 0:
                    sorted_file.write("<td><a name='%s-%s'/><a href='./%s-%s-gplot.html'><font color='orange'>%s (%s results)</a></font></td>" % (b, appgroup, plotgroup, appgroup, appgroup, failures))
                elif failures == 0 and warnings == 0:
                    sorted_file.write("<td><a name='%s-%s'/><a href='./%s-%s-gplot.html'>%s</a></td>" % (b, appgroup, plotgroup, appgroup, appgroup))
                elif failures > 0:
                    sorted_file.write("<td><a name='%s-%s'/><a href='./%s-%s-gplot.html'><font color='red'>%s (%s failures) (%s warnings) </a></font></td>" % (b, appgroup, plotgroup, appgroup, appgroup, failures, warnings))
                elif warnings > 0:
                    sorted_file.write("<td><a name='%s-%s'/><a href='./%s-%s-gplot.html'><font color='#FF9999'>%s (%s failures) (%s warnings) </a></font></td>" % (b, appgroup, plotgroup, appgroup, appgroup, failures, warnings))

                if c % 3 == 0:
                    sorted_file.write("</tr>\n")

                c += 1

                if b not in appgroup_failures[appgroup].keys():
                    appgroup_failures[appgroup][b] = dict()
                # special master plot
                if do_rollupgraph:
                    with open("%s/%s-%s-gplot.html" % (ROOT_DIR, plotgroup, appgroup), 'w') as branch_gplot_master_file:
                        branch_gplot_master_file.write(generate_gplot_file(sorted_bdata, appgroup, "master", "master", appgroup_failures[appgroup]["master"], compareall=True))
                        branch_gplot_master_file.close()
                else:
                    with open("%s/%s-%s-gplot.html" % (ROOT_DIR, plotgroup, appgroup), 'w') as branch_gplot_file:
                        branch_gplot_file.write(generate_gplot_file(sorted_bdata, appgroup, b, REFERENCE_BRANCH, appgroup_failures[appgroup][b] ))
                        branch_gplot_file.close()

        sorted_file.write("</table>")

    # sorted_file.close()


if __name__ == "__main__":
    main()
