#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.

import sys
from datetime import datetime
import traceback
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as dt
import matplotlib.ticker as ticker
import matplotlib.dates as md

def plot_graph(table_name):
    try:
        primary_xdcr_report = './voltxdcr1/xdcr_conflicts/csv_xdcr_' + table_name
        df = pd.read_csv(primary_xdcr_report, sep=':', header=None)
        df.columns = ['cid', 'start', 'end', 'type']
        df.start = df.start - 14400000000 # hack for now for numpy datetime64 issue
        df.start = pd.to_datetime(df.start, unit='us').astype(datetime)
        df.end = df.end - 14400000000
        df.end = pd.to_datetime(df.end, unit='us').astype(datetime)

        secondary_xdcr_report = './voltxdcr2/xdcr_conflicts/csv_xdcr_' + table_name
        df2 = pd.read_csv(secondary_xdcr_report, sep=':', header=None)
        df2.columns = ['cid', 'start', 'end', 'type']
        df2.start = df2.start - 14400000000
        df2.start = pd.to_datetime(df2.start, unit='us').astype(datetime)
        df2.end = df2.end - 14400000000
        df2.end = pd.to_datetime(df2.end, unit='us').astype(datetime)

        color_mapper = np.vectorize(lambda x: {'IICV': 'r', 'IUCV': 'g', 'UUCV': 'b', 'UUTM': 'c', 'UDTM': 'm', 'DDMR': 'y'}.get(x))

        fig = plt.figure()
        ax = fig.add_subplot(111)

        ax.set_title(table_name.title() + " XDCR Conflict Time Series")
        ax.xaxis_date()
        xfmt = md.DateFormatter('%H:%M:%S')
        ax.xaxis.set_major_formatter(xfmt)
        ax.yaxis.set_major_locator(ticker.MultipleLocator(1))
        ax.set_xlabel('time')
        ax.set_ylabel('Client ID')

        plt.hlines(df.cid, dt.date2num(df.start), dt.date2num(df.end), colors=color_mapper(df.type), lw=2)
        plt.hlines(df2.cid + 0.5, dt.date2num(df2.start), dt.date2num(df2.end), colors=color_mapper(df2.type), lw=2)

        plt.show()
    except Exception as ex:
        print 'Error in  plot_graph', ex
        traceback.print_exc()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.stderr.write('Usage: sys.argv[0] <partitioned | replicated>\n')
        sys.exit(1)

    plot_graph(sys.argv[1])
