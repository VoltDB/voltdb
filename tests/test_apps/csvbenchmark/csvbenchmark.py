#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2015 VoltDB Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

from optparse import OptionParser
from random import randint
import os
import sys
import re
from numpy import *
import random
from subprocess import Popen,PIPE
import shlex
import datetime
from voltdbclient import FastSerializer, VoltProcedure
import time

CSVLOADER = "bin/csvloader"
#SQLCMD = "$VOLTDB_HOME/bin/sqlcmd --servers=%s" % servers

# declare cases impmeneted and the data generator code
CASES = {
    "narrow_short_noix"          : "data_narrow_short",
    "narrow_short_ix"            : "data_narrow_short",
    "narrow_short_cmpix"         : "data_narrow_short",
    "narrow_short_hasview"       : "data_narrow_short",
    "narrow_long_noix"           : "data_narrow_long",
    "narrow_long_ix"             : "data_narrow_long",
    "narrow_long_cmpix"          : "data_narrow_long",
    "narrow_long_hasview"        : "data_narrow_long",
    "generic_noix"               : "data_generic",
    "generic_ix"                 : "data_generic",
    "replicated_pk"              : "data_replicated_pk",
    }

def list_cases():
    print "List of implemented csvloader cases:\n"
    for k in sorted(CASES.keys()):
        print "\t%s" % k

# build the reference character set
# user all possible unicode-16 codes (first code page 0000-ffff)
UNICODE_CHARSET = ""
#for c in range(32,64*1024):
for c in range(32,127):
    # 0-31 control chars
    # 34 "
    # 36 $
    # 37 %
    # 38 &
    # 39 '
    # 44 , reserved as field separator
    # 91 [
    # 92 \ just avoid it
    # 93 ]
    # 94 ^ quote reserved for loader
    # 95 _ 37 % for LIKE % bbi escape doesn't work
    # 96 `
    # 123 {
    # 124 | reserved as field separator
    # 125 }
    # 126 ~
    # 127 DLE
    if not (c==44 or c==127):
        UNICODE_CHARSET += unichr(c)
ESCAPE_CHAR="\\"
QUOTE_CHAR="\""
UNICODE_CHARSET_MINUS_QUOTE_CHAR = UNICODE_CHARSET.replace(QUOTE_CHAR, "")
UNICODE_CHARSET_MINUS_WHITESPACE_CHARS = UNICODE_CHARSET.replace(" \t\n","")
NUMERIC_CHARSET="0123456789"

# XXX not yet handling leading/trailing zeroes and many other
# cases which are useful in testing, but this is not a test it is a benchmark.

def gentext(size):
    r = ''.join(random.sample(UNICODE_CHARSET, len(UNICODE_CHARSET)))
    s = r * int(size/len(r)) + r[:size%len(r)]
    m = re.match(r'(.*)([ \t\n]+)$', s)
    if m:
        s = m.group(1) + ''.join(random.sample(UNICODE_CHARSET_MINUS_WHITESPACE_CHARS, len(m.group(2))))
    s = s.replace(QUOTE_CHAR, QUOTE_CHAR+QUOTE_CHAR)[:size]
    if (len(s) == 1 and s[0] == QUOTE_CHAR) or (len(s) > 1 and s[-1] == QUOTE_CHAR and s[-2] != QUOTE_CHAR):
        s = s[:-1] + random.choice(UNICODE_CHARSET_MINUS_QUOTE_CHAR)
    assert len(s) == size
    return QUOTE_CHAR + s[:size] + QUOTE_CHAR

def genfixeddecimalstr(size=38, precision=12, signed=True):
    # voltdb decimal is 16-byte with fixed scale of 12 and precision of 38
    p = -1*precision
    r = ''.join(random.sample(NUMERIC_CHARSET, len(NUMERIC_CHARSET)))
    r = r * int(size/len(r)) + r[:size%len(r)]
    if (p>0):
        r = r[:p] + '.' + r[p:]
    if signed:
        r = random.choose(["-","+",""]) + r
    return r

def gencurrency(size=16, precision=4):
    c = genfixeddecimalstr(size, precision)
    curr = re.match(r'^0*(\d+\.*\d+)0*$', c)
    print curr.group(1)
    return curr.group(1)

def genint(size):
    if size == 1:
        return randint(-2**7+1, 2**7-1)
    elif size == 2:
        return randint(-2**15+1, 2**15-1)
    elif size == 4:
        return randint(-2**31+1, 2**31-1)
    elif size == 8:
        return randint(-2**63+1, 2**63-1)
    else:
        raise RuntimeError ("invalid size for integer %d" % size)

def gennumsequence(__seq):
    # pass in a list of on one number
    assert (isinstance(__seq, list) and len(__seq) == 1)
    __seq[0] += 1
    return __seq[0]

def gentimestamp():
    return datetime.datetime.today().strftime('"%Y-%m-%d %H:%M:%S"')

def gendouble():
    return random.random() * genint(4)

def run_readlines(cmd):
    fd = os.popen(cmd)
    result = fd.read()
    #print result
    fd.close()
    return result

def run_csvloader(schema, data_file):
    rowcount = options.ROW_COUNT
    elapsed_results = []
    parsing_results = []
    loading_results = []
    for I in range(0, options.TRIES):
        home = os.getenv("VOLTDB_HOME")
        before_row_count = get_table_row_count(schema)
        cmd = "%s --servers=%s" % (os.path.join(home, CSVLOADER), ','.join(options.servers))
        if options.csvoptions:
            cmd += " -o " + ",".join(options.csvoptions)
        cmd += " %s -f %s" % (schema, data_file)
        if options.VERBOSE:
            print "starting csvloader with command: " + cmd
        start_time = time.time()
        p = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
        (stdout, stderr) = p.communicate()
        run_time = time.time() - start_time
        stdout_lines = stdout.split('\n')
        if options.VERBOSE:
            for l in stdout_lines:
                print '[csvloader stdout] ' + l
        rc = p.returncode
        actual_row_count = get_table_row_count(schema)
        if rc != 0:
            print "CSVLoader failed with rc %d" % rc
            for l in stderr.split('\n'):
                print '[csvloader stderr] ' + l
            raise RuntimeError ("CSV Loader failed")
        # XXX seems that csvloader doesnt always returncode nonzero if it fails to load rows
        m = re.search(r'^Read (\d+) rows from file and successfully inserted (\d+) rows \(final\)$',
                        stdout, flags=re.M)
        if m is None or int(m.group(1)) != rowcount or m.group(1) != m.group(2):
            raise RuntimeError ("CSV Loader failed to load all rows")
        if int(before_row_count) + rowcount != int(actual_row_count):
            raise RuntimeError ("Actual table row count was not as expected exp:%d act:%d" % (rowcount,actual_row_count))
        elapsed_results.append(float(run_time))

    def analyze_results(perf_results):
        #print "raw perf_results: %s" % perf_results
        pr = sorted(perf_results)[1:-1]
        if len(pr) == 0:
            pr = perf_results
        return (average(pr), std(pr))

    avg, stddev = analyze_results(elapsed_results)
    print "statistics for %s execution time avg: %f stddev: %f rows/sec: %f rows: %d file size: %d tries: %d" %\
                 (schema, avg, stddev, rowcount/avg, rowcount, os.path.getsize(data_file), options.TRIES)
    if options.statsfile:
        with open(options.statsfile, "a") as sf:
            # report duration in milliseconds for stats collector
            print >>sf, "%s,%f,%d,0,0,0,0,0,0,0,0,0,0" % (schema, avg*1000.0, rowcount)
    return (rowcount, avg, stddev)

def get_table_row_count(table_name):
    host = random.choice(options.servers)
    pyclient = FastSerializer(host=host, port=21212)
    count = VoltProcedure(pyclient, '@AdHoc', [FastSerializer.VOLTTYPE_STRING])
    resp = count.call(['select count(*) from %s' % table_name], timeout=360)
    if resp.status != 1 or len(resp.tables[0].tuples) != 1:
        print "Unexpected response to count query from host %s: %s" % (host, resp)
        raise RuntimeError()
    __tuples = resp.tables[0].tuples[0]
    result = __tuples[0]
    print "count query returned: %s" % result
    return result

def get_datafile_path(case):
    return os.path.join(DATA_DIR, "csvbench_%s_%d.dat" % (case, options.ROW_COUNT))

def get_filesize(file):
    return int(run_readlines("wc -l %s" % file).split(' ')[0])

def list_callback (option, opt, value, parser):
    """split the list of strings and store it in the parser options """
    setattr(parser.values, option.dest, value.split(','))

def parse_cmdline():

    global options, args, DATA_DIR

    usage = "usage: %prog [options] path-to-loadfiles"
    parser = OptionParser()

    parser.add_option ("-s", "--servers",
                            type = "string",
                            action = "callback", callback = list_callback,
                            default=["localhost"],
                            help ="list of servers")

    # WNG Don't run more than one case at a time in apprunner if collecting stats
    parser.add_option ("-c", "--case",
                            type = "string",
                            action = "callback", callback = list_callback,
                            default=None,
                            help ="comma separate list of cases to run")

    parser.add_option ("-n", "--rows",
                            type = "int",
                            dest = "ROW_COUNT",
                            default = 100000,
                            help ="number of rows to test")

    parser.add_option ("-r", "--regeneratedata",
                            dest = "REGENERATE",
                            action="store_true", default=False,
                            help ="regenerate the data'")

    parser.add_option ("-t", "--tries",
                            type = "int",
                            dest = "TRIES",
                            default = 1,
                            help ="number of time to run the test case and average the performance results")

    parser.add_option ("-o", "--csvoptions",
                            type = "string",
                            action = "callback", callback = list_callback,
                            default=None,
                            help ="comma separated list of options to be passed to the csvloader")

    parser.add_option ("-v", "--verbose",
                            dest = "VERBOSE",
                            action="store_true", default=False,
                            help ="print csv output'")

    parser.add_option ("-l", "--list",
                            dest = "LIST",
                            action="store_true", default=False,
                            help ="list cases supported and exit'")

    parser.add_option ("--statsfile",
                            type = "string",
                            dest = "statsfile",
                            default=None,
                            help ="file to write statistics for apprunner")

    (options, args) = parser.parse_args()

    if options.LIST:
        list_cases()
        sys.exit(0)

    if len(args) < 1:
        print "ERROR load file directory not specified"
        sys.exit(1)

    DATA_DIR = args[0]
    if not os.path.isdir(DATA_DIR):
        print "ERROR load file directory does not exist, or is not a directory"
        sys.exit(1)

    if options.statsfile:
        f = open(options.statsfile, 'w')
        f.close


def data_narrow_short(rebuild=False):
    data_file = get_datafile_path("narrow_short")
    if rebuild or not os.path.exists(data_file):
        with open(data_file, "w") as f:
            for I in range(0, options.ROW_COUNT):
                print >>f, "%d,%d,%d,%d,%s" % (I, genint(2), genint(1), genint(8), gentext(60))
        print "data file %s was written" % data_file
    return data_file


def data_narrow_long(rebuild=False):
    data_file = get_datafile_path("narrow_long")
    if rebuild or not os.path.exists(data_file):
        with open(data_file, "w") as f:
            for I in range(0, options.ROW_COUNT):
                print >>f, "%d,%d,%d,%d,%s" % (I, randint(-32766,32767),randint(-127,127),randint(-2**63,2**63),gentext(512))
        print "data file %s was written" % data_file
    return data_file


def data_generic(rebuild=False):
    """
    a integer NOT NULL
        , b tinyint
        , c smallint
        , d varchar(1)
        , e timestamp
        , f timestamp
        , h varchar(60)
        , i varchar(60)
        , j varchar(60)
        , k varchar(1024)
        , l varchar(1024)
        , m varchar(1024)
        , n double
        , o bigint
        , p varchar(1)
        , r bigint
      a integer NOT NULL
    , b tinyint
    , c smallint
    , d varchar(1)
    , e timestamp
    , f timestamp
    , h varchar(60)
    , i varchar(60)
    , j varchar(60)
    , k varchar(1024)
    , l varchar(1024)
    , m varchar(1024)
    , n float
    , o bigint
    , p varchar(1)
    , r bigint
    , s decimal(32,4)
    , t decimal(32,4)
    , u decimal(32,4)
    """

    case = "generic"
    data_file = get_datafile_path(case)
    if rebuild or not os.path.exists(data_file) or get_filesize(data_file) != options.ROW_COUNT:
        with open(data_file, "w") as f:
            for I in range(0, options.ROW_COUNT):
                    print >>f, "%d,%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%f,%d,%s,%d" \
                                      %  ( I,
                                          genint(1),
                                          genint(2),
                                          gentext(1),
                                          gentimestamp(),
                                          gentimestamp(),
                                          gentext(60),
                                          gentext(60),
                                          gentext(60),
                                          gentext(1024),
                                          gentext(1024),
                                          gentext(1024),
                                          gendouble(),
                                          genint(8),
                                          gentext(1),
                                          genint(8)
                                         )
        print "data file %s was written" % data_file
    return data_file

def case_generic_noix():
    schema = "generic_noix"
    data_file = data_generic(False)
    run_csvloader(schema, data_file)

def data_replicated_pk(rebuild=False):
    data_file = get_datafile_path("replicated_pk")
    if rebuild or not os.path.exists(data_file) or get_filesize(data_file) != options.ROW_COUNT:
        myseq = [0]
        with open(data_file, "w") as f:
            for I in range(0, options.ROW_COUNT):
                print >>f, "%d,%s,%s,%s,%s,%s" % (gennumsequence(myseq),
                                                    gentext(60),
                                                    gentext(1024),
                                                    gentimestamp(),
                                                    gentext(30),
                                                    genfixeddecimalstr(size=1, precision=0, signed=False)
                                                    )
        print "data file %s was written" % data_file
    return data_file

parse_cmdline()

cases = options.case or CASES.keys()

for schema in cases:
    if schema not in CASES:
        print "ERROR unknown case: %s" % c
        print list_cases()
        sys.exit(1)

    data_file = globals()[CASES[schema]](options.REGENERATE)
    run_csvloader(schema, data_file)
