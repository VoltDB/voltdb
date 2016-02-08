#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.
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

import sys
import os
import errno
from subprocess import Popen, PIPE
import tempfile
import shutil

"""
Preprocess and modify properties for ycsb

Support a new ycsb property voltdb.scalefactor and voltdb.recordsize
outside of ycsb base permitting scaling ycsb rowcounts relative to
available memory for a voltdb database.

voltdb.scalefactor: floatpoint value > 0, sf > 1.0 overcommits memory
voltdb.recordsize:  recordsize of voltdb schema

Launches ycsb with modified propertie(s) file(s)
"""


def call_sqlcmd(command, server):

    voltdb_home = os.environ['VOLTDIST']
    cmd = 'echo "%s" | %s/sqlcmd --servers=%s --output-format=csv --output-skip-metadata' % (command, voltdb_home+'/bin', server)
    sp = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
    stdout, stderr = sp.communicate()
    if sp.returncode != 0:
        print 'voltdb command failed: %s' % cmd
        print stderr
        sys.exit(-1)
    return stdout


def get_system_information(prop, server):

    result = call_sqlcmd("exec @SystemInformation DEPLOYMENT", server)
    for r in result.split('\n'):
        try:
            print r
            z = r.split('"')
            if z[1] == prop:
                return z[3]
        except:
            pass
    raise Exception("property '%s' not found" % prop)


if __name__ == "__main__":

    voltdb_rowsize = None
    voltdb_scalefactor = None
    recordcount = None

    pristine = dict()   # properties frozen set copy
    properties = dict() # properties working dir

    for p in range(0, len(sys.argv)):
        if sys.argv[p] == '-P':
            prop = sys.argv[p+1]
            print prop
            pd = dict()
            with open(prop, 'r') as f:
                for line in f.readlines():
                    l = line.strip()
                    if len(l)==0 or l[0] == '#':
                        continue
                    k,v = l.split('=')
                    pd[k] = v
            pristine[prop] = frozenset(pd.items())
            properties[prop] = pd

            # pull out stuff we'll need later
            if 'recordcount' in pd.keys():
                recordcountIsIn = pd
                recordcount = int(pd['recordcount'])
            if 'voltdb.servers' in pd.keys():
                servers = pd['voltdb.servers'].split(',')
            if 'voltdb.rowsize' in pd.keys():
                voltdb_rowsize = int(pd['voltdb.rowsize'])
                del pd['voltdb.rowsize']
            if 'voltdb.scalefactor' in pd.keys():
                voltdb_scalefactor = float(pd['voltdb.scalefactor'])
                del pd['voltdb.scalefactor']

        # TODO: XXX/PSR also need to handle above properties via -p (solo properties) too???
        elif sys.argv[p] == '-p':
            prop = sys.argv[p+1]
            k,v = prop.split('=')
            if k == 'voltdb.servers':
                servers = v.split(',')

    # validate inputs
    if bool(voltdb_rowsize is None) ^ bool(voltdb_scalefactor is None):
        print "ERROR both of voltdb.rowsize and voltdb.scalefactor required to project rowcount"
        sys.exit(1)

    # if both of these properties are not specified just invoke ycsb with unmodified parameters/properties
    if voltdb_rowsize is not None and voltdb_scalefactor is not None:

        if voltdb_rowsize <= 0:
            print "ERROR voltdb.rowsize must be > 0"
            sys.exit(1)

        if voltdb_scalefactor <= 0:
            print "ERROR voltdb.rowsize must be > 0"
            sys.exit(1)

        # get phys memory installed on one server
        # assume servers are symmetrical
        physical_memory = int(call_sqlcmd("exec @Statistics MEMORY 0", servers[0]).split('"')[-4]) * 1024

        # get kfactor of cluster
        kfactor = int(get_system_information("kfactor", servers[0]))

        print "INFO recomputing recordcount -- voltdb memory: %s, kfactor %s, scalefactor: %s (<=1.0 = inmemory)" %\
              (physical_memory, kfactor, voltdb_scalefactor)

        # maybe a close enough approximation of the correct value
        scaled_recordcount = int(round(float(len(servers) * physical_memory * voltdb_scalefactor)
                                       / (kfactor+1) / voltdb_rowsize))
        # reset the property
        recordcountIsIn['recordcount'] = scaled_recordcount

        print "INFO recordcount property has been (re)computed to %d" % scaled_recordcount

    try:
        tempdir = tempfile.mkdtemp()
    except:
        print "ERROR failed creating temporary directory"
        raise

    try:
        # write changed properties files
        cmd = []
        p = 1
        while p < len(sys.argv) :
            if sys.argv[p] == '-P':
                props = sys.argv[p+1]
            if sys.argv[p] == '-P' and not frozenset(properties[props].items()) == frozenset(pristine[props]):
                nfn = os.path.join(tempdir, os.path.basename(props))
                print "rewriting properties to %s:" % nfn
                with open(nfn, 'w') as f:
                    for k,v in properties[props].iteritems():
                        f.write("%s=%s\n" % (k,v))
                        print "\t%s=%s" % (k,v)
                cmd.append(sys.argv[p])
                cmd.append(nfn)
                p+=2
            else:
                cmd.append(sys.argv[p])
                p+=1

        cmdline = " ".join(cmd)

        print "Starting YCSB..."
        ycsb = None
        ycsb = Popen(cmdline, shell=True)
        ycsb.wait()

    finally:
        try:
            shutil.rmtree(tempdir)  # delete temporary directory

        except OSError as exc:
            if exc.errno != errno.ENOENT:  # ENOENT - no such file or directory
                raise  # re-raise exception

    if ycsb:
        sys.exit(ycsb.returncode)
    else:
        sys.exit(-1)




