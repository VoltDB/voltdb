# This file is part of VoltDB.

# Copyright (C) 2008-2011 VoltDB Inc.
#
# This file contains original code and/or modifications of original code.
# Any modifications made by VoltDB Inc. are licensed under the following
# terms and conditions:
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

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

# Provides global meta-data that is derived from the environment.

__author__ = 'scooper'

import os
import glob
import re
import shlex

import vcli_util
import vcli_meta

# Java configuration
if 'JAVA_HOME' in os.environ:
    java = os.path.join(os.environ['JAVA_HOME'], 'java')
else:
    java = vcli_util.find_in_path('java')
if not java:
    vcli_util.abort('Could not find java in environment, set JAVA_HOME or put java in the path.')
java_opts = []
if 'JAVA_HEAP_MAX' in os.environ:
    java_opts.append(os.environ.get('JAVA_HEAP_MAX'))
if 'VOLTDB_OPTS' in os.environ:
    java_opts.extend(shlex.split(os.environ['VOLTDB_OPTS']))
if 'JAVA_OPTS' in os.environ:
    java_opts.extend(shlex.split(os.environ['JAVA_OPTS']))
if not [opt for opt in java_opts if opt.startswith('-Xmx')]:
    java_opts.append('-Xmx1024m')

# VoltDB jar file (filled in during startup)
re_voltdb_jar = re.compile('^voltdb-2[.0-9]+[.]jar$')
voltdb_jar = None

# Classpath (filled in during startup)
classpath = None

def initialize():
    """Set the VOLTDB_HOME, VOLTDB_LIB and VOLTDB_VOLTDB environment variables
    based on the location of this script."""

    dir = vcli_meta.mydir

    required_var_set = set(['VOLTDB_HOME', 'VOLTDB_LIB', 'VOLTDB_VOLTDB'])

    # Return true if all needed VOLTDB_... environment variables are set.
    def env_complete():
        return set(os.environ.keys()).issuperset(required_var_set)

    # Crawl upward and look for the home, lib and voltdb directories.
    # They may be the same directory when installed by a Linux installer.
    # Set the VOLTDB_... environment variables accordingly.
    # Also locate the voltdb jar file.
    while (dir != '/' and not env_complete()):

        vcli_util.debug('Checking potential VoltDB root directory: %s' % dir)

        # Try to set VOLTDB_HOME if not set.
        if not os.environ.get('VOLTDB_HOME', ''):
            for subdir in ('', os.path.join('share', 'voltdb')):
                for chk in ('Click Here to Start.html', 'examples', 'third_party'):
                    if not os.path.exists(os.path.join(dir, subdir, chk)):
                        break
                else:
                    os.environ['VOLTDB_HOME'] = os.path.realpath(os.path.join(dir, subdir))
                    vcli_util.debug('VOLTDB_HOME=>%s' % os.environ['VOLTDB_HOME'])

        # Try to set VOLTDB_LIB if not set.
        if not os.environ.get('VOLTDB_LIB', ''):
            for subdir in ('lib', os.path.join('lib', 'voltdb')):
                if glob.glob(os.path.join(os.path.join(dir, subdir), 'zmq*.jar')):
                    os.environ['VOLTDB_LIB'] = os.path.realpath(os.path.join(dir, subdir))
                    vcli_util.debug('VOLTDB_LIB=>%s' % os.environ['VOLTDB_LIB'])

        # Try to set VOLTDB_VOLTDB if not set. Look for the voltdb jar file.
        global voltdb_jar
        if not os.environ.get('VOLTDB_VOLTDB', ''):
            for subdir in ('voltdb', os.path.join('lib', 'voltdb')):
                for voltdb_jar_chk in glob.glob(os.path.join(os.path.join(dir, subdir), 'voltdb*.jar')):
                    if re_voltdb_jar.match(os.path.basename(voltdb_jar_chk)):
                        voltdb_jar = os.path.realpath(voltdb_jar_chk)
                        os.environ['VOLTDB_VOLTDB'] = os.path.dirname(voltdb_jar)
                        vcli_util.debug('VOLTDB_VOLTDB=>%s' % os.environ['VOLTDB_VOLTDB'])

        dir = os.path.dirname(dir)

    missing = list(required_var_set.difference(os.environ.keys()))
    if missing:
        missing.sort()
        vcli_util.abort('Failed to establish VoltDB environment.',
                            ('You may need to perform a build.',
                             'Initial search directory: %s' % vcli_meta.mydir,
                             'Missing locations:', missing))

    # LOG4J configuration
    if 'LOG4J_CONFIG_PATH' not in os.environ:
        for subdirs in (('$VOLTDB_HOME', 'src', 'frontend'), ('$VOLTDB_HOME', 'voltdb')):
            path = os.path.join(os.path.expandvars(os.path.join(*subdirs)), 'log4j.xml')
            if os.path.exists(path):
                os.environ['LOG4J_CONFIG_PATH'] = path
                break
        else:
            vcli_util.abort('Could not find log4j configuration file or LOG4J_CONFIG_PATH variable.')

    for var in ('VOLTDB_HOME', 'VOLTDB_LIB', 'VOLTDB_VOLTDB', 'LOG4J_CONFIG_PATH'):
        vcli_util.debug('%s=%s' % (var, os.environ[var]))

    # Classpath is the voltdb jar and all the jars in VOLTDB_LIB.
    global classpath
    classpath = [voltdb_jar]
    for path in glob.glob(os.path.join(os.environ['VOLTDB_LIB'], '*.jar')):
        classpath.append(path)
