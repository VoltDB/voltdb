# This file is part of VoltDB.

# Copyright (C) 2008-2012 VoltDB Inc.
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

import sys
import os
import glob
import re
import shlex

from voltcli import utility

re_voltdb_jar = re.compile('^voltdb(client)?-2[.0-9]+[.]jar$')

# Filled in during startup.
version      = None
command_dir  = None
command_name = None
voltdb_jar   = None
classpath    = None

# Assume that we're in a subdirectory of the main volt Python library
# directory.  Add the containing library directory to the Python module load
# path so that verb modules can import any module here. E.g.:
#   from voltcli import <module>...
volt_python = os.path.dirname(os.path.dirname(__file__))
if volt_python not in sys.path:
    sys.path.insert(0, volt_python)

# Java configuration
if 'JAVA_HOME' in os.environ:
    java = os.path.join(os.environ['JAVA_HOME'], 'java')
else:
    java = utility.find_in_path('java')
if not java:
    utility.abort('Could not find java in environment, set JAVA_HOME or put java in the path.')
java_opts = []
if 'JAVA_HEAP_MAX' in os.environ:
    java_opts.append(os.environ.get('JAVA_HEAP_MAX'))
if 'VOLTDB_OPTS' in os.environ:
    java_opts.extend(shlex.split(os.environ['VOLTDB_OPTS']))
if 'JAVA_OPTS' in os.environ:
    java_opts.extend(shlex.split(os.environ['JAVA_OPTS']))
if not [opt for opt in java_opts if opt.startswith('-Xmx')]:
    java_opts.append('-Xmx1024m')

def initialize(command_name_arg, command_dir_arg, version_arg):
    """Set the VOLTDB_HOME, VOLTDB_LIB and VOLTDB_VOLTDB environment variables
    based on the location of this script."""

    global command_name, command_dir, version
    command_name = command_name_arg
    command_dir = command_dir_arg
    version = version_arg

    dirs = [os.getcwd()]
    if command_dir not in dirs:
        dirs.append(command_dir)

    for dir in dirs:

        required_var_set = set(['VOLTDB_HOME', 'VOLTDB_LIB', 'VOLTDB_VOLTDB'])

        # Return true if all needed VOLTDB_... environment variables are set
        # and the VoltDB jar file was found.
        def env_complete():
            return (voltdb_jar is not None and set(os.environ.keys()).issuperset(required_var_set))

        # Crawl upward and look for the home, lib and voltdb directories.
        # They may be the same directory when installed by a Linux installer.
        # Set the VOLTDB_... environment variables accordingly.
        # Also locate the voltdb jar file.
        while (dir != '/' and not env_complete()):

            utility.debug('Checking potential VoltDB root directory: %s' % dir)

            # Try to set VOLTDB_HOME if not set.
            if not os.environ.get('VOLTDB_HOME', ''):
                for subdir in ('', os.path.join('share', 'voltdb')):
                    for chk in ('Click Here to Start.html', 'examples', 'third_party'):
                        if not os.path.exists(os.path.join(dir, subdir, chk)):
                            break
                    else:
                        os.environ['VOLTDB_HOME'] = os.path.realpath(os.path.join(dir, subdir))
                        utility.debug('VOLTDB_HOME=>%s' % os.environ['VOLTDB_HOME'])

            # Try to set VOLTDB_LIB if not set.
            if not os.environ.get('VOLTDB_LIB', ''):
                for subdir in ('lib', os.path.join('lib', 'voltdb')):
                    if glob.glob(os.path.join(os.path.join(dir, subdir), 'zmq*.jar')):
                        os.environ['VOLTDB_LIB'] = os.path.realpath(os.path.join(dir, subdir))
                        utility.debug('VOLTDB_LIB=>%s' % os.environ['VOLTDB_LIB'])

            # Try to set VOLTDB_VOLTDB if not set. Look for the voltdb jar file.
            global voltdb_jar
            if not os.environ.get('VOLTDB_VOLTDB', '') or voltdb_jar is None:
                for subdir in ('voltdb', os.path.join('lib', 'voltdb')):
                    glob_chk = os.path.join(os.path.join(dir, subdir), 'voltdb*.jar')
                    for voltdb_jar_chk in glob.glob(glob_chk):
                        if re_voltdb_jar.match(os.path.basename(voltdb_jar_chk)):
                            voltdb_jar = os.path.realpath(voltdb_jar_chk)
                            if not os.environ.get('VOLTDB_VOLTDB', ''):
                                os.environ['VOLTDB_VOLTDB'] = os.path.dirname(voltdb_jar)
                                utility.debug('VOLTDB_VOLTDB=>%s' % os.environ['VOLTDB_VOLTDB'])

            dir = os.path.dirname(dir)

    if voltdb_jar is None:
        utility.abort('Failed to find the VoltDB jar file.')

    missing = list(required_var_set.difference(os.environ.keys()))
    if missing:
        missing.sort()
        utility.abort('Failed to establish VoltDB environment.',
                            ('You may need to perform a build.',
                             'Initial search directory: %s' % command_dir,
                             'Missing locations:', missing))

    # LOG4J configuration
    if 'LOG4J_CONFIG_PATH' not in os.environ:
        for subdirs in (('$VOLTDB_HOME', 'src', 'frontend'), ('$VOLTDB_HOME', 'voltdb')):
            path = os.path.join(os.path.expandvars(os.path.join(*subdirs)), 'log4j.xml')
            if os.path.exists(path):
                os.environ['LOG4J_CONFIG_PATH'] = path
                break
        else:
            utility.abort('Could not find log4j configuration file or LOG4J_CONFIG_PATH variable.')

    for var in ('VOLTDB_HOME', 'VOLTDB_LIB', 'VOLTDB_VOLTDB', 'LOG4J_CONFIG_PATH'):
        utility.debug('%s=%s' % (var, os.environ[var]))

    # Classpath is the voltdb jar and all the jars in VOLTDB_LIB.
    global classpath
    classpath = [voltdb_jar]
    for path in glob.glob(os.path.join(os.environ['VOLTDB_LIB'], '*.jar')):
        classpath.append(path)
