# This file is part of VoltDB.
# Copyright (C) 2008-2017 VoltDB Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
# Provides global meta-data that is derived from the environment.

__author__ = 'scooper'

import sys
import os
import glob
import re
import shlex
import platform

from voltcli import utility

re_voltdb_jar = re.compile('^voltdb(client)?-[.0-9]+[.]([\w]+\.)*jar$')

config_name = 'volt.cfg'
config_name_local = 'volt_local.cfg'

# Filled in during startup.
standalone   = None
version      = None
pro_version  = False
command_dir  = None
command_name = None
voltdb_jar   = None
classpath    = None

# Location of third_party/python if available.
third_party_python = None

# Assume that we're in a subdirectory of the main volt Python library
# directory.  Add the containing library directory to the Python module load
# path so that verb modules can import any module here. E.g.:
#   from voltcli import <module>...
volt_python = os.path.dirname(os.path.dirname(__file__))
if volt_python not in sys.path:
    sys.path.insert(0, volt_python)

# Java configuration
if 'JAVA_HOME' in os.environ:
    java = os.path.join(os.environ['JAVA_HOME'], 'bin', 'java')
    jar = os.path.join(os.environ['JAVA_HOME'], 'bin', 'jar')
else:
    java = utility.find_in_path('java')
    jar = utility.find_in_path('jar')
if not java:
    utility.abort('Could not find java in environment, set JAVA_HOME or put java in the path.')
java_version = utility.get_java_version(java)
java_opts = []

#If this is a large memory system commit the full heap
specifyMinimumHeapSize = False
if platform.system() == "Linux":
    memory = os.popen("free -m")
    try:
        totalMemory = int(memory.readlines()[1].split()[1])
        specifyMinimumHeapSize = totalMemory > 1024 * 16
    finally:
        memory.close()

if 'VOLTDB_HEAPMAX' in os.environ:
    try:
        java_opts.append('-Xmx%dm' % int(os.environ.get('VOLTDB_HEAPMAX')))
        if specifyMinimumHeapSize:
            java_opts.append('-Xms%dm' % int(os.environ.get('VOLTDB_HEAPMAX')))
            java_opts.append('-XX:+AlwaysPreTouch')
    except ValueError:
        java_opts.append(os.environ.get('VOLTDB_HEAPMAX'))
if 'VOLTDB_OPTS' in os.environ:
    java_opts.extend(shlex.split(os.environ['VOLTDB_OPTS']))
if 'JAVA_OPTS' in os.environ:
    java_opts.extend(shlex.split(os.environ['JAVA_OPTS']))
if not [opt for opt in java_opts if opt.startswith('-Xmx')]:
    java_opts.append('-Xmx2048m')
    if specifyMinimumHeapSize:
        java_opts.append('-Xms2048m')
        java_opts.append('-XX:+AlwaysPreTouch')

# Set common options now.
java_opts.append('-server')
java_opts.append('-Djava.awt.headless=true')
java_opts.append('-Djavax.security.auth.useSubjectCredsOnly=false')
java_opts.append('-Dsun.net.inetaddr.ttl=300')
java_opts.append('-Dsun.net.inetaddr.negative.ttl=3600')
java_opts.append('-XX:+HeapDumpOnOutOfMemoryError')
java_opts.append('-XX:HeapDumpPath=/tmp')
java_opts.append('-XX:+UseParNewGC')
java_opts.append('-XX:+UseConcMarkSweepGC')
java_opts.append('-XX:+CMSParallelRemarkEnabled')
java_opts.append('-XX:+UseTLAB')
java_opts.append('-XX:CMSInitiatingOccupancyFraction=75')
java_opts.append('-XX:+UseCMSInitiatingOccupancyOnly')
java_opts.append('-XX:+UseCondCardMark')
java_opts.append('-Dsun.rmi.dgc.server.gcInterval=9223372036854775807')
java_opts.append('-Dsun.rmi.dgc.client.gcInterval=9223372036854775807')
java_opts.append('-XX:CMSWaitDuration=120000')
java_opts.append('-XX:CMSMaxAbortablePrecleanTime=120000')
java_opts.append('-XX:+ExplicitGCInvokesConcurrent')
java_opts.append('-XX:+CMSScavengeBeforeRemark')
java_opts.append('-XX:+CMSClassUnloadingEnabled')

# skip PermSize in Java 8
if "1.8" not in java_version:
    java_opts.append('-XX:PermSize=64m')

def initialize(standalone_arg, command_name_arg, command_dir_arg, version_arg):
    """
    Set the VOLTDB_LIB and VOLTDB_VOLTDB environment variables based on the
    script location and the working directory.
    """
    global command_name, command_dir, version, pro_version
    command_name = command_name_arg
    command_dir = command_dir_arg
    version = version_arg

    # Stand-alone scripts don't need a develoopment environment.
    global standalone
    standalone = standalone_arg
    if standalone:
        return

    # Add the working directory, the command directory, and VOLTCORE as
    # starting points for the scan.
    dirs = []
    def add_dir(dir):
        if dir and os.path.isdir(dir) and dir not in dirs:
            dirs.append(os.path.realpath(dir))
    add_dir(os.getcwd())
    add_dir(command_dir)
    add_dir(os.environ.get('VOLTCORE', None))
    utility.verbose_info('Base directories for scan:', dirs)

    lib_search_globs    = []
    voltdb_search_globs = []
    for dir in dirs:

        # Crawl upward and look for the lib and voltdb directories.
        # They may be the same directory when installed by a Linux installer.
        # Set the VOLTDB_... environment variables accordingly.
        # Also locate the voltdb jar file.
        global voltdb_jar
        while (dir and dir != '/' and ('VOLTDB_LIB' not in os.environ or not voltdb_jar)):
            utility.debug('Checking potential VoltDB root directory: %s' % os.path.realpath(dir))

            # Try to set VOLTDB_LIB if not set.
            if not os.environ.get('VOLTDB_LIB', ''):
                for subdir in ('lib', os.path.join('lib', 'voltdb')):
                    glob_chk = os.path.join(os.path.realpath(os.path.join(dir, subdir)), 'snappy*.jar')
                    lib_search_globs.append(glob_chk)
                    if glob.glob(glob_chk):
                        os.environ['VOLTDB_LIB'] = os.path.join(dir, subdir)
                        utility.debug('VOLTDB_LIB=>%s' % os.environ['VOLTDB_LIB'])

            # Try to set VOLTDB_VOLTDB if not set. Look for the voltdb jar file.
            if not os.environ.get('VOLTDB_VOLTDB', '') or voltdb_jar is None:
                for subdir in ('voltdb', os.path.join('lib', 'voltdb')):
                    # Need the hyphen to avoid the volt client jar.
                    glob_chk = os.path.join(os.path.realpath(os.path.join(dir, subdir)),
                                            'voltdb-*.jar')
                    voltdb_search_globs.append(glob_chk)
                    for voltdb_jar_chk in glob.glob(glob_chk):
                        if re_voltdb_jar.match(os.path.basename(voltdb_jar_chk)):
                            voltdb_jar = os.path.realpath(voltdb_jar_chk)
                            utility.debug('VoltDB jar: %s' % voltdb_jar)
                            if not os.environ.get('VOLTDB_VOLTDB', ''):
                                os.environ['VOLTDB_VOLTDB'] = os.path.dirname(voltdb_jar)
                                utility.debug('VOLTDB_VOLTDB=>%s' % os.environ['VOLTDB_VOLTDB'])

            # Capture the base third_party python path?
            third_party_python_chk = os.path.join(dir, 'third_party', 'python')
            if os.path.isdir(third_party_python_chk):
                global third_party_python
                third_party_python = third_party_python_chk

            dir = os.path.dirname(dir)

    # If the VoltDB jar was found then VOLTDB_VOLTDB will also be set.
    if voltdb_jar is None:
        utility.abort('Failed to find the VoltDB jar file.',
                        ('You may need to perform a build.',
                         'Searched the following:', voltdb_search_globs))

    if not os.environ.get('VOLTDB_LIB', ''):
        utility.abort('Failed to find the VoltDB library directory.',
                        ('You may need to perform a build.',
                         'Searched the following:', lib_search_globs))

    pro_version = utility.is_pro_version(voltdb_jar)
    utility.debug('VoltDB Pro Version: %s' % pro_version)
    # LOG4J configuration
    if 'LOG4J_CONFIG_PATH' not in os.environ:
        for chk_dir in ('$VOLTDB_LIB/../src/frontend', '$VOLTDB_VOLTDB'):
            path = os.path.join(os.path.realpath(os.path.expandvars(chk_dir)), 'log4j.xml')
            if os.path.exists(path):
                os.environ['LOG4J_CONFIG_PATH'] = path
                utility.debug('LOG4J_CONFIG_PATH=>%s' % os.environ['LOG4J_CONFIG_PATH'])
                break
        else:
            utility.abort('Could not find log4j configuration file or LOG4J_CONFIG_PATH variable.')

    for var in ('VOLTDB_LIB', 'VOLTDB_VOLTDB', 'LOG4J_CONFIG_PATH'):
        utility.verbose_info('Environment: %s=%s' % (var, os.environ[var]))

    # Classpath is the voltdb jar and all the jars in VOLTDB_LIB, and if present,
    # any user supplied jars under VOLTDB/lib/extension
    global classpath
    classpath = [voltdb_jar]
    for path in glob.glob(os.path.join(os.environ['VOLTDB_LIB'], '*.jar')):
        classpath.append(path)
    for path in glob.glob(os.path.join(os.environ['VOLTDB_LIB'], 'extension', '*.jar')):
        classpath.append(path)
    utility.verbose_info('Classpath: %s' % ':'.join(classpath))
