# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.
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

import sys
import os
from optparse import OptionParser
from os.path import expanduser


try:
    # ensure version 2.6+ of python
    if sys.version_info[0] == 2 and sys.version_info[1] < 6:
        for dir in os.environ['PATH'].split(':'):
            for name in ('python2.7', 'python2.6'):
                path = os.path.join(dir, name)
                if os.path.exists(path):
                    print 'Re-running with %s...' % path
                    os.execv(path, [path] + sys.argv)
        sys.stderr.write("This script requires Python 2.6 or newer. Please install " +
                         "a more recent Python release and retry.\n")
        sys.exit(-1)

    cmd_dir, cmd_name = os.path.split(os.path.realpath(sys.argv[0]))
    # Adjust these variables as needed for other base commands, locations, etc..
    base_dir = os.path.realpath(os.path.join(cmd_dir,'../../../'))
    version = open(os.path.join(base_dir, 'version.txt')).read().strip()
    description = 'Command line interface to VoltDB functions.'
    standalone  = False
    # Tweak the Python library path to call voltcli.runner.main().
    # Possible installed library locations.
    if os.path.isdir('/opt/lib/voltdb/python'):
        sys.path.insert(0, '/opt/lib/voltdb/python')
    if os.path.isdir('/usr/share/lib/voltdb/python'):
        sys.path.insert(0, '/usr/share/lib/voltdb/python')
    if os.path.isdir('/usr/lib/voltdb/python'):
        sys.path.insert(0, '/usr/lib/voltdb/python')
    # Library location relative to script.
    sys.path.insert(0, os.path.join(base_dir, 'lib', 'python'))
    sys.path.insert(0, os.path.join(base_dir, 'lib/python', 'vdm'))
    from voltcli import runner
    from server import HTTPListener

# Be selective about exceptions to avoid masking load-time library exceptions.
except (IOError, OSError, ImportError), e:
    sys.stderr.write('Exception (%s): %s\n' % (e.__class__.__name__, str(e)))
    sys.exit(1)


def main():
    parser = OptionParser(usage="usage: %prog [options] filepath",
                          version="%prog 1.0")
    parser.add_option("-p", "--path",
                  action="store", type="string", dest="filepath")
    parser.add_option("-c", "--configpath",
                  action="store", type="string", dest="configpath")
    parser.add_option("-s", "--server",
                  action="store", type="string", dest="server")
    (options, args) = parser.parse_args()

    arr = [{
        "filepath": options.filepath,
        "configpath": options.configpath,
        "server": options.server
    }]

    return arr


if __name__ == '__main__':
    options = main()

    data_path = options[0]['filepath']
    config_path = options[0]['configpath']
    server = options[0]['server']

    HTTPListener.main(runner, HTTPListener, config_path, data_path, server)

