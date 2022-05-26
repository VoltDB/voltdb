# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.
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

# Volt CLI utility functions.

# IMPORTANT: This depends on no other voltcli modules. Please keep it that way.

import sys
import os
import subprocess
import glob
import copy
import inspect
import configparser
import zipfile
import re
import pkgutil
import binascii
import stat
import signal
import textwrap
import string

from voltcli import daemon

#===============================================================================
class Global:
#===============================================================================
    """
    Global data for utilities.
    """
    verbose_enabled = False
    debug_enabled   = False
    dryrun_enabled  = False
    manifest_path   = 'MANIFEST'
    state_directory = ''

#===============================================================================
def set_dryrun(dryrun):
#===============================================================================
    """
    Enable or disable command dry run (display only/no execution).
    """
    Global.dryrun_enabled = dryrun

#===============================================================================
def set_verbose(verbose):
#===============================================================================
    """
    Enable or disable verbose messages. Increases the number of INFO messages.
    """
    Global.verbose_enabled = verbose

#===============================================================================
def set_debug(debug):
#===============================================================================
    """
    Enable or disable DEBUG messages. Also enables verbose INFO messages.
    """
    Global.debug_enabled = debug
    if debug:
        Global.verbose_enabled = True

#===============================================================================
def set_state_directory(directory):
#===============================================================================
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except (OSError, IOError) as e:
            abort('Error creating state directory "%s".' % directory, e)
    Global.state_directory = os.path.expandvars(os.path.expanduser(directory))

#===============================================================================
def get_state_directory():
#===============================================================================
    """
    Return and create as needed a path for saving state.
    """
    return Global.state_directory

#===============================================================================
def is_dryrun():
#===============================================================================
    """
    Return True if dry-run is enabled.
    """
    return Global.dryrun_enabled

#===============================================================================
def is_verbose():
#===============================================================================
    """
    Return True if verbose messages are enabled.
    """
    return Global.verbose_enabled

#===============================================================================
def is_debug():
#===============================================================================
    """
    Return True if debug messages are enabled.
    """
    return Global.debug_enabled

#===============================================================================
def get_state_directory():
#===============================================================================
    return Global.state_directory

#===============================================================================
def display_messages(msgs, f = sys.stdout, tag = None, level = 0):
#===============================================================================
    """
    Low level message display.
    """
    if tag:
        stag = '%s: ' % tag
    else:
        stag = ''
    # Special case to allow a string instead of an iterable.
    try:
        # Raises TypeError if not string
        var = msgs + ' '
        msgs = [msgs]
    except TypeError:
        pass
    sindent = level * '  '
    # Recursively process message list and sub-lists.
    for msg in msgs:
        if msg is not None:
            # Handle exceptions
            if issubclass(msg.__class__, Exception):
                f.write('%s%s%s Exception: %s\n' % (stag, sindent, msg.__class__.__name__, str(msg)))
            else:
                # Handle multi-line strings
                if is_string(msg):
                    # If it is a string slice and dice it by linefeeds.
                    for msg2 in msg.split('\n'):
                        f.write('%s%s%s\n' % (stag, sindent, msg2))
                else:
                    # Recursively display an iterable with indentation added.
                    if hasattr(msg, '__iter__'):
                        display_messages(msg, f = f, tag = tag, level = level + 1)
                    else:
                        for msg2 in str(msg).split('\n'):
                            f.write('%s%s%s\n' % (stag, sindent, msg2))

#===============================================================================
def info(*msgs):
#===============================================================================
    """
    Display INFO level messages.
    """
    display_messages(msgs, tag = 'INFO')

#===============================================================================
def verbose_info(*msgs):
#===============================================================================
    """
    Display verbose INFO level messages if enabled.
    """
    if Global.verbose_enabled:
        display_messages(msgs, tag = 'INFO2')

#===============================================================================
def debug(*msgs):
#===============================================================================
    """
    Display DEBUG level message(s) if debug is enabled.
    """
    if Global.debug_enabled:
        display_messages(msgs, tag = 'DEBUG')

#===============================================================================
def warning(*msgs):
#===============================================================================
    """
    Display WARNING level messages.
    """
    display_messages(msgs, tag = 'WARNING')

#===============================================================================
def error(*msgs):
#===============================================================================
    """
    Display ERROR level messages.
    """
    display_messages(msgs, tag = 'ERROR')

#===============================================================================
def abort(*msgs, **kwargs):
#===============================================================================
    """
    Display ERROR messages and then abort.
    :Keywords:
    return_code: integer result returned to the OS (default=1)
    """
    keys = list(kwargs.keys())
    bad_keywords = [k for k in list(kwargs.keys()) if k != 'return_code']
    if bad_keywords:
        warning('Bad keyword(s) passed to abort(): %s' % ' '.join(bad_keywords))
    return_code = kwargs.get('return_code', 1)
    error(*msgs)
    # Return code must be 0-255 for shell.
    if return_code < 0 or return_code > 255:
        return_code = 1
    sys.exit(return_code)

#===============================================================================
def find_in_path(name):
#===============================================================================
    """
    Find program in the system path.
    """
    # NB: non-portable
    for dir in os.environ['PATH'].split(':'):
        if os.path.exists(os.path.join(dir, name)):
            return os.path.join(dir, name)
    return None

#===============================================================================
def find_programs(*names):
#===============================================================================
    """
    Check for required programs in the path.
    """
    missing = []
    paths = {}
    for name in names:
        paths[name] = find_in_path(name)
        if paths[name] is None:
            missing.append(name)
    if missing:
        abort('Required program(s) are not in the path:', missing)
    return paths

#===============================================================================
class PythonSourceFinder(object):
#===============================================================================
    """
    Find and invoke python source files in a set of directories and resource
    subdirectories (for searching in zip packages).  Execute all discovered
    source files and pass in the symbols provided.
    A typical usage relies on decorators to mark discoverable functions in user
    code. The decorator is called when the source file is executed which serves
    as an opportunity to keep track of discovered functions.
    """
    class Scan(object):
        def __init__(self, package, path):
            self.package = package
            self.path    = path

    def __init__(self):
        self.scan_locs = []
        self.manifests = {}

    def add_path(self, path):
        # Use the absolute path to avoid visiting the same directory more than once.
        full_path = os.path.realpath(path)
        for scan_loc in self.scan_locs:
            if scan_loc.path == full_path:
                break
        else:
            self.scan_locs.append(PythonSourceFinder.Scan(None, full_path))

    def add_resource(self, package, path):
        self.scan_locs.append(PythonSourceFinder.Scan(package, path))

    def search_and_execute(self, **syms):
        for scan_loc in self.scan_locs:
            verbose_info('Scanning "%s" for modules to run...' % scan_loc.path)
            if scan_loc.package:
                # Load the manifest as needed so that individual files can be
                # found in package directories. There doesn't seem to be an
                # easy way to search for resource files, e.g. by glob pattern.
                if scan_loc.package not in self.manifests:
                    try:
                        manifest_raw = pkgutil.get_data(scan_loc.package, Global.manifest_path)
                        self.manifests[scan_loc.package] = manifest_raw.split('\n')
                    except (IOError, OSError) as e:
                        abort('Failed to load package %s.' % Global.manifest_path, e)
                for path in self.manifests[scan_loc.package]:
                    if os.path.dirname(path) == scan_loc.path and path.endswith('.py'):
                        debug('Executing package module "%s"...' % path)
                        try:
                            code = pkgutil.get_data(scan_loc.package, path)
                        except (IOError, OSError) as e:
                            abort('Failed to load package resource "%s".' % path, e)
                        syms_tmp = copy.copy(syms)
                        exec(code, syms_tmp)
            elif os.path.exists(scan_loc.path):
                for modpath in glob.glob(os.path.join(scan_loc.path, '*.py')):
                    debug('Executing module "%s"...' % modpath)
                    syms_tmp = copy.copy(syms)
                    exec(compile(open(modpath, "rb").read(), modpath, 'exec'), syms_tmp)

#===============================================================================
def normalize_list(items, width, filler = None):
#===============================================================================
    """
    Normalize list to a specified width, truncating or filling as needed.
    Filler data can be supplied by caller. The filler will be copied to each
    added item. None will be used as the filler if none is provided.
    """
    assert items is not None
    assert width >= 0
    output = items[:width]
    if len(output) < width:
        output += filler * (width - len(output))
    return tuple(output)

#===============================================================================
def format_table(tuples, caption = None, headings = None, indent = 0, separator = ' '):
#===============================================================================
    """
    Format a table, i.e. tuple list, including an optional caption, optional
    column headings, and rows of data cells. Aligns the headings and data
    cells.  Headings and data rows must be iterable. Each data row must provide
    iterable cells.  For now it only handles stringized data and right
    alignment. Returns the table-formatted string.
    """
    output = []
    sindent = ' ' * indent
    # Display the caption, if supplied.
    if caption:
        output.append('\n%s-- %s --\n' % (sindent, caption))
    rows = []
    # Add a row for headings, if supplied. Underlining is added after widths are known.
    if headings:
        rows.append(headings)
    # Add the data rows.
    rows.extend(tuples)
    # Measure the column widths.
    widths = []
    for row in rows:
        icolumn = 0
        for column in row:
            width = len(str(column))
            if len(widths) == icolumn:
                widths.append(width)
            else:
                widths[icolumn] = max(widths[icolumn], width)
            icolumn += 1
    # If we have headings inject a row with underlining based on the calculated widths.
    if headings:
        rows.insert(1, ['-' * widths[i] for i in range(len(widths))])
    # Generate the format string and then format the headings and rows.
    fmt = '%s%s' % (sindent, separator.join(['%%-%ds' % width for width in widths]))
    for row in rows:
        output.append(fmt % normalize_list(row, len(widths), ''))
    return '\n'.join(output)

#===============================================================================
def format_tables(tuples_list, caption_list = None, heading_list = None, indent = 0):
#===============================================================================
    """
    Format multiple tables, i.e. a list of tuple lists. See format_table() for
    more information.
    """
    output = []
    for i in range(len(tuples_list)):
        if caption_list is None or i >= len(caption_list):
            caption = None
        else:
            caption = caption_list[i]
        if heading_list is None or i >= len(heading_list):
            heading = None
        else:
            heading = heading_list[i]
        s = format_table(tuples_list[i], caption = caption, heading = heading, indent = indent)
        output.append(s)
    return '\n\n'.join(output)

#===============================================================================
def format_volt_table(table, caption = None, headings = True):
#===============================================================================
    """
    Format a VoltTable for display.
    """
    rows = table.tuples
    if headings:
        heading_row = [c.name for c in table.columns]
    else:
        heading_row = None
    return format_table(rows, caption = caption, headings = heading_row)

#===============================================================================
def format_volt_tables(table_list, caption_list = None, headings = True):
#===============================================================================
    """
    Format a list of VoltTable's for display.
    """
    output = []
    if table_list:
        for i in range(len(table_list)):
            if caption_list is None or i >= len(caption_list):
                caption = None
            else:
                caption = caption_list[i]
            output.append(format_volt_table(table_list[i], caption = caption, headings = headings))
    return '\n\n'.join(output)

#===============================================================================
def quote_shell_arg(arg):
#===============================================================================
    """
    Return an argument with quotes added as needed.
    """
    sarg = str(arg)
    if len(sarg) == 0 or len(sarg.split()) > 1:
        return '"%s"' % sarg
    return sarg

#===============================================================================
def unquote_shell_arg(arg):
#===============================================================================
    """
    Return an argument with quotes removed if present.
    """
    sarg = str(arg)
    if len(sarg) == 0:
        return sarg
    quote_char = sarg[-1]
    if quote_char not in ('"', "'"):
        return sarg
    # Deal with a starting quote that might not be at the beginning
    if sarg[0] == quote_char:
        return sarg[1:-1]
    pos = sarg.find(quote_char)
    if pos == len(sarg) - 1:
        # No first quote, don't know what else to do but return as is
        return sarg
    return sarg[:pos] + sarg[pos+1:-1]

#===============================================================================
def quote_shell_args(*args_in):
#===============================================================================
    """
    Return a list of arguments that are quoted as needed.
    """
    return [quote_shell_arg(arg) for arg in args_in]

#===============================================================================
def unquote_shell_args(*args_in):
#===============================================================================
    """
    Return a list of arguments with quotes removed when present.
    """
    return [unquote_shell_arg(arg) for arg in args_in]

#===============================================================================
def join_shell_cmd(cmd, *args):
#===============================================================================
    """
    Join shell command and arguments into one string.
    Add quotes as appropriate.
    """
    return ' '.join(quote_shell_args(cmd, *args))

#===============================================================================
def run_cmd(cmd, *args):
#===============================================================================
    """
    Run external program without capturing or suppressing output and check return code.
    """
    fullcmd = join_shell_cmd(cmd, *args)
    if Global.dryrun_enabled:
        sys.stdout.write('Run: %s\n' % fullcmd)
    else:
        if Global.verbose_enabled:
            verbose_info('Run: %s' % fullcmd)
        retcode = os.system(fullcmd)
        if retcode != 0:
            if (retcode & 255) == 0: # exit(N)
                exsts = retcode >> 8
            else: # terminated by signal
                exsts = 128 # arbitrary choice
            abort(return_code=exsts)

#===============================================================================
def exec_cmd(cmd, *args):
#===============================================================================
    """
    Run external program by replacing the current (Python) process.
    """
    display_cmd = join_shell_cmd(cmd, *args)
    if Global.dryrun_enabled:
        sys.stdout.write('Exec: %s\n' % display_cmd)
    else:
        if Global.verbose_enabled:
            verbose_info('Exec: %s' % display_cmd)
        # Need to strip out quotes because the shell won't be doing it for us.
        cmd_and_args = unquote_shell_args(cmd, *args)
        os.execvp(cmd, cmd_and_args)

#===============================================================================
def pipe_cmd(*args):
#===============================================================================
    """
    Run an external program, capture its output, and yield each output line for
    iteration.
    """
    try:
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in iter(proc.stdout.readline, ''):
            yield line.rstrip()
        proc.stdout.close()
    except Exception as e:
        warning('Exception running command: %s' % ' '.join(args), e)

#===============================================================================
def daemon_file_name(base_name=None, host=None, instance=None):
#===============================================================================
    """
    Build a daemon output file name using optional base name, host, and instance.
    """
    names = []
    if not base_name is None:
        names.append(base_name)
    if not host is None:
        names.append(host.replace(':', '_'))
    if not names:
        names.append('server')
    if not instance is None:
        names.append('_%d' % instance)
    daemon_name = ''.join(names)
    return daemon_name

#===============================================================================
class Daemonizer(daemon.Daemon):
#===============================================================================
    """
    Class that supports daemonization (inherited from the daemon module). The
    current process, i.e. the running Python script is completely replaced by
    the executed program.
    """

    def __init__(self, name, description, output=None):
        """
        Constructor. The optional "output" keyword specifies an override to
        the default ~/.command_name output directory.
        """
        self.name = name
        self.description = description
        self.output_dir = output
        if self.output_dir is None:
            self.output_dir = get_state_directory()
        pid = os.path.join(self.output_dir, '%s.pid' % name)
        out = os.path.join(self.output_dir, '%s.out' % name)
        err = os.path.join(self.output_dir, '%s.err' % name)
        self.output_files = [out, err]
        daemon.Daemon.__init__(self, pid, stdout=out, stderr=err)
        # Clean up PID files of defunct processes.
        self.purge_defunct()

    def start_daemon(self, *args):
        """
        Start a daemon process.
        """
        # Replace existing output files.
        for path in self.output_files:
            if os.path.exists(path):
                try:
                    os.remove(path)
                except (IOError, OSError) as e:
                    abort('Unable to remove the existing output file "%s".' % path, e)
        try:
            info('Starting %s in the background...' % (self.description), [
                    'Output files are in "%s".' % self.output_dir
                ])
            self.start(*args)
        except daemon.Daemon.AlreadyRunningException as e:
            abort('A %s background process appears to be running.' % self.description, (
                     'Process ID (PID): %d' % e.pid,
                     'PID file: %s' % e.pidfile),
                  'Please stop the process and try again.')
        except (IOError, OSError) as e:
            abort('Unable to start the %s background process.' % self.description, e)

    def stop_daemon(self, kill_signal=signal.SIGTERM):
        """
        Stop a daemon process.
        """
        try:
            daemon.Daemon.stop(self, kill_signal=kill_signal)
            info("%s (process ID %d) was stopped." % (self.description, self.pid))
        except daemon.Daemon.NotRunningException as e:
            if e.pid != -1:
                addendum = ' as process ID %d' % e.pid
            else:
                addendum = ''
            abort('%s is no longer running%s.' % (self.description, addendum))
        except (IOError, OSError) as e:
            abort('Unable to stop the %s background process.' % self.description, e)

    def on_started(self, *args_in):
        """
        Post-daemonization call-back.
        """
        # Strip out things requiring shell interpretation, e.g. quotes.
        args = [str(arg).replace('"', '') for arg in args_in]
        try:
            os.execvp(args[0], args)
        except (OSError, IOError) as e:
            abort('Failed to exec:', args, e)

    def get_running(self):
        """
        Scan for PID files that have running processes.
        Returns a list of the current running PIDs.
        """
        running = []
        for path in glob.glob(os.path.join(self.output_dir, "*.pid")):
            pid, alive = daemon.get_status(path)
            if alive:
                running.append(pid)
        return running

    def purge_defunct(self):
        """
        Purge PID files of defunct daemon processes.
        """
        for path in glob.glob(os.path.join(self.output_dir, "*.pid")):
            pid, alive = daemon.get_status(path)
            if not alive:
                try:
                    info('Deleting stale PID file "%s"...' % path)
                    os.remove(path)
                except (OSError, IOError) as e:
                    warning('Failed to delete PID file "%s".' % path, e)

#===============================================================================
def is_string(item):
#===============================================================================
    """
    Return True if the item behaves like a string.
    """
    try:
        test_string = item + ''
        return True
    except TypeError:
        return False

#===============================================================================
def is_sequence(item):
#===============================================================================
    """
    Return True if the item behaves like an iterable sequence.
    """
    if is_string(item):
        return False
    try:
        for var in item:
            break
        return True
    except TypeError:
        return False

#===============================================================================
def _flatten(item):
#===============================================================================
    """
    Internal function to recursively iterate a potentially nested sequence.
    None items are filtered out.
    """
    if item is not None:
        if is_sequence(item):
            for subitem in item:
                for subsubitem in _flatten(subitem):
                    if subsubitem is not None:
                        yield subsubitem
        else:
            yield item

#===============================================================================
def flatten(*items):
#===============================================================================
    """
    Flatten and yield individual items from a potentially nested list or tuple.
    """
    for item in _flatten(items):
        yield item

#===============================================================================
def flatten_to_list(*items):
#===============================================================================
    """
    Flatten a potentially nested list or tuple to a simple list.
    """
    return [item for item in flatten(*items)]

#===============================================================================
def to_display_string(item):
#===============================================================================
    """
    Recursively convert simple items and potentially nested sequences to a
    string, using square brackets and commas to format sequences.
    """
    if not is_sequence(item):
        return str(item)
    s = ''
    for subitem in item:
        if s:
            s += ', '
        s += to_display_string(subitem)
    return '[%s]' % s

#===============================================================================
class Zipper(object):
#===============================================================================
    """
    The Zipper class creates a zip file using the directories, strings, and
    exclusion regular expresions provided. It can also add a preamble and make
    the resulting file executable in order to support making a self-executable
    compressed Python program.
    """
    def __init__(self, excludes = []):
        self.output_file = None
        self.output_zip  = None
        self.output_path = None
        self.manifest    = []
        self.re_excludes = [re.compile(exclude) for exclude in excludes]

    def open(self, output_path, preamble = None, force = False):
        if os.path.exists(output_path) and not force:
            if choose('Overwrite "%s"?' % output_path, 'yes', 'no') == 'n':
                abort()
        self.output_path = output_path
        if not Global.dryrun_enabled:
            try:
                self.output_file = open(output_path, 'w');
                if preamble:
                    self.output_file.write(preamble)
                self.output_zip = zipfile.ZipFile(self.output_file, 'w', zipfile.ZIP_DEFLATED)
            except (IOError, OSError) as e:
                self._abort('Failed to open for writing.', e)

    def close(self, make_executable = False):
        if self.output_zip:
            # Write the manifest.
            try:
                self.output_zip.writestr(Global.manifest_path, '\n'.join(self.manifest))
            except (IOError, OSError) as e:
                self._abort('Failed to write %s.' % Global.manifest_path, e)
            self.output_zip.close()
            self.output_file.close()
            if make_executable:
                mode = os.stat(self.output_path).st_mode
                try:
                    os.chmod(self.output_path, mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
                except (IOError, OSError) as e:
                    self._abort('Failed to add executable permission.', e)

    def add_file(self, path_in, path_out):
        for re_exclude in self.re_excludes:
            path_in_full = os.path.realpath(path_in)
            if re_exclude.search(path_in):
                self._verbose_info('skip "%s"' % path_in_full)
                break
        else:
            self._verbose_info('add "%s" as "%s"' % (path_in_full, path_out))
            try:
                if self.output_zip:
                    self.output_zip.write(path_in, path_out)
                    self.manifest.append(path_out)
            except (IOError, OSError) as e:
                self._abort('Failed to write file "%s".' % path_out, e)

    def add_file_from_string(self, s, path_out):
        self._verbose_info('write string to "%s"' % path_out)
        if self.output_zip:
            try:
                self.output_zip.writestr(path_out, s)
                self.manifest.append(path_out)
            except (IOError, OSError) as e:
                self._abort('Failed to write string to file "%s".' % path_out, e)

    def add_directory(self, path_in, dst, excludes = []):
        if not os.path.isdir(path_in):
            self._abort('Zip source directory "%s" does not exist.' % path_in)
        self._verbose_info('add directory "%s" to "%s"' % (path_in, dst))
        savedir = os.getcwd()
        # Get nice relative paths by temporarily switching directories.
        os.chdir(path_in)
        try:
            for basedir, subdirs, filenames in os.walk('.'):
                for filename in filenames:
                    file_path_in = os.path.join(basedir[2:], filename)
                    file_path_out = os.path.join(dst, basedir[2:], filename)
                    self.add_file(file_path_in, file_path_out)
        finally:
            os.chdir(savedir)

    def _verbose_info(self, msg):
        verbose_info('%s: %s' % (self.output_path, msg))

    def _abort(self, *msgs):
        abort('Fatal error writing zip file "%s".' % self.output_path, msgs)

#===============================================================================
def merge_java_options(*opts):
#===============================================================================
    """
    Merge redundant -X... java command line options. Keep others intact.
    Arguments can be lists or individual arguments. Returns the reduced list.
    """
    ret_opts = []
    xargs = set()
    for opt in flatten(*opts):
        if opt is not None:
            # This is somewhat simplistic logic that might have unlikely failure scenarios.
            if opt.startswith('-X'):
                # The symbol is the initial string of contiguous alphabetic characters.
                sym = ''.join([c for c in opt[2:] if c.isalpha()])
                if sym not in xargs:
                    xargs.add(sym)
                    ret_opts.append(opt)
            else:
                ret_opts.append(opt)
    return ret_opts

#===============================================================================
def get_java_version(javaHome="java", verbose=False):
#===============================================================================
    """
    Assumes caller has already run "find_in_path(java)" so we know it can be checked.
    """
    try:
        version = subprocess.Popen([javaHome, '-version'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        grep = subprocess.Popen(['grep', 'java \|openjdk'], stdin=version.stdout, stdout=subprocess.PIPE)
        version.stdout.close()
        out, err = grep.communicate()
        version.wait()
        if verbose:
            return out
        for version in ('17.0', '11.0', '1.8', '1.7'):
            if version in out.decode("utf-8"):
                return version
        return ""
    except (OSError):
        return ""

#===============================================================================
def get_java_version_major(javaHome="java"):
#===============================================================================
    """
    Find the major release of java based on "get_java_version()"
    """
    try:
        version = get_java_version(javaHome)
        if version in "1.7":
            return "7"
        elif version in "1.8":
            return "8"
        elif version in '11.0':
            return "11"
        elif version in '17.0':
            return "17"
        else:
            return ""
    except (OSError):
        return ""


#===============================================================================
def is_pro_version(voltdb_jar):
#===============================================================================
    """
    Assumes caller has already run "find_in_path(jar)" so we know it can be checked.
    The jar is already validated as present before this is called.
    """
    try:
        zf = zipfile.ZipFile(voltdb_jar, 'r')
    except (IOError, OSError) as e:
        print('Error reading zip file "%s".' % voltdb_jar, e)
        return False
    try:
        for ze in zf.infolist():
            if "org/voltdb/CommandLogImpl.class" == ze.filename:
                return True
        return False
    except (OSError):
        return False
    finally:
        zf.close()

#===============================================================================
def kwargs_merge_list(kwargs, name, *args):
#===============================================================================
    """
    Merge and flatten kwargs list with additional items.
    """
    kwargs[name] = flatten_to_list(kwargs.get(name, None), *args)

#===============================================================================
def kwargs_merge_java_options(kwargs, name, *args):
#===============================================================================
    """
    Merge and flatten kwargs Java options list with additional options.
    """
    kwargs[name] = merge_java_options(kwargs.get(name, None), *args)

#===============================================================================
def choose(prompt, *choices):
#===============================================================================
    """
    Prompt the user for multiple choice input. Keep prompting until a valid
    choice is received. Choice shortcuts require unique first letters. The user
    can either respond with a single letter or an entire word.
    """
    letters = set()
    choice_list = []
    for choice in choices:
        if not choice:
            abort('Empty choice passed to choose().')
        if choice[0] in letters:
            abort('Non-unique choices %s passed to choose().' % str(choices))
        letters.add(choice[0])
        choice_list.append('[%s]%s' % (choice[0], choice[1:]))
    while True:
        sys.stdout.write('%s (%s) ' % (prompt, '/'.join(choice_list)))
        sys.stdout.flush()
        response = sys.stdin.readline().strip()
        if response in letters or response in choices:
            return response[0]

#===============================================================================
def dict_to_sorted_pairs(d):
#===============================================================================
    """
    Convert a dictionary to a list of key/value pairs sorted by key.
    """
    keys = list(d.keys())
    keys.sort()
    results = []
    for key in keys:
        results.append((key, d[key]))
    return results

#===============================================================================
def pluralize(s, count):
#===============================================================================
    """
    Return word with 's' appended if the count > 1.
    """
    if count > 1:
        return '%ss' % s
    return s

#===============================================================================
def kwargs_extract(kwargs, defaults, remove = True, check_extras = False):
#===============================================================================
    """
    Extract and optionally remove valid keyword arguments and convert to an
    object with attributes.  The defaults argument specifies both the list of
    valid keywords and their default values. Abort on any invalid keyword.
    """
    class O(object):
        pass
    o = O()
    if check_extras:
        bad = list(set(kwargs.keys()).difference(set(defaults.keys())))
        if bad:
            bad.sort()
            abort('Bad keywords passed to kwargs_extract():', bad)
    for name in defaults:
        if name in kwargs:
            if remove:
                value = kwargs.pop(name)
            else:
                value = kwargs[name]
        else:
            value = defaults[name]
        setattr(o, name, value)
    return o

#===============================================================================
def kwargs_get(kwargs, name, remove = True, default = None):
#===============================================================================
    defaults = {name: default}
    args = kwargs_extract(kwargs, defaults, remove = remove, check_extras = False)
    return getattr(args, name)

#===============================================================================
def kwargs_get_string(kwargs, name, remove = True, default = None):
#===============================================================================
    value = kwargs_get(kwargs, name, remove = remove, default = default)
    if value is not None:
        value = str(value)
    return value

#===============================================================================
def kwargs_get_integer(kwargs, name, remove = True, default = None):
#===============================================================================
    value = kwargs_get(kwargs, name, remove = remove, default = default)
    if value is not None:
        try:
            value = int(value)
        except (ValueError, TypeError):
            abort('Keyword argument "%s" must be an integer: %s' % (name, str(value)))
    return value

#===============================================================================
def kwargs_get_boolean(kwargs, name, remove = True, default = None):
#===============================================================================
    value = kwargs_get(kwargs, name, remove = remove, default = default)
    if value is None or value == True or value == False:
        return value
    abort('Keyword argument "%s" must be a boolean value: %s' % (name, str(value)))

#===============================================================================
def kwargs_get_list(kwargs, name, remove = True, default = []):
#===============================================================================
    return flatten_to_list(kwargs_get(kwargs, name, remove = remove, default = default))

#===============================================================================
def kwargs_set_defaults(kwargs, **defaults):
#===============================================================================
    for name in defaults:
        if name not in kwargs:
            kwargs[name] = defaults[name]

#===============================================================================
def parse_hosts(host_string, min_hosts = None, max_hosts = None, default_port = None):
#===============================================================================
    """
    Split host string on commas, extract optional port for each and return list
    of host objects. Check against minimum/maximum quantities if specified.
    We attempt to classify strings as one of:
       <ip4 address>  |  <ip4 address> : <port>
       <ip6_address>  |  [<ip6 address>]  |  [<ip6 address>] : <port>
       <host name>    |  <host name> : port
    Brackets are required around IPv6 addresses; this is consistent with
    VoltDB parsing. Also, don't forget about IPv4-mapped IPv6 addresses,
    formatted like ::ffff:127.0.0.1:21212.
    """
    class Host(object):
        def __init__(self, host, port):
            self.host = host
            self.port = port
    hosts = []
    # approximate syntax only, sufficient only to discriminate
    opt_port = r'(:[0-9]+)?$'
    addr4 = re.compile(r'[0-9.]+' + opt_port)
    addr6 = re.compile(r'\[[0-9A-Fa-f:]+([0-9.]+)?\]' + opt_port)
    name = re.compile(r'[0-9A-Za-z.-]+' + opt_port)
    for host_port in host_string.split(','):
        if host_port.count(':') >= 2 and addr6.match(host_port):
            host, port = _split_ip6(host_port)
        elif '.' in host_port and addr4.match(host_port):
            host, port = _split_ip4(host_port)
        elif name.match(host_port):
            host, port = _split_name(host_port)
        else:
            abort('''Unrecognized address syntax "%s"
  Use one of the following: (including the brackets)
    hostname     hostname:port
    ip4addr      ip4addr:port
    [ip6addr]    [ip6addr]:port''' % host_port)
        if port is None:
            port = default_port
        if port is not None:
            try:
                port = int(port)
            except ValueError:
                abort('Bad port value "%s" for host: %s' % (port, host_port))
        #print("# host_port=%s --> host=%s port=%s" % (host_port, host,port))
        hosts.append(Host(host, port))
    if min_hosts is not None and len(hosts) < min_hosts:
        abort('Too few hosts in host string "%s". The minimum is %d.'
                    % (host_string, min_hosts))
    if max_hosts is not None and len(hosts) > max_hosts:
        abort('Too many hosts in host string "%s". The maximum is %d.'
                    % (host_string, max_hosts))
    return hosts

def _split_ip4(host_port):
    split_host = host_port.split(':')
    if len(split_host) == 1:
        return (host_port, None)
    elif len(split_host) == 2:
        return (split_host[0], split_host[1])
    else:
        abort('Bad HOST:PORT format "%s" - too many colons.' % host_port)

def _split_ip6(host_port):
    addr = port = None
    if host_port[0] == '[': # bracketed address is unambiguous
        split_host = host_port[1:].split(']',1)
        if len(split_host) != 2:
            abort('Bad [HOST]:PORT format "%s" - missing bracket.' % host_port)
        addr = split_host[0]
        if split_host[1] != '':
            if split_host[1][0] != ':':
                abort('Bad [HOST]:PORT format "%s" - colon required.' % host_port)
            port = split_host[1][1:]
    else: # we must assume it's just an IPv6 address
        addr = host_port
    return (addr, port)

def _split_name(host_port):
    return _split_ip4(host_port)

#===============================================================================
def paragraph(*lines):
#===============================================================================
    """
    Strip leading and trailing whitespace and wrap text into a paragraph block.
    The arguments can include arbitrarily nested sequences.
    """
    wlines = []
    for line in flatten_to_list(lines):
        wlines.extend(line.strip().split('\n'))
    return textwrap.fill('\n'.join(wlines))

#===============================================================================
class File(object):
#===============================================================================
    """
    File reader/writer object that aborts on any error. Must explicitly call
    close(). The main point is to standardize the error-handling.
    """
    def __init__(self, path, mode = 'r', make_dirs=False):
        if mode not in ('r', 'w'):
            abort('Invalid file mode "%s".' % mode)
        self.path      = path
        self.mode      = mode
        self.make_dirs = make_dirs
        self.f         = None
    def open(self):
        self.close()
        if self.mode == 'w' and self.make_dirs:
            dir = os.path.dirname(self.path)
            if dir and not os.path.exists(dir):
                try:
                    os.makedirs(dir)
                except (IOError, OSError) as e:
                    self._abort('Unable to create directory "%s".' % dir)
        self.f = self._open()
    def read(self):
        if self.mode != 'r':
            self._abort('File is not open for reading in call to read().')
        # Reading the entire file, so we can automatically open and close here.
        if self.f is None:
            f = self._open()
        else:
            f = self.f
        try:
            try:
                return f.read()
            except (IOError, OSError) as e:
                self._abort('Read error.', e)
        finally:
            # Close locally-opened file.
            if self.f is None:
                f.close()
    def read_hex(self):
        content = self.read()
        return binascii.hexlify(content.encode())
    def write(self, s):
        if self.mode != 'w':
            self._abort('File is not open for writing in call to write().')
        if self.f is None:
            self._abort('File was not opened in call to write().')
        try:
            self.f.write(s)
        except (IOError, OSError) as e:
            self._abort('Write error.', e)
    def close(self):
        if self.f:
            self.f.close()
    def _open(self):
        try:
            return open(self.path, self.mode)
        except (IOError, OSError) as e:
            self._abort('File open error.', e)
    def _abort(self, msg, e = None):
        msgs = ['''File("%s",'%s'): %s''' % (self.path, self.mode, msg)]
        if e:
            msgs.append(str(e))
        abort(*msgs)

#===============================================================================
class FileGenerator(object):
#===============================================================================
    """
    File generator.
    """

    def __init__(self, resource_finder, **symbols):
        """
        resource_finder must implement a find_resource(path) method.
        """
        self.resource_finder = resource_finder
        self.symbols = copy.copy(symbols)
        self.generated = []

    def add_symbols(self, **symbols):
        self.symbols.update(symbols)

    def from_template(self, src, tgt=None, permissions=None):
        if tgt is None:
            tgt = src
        info('Generating "%s"...' % tgt)
        src_path = self.resource_finder.find_resource(src)
        src_file = File(src_path)
        src_file.open()
        try:
            template = string.Template(src_file.read())
            s = template.safe_substitute(**self.symbols)
        finally:
            src_file.close()
        tgt_file = File(tgt, mode='w', make_dirs=True)
        tgt_file.open()
        try:
            tgt_file.write(s)
            self.generated.append(tgt)
        finally:
            tgt_file.close()
        if permissions is not None:
            os.chmod(tgt, 0o755)

    def custom(self, tgt, callback):
        info('Generating "%s"...' % tgt)
        output_stream = File(tgt, 'w')
        output_stream.open()
        try:
            callback(output_stream)
            self.generated.append(tgt)
        finally:
            output_stream.close()


#===============================================================================
class INIConfigManager(object):
#===============================================================================
    """
    Loads/saves INI format configuration to and from a dictionary.
    """

    def load(self, path):
        parser = configparser.SafeConfigParser()
        parser.read(path)
        d = dict()
        for section in parser.sections():
            for name, value in parser.items(section):
                d['%s.%s' % (section, name)] = value
        return d

    def save(self, path, d):
        parser = configparser.SafeConfigParser()
        keys = list(d.keys())
        keys.sort()
        cur_section = None
        for key in keys:
            if key.find('.') == -1:
                abort('Key "%s" must have a section, e.g. "volt.%s"' % (key, key))
            else:
                section, name = key.split('.', 1)
            if cur_section is None or section != cur_section:
                parser.add_section(section)
                cur_section = section
            parser.set(cur_section, name, d[key])
        f = File(path, 'w')
        f.open()
        try:
            parser.write(f)
        finally:
            f.close()

#===============================================================================
class PersistentConfig(object):
#===============================================================================
    """
    Persistent access to configuration data. Manages two configuration
    files, one for permanent configuration and the other for local state.
    """

    def __init__(self, format, path, local_path):
        """
        Construct persistent configuration based on specified format name, path
        to permanent config file, and path to local config file.
        """
        self.path = path
        self.local_path = local_path
        if format.lower() == 'ini':
            self.config_manager = INIConfigManager()
        else:
            abort('Unsupported configuration format "%s".' % format)
        self.permanent = self.config_manager.load(self.path)
        if self.local_path:
            self.local = self.config_manager.load(self.local_path)
        else:
            self.local = {}

    def save_permanent(self):
        """
        Save the permanent configuration.
        """
        self.config_manager.save(self.path, self.permanent)

    def save_local(self):
        """
        Save the local configuration (overrides and additions to permanent).
        """
        if self.local:
            self.config_manager.save(self.local_path, self.local)
        else:
            error('No local configuration was specified. (%s)' % tag,
                  'For reference, the permanent configuration is "%s".' % self.path)

    def get(self, key):
        """
        Get a value for a key from the merged configuration.
        """
        if key in self.local:
            return self.local[key]
        return self.permanent.get(key, None)

    def set_permanent(self, key, value):
        """
        Set a key/value pair in the permanent configuration.
        """
        self.permanent[key] = value
        self.save_permanent()

    def set_local(self, key, value):
        """
        Set a key/value pair in the local configuration.
        """
        self.local[key] = value
        if self.local:
            self.save_local()

    def query(self, filter = None):
        """
        Query for keys and values as a merged dictionary.
        The optional filter is matched against the start of each key.
        """
        if filter:
            results = {}
            for key in self.local:
                if key.startswith(filter):
                    results[key] = self.local[key]
            for key in self.permanent:
                if key not in results and key.startswith(filter):
                    results[key] = self.permanent[key]
        else:
            results = self.local
            for key in self.permanent:
                if key not in results:
                    results[key] = self.permanent[key]
        return results

    def query_pairs(self, filter = None):
        """
        Query for keys and values as a sorted list of (key, value) pairs.
        The optional filter is matched against the start of each key.
        """
        return dict_to_sorted_pairs(self.query(filter = filter))

#===============================================================================
class VoltTupleWrapper(object):
#===============================================================================
    """
    Wraps a Volt tuple to add error handling, type safety, etc..
    """
    def __init__(self, tuple):
        self.tuple = tuple
    def column_count(self, index):
        return len(self.tuple)
    def column(self, index):
        if index < 0 or index >= len(self.tuple):
            abort('Bad column index %d (%d columns available).' % (index, len(self.tuple)))
        return self.tuple[index]
    def column_string(self, index):
        return str(self.column(index))
    def column_integer(self, index):
        try:
            return int(self.column(index))
        except ValueError:
            abort('Column %d value (%s) is not an integer.' % (index, str(self.column(index))))
    def __str__(self):
        return format_table([self.tuple])

#===============================================================================
class VoltTableWrapper(object):
#===============================================================================
    """
    Wraps a voltdbclient.VoltTable to add error handling, type safety, etc..
    """
    def __init__(self, table):
        self.table = table
    def tuple_count(self):
        return len(self.table.tuples)
    def tuple(self, index):
        if index < 0 or index >= len(self.table.tuples):
            abort('Bad tuple index %d (%d tuples available).' % (index, len(self.table.tuples)))
        return VoltTupleWrapper(self.table.tuples[index])
    def tuples(self):
        return self.table.tuples
    def format_table(self, caption = None):
        return format_volt_table(self.table, caption = caption)
    def __str__(self):
        return self.format_table()

#===============================================================================
class VoltResponseWrapper(object):
#===============================================================================
    """
    Wraps a voltdbclient.VoltResponse to add error handling, type safety, etc..
    """
    def __init__(self, response):
        self.response = response
    def status(self):
        return self.response.status
    def table_count(self):
        if not self.response.tables:
            return 0
        return len(self.response.tables)
    def table(self, index):
        if index < 0 or index >= self.table_count():
            abort('Bad table index %d (%d tables available).' % (index, self.table_count()))
        return VoltTableWrapper(self.response.tables[index])
    def format_tables(self, caption_list = None):
        return format_volt_tables(self.response.tables, caption_list = caption_list)
    def __str__(self):
        output = [str(self.response)]
        if self.table_count() > 0:
            output.append(self.format_tables())
        return '\n\n'.join(output)

#===============================================================================
class MessageDict(dict):
#===============================================================================
    """
    Message dictionary provides message numbers as attributes or the messages
    by looking up that message number in the underlying dictionary.
        messages.MY_MESSAGE == <integer index>
        messages[messages.MY_MESSAGE] == <string>
    """
    def __init__(self, **kwargs):
        dict.__init__(self)
        i = 0
        for key in kwargs:
            i += 1
            self[i] = kwargs[key]
            setattr(self, key, i)

#===============================================================================
class CodeFormatter(object):
#===============================================================================
    """
    Useful for formatting generated code. It is currently geared for DDL, but
    this isn't etched in stone.
    """
    def __init__(self, separator=',', vcomment_prefix='', indent_string='    '):
        self.separator = separator
        self.vcomment_prefix = vcomment_prefix
        self.indent_string = indent_string
        self.level = 0
        self.lines = []
        self.pending_separator = [-1]
        self.block_start_index = [0]
    def _line(self, needs_separator, *lines):
        if needs_separator and self.separator and self.pending_separator[-1] >= 0:
            self.lines[self.pending_separator[-1]] += self.separator
        for line in lines:
            self.lines.append('%s%s' % (self.indent_string * self.level, line))
        if needs_separator:
            self.pending_separator[-1] = len(self.lines) - 1
    def _block_line(self, *lines):
        if self.pending_separator[-1] >= self.block_start_index[-1]:
            self.pending_separator[-1] += len(lines)
        for line in lines:
            self.lines.insert(self.block_start_index[-1], line)
            self.block_start_index[-1] += 1
    def block_start(self, *lines):
        if self.level == 0:
            self._line(False, '')
        self.block_start_index.append(len(self.lines))
        self._line(False, *lines)
        self._line(False, '(')
        self.level += 1
        self.pending_separator.append(-1)
    def block_end(self, *lines):
        self.level -= 1
        self.pending_separator.pop()
        if self.level == 0:
            self._line(False, ');')
        else:
            self._line(True, ')')
        self.block_start_index.pop()
    def code(self, *lines):
        self._line(False, *lines)
    def code_fragment(self, *lines):
        self._line(True, *lines)
    def comment(self, *lines):
        for line in lines:
            self._line(False, '-- %s' % line)
    def vcomment(self, *lines):
        for line in lines:
            self._line(False, '--%s %s' % (self.vcomment_prefix, line))
    def block_comment(self, *lines):
        for line in lines:
            self._block_line('-- %s' % line)
    def block_vcomment(self, *lines):
        for line in lines:
            self._block_line('--%s %s' % (self.vcomment_prefix, line))
    def blank(self, n=1):
        for line in range(n):
            self._line(False, '')
    def __str__(self):
        return '\n'.join(self.lines)
