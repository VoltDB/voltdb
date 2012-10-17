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

# Volt CLI utility functions.

# IMPORTANT: This depends on no other voltcli modules. Please keep it that way.

import sys
import os
import subprocess
import glob
import copy
import inspect
import ConfigParser
import zipfile
import re
from xml.etree import ElementTree

__author__ = 'scooper'

#===============================================================================
class Global:
#===============================================================================
    """
    Global data for utilities.
    """
    debug_enabled = False
    dryrun_enabled = False

#===============================================================================
def set_debug(debug):
#===============================================================================
    """
    Enable or disable debug messages.
    """
    Global.debug_enabled = debug

#===============================================================================
def set_dryrun(dryrun):
#===============================================================================
    """
    Enable or disable command dry run (display only/no execution).
    """
    Global.dryrun_enabled = dryrun

#===============================================================================
def display_messages(msgs, f = sys.stdout, tag = None, level = 0):
#===============================================================================
    """
    Low level message display.
    """
    if tag:
        stag = '%8s: ' % tag
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
                try:
                    # Raises TypeError if not string
                    var = msg + ' '
                    # If it is a string slice and dice it by linefeeds.
                    for msg2 in msg.split('\n'):
                        f.write('%s%s%s\n' % (stag, sindent, msg2))
                except TypeError:
                    # Recursively display an iterable with indentation added.
                    if hasattr(msg, '__iter__'):
                        display_messages(msg, f = f, tag = tag, level = level + 1)
                    else:
                        f.write('%s%s%s\n' % (stag, sindent, str(msg)))

#===============================================================================
def info(*msgs):
#===============================================================================
    """
    Display INFO level messages.
    """
    display_messages(msgs, tag = 'INFO')

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
def abort(*msgs):
#===============================================================================
    """
    Display ERROR messages and then abort.
    """
    error(*msgs)
    display_messages('Exiting.', f = sys.stderr, tag = 'FATAL')
    sys.exit(1)

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
def search_and_execute(scan_dirs, **syms):
#===============================================================================
    """
    Look for python source files in a named subdirectory of a set of base
    directories. Execute all discovered source files and pass in the symbols
    provided.
    A typical usage will be to supply decorators among the symbols which can
    mark discoverable functions in user code. The decorator will be called when
    the source file is executed which serves as an opportunity to keep track of
    the discovered functions.
    """
    processed_dirs = set()
    for scan_dir in scan_dirs:
        if scan_dir not in processed_dirs:
            processed_dirs.add(scan_dir)
            if os.path.exists(scan_dir):
                for modpath in glob.glob(os.path.join(scan_dir, '*.py')):
                    debug('Executing module "%s"...' % modpath)
                    execfile(modpath, syms)

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
    output = []
    for item in items:
        if len(output) == width:
            break
        output.append(item)
    if len(output) < width:
        output += filler * (width - len(output))
    return tuple(output)

#===============================================================================
def format_table(caption, headings, data_rows):
#===============================================================================
    """
    Format a tabular display including an optional caption, optional column
    headings, and rows of data cells. Aligns the headings and data cells.
    Headings and data rows must be iterable. Each data row must provide
    iterable cells.  For now it only handles stringized data and right
    alignment. Returns the table-formatted string.
    """
    output = []
    # Display the caption, if supplied.
    if caption:
        output.append('-- %s --\n' % caption)
    # Add a row for headings, if supplied.
    rows = []
    if headings:
        rows[0] = heading_row = []
        for heading in headings:
            heading_row.append('- %s -' % heading)
    rows.extend(data_rows)
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
    # Generate the format string and then format the headings and rows.
    fmt = '  '.join(['%%-%ds' % width for width in widths])
    for row in rows:
        output.append(fmt % normalize_list(row, len(widths), ''))
    return '\n'.join(output)

#===============================================================================
def debug(*msgs):
#===============================================================================
    """
    Display debug message(s) if debug is enabled.
    """
    if Global.debug_enabled:
        display_messages(msgs, tag = 'DEBUG')

#===============================================================================
def parse_xml(xml_path):
#===============================================================================
    """
    Parses XML and returns an ElementTree object to provide access to element data.
    """
    et = ElementTree.ElementTree()
    try:
        return et.parse(xml_path)
    except (OSError, IOError), e:
        abort('Failed to parse XML file.', (xml_path, e))

#===============================================================================
def run_cmd(cmd, *args):
#===============================================================================
    """
    Run external program without capturing or suppressing output and check return code.
    """
    fullcmd = cmd
    for arg in args:
        if len(arg.split()) > 1:
            fullcmd += ' "%s"' % arg
        else:
            fullcmd += ' %s' % arg
    if Global.dryrun_enabled:
        sys.stdout.write('%s\n' % fullcmd)
    else:
        retcode = os.system(fullcmd)
        if retcode != 0:
            abort('Command "%s ..." failed with return code %d.' % (cmd, retcode))

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
    except Exception, e:
        warning('Exception running command: %s' % ' '.join(args), e)

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
def _flatten(*items):
#===============================================================================
    """
    Internal function to recursively iterate a potentially nested sequence.
    None items are filtered out.
    """
    for item in items:
        if is_sequence(item):
            for subitem in item:
                for subsubitem in _flatten(subitem):
                    if subitem is not None:
                        yield subsubitem
        else:
            if item is not None:
                yield item

#===============================================================================
def flatten(*items):
#===============================================================================
    """
    Flatten and yield individual items from a potentially nested list or tuple.
    """
    for item in _flatten(*items):
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
    exclusion regular expresions provided.
    """
    def __init__(self, *excludes):
        self.output_zip = None
        self.output_path = None
        self.re_excludes = [re.compile(exclude) for exclude in excludes]

    def open(self, output_path):
        self.output_path = output_path
        if not Global.dryrun_enabled:
            try:
                self.output_zip = zipfile.ZipFile(self.output_path, 'w', zipfile.ZIP_DEFLATED)
            except (IOError, OSError), e:
                self._abort('Failed to open for writing.', e)

    def close(self):
        if self.output_zip:
            self.output_zip.close()

    def add_file(self, path_in, path_out):
        for re_exclude in self.re_excludes:
            path_in_full = os.path.realpath(path_in)
            if re_exclude.search(path_in):
                self._debug('skip "%s"' % path_in_full)
                break
        else:
            self._debug('add "%s" as "%s"' % (path_in_full, path_out))
            try:
                if self.output_zip:
                    self.output_zip.write(path_in, path_out)
            except (IOError, OSError), e:
                self._abort('Failed to write file "%s" to output zip file "%s".', path_out, e)

    def add_string(self, s, path_out):
        self._debug('write string to "%s"' % path_out)
        if self.output_zip:
            try:
                self.output_zip.writestr(path_out, s)
            except (IOError, OSError), e:
                self._abort('Failed to write string to file "%s".' % path_out, e)

    def add_directory(self, path_in, dst, excludes = []):
        if not os.path.isdir(path_in):
            self._abort('Zip source directory "%s" does not exist.' % path_in)
        savedir = os.getcwd()
        # Get nice relative paths by temporarily switching directories.
        os.chdir(path_in)
        try:
            for basedir, subdirs, filenames in os.walk('.'):
                for filename in filenames:
                    file_path_in = os.path.join(basedir, filename)[2:]
                    file_path_out = os.path.join(dst, basedir[2:], filename)
                    self.add_file(file_path_in, file_path_out)
        finally:
            os.chdir(savedir)

    def _debug(self, msg):
        debug('%s: %s' % (self.output_path, msg))

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
class FileWriter(object):
#===============================================================================
    """
    File writing object that aborts on any error. Must explicitly call close().
    """
    def __init__(self, path):
        self.path = path
        self.f    = None
    def open(self):
        try:
            self.f = open(path, 'w')
        except (IOError, OSError), e:
            abort('%s: Error opening "%s"' % (self.__class__.__name__, self.path), e)
    def write(self, s):
        try:
            parser.write(self.f, s)
        except (IOError, OSError), e:
            abort('%s: Error writing to "%s"' % (self.__class__.__name__, self.path), e)
    def close(self):
        if self.f:
            self.f.close()

#===============================================================================
class XMLConfigManager(object):
#===============================================================================
    """
    Loads/saves XML format configuration to and from a dictionary.
    """

    def load(self, path):
        parser = ConfigParser.SafeConfigParser()
        parser.read(path)
        d = dict()
        for section in parser.sections():
            for name, value in parser.items(section):
                d['%s.%s' % (section, name)] = value
        return d

    def save(self, path, d):
        parser = ConfigParser.SafeConfigParser()
        keys = d.keys()
        keys.sort()
        cur_section = None
        for key in keys:
            section, name = key.split('.', 1)
            if cur_section is None or section != cur_section:
                parser.add_section(section)
                cur_section = section
            parser.set(cur_section, name, d[name])
            f = FileWriter(path)
            try:
                parser.write(f)
            finally:
                f.close()

#===============================================================================
class INIConfigManager(object):
#===============================================================================
    """
    Loads/saves INI format configuration to and from a dictionary.
    """

    def load(self, path):
        parser = ConfigParser.SafeConfigParser()
        parser.read(path)
        d = dict()
        for section in parser.sections():
            for name, value in parser.items(section):
                d['%s.%s' % (section, name)] = value
        return d

    def save(self, path, d):
        parser = ConfigParser.SafeConfigParser()
        keys = d.keys()
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
        f = FileWriter(path)
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

    def __init__(self, format, permanent_path, local_path):
        """
        Construct persistent configuration based on specified format name, path
        to permanent config file, and path to local config file.
        """
        self.permanent_path = permanent_path
        self.local_path     = local_path
        if format.lower() == 'ini':
            self.config_manager = INIConfigManager()
        else:
            abort('Unsupported configuration format "%s".' % format)
        self.permanent = self.config_manager.load(self.permanent_path)
        self.local     = self.config_manager.load(self.local_path)

    def save_permanent(self):
        """
        Save the permanent configuration.
        """
        self.config_manager.save(self.permanent_path, self.permanent)

    def save_local(self):
        """
        Save the local configuration (overrides and additions to permanent).
        """
        self.config_manager.save(self.local_path, self.local)

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
        self.save_local()

    def query(self, filter = filter):
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
                    self.results[key] = self.permanent[key]
        else:
            results = self.local
            for key in self.permanent:
                if key not in results:
                    self.results[key] = self.permanent[key]
        return results

    def query_pairs(self, filter = None):
        """
        Query for keys and values as a sorted list of (key, value) pairs.
        The optional filter is matched against the start of each key.
        """
        d = self.query(filter = filter)
        keys = d.keys()
        keys.sort()
        results = []
        for key in keys:
            results.append((key, d[key]))
        return results
