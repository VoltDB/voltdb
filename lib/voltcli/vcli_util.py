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

# IMPORTANT: This depends on no other vcli_... modules. Please keep it that way.

import sys
import os
import subprocess
import glob
import copy
import inspect
import ConfigParser
from xml.etree import ElementTree

__author__ = 'scooper'

# Runtime options
debug_enabled = False
dryrun_enabled = False

def set_debug(debug):
    """
    Enable or disable debug messages.
    """
    global debug_enabled
    debug_enabled = debug

def set_dryrun(dryrun):
    """
    Enable or disable command dry run (display only/no execution).
    """
    global dryrun_enabled
    dryrun_enabled = dryrun

def display_messages(msgs, f = sys.stdout, tag = None, level = 0):
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

def info(*msgs):
    """
    Display INFO level messages.
    """
    display_messages(msgs, tag = 'INFO')

def warning(*msgs):
    """
    Display WARNING level messages.
    """
    display_messages(msgs, tag = 'WARNING')

def error(*msgs):
    """
    Display ERROR level messages.
    """
    display_messages(msgs, tag = 'ERROR')

def abort(*msgs):
    """
    Display ERROR messages and then abort.
    """
    error(*msgs)
    display_messages('Exiting.', f = sys.stderr, tag = 'FATAL')
    sys.exit(1)

def find_in_path(name):
    """
    Find program in the system path.
    """
    # NB: non-portable
    for dir in os.environ['PATH'].split(':'):
        if os.path.exists(os.path.join(dir, name)):
            return os.path.join(dir, name)
    return None

def find_and_load_subclasses(base_class, base_dirs, sub_dir, **kwargs):
    """
    Look for python modules in a named subdirectory of a set of base
    directories. Load any modules found and find all classes derived from the
    specified base class. Return the classes found. Classes should implement
    a __cmp__() method to properly support duplicate detection.
    """
    # found holds (class, path) pairs
    found = []
    processed_dirs = set()
    for base_dir in base_dirs:
        scan_dir = os.path.realpath(os.path.join(base_dir, sub_dir))
        if scan_dir not in processed_dirs:
            processed_dirs.add(scan_dir)
            if os.path.exists(scan_dir):
                for modpath in glob.glob(os.path.join(scan_dir, '*.py')):
                    filename = os.path.basename(modpath)
                    name = os.path.splitext(filename)[0]
                    # Start with an initial symbol table based on what the caller provided.
                    syms = copy.copy(kwargs)
                    before = set(syms.keys())
                    # Execute the module. The symbol table gets populated with the module symbols.
                    execfile(modpath, syms)
                    # Check the symbol table additions for subclasses of base_class.
                    after = set(syms)
                    for name in after.difference(before):
                        # Ignore modules starting with '_' to allow utility modules.
                        if not name.startswith('_'):
                            o = syms[name]
                            if inspect.isclass(o) and issubclass(o, base_class):
                                subclass = o()
                                for subclass2, modpath2 in found:
                                    if subclass2 == subclass:
                                        warning('Ignoring class "%s" in "%s"'
                                                        % (o.__name__, modpath), [
                                                    'It conflicts with "%s" in "%s".'
                                                            % (subclass2.__class__.__name__,
                                                               modpath2),
                                                    'Check metadata for conflicting identifier.'])
                                        break
                                else:
                                    found.append((subclass, modpath))
    return [subclass for subclass, modpath in found]

def normalize_list(items, width, filler = None):
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

def format_table(caption, headings, data_rows):
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
def debug(*msgs):
    """
    Display debug message(s) if debug is enabled.
    """
    if debug_enabled:
        display_messages(msgs, tag = 'DEBUG')

def parse_xml(xml_path):
    """
    Parses XML and returns an ElementTree object to provide access to element data.
    """
    et = ElementTree.ElementTree()
    try:
        return et.parse(xml_path)
    except (OSError, IOError), e:
        abort('Failed to parse XML file.', (xml_path, e))

def run_cmd(cmd, *args):
    """
    Run external program without capturing or suppressing output and check return code.
    """
    fullcmd = cmd
    for arg in args:
        if len(arg.split()) > 1:
            fullcmd += ' "%s"' % arg
        else:
            fullcmd += ' %s' % arg
    if dryrun_enabled:
        print fullcmd
    else:
        retcode = os.system(fullcmd)
        if retcode != 0:
            abort('Command "%s ..." failed with return code %d.' % (cmd, retcode))

def pipe_cmd(*args):
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

def _flatten(*items):
    for item in items:
        try:
            test_string = item + ''
            # It's a string
            yield item
        except TypeError:
            try:
                test_iter = iter(item)
                # Use recursion in case it's nested further.
                for subitem in item:
                    for subsubitem in _flatten(subitem):
                        yield subsubitem
            except TypeError:
                # It's a non-iterable non-string
                yield item

def flatten(*items):
    """
    Flatten and yield individual items from a potentially nested list or tuple.
    """
    for item in _flatten(*items):
        yield item

def merge_java_options(*opts):
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

class XMLConfigManager(object):
    """Loads/saves XML format configuration to and from a dictionary."""

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
        with open(path, 'w') as f:
            parser.write(f)

class INIConfigManager(object):
    """Loads/saves INI format configuration to and from a dictionary."""

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
        with open(path, 'w') as f:
            parser.write(f)

class PersistentConfig(object):
    """Persistent access to configuration data. Manages two configuration
    files, one for permanent configuration and the other for local state."""

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
