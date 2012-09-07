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

# Completely independent simple utility functions. Can be safely imported
# before full initialization complets. This namespace gets absorbed into util,
# so most places should import vcli_util rather than _util.

__author__ = 'scooper'

import sys
import os
import glob
import copy
import inspect
import ConfigParser

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
