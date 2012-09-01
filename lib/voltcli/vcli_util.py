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

import sys
import subprocess
from xml.etree import ElementTree

import vcli_opt

# Republish all the symbols from _util.
from _util import *

__author__ = 'scooper'

def debug(*msgs):
    """
    Display debug message(s) if debug is enabled.
    """
    if vcli_opt.debug:
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
    if vcli_opt.dryrun:
        print fullcmd
    else:
        retcode = os.system(fullcmd)
        if retcode != 0:
            abort('return code %d: %s' % (retcode, fullcmd))

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
