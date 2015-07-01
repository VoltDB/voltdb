#!/usr/bin/env python

# Allows quick bulk testing of regular expressions against test strings.
# You need one or more input files with labeled regular expressions
# and test strings. See the usage message for syntax.
#
# Here is a sample:
#
# pattern:
# ^(apple|orange).*$
#
# text:
# apple whatever
#
# text:
# orange blah
#
# pattern:
# ...

import sys
import os
import re

MAX_DISPLAY_LENGTH = 30

RE_DIRECTIVE = re.compile('^\s*(pattern|text)\s*:\s*(.*)$', re.IGNORECASE)
RE_CAPTURE_LABEL = re.compile(r'[(][?]<[^>]*>')

USAGE = ('''
Usage: %s FILE ...

FILE format:

    pattern:
    (regular expression lines)

    text:
    (text lines to test against regular expression)

    ...

Regular expressions and text blocks can span multiple lines.

Multiple text blocks can follow a pattern.

All whitespace is stripped from regular expressions to allow
formatting for better readability.

Text can be on the same line as the pattern or text directive.

It gives up after the first pattern match failure.
''' % os.path.basename(sys.argv[0])).strip()

def shorten(s, maxLen):
    if s is None:
        return 'None'
    if len(s) <= maxLen:
        return s
    left = '%s...' % s[:(maxLen - 3) / 2]
    right = s[-(maxLen - len(left)):]
    return ''.join((left, right))

def shorten_all(maxLength, *args):
    return tuple((shorten(s, maxLength) for s in args))

class Tester(object):
    def __init__(self, *lines):
        self.counter = 0
        expr = ''.join(lines).replace(' ', '').replace('\n', '')
        self.pattern = RE_CAPTURE_LABEL.sub('(', expr)
        self.matcher = re.compile(self.pattern, re.IGNORECASE|re.DOTALL|re.MULTILINE)
    def test(self, *lines):
        statement = '\n'.join(lines).strip().replace(r'\n', '\n')
        self.counter += 1
        if self.counter == 1:
            print ''
            print '========================================='
            print self.pattern
            print '----------'
        print statement.replace('\n', '\\n\n')
        m = self.matcher.match(statement)
        if not m is None:
            print 'MATCH: %s' % str(shorten_all(MAX_DISPLAY_LENGTH, *m.groups()))
        else:
            print '* NO MATCH * (quitting)'
            sys.exit(1)
        print '----------'

def _parse_lines(path):
    with open(path) as f:
        for line in f:
            s = line.rstrip()
            m = RE_DIRECTIVE.match(s)
            if not m is None:
                yield (m.group(1), m.group(2))
            else:
                yield (None, s)

def test(path):
    tester = None
    mode = None
    lines = []
    for (next_mode, line) in _parse_lines(path):
        if not next_mode is None:
            if mode == 'pattern':
                tester = Tester(*lines)
            elif mode == 'text':
                if not tester is None:
                    tester.test(*lines)
                else:
                    print '* No pattern to test against *'
            lines = []
            mode = next_mode
        lines.append(line)
    if not tester is None and mode == 'text' and len(lines) > 0:
        tester.test(*lines)

if __name__ == '__main__':
    paths = sys.argv[1:]
    if len(paths) == 0:
        print USAGE
        sys.exit(1)
    try:
        for path in paths:
            test(path)
    except (IOError, OSError), e:
        print str(e)
        sys.exit(1)
