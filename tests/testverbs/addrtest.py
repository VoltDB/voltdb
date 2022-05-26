#!/usr/bin/env python
# -*-mode: python-*-

# This file is part of VoltDB.
# Copyright (C) 2022 Volt Active Data Inc.
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
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import sys

try:
    from utility import parse_hosts
except ImportError as ex:
    print(ex)
    print('\n This program tests parse_hosts from lib/python/voltcli/utility.py'
          '\n Please define PYTHONPATH apppropriately to locate that file.\n')
    sys.exit(2)

count = 0
succeeded = 0
failed = 0

def do_parse(host_string, default_port, expected_host, expected_port):
    global count
    count = count + 1
    report("** Test case #%d: '%s' (default port %s)" % (count, host_string, default_port))
    result = parse_hosts(host_string=host_string, default_port=default_port)
    result_host = result[0].host
    result_port = result[0].port
    report("Host='%s', expected='%s', %s" % (result_host, expected_host, comparison(result_host, expected_host)))
    report("Port='%s', expected='%s', %s" % (result_port, expected_port, comparison(result_port, expected_port)))
    return result_host == expected_host and result_port == expected_port

def comparison(a, b):
    return 'same' if a == b else 'not same'

def test_parse(host_string, default_port, expected_host, expected_port):
    try:
        if do_parse(host_string, default_port, expected_host, expected_port):
            succeed()
        else:
            fail("Result does not match expectation")
    except SystemExit:
        fail("Unexpected test abort")
    except Exception as ex:
        fail("Exception: %s" % ex)

def test_abort(host_string, default_port, expected_host, expected_port):
    try:
        do_parse(host_string, default_port, expected_host, expected_port)
        fail("Test did not abort as expected")
    except SystemExit:
        report('Test aborted as expected')
        succeed()
    except Exception as ex:
        fail("Exception: %s" % ex)

def succeed():
    global succeeded
    succeeded = succeeded + 1
    report('** Test #%d succeeded\n' % count)

def fail(msg):
    global failed
    failed = failed + 1
    report(msg)
    report('** Test #%d failed\n' % count)

def report(msg):
    print(msg);

def main():
    print((sys.argv))

    # These are all legitimate address expressions
    test_parse('127.0.0.1:21212', None, '127.0.0.1', 21212)
    test_parse('127.0.0.1', 21212, '127.0.0.1', 21212)
    test_parse('[::1]:21212', None, '::1', 21212)
    test_parse('[::1]', 21212, '::1', 21212)
    test_parse('[::ffff:127.0.0.1]:21212', None, '::ffff:127.0.0.1', 21212)
    test_parse('[::ffff:127.0.0.1]', 21212, '::ffff:127.0.0.1', 21212)

    # These all fail with unexpected syntax errors (which
    # causes the parser to rudely call sys.exit())
    test_abort('127.0.0.1:212:12', None, '127.0.0.1', 21212)
    test_abort('127.0.0.1:', 21212, '127.0.0.1', 21212)
    test_abort('::1:21212', None, '::1', 21212)
    test_abort('::1', 21212, '::1', 21212)
    test_abort('::ffff:127.0.0.1:21212', None, '::ffff:127.0.0.1', 21212)
    test_abort('::ffff:127.0.0.1', 21212, '::ffff:127.0.0.1', 21212)

    report("Tests run %d, succeeded %d, failed %d" % (count, succeeded, failed))

if __name__ == "__main__":
    main()
