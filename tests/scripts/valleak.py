#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2015 VoltDB Inc.
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

# Exit with an error code if Valgrind finds "definitely lost" memory.

import os
import subprocess
import sys
import tempfile

VALGRIND = "valgrind"

def valleak(executable):
    """Returns (error, stdout, stderr).

    error == 0 if successful, an integer > 0 if there are memory leaks or errors."""

    valgrind_output = tempfile.NamedTemporaryFile()

    valgrind_command = (
            VALGRIND,
            "--leak-check=full",
            "--log-file-exactly=" + valgrind_output.name,
            "--error-exitcode=1",
            executable)
    process = subprocess.Popen(
            valgrind_command,
            bufsize = -1,
            stdin = subprocess.PIPE,
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE,
            close_fds = True)

    process.stdin.close()
    stdout = process.stdout.read()
    stderr = process.stderr.read()
    error = process.wait()

    valgrind_error_file = open(valgrind_output.name)
    valgrind_error = valgrind_error_file.read()
    valgrind_error_file.close()
    valgrind_output.close()

    # Find the last summary block in the valgrind report
    # This ignores forks
    summary_start = valgrind_error.rindex("== ERROR SUMMARY:")
    summary = valgrind_error[summary_start:]

    append_valgrind = False
    if error == 0:
        assert "== ERROR SUMMARY: 0 errors" in summary
        # Check for memory leaks
        if "==    definitely lost:" in summary:
            error = 1
            append_valgrind = True
    elif "== ERROR SUMMARY: 0 errors" not in summary:
        # We also have valgrind errors: append the log to stderr
        append_valgrind = True
    if append_valgrind:
        stderr = stderr + "\n\n" + valgrind_error

    return (error, stdout, stderr)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.stderr.write("valleak.py [executable]\n")
        sys.exit(1)

    exe = sys.argv[1]
    error, stdin, stderr = valleak(exe)
    sys.stdout.write(stdin)
    sys.stderr.write(stderr)
    sys.exit(error)
