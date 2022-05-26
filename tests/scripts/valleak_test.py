#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.
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

import os
import shutil
import subprocess
import tempfile
import unittest

import valleak


class TempDir(object):
    def __init__(self):
        self.name = tempfile.mkdtemp()

    def __del__(self):
        shutil.rmtree(self.name)


def compile(attach_object, text):
    attach_object.tempdir = TempDir()

    exe_name = attach_object.tempdir.name + "/test"
    source_name = exe_name + ".c"
    out = open(source_name, "w")
    out.write(text)
    out.close()
    gcc = subprocess.Popen(
            ("gcc", "-Wall", "-Werror", "-o", exe_name, source_name),
            stdout = subprocess.PIPE)
    error = gcc.wait()
    os.unlink(source_name)

    if error != 0:
        raise Exception("compile failed")

    return exe_name


class ValLeakTest(unittest.TestCase):
    def valleak(self):
        return valleak.valleak(self.tempdir.name + "/test")

    def testNoError(self):
        compile(self, """
#include <stdio.h>

int main() {
  printf("hello world\\n");
  fprintf(stderr, "hello stderr\\n");
  return 0;
}
""")
        error, stdout, stderr = self.valleak()
        self.assertEquals(0, error)
        self.assertEquals("hello world\n", stdout)
        self.assertEquals("hello stderr\n", stderr)

    def testSimpleError(self):
        compile(self, """
#include <stdio.h>

int main() {
  printf("hello world\\n");
  fprintf(stderr, "hello stderr\\n");
  return 5;
}
""")
        error, stdout, stderr = self.valleak()
        self.assertEquals(5, error)
        self.assertEquals("hello world\n", stdout)
        # Note: no valgrind output
        self.assertEquals("hello stderr\n", stderr)

    def testSimpleValgrindError(self):
        compile(self, """
#include <stdio.h>
#include <stdlib.h>

int main() {
  int* array = (int*) malloc(sizeof(*array) * 2);
  printf("hello world %d\\n", array[2]);
  free(array);
  return 0;
}
""")
        error, stdout, stderr = self.valleak()
        self.assertEquals(1, error)
        assert stdout.startswith("hello world")
        # valgrind output
        assert "== ERROR SUMMARY" in stderr

    def testValgrindLeak(self):
        compile(self, """
#include <stdio.h>
#include <stdlib.h>

int main() {
  int* array = (int*) malloc(sizeof(*array) * 2);
  return 0;
}
""")
        error, stdout, stderr = self.valleak()
        self.assertEquals(1, error)
        self.assertEquals("", stdout)
        # valgrind output
        assert "== ERROR SUMMARY" in stderr


if __name__ == "__main__":
    unittest.main()
