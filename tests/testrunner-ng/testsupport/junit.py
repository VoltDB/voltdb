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

from runner import Runner
from datetime import datetime, timedelta
import os
from subprocess import Popen, PIPE
import time

class JUnit(Runner):
    def run(self):
        print "Running JUnit test: %s" % (self.name)

        command =  ["java"]
        command += ["-ea"]
        command += ["-Xmx1024m"]
        command += ["-Djava.library.path=nativelibs"]
        command += ["org.voltdb.VoltTestRunner"]
        command += [self.task]
        command += [self.timestamp]

        classpath =  "../../third_party/java/jars/junit-4.8.2.jar"
        classpath += ":../../third_party"
        classpath += ":prod"
        classpath += ":test"
        env = { 'CLASSPATH' : classpath }

        pipe = None

        try:
            start = datetime.now()

            pipe = Popen(args=command, env=env, stdout=PIPE)

            self.duration = (datetime.now() - start).seconds
            while self.duration < self.timeout:
                pipe.poll()
                if pipe.returncode != None:
                    self.processResults(pipe.stdout)
                    return
                else:
                    time.sleep(1)
                self.duration = (datetime.now() - start).seconds
            self.didtimeout = True
            self.failures = 1
            self.testsrun = 1
        finally:
            if (pipe != None):
                pipe.poll()
                if pipe.returncode == None:
                    os.system("kill -9 %d" % pipe.pid)

    def processResults(self, stdout):
        lines = stdout.readlines()
        for line in lines:
            if line.startswith("RESULTS:"):
                parts = line.split(" ")
                parts = parts[1].strip().split("/")
                passed = int(parts[0])
                run = int(parts[1])
                self.testsrun = run
                self.failures = run - passed
                return