#!/usr/bin/env python

# Copyright (C) 2008-2015 VoltDB Inc.
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
#
# This module supports process daemonization for Unix-like systems. This is a
# fairly heavily modified version of the following project.
# It is provided and licensed as follows:
#
#   Source: https://github.com/martinrusev/python-daemon
#   Author: Martin Rusev
#   License: BSD
#
# Copyright (c) 2011-2013, Martin Rusev
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import sys
import os
import time
import atexit
import signal

class Daemon(object):
    """
        A generic daemon class.

        Usage: subclass the Daemon class and override the on_started() method
    """

    ### Exceptions

    class ExceptionBase(Exception):
        def __init__(self, daemon, cause=None):
            self.pid = daemon.pid
            self.pidfile = daemon.pidfile
            self.cause = cause

    class AlreadyRunningException(ExceptionBase):
        def __init__(self, daemon):
            Daemon.ExceptionBase.__init__(self, daemon)

    class NotRunningException(ExceptionBase):
        def __init__(self, daemon):
            Daemon.ExceptionBase.__init__(self, daemon)

    class DeletePIDFileException(ExceptionBase):
        def __init__(self, daemon, cause):
            Daemon.ExceptionBase.__init__(self, daemon, cause=cause)

    class KillException(ExceptionBase):
        def __init__(self, daemon, cause):
            Daemon.ExceptionBase.__init__(self, daemon, cause=cause)

    class BadSubclassException(ExceptionBase):
        def __init__(self, daemon):
            Daemon.ExceptionBase.__init__(self, daemon)

    ### Methods

    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        """
        Constructor.
        Requires a PID file path and supports optional i/o stream overrides.
        """
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
        self.pid = None

    def daemonize(self):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try:
            self.pid = os.fork()
            if self.pid > 0:
                # exit first parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # decouple from parent environment
        os.chdir(".")
        os.setsid()
        os.umask(0)

        # do second fork
        try:
            self.pid = os.fork()
            if self.pid > 0:
                # exit from second parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # redirect standard file descriptors
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)

        self.pid = os.getpid()
        sys.stdout.write('Background process started with process ID %d.\n' % self.pid)
        sys.stdout.flush()

        if self.pidfile:
            file(self.pidfile,'w+').write("%d\n" % self.pid)

        atexit.register(self.delete_pid_file)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

    def delete_pid_file(self):
        """
        Delete the PID file.
        Return True if the file exists and it was deleted.
        """
        if os.path.exists(self.pidfile):
            try:
                os.remove(self.pidfile)
            except (IOError, OSError), e:
                raise Daemon.DeletePIDFileException(self, e)
            return True
        return False

    def start(self, *args):
        """
        Start the daemon
        """
        # Check for a pidfile to see if the daemon already runs
        self.pid = read_pid_file(self.pidfile)

        if self.pid != -1:
            raise Daemon.AlreadyRunningException(self)

        # Start the daemon
        self.daemonize()
        self.on_started(*args)

    def stop(self, kill_signal=signal.SIGTERM, expect_running=True):
        """
        Stop the daemon
        """
        self.pid, alive = get_status(self.pidfile)
        if alive:
            try:
                while 1:
                    os.kill(self.pid, kill_signal)
                    time.sleep(1.0)
            except OSError, err:
                if str(err).find("No such process") > 0:
                    if self.delete_pid_file():
                        return
                raise Daemon.KillException(self, err)
        else:
            if expect_running:
                raise Daemon.NotRunningException(self)

    def restart(self, kill_signal=signal.SIGTERM):
        """
        Restart the daemon
        """
        self.stop(kill_signal=kill_signal, expect_running=False)
        self.start()

    def on_started(self, *args):
        """
        You should override this method when you subclass Daemon. It will be called after the process has been
        daemonized by start() or restart().
        """
        raise Daemon.BadSubclassException(self)

def read_pid_file(pidfile):
    """
    Read the PID file and return the PID or -1.
    """
    pid = -1
    if os.path.exists(pidfile):
        try:
            pf = file(pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pass
    return pid

def get_status(pidfile):
    """
    Get the running PID and a boolean True if alive.
    """
    pid = read_pid_file(pidfile)
    alive = False
    if pid != -1:
        try:
            os.kill(pid, 0)
            alive = True
        except (IOError, OSError):
            pass
    return pid, alive
