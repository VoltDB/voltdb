/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.taskdefs.optional.junit.JUnitResultFormatter;
import org.apache.tools.ant.taskdefs.optional.junit.JUnitTest;

import junit.framework.AssertionFailedError;
import junit.framework.Test;

public class VoltJUnitFormatter implements JUnitResultFormatter {

    static final int DURATION_UPDATE_PERIOD_MIN = 2;

    PrintStream out = System.out;
    JUnitTest m_currentSuite;
    int m_tests;
    int m_errs;
    int m_failures;
    long m_start;
    Timer m_updateTimer;

    class DurationUpdater extends TimerTask {
        AtomicInteger m_minutes = new AtomicInteger(0);

        @Override
        public void run() {
            out.printf("    %d minutes have passed.\n", m_minutes.addAndGet(DURATION_UPDATE_PERIOD_MIN));
            out.flush();
        }
    }

    @Override
    public void setOutput(OutputStream outputStream) {
        out = new PrintStream(outputStream);
    }

    @Override
    public void setSystemError(String arg0) {
        //out.println("SYSERR: " + arg0);
    }

    @Override
    public void setSystemOutput(String arg0) {
        //out.println("SYSOUT: " + arg0);
    }

    @Override
    public void startTestSuite(JUnitTest suite) throws BuildException {
        m_currentSuite = suite;
        m_tests = m_errs = m_failures = 0;
        m_start = System.currentTimeMillis();

        // this code is 10 lines instead of 1 because it's safely trying to get information
        // from ant about where we are in the set of unit tests with a graceful fallback
        String testIndexStr = System.getProperty("testindex");
        String testCountStr = System.getProperty("testcount");
        try {
            long testIndex = Long.parseLong(testIndexStr);
            long testCount = Long.parseLong(testCountStr);
            out.printf("  Running test %d of %d: %s\n", testIndex, testCount, suite.getName());
        }
        catch (Exception e) {
            out.printf("  Running %s\n", suite.getName());
        }

        // print a message to the console every few minutes so you know
        // roughly how long you've been running a test
        assert(m_updateTimer == null);
        DurationUpdater updateTask = new DurationUpdater();
        m_updateTimer = new Timer("Duration Printer from JUnit Results Formatter");
        int duration = DURATION_UPDATE_PERIOD_MIN * 60 * 1000; // ms
        m_updateTimer.schedule(updateTask, duration, duration);
    }

    @Override
    public void endTestSuite(JUnitTest suite) throws BuildException {
        m_updateTimer.cancel();
        m_updateTimer = null;

        if ((m_failures > 0) || (m_errs > 0)) {
            out.print("(FAIL-JUNIT) ");
        }
        else {
            out.print("             ");
        }
        synchronized (out) {
            out.flush();
            out.printf("Tests run: %3d, Failures: %3d, Errors: %3d, Time elapsed: %.2f sec\n",
                    m_tests, m_failures, m_errs, (System.currentTimeMillis() - m_start) / 1000.0);
            out.flush();
        }
    }

    @Override
    public void startTest(Test arg0) {

    }

    @Override
    public void endTest(Test arg0) {
        out.flush();
        m_tests++;
    }

    @Override
    public void addError(Test arg0, Throwable arg1) {
        String testName = "unknown";
        if (arg0 != null) {
            testName = arg0.toString();
            if (arg0.toString().indexOf('(') != -1)
                testName = testName.substring(0, testName.indexOf('('));
        }

        out.println("    " + testName + " had an error:");
        out.println("    " + arg1.toString());
        if (arg1.getMessage() != null) {
            out.println("    \"" + arg1.getMessage() + "\"");
        }
        StackTraceElement[] st = arg1.getStackTrace();
        int i = 0;
        for (StackTraceElement ste : st) {
            if (ste.getClassName().contains("org.voltdb") == false)
                continue;
            out.printf("        %s(%s:%d)\n", ste.getClassName(), ste.getFileName(), ste.getLineNumber());
            if (++i == 3) break;
        }
        m_errs++;
    }

    @Override
    public void addFailure(Test arg0, AssertionFailedError arg1) {
        String testName = arg0.toString();
        if (testName.indexOf('(') != -1) {
            testName = testName.substring(0, testName.indexOf('('));
        }

        out.println("    " + testName + " failed an assertion.");
        StackTraceElement[] st = arg1.getStackTrace();
        int i = 0;
        for (StackTraceElement ste : st) {
            if (ste.getClassName().contains("org.voltdb") == false)
                continue;
            out.printf("        %s(%s:%d)\n", ste.getClassName(), ste.getFileName(), ste.getLineNumber());
            if (++i == 3) break;
        }
        m_failures++;
    }

}
