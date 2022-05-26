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

package vmcTest.tests

import java.text.SimpleDateFormat
import java.util.Date
import java.util.List
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.FutureTask
import java.util.concurrent.TimeUnit

import geb.Page
import geb.spock.GebReportingSpec

import org.junit.Rule
import org.junit.rules.TestName
import org.openqa.selenium.Dimension

import spock.lang.Shared
import vmcTest.pages.*

/**
 * This class is the base class for all of the test classes; it provides
 * initialization and convenience methods.
 */
class TestBase extends GebReportingSpec {
    @Rule public TestName tName = new TestName()

    // Set this to true, if you want to see debug print
    static final boolean DEFAULT_DEBUG_PRINT = false
    static final int DEFAULT_WINDOW_WIDTH  = 1500
    static final int DEFAULT_WINDOW_HEIGHT = 1000
    static final int MAX_SECS_WAIT_FOR_PAGE = 60
    static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    static int numberOfTrials   = 20
    static int waitTime         = 30
    boolean testStatus          = false

    static Boolean doesExpectedPageOpenFirst = null
    @Shared boolean firstDebugMessage = true

    private static final ExecutorService executor = Executors.newFixedThreadPool(5);

    def setupSpec() { // called once (per test class), before any tests
        // If the window is not the right size, resize it
        def winSize = driver.manage().window().size
        int desiredWidth  = getIntSystemProperty("windowWidth", DEFAULT_WINDOW_WIDTH)
        int desiredHeight = getIntSystemProperty("windowHeight", DEFAULT_WINDOW_HEIGHT)
        if (winSize.width != desiredWidth || winSize.height != desiredHeight) {
            driver.manage().window().setSize(new Dimension(desiredWidth, desiredHeight))
            debugPrint 'Window resized, from (' + winSize.width + ', ' + winSize.height +
                       ') to (' + desiredWidth + ', ' + desiredHeight + ') [in TestBase.setupSpec()]'
        }
    }

    def setup() { // called before each test
        debugPrint "\nTest: " + tName.getMethodName()

        if (!(page instanceof VoltDBManagementCenterPage)) {
            when: 'Open VMC page'
            ensureOnVoltDBManagementCenterPage()
            then: 'should be on VMC page'
            at VoltDBManagementCenterPage
        }

        // Confirm that one of the VMC pages opens initially, the first time:
        // it used to always be the 'DB Monitor' page, but we no longer care
        // which one, since it remembers via a cookie
        // (this is used by NavigatePagesBasicTest)
        if (doesExpectedPageOpenFirst == null) {
            boolean dbMonitorPage = page.isDbMonitorPageOpen()
            boolean adminPage     = page.isAdminPageOpen()
            boolean schemaPage    = page.isSchemaPageOpen()
            boolean sqlQueryPage  = page.isSqlQueryPageOpen()
            doesExpectedPageOpenFirst = dbMonitorPage || adminPage || schemaPage || sqlQueryPage
            debugPrint 'Initially open Page [in TestBase.setup()]: ' + page.toString()
            debugPrint '  isDbMonitorPage: ' + dbMonitorPage
            debugPrint '  isAdminPage    : ' + adminPage
            debugPrint '  isSchemaPage   : ' + schemaPage
            debugPrint '  isSqlQueryPage : ' + sqlQueryPage
        }
    }

    /**
     * If already on the VoltDBManagementCenterPage, do nothing; otherwise,
     * point the browser to the VoltDBManagementCenterPage's URL; if it does
     * not work the first time, allow a second attempt.
     */
    def ensureOnVoltDBManagementCenterPage() {
        if (!(page instanceof VoltDBManagementCenterPage)) {
            // Attempt to catch any problems here, including a failure due
            // to the VoltDBManagementCenterPage not being shown, or a hang
            // (waiting for the browser/VMC to come up, I suspect)
            try {
                //debugPrint 'Attempting to reach VoltDBManagementCenterPage, at: ' + sdf.format(new Date())
                waitForVoltDBManagementCenterPage()
                //debugPrint 'Succeeded:  reached VoltDBManagementCenterPage, at: ' + sdf.format(new Date())
            } catch (Throwable e) {
                // If an exception is encountered, make a second attempt
                String message = '\nCaught an exception attempting to reach VoltDBManagementCenterPage ' +
                                 '[in TestBase.ensureOnVoltDBManagementCenterPage()]'
                System.err.println message + ':'
                e.printStackTrace()
                println message + '; see Standard error for details.'
                println 'Will refresh page and try again... (' + sdf.format(new Date()) + ')'
                driver.navigate().refresh()
                waitForVoltDBManagementCenterPage()
                println '... second attempt succeeded, at :  ' + sdf.format(new Date())
            }
        }
        page.loginIfNeeded()
    }

    /**
     * Point the browser to the VoltDBManagementCenterPage's URL, using a
     * FutureTask with a time limit, so that this will not hang.
     */
    def waitForVoltDBManagementCenterPage() {
        Future<VoltDBManagementCenterPage> toVMCPage = new FutureTask(new Callable() {
            @Override
            public VoltDBManagementCenterPage call() {
                return toVoltDBManagementCenterPage();
            }})
        executor.execute(toVMCPage)
        toVMCPage.get(MAX_SECS_WAIT_FOR_PAGE, TimeUnit.SECONDS)
    }

    /**
     * Point the browser to the VoltDBManagementCenterPage; this is a separate
     * method only so that it can be called via a FutureTask, and therefore
     * killed if it exceeds the maximum time, since otherwise it sometimes
     * hangs (when run on Jenkins).
     */
    private VoltDBManagementCenterPage toVoltDBManagementCenterPage() {
        to VoltDBManagementCenterPage
    }

    /**
     * Returns the specified System Property as an int value; or the default
     * value, if the System Property is not set, or cannot be parsed as an int.
     * @param propertyName - name of the System Property whose value you want.
     * @param defaultValue - a default value to be used, if the System Property
     * cannot be retrieved.
     * @return the specified System Property (as int); or the default value.
     */
    static int getIntSystemProperty(String propertyName, int defaultValue) {
        String sysPropValue = System.getProperty(propertyName, "_NOT_SET_")
        if (sysPropValue == "_NOT_SET_") {
            return defaultValue
        } else {
            try {
                return Integer.parseInt(sysPropValue)
            } catch (Throwable e) {
                println "Property '" + propertyName + "' should be int, not '" + sysPropValue + "'."
                return defaultValue
            }
        }
    }

    /**
     * Returns the specified System Property as a boolean value; or the default
     * value, if the System Property is not set, or cannot be parsed as a boolean.
     * @param propertyName - name of the System Property whose value you want.
     * @param defaultValue - a default value to be used, if the System Property
     * cannot be retrieved.
     * @return the specified System Property (as boolean); or the default value.
     */
    static boolean getBooleanSystemProperty(String propertyName, boolean defaultValue) {
        String sysPropValue = System.getProperty(propertyName, "_NOT_SET_")
        if (sysPropValue == "_NOT_SET_") {
            return defaultValue
        } else {
            try {
                return Boolean.parseBoolean(sysPropValue)
            } catch (Throwable e) {
                println "Property '" + propertyName + "' should be boolean, not '" + sysPropValue + "'."
                return defaultValue
            }
        }
    }

    /**
     * Returns a list of lines from the specified file.
     * @param file - the file whose lines are to be returned.
     * @param commentChars - the character(s) that indicate the beginning of a
     * comment, which should be ignored (default is '#').
     * @param includeBlankLines - whether or not to include blank (or
     * white-space only) lines in the output (default is true).
     * @param finalChars - the character(s) that, if specified, indicate the
     * end of a 'line', or command; consecutive lines will be concatenated
     * until this is found at the end of a line, and then returned as a single
     * line; default is '', an empty string, meaning each line will be returned
     * separately, without concatenation.
     * @return a list of lines from the specified file.
     */
    def List<String> getFileLines(File file, String commentChars='#',
                                  boolean includeBlankLines=true,
                                  String finalChars='') {
        def lines = []
        if (file.size() > 0) {
            String tmp = ''
            file.eachLine { line ->
                def trimmedLine = line.trim()
                if ((includeBlankLines || !trimmedLine.isEmpty()) &&
                        !trimmedLine.startsWith(commentChars)) {
                    if (line.endsWith(finalChars)) {
                        lines.add(tmp + line)
                        tmp = ''
                    } else {
                        tmp += line
                    }
                }
            }
        }
        return lines
    }

    /**
     * Optionally (if DEBUG is true), prints a list of items (found somewhere
     * in the UI); and, also optionally (depending on the <i>compare</i>
     * argument), compares that list to a list of expected items.
     *
     * @param typesToCompare - the type of items being compared (e.g. 'Tables'
     * or 'System Stored Procedures'), for print output purposes.
     * @param fileName - the name (perhaps including the path) of the file
     * containing the list of expected items, for an error message, if needed.
     * @param expected - the list of values expected to be found.
     * @param actual - the list of actual values found (in the UI).
     * @param compare - whether or not you want to do the comparison part of
     * the test (if false, the comparison is skipped).
     * @return true if the test completed successfully; otherwise, throws an
     * AssertionError.
     */
    def <T> boolean printAndCompare(String typesToCompare, String fileName,
                                    boolean compare, List<T> expected, List<T> actual) {
        // Print out the list of (actual) items - if DEBUG is true
        debugPrint '\n# ' + typesToCompare + ': (compare with ' + fileName + ')'
        actual.each { debugPrint it }

        // Check that the expected and actual stored procedures match
        if (expected == null || expected.isEmpty()) {
            assert false, 'ERROR: No expected ' + typesToCompare + ' found! Need to specify some in ' + fileName
        } else if (compare) {
            assert expected == actual
        }
        return true
    }

    /**
     * Optionally (if the <i>debug</i> argument is true), prints the specified
     * text message (to stdout).
     * @param text - the text to be printed.
     * @param debug - whether or not to print the text.
     */
    void debugPrint(Object text, boolean debug) {
        if (debug) {
            if (firstDebugMessage) {
                firstDebugMessage = false
                println sdf.format(new Date())
            }
            println text.toString()
        }
    }

    /**
     * Optionally (if the <i>DEBUG</i> constant is true), prints the specified
     * text message (to stdout).
     * @param text - the text to be printed.
     */
    void debugPrint(Object text) {
        debugPrint(text, getBooleanSystemProperty("debugPrint", DEFAULT_DEBUG_PRINT))
    }

    def cleanupSpec() {

    }
}
