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

import geb.waiting.WaitTimeoutException

import vmcTest.pages.SqlQueryPage
import vmcTest.pages.VoltDBManagementCenterPage.ColumnHeaderCase

/**
 * This class is the base class for test classes that test the 'SQL Query' tab
 * of the VoltDB Management Center (VMC) page; it provides initialization and
 * convenience methods.
 */
class SqlQueriesTestBase extends TestBase {

    def setup() { // called before each test
        // TestBase.setup gets called first (automatically)
        when: 'click the SQL Query link (if needed)'
        ensureOnSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage
    }

    def ensureOnSqlQueryPage() {
        ensureOnVoltDBManagementCenterPage()
        try {
            //debugPrint 'Attempting to open SqlQueryPage, at: ' + sdf.format(new Date())
            page.openSqlQueryPage()
            //debugPrint 'Succeeded:  opened SqlQueryPage, at: ' + sdf.format(new Date())
        } catch (WaitTimeoutException e) {
            // If a WaitTimeoutException is encountered, make a second attempt
            String message = '\nCaught a WaitTimeoutException attempting to open SqlQueryPage ' +
                             '[in SqlQueriesTestBase.ensureOnSqlQueryPage()]'
            System.err.println message + ':'
            e.printStackTrace()
            println message + '; see Standard error for details.'
            println 'Will refresh page and try again...    (' + sdf.format(new Date()) + ')'
            driver.navigate().refresh()
            ensureOnVoltDBManagementCenterPage()
            page.openSqlQueryPage()
            println '... second open attempt succeeded, at: ' + sdf.format(new Date())
        }
    }

    /**
     * Runs, on the specified SqlQueryPage, the specified query, and returns
     * the result. (Also, if DEBUG is true, prints: the query, the result, an
     * error message, if any, and the query duration.)
     * @param sqp - the SqlQueryPage on which to run the query.
     * @param query - the query to be run.
     * @param colHeaderFormat - the case in which you want the table's column
     * headers returned: converted to lower case, to upper case, or as-is.
     * @return the query result (as a Map of Lists of Strings).
     */
    def Map<String,List<String>> runQuery(SqlQueryPage sqp, String query,
            ColumnHeaderCase colHeaderFormat=ColumnHeaderCase.TO_LOWER_CASE) {
        sqp.runQuery(query)
        def qResult  = sqp.getQueryResult(colHeaderFormat)
        def error    = sqp.getQueryError()
        def duration = sqp.getQueryDuration()
        def qResText = sqp.getQueryResultText()

        // If no connection to database, give it a second chance
        if (qResText.contains("Connect to a datasource first") || qResText.contains("No connections")) {
            debugPrint "\nQuery : " + query
            debugPrint "All result text:\n" + qResText
            debugPrint "Result: " + qResult
            debugPrint "Duration: " + duration
            debugPrint "Error : " + error
            debugPrint "Reloading and trying again..."
            driver.navigate().refresh()
            if (!sqp.verifyAtSafely()) {
                debugPrint "  Moving to VoltDBManagementCenterPage"
                to VoltDBManagementCenterPage
                page.loginIfNeeded()
                debugPrint "  Opening SqlQueryPage"
                page.openSqlQueryPage()
            }
            sqp.verifyAt()
            sqp.runQuery(query)
            qResult  = sqp.getQueryResult()
            error    = sqp.getQueryError()
            duration = sqp.getQueryDuration()
            qResText = sqp.getQueryResultText()
        }

        // If 'sleepSeconds' property is set and greater than zero, sleep
        int sleepSeconds = getIntSystemProperty("sleepSeconds", 0)
        if (sleepSeconds > 0) {
            try {
                Thread.sleep(1000 * sleepSeconds)
            } catch (InterruptedException e) {
                String message = "\nIn SqlQueriesTestBase.runQuery, caught:\n  " + e.toString()
                println message + '.'
                println 'See Standard error for stack trace.\n'
                System.err.println message + ':'
                e.printStackTrace()
            }
        }

        debugPrint "\nQuery : " + query
        debugPrint "Result: " + qResult
        debugPrint "Duration: " + duration
        if (error != null) {
            debugPrint "Error : " + error
        } else if (duration != null && duration.contains('error')) {
            debugPrint "All result text:\n" + qResText
        }

        return qResult
    }
}
