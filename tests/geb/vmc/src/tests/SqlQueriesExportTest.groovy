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

import geb.*
import groovy.json.*
import java.util.List;
import java.util.Map;
import spock.lang.*
import vmcTest.pages.*

/**
 * This class contains tests of the 'SQL Query' tab of the VoltDB Management
 * Center (VMC) page, which is the VoltDB (new) web UI.
 */
class SqlQueriesExportTest extends TestBase {
    def setup() {
        when: 'click the SQL Query link (if needed)'
        openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage
    }

    def checkHtmlExport() {
        String createQuery = page.getQueryToCreateTable()
        String deleteQuery = page.getQueryToDeleteTable()
        String tablename = page.getTablename()

        when: 'set create query in the box'
        page.setQueryText(createQuery)
        then: 'run the query'
        page.runQuery()

        when: 'set select query in the box'
        page.setQueryText("SELECT * FROM " + tablename)
        then: 'run the query'
        page.runQuery()

        page.qrFormatDropDown.value("HTML")
        waitFor(30) { resultHtml.isDisplayed() }

        when: 'set delete query in the box'
        page.setQueryText(deleteQuery)
        then: 'run the query'
        page.runQuery()
    }

     def checkCsvExport() {
        String createQuery = page.getQueryToCreateTable()
        String deleteQuery = page.getQueryToDeleteTable()
        String tablename = page.getTablename()

        when: 'set create query in the box'
        page.setQueryText(createQuery)
        then: 'run the query'
        page.runQuery()

        when: 'set select query in the box'
        page.setQueryText("SELECT * FROM " + tablename)
        then: 'run the query'
        page.runQuery()

        page.qrFormatDropDown.value("CSV")
        waitFor(30) { resultCsv.isDisplayed() }

        when: 'set delete query in the box'
        page.setQueryText(deleteQuery)
        then: 'run the query'
        page.runQuery()
    }

     def checkMonospaceExport() {
        String createQuery = page.getQueryToCreateTable()
        String deleteQuery = page.getQueryToDeleteTable()
        String tablename = page.getTablename()

        when: 'set create query in the box'
        page.setQueryText(createQuery)
        then: 'run the query'
        page.runQuery()

        when: 'set select query in the box'
        page.setQueryText("SELECT * FROM " + tablename)
        then: 'run the query'
        page.runQuery()

        page.qrFormatDropDown.value("Monospace")
        waitFor(30) { resultMonospace.isDisplayed() }

        when: 'set delete query in the box'
        page.setQueryText(deleteQuery)
        then: 'run the query'
        page.runQuery()
    }

    def cleanupSpec() {
        if (!(page instanceof VoltDBManagementCenterPage)) {
            when: 'Open VMC page'
            ensureOnVoltDBManagementCenterPage()
            then: 'to be on VMC page'
            at VoltDBManagementCenterPage
        }

        page.loginIfNeeded()

        when: 'click the Schema link (if needed)'
        page.openSqlQueryPage()
        then: 'should be on DB Monitor page'
        at SqlQueryPage
        String deleteQuery = page.getQueryToDeleteTable()
        page.setQueryText(deleteQuery)

        page.runQuery()
    }
}
