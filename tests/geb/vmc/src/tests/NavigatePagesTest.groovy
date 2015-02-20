/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import org.junit.Rule
import org.junit.rules.TestName
import vmcTest.pages.*

/**
 * This class prints a message every time we switch from one page (tab) to
 * another.
 */
class EchoingPageChangeListener implements PageChangeListener {
    void pageWillChange(Browser browser, Page oldPage, Page newPage) {
        if (NavigatePagesTest.getBooleanSystemProperty("debugPrint", NavigatePagesTest.DEFAULT_DEBUG_PRINT)) {
            println "Browser ($browser) changing page from '$oldPage' to '$newPage'"
        }
    }
}

/**
 * This class tests navigation between pages (or tabs), of the the VoltDB
 * Management Center (VMC), which is the VoltDB (new) web UI.
 */
class NavigatePagesTest extends TestBase {

    def setup() { // called before each test
        // TestBase.setup gets called first (automatically)
        def listener = new EchoingPageChangeListener()
        browser.registerPageChangeListener(listener)
    }

    def 'confirm DB Monitor page open initially'() {
        expect: 'DB Monitor page open initially'
        page.isDbMonitorPageOpen()
    }

    def navigatePages() {
        when: 'click the Schema link (from DB Monitor page)'
        page.openSchemaPage()
        then: 'should be on Schema page'
        at SchemaPage

        when: 'click the SQL Query link (from Schema page)'
        page.openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage

        when: 'click the DB Monitor link (from SQL Query page)'
        page.openDbMonitorPage()
        then: 'should be on DB Monitor page'
        at DbMonitorPage

        when: 'click the SQL Query link (from DB Monitor page)'
        page.openSqlQueryPage()
        then: 'should be on SQL Query page (again)'
        at SqlQueryPage

        when: 'click the Schema link (from SQL Query page)'
        page.openSchemaPage()
        then: 'should be on Schema page (again)'
        at SchemaPage

        when: 'click the DB Monitor link (from Schema page)'
        page.openDbMonitorPage()
        then: 'should be on DB Monitor page (again)'
        at DbMonitorPage
    }
}
