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
import org.junit.Rule
import org.junit.rules.TestName
// TODO: might want to switch to using @Requires, here and below,
// once we update to a version of Spock that supports it
import spock.lang.IgnoreIf
import vmcTest.pages.*

/**
 * This class prints a message every time we switch from one page (tab) to
 * another.
 */
class EchoingPageChangeListener implements PageChangeListener {
    void pageWillChange(Browser browser, Page oldPage, Page newPage) {
        if (NavigatePagesBasicTest.getBooleanSystemProperty("debugPrint", NavigatePagesBasicTest.DEFAULT_DEBUG_PRINT)) {
            println "Browser ($browser) changing page from '$oldPage' to '$newPage'"
        }
    }
}

/**
 * This class tests navigation between pages (or tabs), of the the VoltDB
 * Management Center (VMC), which is the VoltDB web UI.
 */
class NavigatePagesBasicTest extends TestBase {

    def setup() { // called before each test
        // TestBase.setup gets called first (automatically)
        def listener = new EchoingPageChangeListener()
        browser.registerPageChangeListener(listener)
    }

    // Test that one of the VMC pages opens initially, the first time: it used
    // to always be the 'DB Monitor' page, but we no longer care which one,
    // since it remembers via a cookie (this value is set in TestBase)
    def 'confirm a VMC page opens initially'() {
        expect: 'VMC page was open initially'
        doesExpectedPageOpenFirst
    }

    // Test that the usual, always visible page (tab) links are visible; and
    // that the DR and Importer page (tab) links are visible if and only if
    // they are supposed to be, based on System properties
    def checkVisibilityOfTabLinks() {
        expect: 'DB Monitor link (to DB Monitor tab) visible'
        page.isDbMonitorLinkVisible()

        and: 'Analysis link (to Analysis tab) visible'
        page.isAnalysisLinkVisible()

        and: 'Admin link (to Admin tab) visible'
        page.isAdminLinkVisible()

        and: 'Schema link (to Schema tab) visible'
        page.isSchemaLinkVisible()

        and: 'SQL Query link (to SQL Query tab) visible'
        page.isSqlQueryLinkVisible()

        and: 'DR link (to DR tab), visible only if "dr" System property set, \
              which it should be only when DR is enabled, i.e., <dr> tag is \
              present in the deployment file (and running VoltDB pro)'
        page.isDrLinkVisible() == getBooleanSystemProperty("dr", false)

        and: 'Importer link (to Importer tab), visible only if "importer" System \
              property set, which it should be only when import connector(s) \
              enabled, i.e., <importer> tag is present in the deployment file \
              (and running VoltDB pro)'
        page.isImporterLinkVisible() == getBooleanSystemProperty("importer", false)
    }

    // Test navigating from one page (tab) to another: just the pages that are
    // always visible (or should be)
    def navigateUsualPages() {

        // Visit each page (tab), moving from left to right
        when: 'click the DB Monitor link (if not already on DB Monitor page)'
        page.openDbMonitorPage()
        then: 'should be on DB Monitor page'
        at DbMonitorPage

        when: 'click the Analysis link (from DB Monitor page)'
        page.openAnalysisPage()
        then: 'should be on Analysis page'
        at AnalysisPage

        when: 'click the Admin link (from Analysis page)'
        page.openAdminPage()
        then: 'should be on Admin page'
        at AdminPage

        when: 'click the Schema link (from Admin page)'
        page.openSchemaPage()
        then: 'should be on Schema page'
        at SchemaPage

        when: 'click the SQL Query link (from Schema page)'
        page.openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage

        when: 'click the DB Monitor link (from SQL Query page)'
        page.openDbMonitorPage()
        then: 'should be on DB Monitor page (again)'
        at DbMonitorPage

        // Visit each page (tab), moving from right to left
        when: 'click the SQL Query link (from DB Monitor page)'
        page.openSqlQueryPage()
        then: 'should be on SQL Query page (again)'
        at SqlQueryPage

        when: 'click the Schema link (from SQL Query page)'
        page.openSchemaPage()
        then: 'should be on Schema page (again)'
        at SchemaPage

        when: 'click the Admin link (from Schema page)'
        page.openAdminPage()
        then: 'should be on Admin page (again)'
        at AdminPage

        when: 'click the Analysis link (from Admin page)'
        page.openAnalysisPage()
        then: 'should be on Analysis page (again)'
        at AnalysisPage

        when: 'click the DB Monitor link (from Analysis page)'
        page.openDbMonitorPage()
        then: 'should be on DB Monitor page (yet again)'
        at DbMonitorPage

        // Visit each page (tab), coming from the pages not covered above,
        // in a "star" pattern
        when: 'click the Admin link (from DB Monitor page)'
        page.openAdminPage()
        then: 'should be on Admin page (yet again)'
        at AdminPage

        when: 'click the SQL Query link (from Admin page)'
        page.openSqlQueryPage()
        then: 'should be on SQL Query page (yet again)'
        at SqlQueryPage

        when: 'click the Analysis link (from SQL Query page)'
        page.openAnalysisPage()
        then: 'should be on Analysis page (yet again)'
        at AnalysisPage

        when: 'click the Schema link (from Analysis page)'
        page.openSchemaPage()
        then: 'should be on Schema page (yet again)'
        at SchemaPage

        when: 'click the DB Monitor link (from Schema page)'
        page.openDbMonitorPage()
        then: 'should be on DB Monitor page (still yet again)'
        at DbMonitorPage

        // Visit each page (tab), in the reverse of the "star" pattern above
        when: 'click the Schema link (from DB Monitor page)'
        page.openSchemaPage()
        then: 'should be on Schema page (one last time)'
        at SchemaPage

        when: 'click the Analysis link (from Schema page)'
        page.openAnalysisPage()
        then: 'should be on Analysis page (one last time)'
        at AnalysisPage

        when: 'click the SQL Query link (from Analysis page)'
        page.openSqlQueryPage()
        then: 'should be on SQL Query page (one last time)'
        at SqlQueryPage

        when: 'click the Admin link (from SQL Query page)'
        page.openAdminPage()
        then: 'should be on Admin page (one last time)'
        at AdminPage

        when: 'click the DB Monitor link (from Admin page)'
        page.openDbMonitorPage()
        then: 'should be on DB Monitor page (one final time)'
        at DbMonitorPage
    }

    // Test navigating from one page (tab) to another: the DR page (tab),
    // which is sometimes visible, and sometimes not
    @IgnoreIf({ !TestBase.getBooleanSystemProperty('dr', false) })
    def navigateDrPage() {

        // Visit each page (tab), from (and to) the DR page (tab)
        when: 'click the DB Monitor link (if not already on DB Monitor page)'
        page.openDbMonitorPage()
        then: 'should be on DB Monitor page'
        at DbMonitorPage

        when: 'click the DR link (from DB Monitor page)'
        page.openDrPage()
        then: 'should be on DR page'
        at DrPage

        when: 'click the Analysis link (from DR page)'
        page.openAnalysisPage()
        then: 'should be on Analysis page'
        at AnalysisPage

        when: 'click the DR link (from Analysis page)'
        page.openDrPage()
        then: 'should be on DR page (yet again)'
        at DrPage

        when: 'click the Admin link (from DR page)'
        page.openAdminPage()
        then: 'should be on Admin page'
        at AdminPage

        when: 'click the DR link (from Admin page)'
        page.openDrPage()
        then: 'should be on DR page (still yet again)'
        at DrPage

        when: 'click the Schema link (from DR page)'
        page.openSchemaPage()
        then: 'should be on Schema page'
        at SchemaPage

        when: 'click the DR link (from Schema page)'
        page.openDrPage()
        then: 'should be on DR page (and again)'
        at DrPage

        when: 'click the SQL Query link (from DR page)'
        page.openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage

        when: 'click the DR link (from Schema page)'
        page.openDrPage()
        then: 'should be on DR page (yet again)'
        at DrPage

        when: 'click the DB Monitor link (from DR page)'
        page.openDbMonitorPage()
        then: 'should be on DB Monitor page (one final time)'
        at DbMonitorPage
    }

    // Test navigating from one page (tab) to another: the Importer page (tab),
    // which is sometimes visible, and sometimes not
    @IgnoreIf({ !TestBase.getBooleanSystemProperty('importer', false) })
    def navigateImporterPage() {

        // Visit each page (tab), from (and to) the Importer page (tab)
        when: 'click the DB Monitor link (if not already on DB Monitor page)'
        page.openDbMonitorPage()
        then: 'should be on DB Monitor page'
        at DbMonitorPage

        when: 'click the Importer link (from DB Monitor page)'
        page.openImporterPage()
        then: 'should be on Importer page'
        at ImporterPage

        when: 'click the Analysis link (from Importer page)'
        page.openAnalysisPage()
        then: 'should be on Analysis page'
        at AnalysisPage

        when: 'click the Importer link (from Analysis page)'
        page.openImporterPage()
        then: 'should be on Importer page (yet again)'
        at ImporterPage

        when: 'click the Admin link (from Importer page)'
        page.openAdminPage()
        then: 'should be on Admin page'
        at AdminPage

        when: 'click the Importer link (from Admin page)'
        page.openImporterPage()
        then: 'should be on Importer page (still yet again)'
        at ImporterPage

        when: 'click the Schema link (from Importer page)'
        page.openSchemaPage()
        then: 'should be on Schema page'
        at SchemaPage

        when: 'click the Importer link (from Schema page)'
        page.openImporterPage()
        then: 'should be on Importer page (and again)'
        at ImporterPage

        when: 'click the SQL Query link (from Importer page)'
        page.openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage

        when: 'click the Importer link (from Schema page)'
        page.openImporterPage()
        then: 'should be on Importer page (one last time)'
        at ImporterPage

        when: 'click the DB Monitor link (from Importer page)'
        page.openDbMonitorPage()
        then: 'should be on DB Monitor page (one final time)'
        at DbMonitorPage
    }

    // Test navigating from one page (tab) to another: the Importer page (tab),
    // which is sometimes visible, and sometimes not
    @IgnoreIf({ !TestBase.getBooleanSystemProperty('dr', false) ||
                !TestBase.getBooleanSystemProperty('importer', false) })
    def navigateDrAndImporterPages() {

        // Visit the DR page (tab) and the Importer page (tab), from (and to) each other
        when: 'click the DR link (if not already on it)'
        page.openDrPage()
        then: 'should be on DR page'
        at DrPage

        when: 'click the Importer link (from DR page)'
        page.openImporterPage()
        then: 'should be on Importer page'
        at ImporterPage

        when: 'click the DR link (from Importer page)'
        page.openDrPage()
        then: 'should be on DR page (once final time)'
        at DrPage
    }
}
