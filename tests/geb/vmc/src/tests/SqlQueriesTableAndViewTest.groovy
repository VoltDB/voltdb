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

class SqlQueriesTableAndViewTest extends SqlQueriesTestBase {
    def setup() {
        when: 'click the SQL Query link (if needed)'
        openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage
    }

    def checkCreatedTableByRefreshingInSqlQueryTabAndSchemaTab() {
        when: 'click the SQL Query link (if needed)'
        openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage

        String checkQuery = page.getQueryToCreateTable()

        when: 'set create query in the box'
        page.setQueryText(checkQuery)
        then: 'run the query'
        page.runQuery()

        try {
            waitFor(waitTime) {
                page.cancelpopupquery.isDisplayed()
                page.okpopupquery.click()
            }
        } catch (geb.error.RequiredPageContentNotPresent e) {
            println("pop up won't occurr due to already in running state")
        } catch (geb.waiting.WaitTimeoutException e) {
            println("already in admin port state")
        }

        waitFor(waitTime){page.refreshquery.isDisplayed()}
        page.refreshquery.click()
        println("refresh button clicked and created table shown in SQLQuery tab!!")

        // In Schema Page Schema Tab
        when: 'click the Schema (page) link'
        page.openSchemaPage()
        then: 'should be on Schema page'
        at SchemaPage

        when: 'go to schema tab'
        page.openSchemaPageSchemaTab()
        then: 'at schema tab'
        at SchemaPageSchemaTab
        waitFor(waitTime){page.refreshtableschema.isDisplayed()}
        page.refreshtableschema.click()
        println("refresh button clicked and created table shown in schema page of schema tab")

        // In Schema page DDL Source tab
        when: 'go to DDL source tab'
        page.openSchemaPageDdlSourceTab()
        then: 'at DDL source tab'
        at SchemaPageDdlSourceTab

        waitFor(waitTime){page.refreshddl.isDisplayed()}
        page.refreshddl.click()
        println("refresh button clicked and created table shown in Schema page of DDL source tab")

        // In Size and Worksheet tab
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        waitFor(waitTime){page.refreshtableworksheet.isDisplayed()}
        page.refreshtableworksheet.click()
        println("refresh button clicked and created table shown in schema page of Size and worksheet tab")

        when: 'click the SQL Query link (if needed)'
        openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage

        String createQuery = page.getQueryToCreateTable()
        String deleteQuery = page.getQueryToDeleteTable()
        String tablename =  page.getTablename()

        when: 'set delete query in the box'
        page.setQueryText(deleteQuery)
        then: 'run the query'
        page.runQuery()
        waitFor(waitTime){page.refreshquery.isDisplayed()}
        page.refreshquery.click()
        println("created table deleted!! in SQL Query tab")

        // In Schema Page Schema Tab for checking deleted table
        when: 'click the Schema (page) link'
        page.openSchemaPage()
        then: 'should be on Schema page'
        at SchemaPage

        when: 'go to schema tab'
        page.openSchemaPageSchemaTab()
        then: 'at schema tab'
        at SchemaPageSchemaTab
        waitFor(waitTime){page.refreshtableschema.isDisplayed()}
        page.refreshtableschema.click()
        println("refresh button clicked and created table deleted in schema tab")

        // In Schema page DDL Source tab for checking deleted table
        when: 'go to DDL source tab'
        page.openSchemaPageDdlSourceTab()
        then: 'at DDL source tab'
        at SchemaPageDdlSourceTab

        waitFor(waitTime){page.refreshddl.isDisplayed()}
        page.refreshddl.click()
        println("refresh button clicked and created table deleted in Schema page of DDL source tab")

        // In Size and Worksheet tab for checking deleted table
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        waitFor(waitTime){page.refreshtableworksheet.isDisplayed()}
        page.refreshtableworksheet.click()
        println("refresh button clicked and created table deleted in schema page of Size and worksheet tab")
    }

    def checkCreatedViewsByRefreshingInSqlQueryTabAndSchemaTab() {
        when: 'click the SQL Query link (if needed)'
        openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage

        String checkQuery = page.getQueryToCreateView()

        when: 'set create query in the box'
        page.setQueryText(checkQuery)
        then: 'run the query'
        page.runQuery()

        try {
            waitFor(waitTime) {
                page.cancelpopupquery.isDisplayed()
                page.okpopupquery.click()
            }
        } catch (geb.error.RequiredPageContentNotPresent e) {
            println("pop up won't occurr due to already in running state")
        } catch (geb.waiting.WaitTimeoutException e) {
            println("already in admin port state")
        }

        waitFor(waitTime){page.refreshquery.isDisplayed()}
        page.refreshquery.click()
        try {
            waitFor(waitTime){page.checkview.isDisplayed()}
            page.checkview.click()}
        catch (geb.error.RequiredPageContentNotPresent e) {println("element not found")}
        catch (geb.waiting.WaitTimeoutException e){println("waiting timeout")}
        println("views that is created has been displayed!!")

        // In Schema Page Schema Tab
        when: 'click the Schema (page) link'
        page.openSchemaPage()
        then: 'should be on Schema page'
        at SchemaPage

        when: 'go to schema tab'
        page.openSchemaPageSchemaTab()
        then: 'at schema tab'
        at SchemaPageSchemaTab
        waitFor(waitTime){page.refreshtableschema.isDisplayed()}
        page.refreshtableschema.click()
        println("refresh button clicked and created views shown in schema page of schema tab")

        // In Schema page DDL Source tab
        when: 'go to DDL source tab'
        page.openSchemaPageDdlSourceTab()
        then: 'at DDL source tab'
        at SchemaPageDdlSourceTab

        waitFor(waitTime){page.refreshddl.isDisplayed()}
        page.refreshddl.click()
        println("refresh button clicked and created views shown in Schema page of DDL source tab")

        // In Size and Worksheet tab
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        waitFor(waitTime){page.refreshtableworksheet.isDisplayed()}
        page.refreshtableworksheet.click()
        println("refresh button clicked and created views shown in schema page of Size and worksheet tab")

        when: 'click the SQL Query link (if needed)'
        openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage

        String createQuery = page.getQueryToCreateView()
        String deleteQuery = page.getQueryToDeleteView()
        String tablename =  page.getTablename()

        when: 'set delete query in the box'
        page.setQueryText(deleteQuery)
        then: 'run the query'
        page.runQuery()

        waitFor(waitTime){page.refreshquery.isDisplayed()}
        page.refreshquery.click()
        waitFor(waitTime){page.checkview.isDisplayed()}
        page.checkview.click()
        println("created views has been deleted!! in SQL Query tab")

        // In Schema Page Schema Tab for checking deleted table
        when: 'click the Schema (page) link'
        page.openSchemaPage()
        then: 'should be on Schema page'
        at SchemaPage

        when: 'go to schema tab'
        page.openSchemaPageSchemaTab()
        then: 'at schema tab'
        at SchemaPageSchemaTab
        waitFor(waitTime){page.refreshtableschema.isDisplayed()}
        page.refreshtableschema.click()
        println("refresh button clicked and created views deleted in schema tab")

        // In Schema page DDL Source tab for checking deleted table
        when: 'go to DDL source tab'
        page.openSchemaPageDdlSourceTab()
        then: 'at DDL source tab'
        at SchemaPageDdlSourceTab

        waitFor(waitTime){page.refreshddl.isDisplayed()}
        page.refreshddl.click()
        println("refresh button clicked and created views deleted in Schema page of DDL source tab")

        // In Size and Worksheet tab for checking deleted table
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        waitFor(waitTime){page.refreshtableworksheet.isDisplayed()}
        page.refreshtableworksheet.click()
        println("refresh button clicked and created views deleted in schema page of Size and worksheet tab")
    }
}
