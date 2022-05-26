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
import java.util.regex.Matcher;
import spock.lang.*
import vmcTest.pages.*

/**
 * This class contains tests of the 'SQL Query' tab of the VoltDB Management
 * Center (VMC) page, which is the VoltDB (new) web UI.
 */

class SqlQueriesAdminTest extends SqlQueriesTestBase {

    int count = 0
    int numberOfTrials = 5
    // No setup() needed: SqlQueriesTestBase.setup gets called before each test (automatically)

    // SQL queries test for admin-client port
    def checkSqlqueryClientToAdminPortSwitchingForCancelPopup() {
        when: 'click the SQL Query link (if needed)'
        page.openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage

        String checkQuery = page.getQueryToCreateTable()

        when: 'set create query in the box'
        page.setQueryText(checkQuery)
        then: 'run the query'
        page.runQuery()
        report "runCreateQuery"
        try {
            waitFor(10) {
                page.cancelpopupquery.isDisplayed()
                page.cancelpopupquery.click()
                page.queryDurHtml.isDisplayed()
                println("result shown without popup, hence it is in admin port")
                println("cancel button clicked")
            }
        } catch (geb.error.RequiredPageContentNotPresent e) {
            println("pop up won't occurr due to already in running state")
            println("it is already in admin port")
        } catch (geb.waiting.WaitTimeoutException e) {
            println("already in admin port state")
        }

        when: 'click the Admin link (if needed)'
        page.openAdminPage()
        then: 'should be on Admin page'
        at AdminPage
        report "atAdminPage"
        try {
            waitFor(10) {
                page.networkInterfaces.clusterClientPortValue.isDisplayed()
                cluster.pausebutton.isDisplayed()
            }
            cluster.pausebutton.click()
            waitFor(10) { cluster.pauseok.isDisplayed() }
            cluster.pauseok.click()
            println("Pause button displayed and clicked!!")
        } catch (geb.error.RequiredPageContentNotPresent e) {
            println("Already in pause state!! in admin page.")
        } catch (geb.waiting.WaitTimeoutException e) {
            page.networkInterfaces.clusterClientPortValue.isDisplayed()
            println("rechecking due to geb waiting exception")
        }

        when: 'click the SQL Query link (if needed)'
        page.openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage

        String createQuery = page.getQueryToCreateTable()
        String deleteQuery = page.getQueryToDeleteTableOnly()
        String tablename = page.getTablename()

        when: 'set create query in the box'
        page.setQueryText(createQuery)
        then: 'run the query'
        page.runQuery()
        try {
            waitFor(waitTime) {
                page.cancelpopupquery.isDisplayed()
                page.okpopupquery.isDisplayed()
                page.switchadminport.isDisplayed()
                page.queryexecutionerror.isDisplayed()
                page.queryerrortxt.isDisplayed()
            }
            page.cancelpopupquery.click()
            println("all popup query verified for creating table!!")
        } catch (geb.waiting.WaitTimeoutException e) {
            println("waiting time exceed here")
        }
        report "runcreate"
//        when: 'set select query in the box'
//        page.setQueryText("SELECT * FROM " + tablename)
//        then: 'run the query'
//        page.runQuery()
//        report 'runselect'
//        waitFor(waitTime) {
//            page.cancelpopupquery.isDisplayed()
//            page.okpopupquery.isDisplayed()
//            //page.switchadminport.isDisplayed()
//            //page.queryexecutionerror.isDisplayed()
//            //page.queryerrortxt.isDisplayed()
//        }
//        page.cancelpopupquery.click()
//        println("all popup query verified for selecting data from table!!")
//
//        when: 'set delete query in the box'
//        page.setQueryText(deleteQuery)
//        then: 'run the query'
//        page.runQuery()
//        waitFor(waitTime) {
//            page.cancelpopupquery.isDisplayed()
//            page.okpopupquery.isDisplayed()
//            //page.switchadminport.isDisplayed()
//            //page.queryexecutionerror.isDisplayed()
//            //page.queryerrortxt.isDisplayed()
//        }
//        //for(count=0; count<numberOfTrials; count++) {
//        try {
//            page.cancelpopupquery.click()
//            waitFor(waitTime) { !page.cancelpopupquery.isDisplayed() }
//            println("Cancelled")
//            report "inside"
//            //break
//        } catch (geb.waiting.WaitTimeoutException exception) {
//            println("Waiting for popup to close")
//        }
//        // }
//        println("all popup for query verified for deleting data from table!!")

        report "ended"
    }

    def checkSqlqueryClientToAdminPortSwitchingForOkPoup() {
        when: 'click the Admin link (if needed)'
        page.openAdminPage()
        then: 'should be on Admin page'
        at AdminPage

        try {
            waitFor(10) {
                page.networkInterfaces.clusterClientPortValue.isDisplayed()
                cluster.pausebutton.click()
                cluster.pauseok.click()
                println("Pause button displayed and clicked!!")}
        } catch (geb.error.RequiredPageContentNotPresent e) {
            println("Already in resume state!!")
        } catch (geb.waiting.WaitTimeoutException e) {
            page.networkInterfaces.clusterClientPortValue.isDisplayed()
            println("rechecking due to geb waiting exception")
        }

        when: 'click the SQL Query link (if needed)'
        page.openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage

        String createQuery = page.getQueryToCreateTable()
        String deleteQuery = page.getQueryToDeleteTableOnly()
        String tablename = page.getTablename()

        when: 'set create query in the box'
        page.setQueryText(createQuery)
        then: 'run the query'
        page.runQuery()

        try {
            waitFor(10) {
                page.cancelpopupquery.isDisplayed()
                page.okpopupquery.isDisplayed()
                page.switchadminport.isDisplayed()
                page.queryexecutionerror.isDisplayed()
                page.queryerrortxt.isDisplayed()
            }
            page.okpopupquery.click()
            println("all popup query verified for creating table!!")
        } catch(geb.waiting.WaitTimeoutException e) {println("waiting time exceed")}

        try {
            if(waitFor(5){page.htmlresultallcolumns.isDisplayed()}){
                println("all columns displayed for creating table as: " +page.htmlresultallcolumns.text())}
            if(waitFor(5){page.htmltableresult.isDisplayed()}){
                println("table result shown for creating table HTML format i.e, "+page.htmltableresult.text())
            }
        } catch (geb.waiting.WaitTimeoutException e) {
            println("couldn't check due to server not online error or waiting time error")
        }

        when: 'set select query in the box'
        page.setQueryText("SELECT * FROM " + tablename)
        then: 'run the query'
        page.runQuery()

        try {
            if(waitFor(5){page.htmlresultselect.isDisplayed()}){
                println("all columns displayed for selecting table as: " +page.htmlresultselect.text())}

        } catch (geb.waiting.WaitTimeoutException e) {
            println("couldn't check due to server not online error or waiting time error")
        }

        when: 'set delete query in the box'
        page.setQueryText(deleteQuery)
        then: 'run the query'
        page.runQuery()

        report "ended"
    }

    def cleanup() {
        // Go to admin page and resume
        when: 'click the Admin link (if needed)'
        page.openAdminPage()
        then: 'should be on Admin page'
        at AdminPage

        // Checking if the resume button is displayed
        // If resumebutton gives RequiredPageContentNotPresent error, it means it is already in resumed state
        try {
            waitFor(waitTime) {
                page.networkInterfaces.clusterClientPortValue.isDisplayed()
                cluster.resumebutton.click()
                cluster.resumeok.click()
            }
            println("Resume okay")
        } catch (geb.error.RequiredPageContentNotPresent e) {
            println("Already in resume state!!")
        } catch (geb.waiting.WaitTimeoutException e) {
            println("rechecking due to geb waiting exception")
        }
    }
}
