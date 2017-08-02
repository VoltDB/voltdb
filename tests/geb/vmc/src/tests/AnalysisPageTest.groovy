/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import vmcTest.pages.*
import geb.Page.*

class AnalysisPageTest extends TestBase {

    def setup() { // called before each test
        int count = 0

        while(count<2) {
            count ++
            try {
                setup: 'Open VMC page'
                to VoltDBManagementCenterPage
                expect: 'to be on VMC page'
                at VoltDBManagementCenterPage
                browser.driver.executeScript("localStorage.clear()")
                when: 'click the Analysis link (if needed)'
                page.openAnalysisPage()
                then: 'should be on Importer page'
                at AnalysisPage
                break
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                println("ElementNotVisibleException: Unable to Start the test")
                println("Retrying")
                try {
                    waitFor(waitTime) { 1 == 0 }
                } catch (geb.waiting.WaitTimeoutException exception) {

                }
            }
        }
    }

    def checkAnalysisTabOpened(){
        when:
        waitFor(10){ page.divAnalysis.isDisplayed() }
        then:
        println("Analysis tab is opened")
    }

    def checkIfAnalyzeNowMsgDisplayed(){
        when:
        waitFor(2){ page.analyzeNowContent.isDisplayed() }
        then:
        println("Analyze now message is displayed")
    }

    def check

    def checkAnalyzeBtn(){
        //CLick on procedure tab and verify its content are displayed.
        when:
        waitFor(2){ page.analyzeNowContent.isDisplayed() }
        waitFor(2){ page.tabProcedureBtn.isDisplayed() }
        page.tabProcedureBtn.click();
        then:
        waitFor(2){ page.tabProcedure.isDisplayed() }

        //Click on Analyze Now button and verify if it is working.
        when:
        waitFor(2){ page.btnAnalyzeNow.isDisplayed() }
        page.btnAnalyzeNow.click();
        then:
        if(!page.checkForProcedureNoDataContent()){
            if(page.checkForProcedureDataContent()){
               println("Data is available")
            } else {
                assert false;
            }
        } else {
            page.procedureNoDataMsg.equals("No data is available.")
            println("No data is available." )
        }
    }

    def checkProcedureSubTabs(){
        String createQuery = page.getQueryToCreateTable()
        String createProcedureQuery = page.getQueryToCreateStoredProcedure()
        String execProcedureQuery = page.getQueryToExecuteProcedureQuery()
        String dropTableAndProcQuery = page.getQueryToDropTableAndProcedureQuery()
        when: 'Click SQL tab'
        page.gotoSqlQuery()
        then: 'at SQL page'
        at SqlQueryPage

        when: 'Set create table query in the box'
        page.setQueryText(createQuery)
        then: 'run the query'
        page.runQuery()

        when: 'Set create procedure query in the box'
        page.setQueryText(createProcedureQuery)
        and: 'run the query'
        page.runQuery()
        then: 'refresh the query tab'
        page.refreshquery.click()

        when: 'Set exec procedure query in the box'
        page.setQueryText(execProcedureQuery)
        and: 'run the query'
        page.runQuery()
        then: 'refresh the page'
        page.refreshquery.click()

        when: 'Analysis tab is clicked'
        page.gotoAnalysis()
        then: 'at Analysis Page'
        at AnalysisPage

        when:
        waitFor(2){ page.analyzeNowContent.isDisplayed() }
        waitFor(2){ page.tabProcedureBtn.isDisplayed() }
        page.tabProcedureBtn.click();
        then:
        waitFor(2){ page.tabProcedure.isDisplayed() }

        //Click on Analyze Now button and verify if it is working.
        when:
        waitFor(2){ page.btnAnalyzeNow.isDisplayed() }
        page.btnAnalyzeNow.click();
        then:
        if(page.checkForProcedureDataContent()){
            //Check if procedure sub tab are present
            waitFor(2){ executionTimeSubTab.isDisplayed() }
            waitFor(2){ frequencySubTab.isDisplayed() }
            waitFor(2){ processingTimeSubTab.isDisplayed() }
            println("Tabs are displayed properly.")
        } else {
            assert false;
        }

        when: 'Click SQL tab'
        page.gotoSqlQuery()
        then: 'at SQL page'
        at SqlQueryPage

        when: 'Set drop table and procedure query in the box'
        page.setQueryText(dropTableAndProcQuery)
        then: 'run the query'
        page.runQuery()
        println("Table created successfully.")

    }
}
