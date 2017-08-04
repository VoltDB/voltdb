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

    def createExecuteTableAndProcedure(createQuery, createProcedureQuery, execProcedureQuery){
        page.setQueryText(createQuery)
        page.runQuery()

        page.setQueryText(createProcedureQuery)
        page.runQuery()
        page.refreshquery.click()

        page.setQueryText(execProcedureQuery)
        page.runQuery()
        page.refreshquery.click()
    }

    def deleteTableAndProcedure(dropTableAndProcQuery){
        try{
            page.setQueryText(dropTableAndProcQuery)
            page.runQuery()
//            println("Table deleted successfully.")
            return true;
        } catch (Exception e){
//            println("sdfsdfsdfsdffsdf")
            return false;
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

    def checkProcedureSubTabsAndGraph(){
        //Create necessary tables and procedures
        String createQuery = page.getQueryToCreateTable()
        String createProcedureQuery = page.getQueryToCreateStoredProcedure()
        String execProcedureQuery = page.getQueryToExecuteProcedureQuery()

        when: 'Click SQL tab'
        page.gotoSqlQuery()
        then: 'at SQL page'
        at SqlQueryPage

        createExecuteTableAndProcedure(createQuery, createProcedureQuery, execProcedureQuery)

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
            executionTimeSubTab.click()
            chartLatencyAnalysis.isDisplayed()

            waitFor(2){ frequencySubTab.isDisplayed() }
            frequencySubTab.click()
            chartFrequencyAnalysis.isDisplayed()

            waitFor(2){ processingTimeSubTab.isDisplayed() }
            processingTimeSubTab.click()
            chartProcessingTimeAnalysis.isDisplayed()
            println("Tabs are displayed properly.")
        } else {
            assert false;
        }

        //deleted the created test talbes and procedures.
        String dropTableAndProcQuery = page.getQueryToDropTableAndProcedureQuery()
        when: 'Click SQL tab'
        page.gotoSqlQuery()
        then: 'at SQL page'
        at SqlQueryPage

        deleteTableAndProcedure(dropTableAndProcQuery)
    }

    def checkThresholdPopup(){
        when:
        waitFor(5){ btnThreshold.isDisplayed() }
        btnThreshold.click()
        then:
        waitFor(10){ settingMessage.isDisplayed() }
        tblAnalysisSettings.isDisplayed()
        btnSaveThreshold.isDisplayed()
        btnCancelThreshold.isDisplayed()
    }

    def checkShowSysProcedure(){
        when:
        waitFor(2){ btnThreshold.isDisplayed() }
        btnThreshold.click()
        then:
        waitFor(2){ settingMessage.isDisplayed() }

        when:
        if(page.chkShowSysProcedureDiv.hasClass('checked')){
            btnCancelThreshold.click()
        } else {
            chkShowSysProcedure.click()
            btnSaveThreshold.click()
        }
        then:
        waitFor(2){ btnAnalyzeNow.isDisplayed() }
        btnAnalyzeNow.click()
        waitFor(2){ foreignObjectForSys.isDisplayed() }

        when:
        btnThreshold.click()
        then:
        chkShowSysProcedure.click()
        btnSaveThreshold.click()

        when:
        waitFor(2){ btnAnalyzeNow.isDisplayed() }
        btnAnalyzeNow.click()
        then:
        try{
            waitFor(2){ foreignObjectForSys.isDisplayed() }
        } catch(geb.waiting.WaitTimeoutException e){
            assert true;
        }
    }

    def checkAverageExecutionThresholdAndCheckRemark(){
        //Create necessary tables and procedures
        String createQuery = page.getQueryToCreateTable()
        String createProcedureQuery = page.getQueryToCreateStoredProcedure()
        String execProcedureQuery = page.getQueryToExecuteProcedureQuery()

        when: 'Click SQL tab'
        page.gotoSqlQuery()
        then: 'at SQL page'
        at SqlQueryPage
        createExecuteTableAndProcedure(createQuery, createProcedureQuery, execProcedureQuery)

        when: 'Analysis tab is clicked'
        page.gotoAnalysis()
        then: 'at Analysis Page'
        at AnalysisPage

        when:
        waitFor(2){ btnThreshold.isDisplayed() }
        btnThreshold.click()
        then:
        waitFor(2){ settingMessage.isDisplayed() }

        when:
        waitFor(2){ averageExecutionTime.isDisplayed() }
        averageExecutionTime.value("0")
        btnSaveThreshold.click()
        then:

        waitFor(2){ btnAnalyzeNow.isDisplayed() }
        btnAnalyzeNow.click()
        report 'test warning'
        waitFor(5){ analysisRemarks.isDisplayed() }

        //deleted the created test talbes and procedures.
        String dropTableAndProcQuery = page.getQueryToDropTableAndProcedureQuery()
        when: 'Click SQL tab'
        page.gotoSqlQuery()
        then: 'at SQL page'
        at SqlQueryPage

        deleteTableAndProcedure(dropTableAndProcQuery)
    }

    def openAndCloseGraphDetailPopup() {
        given:
        String createQuery = page.getQueryToCreateTable()
        String createProcedureQuery = page.getQueryToCreateStoredProcedure()
        String execProcedureQuery = page.getQueryToExecuteProcedureQuery()

        when: 'Click SQL tab'
        page.gotoSqlQuery()
        then: 'at SQL page'
        at SqlQueryPage
        createExecuteTableAndProcedure(createQuery, createProcedureQuery, execProcedureQuery)

        when: 'Analysis tab is clicked'
        page.gotoAnalysis()
        then: 'at Analysis Page'
        at AnalysisPage

        when: 'wait for analyze now button to be displayed'
        waitFor(waitTime) { page.btnAnalyzeNow.isDisplayed() }
        then: 'click analyze now button'
        page.btnAnalyzeNow.click()

        when: 'click open the first bar'
        page.firstBar.click();
        then: ''
        assert page.detailsPopup.isDisplayed();

        when:
        page.detailsPopupCloseButton.click();
        report "hello"
        then:
        assert !page.detailsPopupCloseButton.isDisplayed()
        println();
    }

    def cleanup() {
        String dropTableAndProcQuery = page.getQueryToDropTableAndProcedureQuery()
        when: 'Click SQL tab'
        page.gotoSqlQuery()
        then: 'at SQL page'
        at SqlQueryPage
        deleteTableAndProcedure(dropTableAndProcQuery)
    }
//    #visualiseLatencyAnalysis > g > g > g.nv-x.nv-axis.nvd3-svg > g > g > g > foreignObject > p
//    #visualiseLatencyAnalysis > g > g > g.nv-x.nv-axis.nvd3-svg > g > g > g:nth-child(2) > foreignObject > p
}
