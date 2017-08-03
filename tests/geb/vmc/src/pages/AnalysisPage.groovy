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

package vmcTest.pages

import geb.navigator.Navigator

class AnalysisPage extends VoltDBManagementCenterPage {
    static content = {
        divAnalysis                 { $('#divAnalysis') }
        analyzeNowContent           { $('.analyzeNowContent') }
        tabProcedureBtn             { $('#tabProcedureBtn') }
        tabProcedure                { $('#tabProcedure') }
        btnThreshold                { $('#btnThreshold') }
        btnAnalyzeNow               { $('#btnAnalyzeNow') }
        procedureNoDataContent      { $('#tabProcedure > div.mainContentAnalysis.noDataContent') }
        procedureNoDataMsg          { $('#tabProcedure > div.mainContentAnalysis.noDataContent > p') }
        divTabProcedure             { $('#divTabProcedure') }
        executionTimeSubTab         { $('#ulProcedure > li:nth-child(1)') }
        frequencySubTab             { $('#ulProcedure > li:nth-child(2)') }
        processingTimeSubTab        { $('#ulProcedure > li:nth-child(3)') }
        chartLatencyAnalysis        { $('#chartLatencyAnalysis') }
        chartFrequencyAnalysis      { $('#chartFrequencyAnalysis') }
        chartProcessingTimeAnalysis { $('#chartProcessingTimeAnalysis') }
        settingMessage              { $('.settingMessage') }
        tblAnalysisSettings         { $('#tblAnalysisSettings') }
        btnSaveThreshold            { $('#btnSaveThreshold') }
        btnCancelThreshold          { $('#btnCancelThreshold') }
        chkShowSysProcedureDiv      { $('#trShowHideSysProcedures > td:nth-child(2) > div') }
        chkShowSysProcedure         { $('#trShowHideSysProcedures > td:nth-child(2) > div > ins') }
        foreignObjectForSys         { $('p', text: 'org.voltdb.sysprocs.UpdateCore')}
        averageExecutionTime        { $('#averageExecutionTime') }
        warningSign                 { $('foreignObject', text: '⚠')}
        analysisRemarks             { $('#analysisRemarks') }
    }

    static at = {
        analysisTab.displayed
        analysisTab.attr('class') == 'active'
    }

    def boolean checkSysProcedureDisplayed(){
        return foreignObjectForSys.displayed
    }

    def isAnalyzeNowContentDisplayed(){
        return analyzeNowContent.displayed
    }

    def checkForProcedureNoDataContent(){
        return procedureNoDataContent.displayed
    }

    def checkForProcedureDataContent(){
        return divTabProcedure.displayed
    }

    /*
     * click SQL Query to go to SqlQueryPage
     */
    def boolean gotoSqlQuery() {
        header.tabSQLQuery.click()
    }

    /*
     * get query to create a table
     */
    def String getQueryToCreateTable() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryAnalysis.txt"));
        String line;
        String query = ""

        while((line = br.readLine()) != "#create") {
        }

        while ((line = br.readLine()) != "#delete") {
            // process the line.
            query = query + line + "\n"
        }

        return query
    }

    /*
     * get query to delete a table
     */
    def String getQueryToDeleteTable() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryAnalysis.txt"));
        String line;
        String query = ""

        while((line = br.readLine()) != "#delete") {
        }

        while ((line = br.readLine()) != "#name") {
            // process the line.
            query = query + line + "\n"
        }

        return query
    }

    /*

     */
    def String getQueryToCreateStoredProcedure() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryAnalysis.txt"));
        String line;
        String query = ""

        while((line = br.readLine()) != "#createStoredProcedure") {
        }

        while ((line = br.readLine()) != "#executeStoredProcedure") {
            // process the line.
            query = query + line + "\n"
        }

        return query
    }

    /*

     */
    def String getQueryToExecuteProcedureQuery() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryAnalysis.txt"));
        String line;
        String query = ""

        while((line = br.readLine()) != "#executeStoredProcedure") {
        }

        while ((line = br.readLine()) != "#dropStoredProcedure") {
            // process the line.
            query = query + line + "\n"
        }

        return query
    }

    /*

     */
    def String getQueryToDropProcedureQuery() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryAnalysis.txt"));
        String line;
        String query = ""

        while((line = br.readLine()) != "#dropStoredProcedure") {
        }

        while ((line = br.readLine()) != "#procedureName") {
            // process the line.
            query = query + line + "\n"
        }

        return query
    }

    /*

     */
    def String getNameOfStoredProcedure() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryAnalysis.txt"));
        String line;
        String query = ""

        while((line = br.readLine()) != "#procedureName") {
        }

        query = br.readLine()

        return query
    }

    /*
     get tablename that is created and deleted
    */
    def String getTableName() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryAnalysis.txt"));
        String line;
        String query = ""

        while((line = br.readLine()) != "#name") {
        }

        query = br.readLine()

        return query
    }

    /*

     */
    def String getQueryToDropTableAndProcedureQuery() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryAnalysis.txt"));
        String line;
        String query = ""

        while((line = br.readLine()) != "#dropTableAndProcedure") {
        }

        while ((line = br.readLine()) != "#endQuery") {
            // process the line.
            query = query + line + "\n"
        }

        return query
    }

}
