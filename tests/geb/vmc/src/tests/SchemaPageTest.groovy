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

import org.junit.Test

import java.util.List;

import spock.lang.*
import vmcTest.pages.*
import org.openqa.selenium.Keys

/**
 * This class tests navigation between pages (or tabs), of the the VoltDB
 * Management Center (VMC), which is the VoltDB (new) web UI.
 */
class SchemaPageTest extends TestBase {

    static final String DDL_SOURCE_FILE = 'src/resources/newExpectedDdlSource.txt';
    static final String VOTER_DDL_SOURCE_FILE = 'src/resources/expectedVoterDdlSource.txt';
    static final String GENQA_DDL_SOURCE_FILE = 'src/resources/expectedGenqaDdlSource.txt';

    static Boolean runningVoter = null;
    static Boolean runningGenqa = null;

    @Shared def ddlExpectedSourceLines = []

    int count = 0

    def setupSpec() { // called once, before any tests
        // Move contents of the specified file into memory
        ddlExpectedSourceLines = getFileLines(new File(DDL_SOURCE_FILE))
    }

    def setup() { // called before each test
        // TestBase.setup gets called first (automatically)
        count = 0

        while(count<numberOfTrials) {
            count ++
            try {
                when: 'click the Schema (page) link'
                page.openSchemaPage()
                then: 'should be on Schema page'
                at SchemaPage

                break
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                println("ElementNotVisibleException: Unable to Start the test")
                println("Retrying")
            }
        }
    }

    /**
     * Returns whether or not we are currently running the 'voter' example app,
     * based on whether the expected DDL Source is listed on the page.
     * @param spdst - the SchemaPageDdlSourceTab from which to get the DDL source.
     * @return true if we are currently running the 'voter' example app.
     */
    static boolean isRunningVoter(SchemaPageDdlSourceTab spdst) {
        if (runningVoter == null) {
            def ddlSource = spdst.getDdlSource()
            runningVoter = true
            for (table in ['AREA_CODE_STATE', 'CONTESTANTS', 'VOTES']) {
                if (!ddlSource.contains(table)) {
                    runningVoter = false
                    break
                }
            }
         }
        return runningVoter
    }

    /**
     * Returns whether or not we are currently running the 'genqa' test app,
     * based on whether the expected DDL Source is listed on the page.
     * @param spdst - the SchemaPageDdlSourceTab from which to get the DDL source.
     * @return true if we are currently running the 'genqa' test app.
     */
    static boolean isRunningGenqa(SchemaPageDdlSourceTab spdst) {
        if (runningGenqa == null) {
            def ddlSource = spdst.getDdlSource()
            runningGenqa = true
            for (text in ['EXPORT_MIRROR_PARTITIONED_TABLE', 'PARTITIONED_TABLE',
                          //'REPLICATED_TABLE',
                          'JiggleExportDoneTable', 'JiggleExportSinglePartition', 'JiggleSkinnyExportSinglePartition']) {
                if (!ddlSource.contains(text)) {
                    runningGenqa = false
                    break
                }
            }
        }
        return runningGenqa
    }

    // HEADER TESTS

    def headerBannerExists() {
        when:
        at SchemaPage
        then:
        waitFor(waitTime) { header.banner.isDisplayed() }
    }

    def headerImageExists() {
        when:
        at SchemaPage
        then:
        waitFor(waitTime) { header.image.isDisplayed() }
    }

    def headerUsernameExists() {
        when:
        at SchemaPage
        then:
        waitFor(waitTime) { header.usernameInHeader.isDisplayed() }
    }

    def headerLogoutExists() {
        when: 'click the Admin link (if needed)'
        page.openAdminPage()
        then: 'should be on Admin page'
        at AdminPage

        when:'Check Security Enabled'
        waitFor(waitTime) { page.overview.securityValue.isDisplayed() }
        String security = page.overview.securityValue.text();
        then:
        if(page.overview.securityValue.text().equals("Off")) {
            println("PASS")
        }

        when: 'click the Schema link (if needed)'
        page.openSchemaPage()
        then: 'should be on Schema page'
        at SchemaPage
        if(security=="On") {
            waitFor(30) {  header.logout.isDisplayed() }
        }
    }

    def headerHelpExists() {
        when:
        at SchemaPage
        then:
        waitFor(30) { page.header.help.isDisplayed() }
        int count = 0
        while(count<5) {
            count++
            try {
                interact {
                    moveToElement(page.header.help)
                }
                waitFor(30) { page.header.showHelp.isDisplayed() }
                break
            } catch (geb.waiting.WaitTimeoutException e) {
                println("Already tried")
            }
        }
    }

    // HEADER TAB TESTS

    def headerTabDbmonitorExists() {
        when:
        at SchemaPage
        then:
        waitFor(waitTime) {
            header.tabDBMonitor.isDisplayed()
            header.tabDBMonitor.text().toLowerCase().equals("DB Monitor".toLowerCase())
        }
    }

    def headerTabAdminExists() {
        when:
        at SchemaPage
        then:
        waitFor(waitTime) {
            header.tabAdmin.isDisplayed()
            header.tabAdmin.text().toLowerCase().equals("Admin".toLowerCase())
        }
    }

    def headerTabSchemaExists() {
        when:
        at SchemaPage
        then:
        waitFor(waitTime) {
            header.tabSchema.isDisplayed()
            header.tabSchema.text().toLowerCase().equals("Schema".toLowerCase())
        }
    }

    def headerTabSqlQueryExists() {
        when:
        at SchemaPage
        then:
        waitFor(waitTime) { header.tabSQLQuery.isDisplayed()
            header.tabSQLQuery.text().toLowerCase().equals("SQL Query".toLowerCase())
        }
    }

    def headerUsernameCheck() {
        when: 'click the Admin link (if needed)'
        page.openAdminPage()
        then: 'should be on Admin page'
        at AdminPage

        when:'Check Security Enabled'
        waitFor(waitTime) { page.overview.securityValue.isDisplayed() }
        String security = page.overview.securityValue.text();
        then:
        if(page.overview.securityValue.text().equals("Off")) {
            println("PASS")
        }

        when: 'click the Schema Page link (if needed)'
        page.openSchemaPage()
        then:
        at SchemaPage
        String username = page.getUsername()
        if(security=="On") {
            waitFor(30) {  header.usernameInHeader.isDisplayed()
                header.usernameInHeader.text().equals(username) }
        }
    }

    def headerUsernameClickAndClose() {
        when:
        at SchemaPage
        then:
        waitFor(waitTime) { header.usernameInHeader.isDisplayed() }
        header.usernameInHeader.click()
        waitFor(waitTime) {
            header.logoutPopupOkButton.isDisplayed()
            header.logoutPopupCancelButton.isDisplayed()
            header.popupClose.isDisplayed()
        }
        header.popupClose.click()
    }

    def headerUsernameClickAndCancel() {
        when:
        at SchemaPage
        then:
        waitFor(waitTime) { header.usernameInHeader.isDisplayed() }
        header.usernameInHeader.click()
        waitFor(waitTime) {
            header.logoutPopupOkButton.isDisplayed()
            header.logoutPopupCancelButton.isDisplayed()
            header.popupClose.isDisplayed()
        }
        header.logoutPopupCancelButton.click()
    }

    // LOGOUT TEST

    def logoutButtonCloseTest()  {
        when: 'click the Admin link (if needed)'
        page.openAdminPage()
        then: 'should be on Admin page'
        at AdminPage

        when:'Check Security Enabled'
        waitFor(waitTime) { page.overview.securityValue.isDisplayed() }
        String security = page.overview.securityValue.text();
        then:
        if(page.overview.securityValue.text().equals("Off")) {
            println("PASS")
        }

        when: 'click the Schema Page link (if needed)'
        page.openSchemaPage()
        then:
        at SchemaPage
        String username = page.getUsername()
        if(security=="On") {
            waitFor(waitTime) { header.logout.isDisplayed() }
            header.logout.click()
            waitFor(waitTime) {
                header.logoutPopupOkButton.isDisplayed()
                header.logoutPopupCancelButton.isDisplayed()
                header.popupClose.isDisplayed()
            }
            header.popupClose.click()
        }
    }

    def logoutButtonCancelTest()  {
        when: 'click the Admin link (if needed)'
        page.openAdminPage()
        then: 'should be on Admin page'
        at AdminPage

        when:'Check Security Enabled'
        waitFor(waitTime) { page.overview.securityValue.isDisplayed() }
        String security = page.overview.securityValue.text();
        then:
        if(page.overview.securityValue.text().equals("Off")) {
            println("PASS")
        }

        when: 'click the Schema Page link (if needed)'
        page.openSchemaPage()
        then:
        at SchemaPage
        String username = page.getUsername()
        if(security=="On") {
            waitFor(waitTime) { header.logout.isDisplayed() }
            header.logout.click()
            waitFor(waitTime) {
                header.logoutPopupOkButton.isDisplayed()
                header.logoutPopupCancelButton.isDisplayed()
                header.popupClose.isDisplayed()
            }
            header.popupClose.click()
        }
    }

    // HELP POPUP TEST

    def helpPopupExistance() {
        when: 'at Schema Page'
        at SchemaPage
        then: 'move to element help'
        waitFor(waitTime) { page.header.help.isDisplayed() }
        int count = 0
        while(count<5) {
            count++
            try {
                interact {
                    moveToElement(page.header.help)
                }
                waitFor(30) { page.header.showHelp.isDisplayed() }
                break
            } catch (geb.waiting.WaitTimeoutException e) {
                println("Already tried")
            }
        }

        when: 'click on showhelp'
        page.header.showHelp.click()
        then: 'check the popup'
        waitFor(waitTime) { page.header.popupClose.isDisplayed() }
        waitFor(waitTime) { page.header.popupTitle.text().toLowerCase().contains("help".toLowerCase()) }
    }

    // FOOTER TESTS

    def footerExists() {
        when: 'at Schema Page'
        at SchemaPage
        then: 'footer banner is displayed'
        waitFor(waitTime) { footer.banner.isDisplayed() }
    }

    def footerTextExistsAndValid() {
        when: 'at Schema Page'
        at SchemaPage
        then: 'check the footer texts'
        waitFor(waitTime) {
            footer.banner.isDisplayed()
            footer.text.isDisplayed()
            footer.text.text().toLowerCase().contains("Copyright (C) 2008-2022 Volt Active Data Inc. All rights reserved.".toLowerCase())
        }
    }

    def confirmOverviewTabOpenInitially() {
        expect: 'Overview tab open initially'
        page.isSchemaPageOverviewTabOpen()
    }

    def navigateSchemaPageTabs() {
        when: 'click the Schema (sub-)link (from Overview tab)'
        page.openSchemaPageSchemaTab()
        then: 'should be on Schema tab (of Schema page)'
        at SchemaPageSchemaTab

        when: 'click the Procedures & SQL link (from Schema tab)'
        page.openSchemaPageProceduresAndSqlTab()
        then: 'should be on Procedures & SQL tab'
        at SchemaPageProceduresAndSqlTab

        when: 'click the Size Worksheet link (from Procedures & SQL tab)'
        page.openSchemaPageSizeWorksheetTab()
        then: 'should be on Size Worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'click the DDL Source link (from Size Worksheet tab)'
        page.openSchemaPageDdlSourceTab()
        then: 'should be on DDL Source tab'
        at SchemaPageDdlSourceTab

        when: 'click the Overview link (from DDL Source tab)'
        page.openSchemaPageOverviewTab()
        then: 'should be on Overview tab'
        at SchemaPageOverviewTab

        when: 'click the DDL Source link (from Overview tab)'
        page.openSchemaPageDdlSourceTab()
        then: 'should be on DDL Source tab'
        at SchemaPageDdlSourceTab

        when: 'click the Size Worksheet link (from DDL Source tab)'
        page.openSchemaPageSizeWorksheetTab()
        then: 'should be on Size Worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'click the Procedures & SQL link (from Size Worksheet tab)'
        page.openSchemaPageProceduresAndSqlTab()
        then: 'should be on Procedures & SQL tab'
        at SchemaPageProceduresAndSqlTab

        when: 'click the Schema (sub-)link (from Procedures & SQL tab)'
        page.openSchemaPageSchemaTab()
        then: 'should be on Schema tab (of Schema page)'
        at SchemaPageSchemaTab

        when: 'click the Overview link (from Schema tab)'
        page.openSchemaPageOverviewTab()
        then: 'should be on Overview tab'
        at SchemaPageOverviewTab
    }

    /**
     * Check that the DDL Source displayed on the DDL Source tab matches the
     * expected list (for the 'genqa' test app).
     */
    def checkSchemaTab() {
        when: 'click the Schema tab link'
        page.openSchemaPageSchemaTab()

        then: 'should be on Schema tab'
        at SchemaPageSchemaTab

        // TODO: should make these into tests, not just debug print
        cleanup: 'for now, just print the table, row-wise'
        debugPrint "\nSchema (tab) table, row-wise:\n" + page.getSchemaTableByRow()

        and: 'for now, just print the table, column-wise'
        debugPrint "\nSchema (tab) table, column-wise:\n" + page.getSchemaTableByColumn()
    }

    /**
     * Check that the DDL Source displayed on the DDL Source tab matches the
     * expected list (for the 'genqa' test app).
     */
    def checkDdlSourceTab() {
        when: 'click the DDL Source tab link'
        page.openSchemaPageDdlSourceTab()

        then: 'should be on DDL Source tab'
        at SchemaPageDdlSourceTab

        when: 'get the DDL Source (lines of text), from the DDL Source tab'
        List<String> ddlActualSourceLines = page.getDdlSourceLines()

        and: 'check which VoltDB server we are running against'
        String fileName = DDL_SOURCE_FILE
        if (isRunningVoter(page)) {
            fileName = VOTER_DDL_SOURCE_FILE
            ddlExpectedSourceLines = getFileLines(new File(VOTER_DDL_SOURCE_FILE))
        } else if (isRunningGenqa(page)) {
            fileName = GENQA_DDL_SOURCE_FILE
            ddlExpectedSourceLines = getFileLines(new File(GENQA_DDL_SOURCE_FILE))
        }

        then: 'test & remove the first few lines, which are descriptive text'
        ddlActualSourceLines.remove(0).startsWith('-- This file was generated by VoltDB version ')
        ddlActualSourceLines.remove(0).equals('-- This file represents the current database schema.')
        ddlActualSourceLines.remove(0).equals('-- Use this file as input to reproduce the current database structure in another database instance.')

//        ddlActualSourceLines.remove(0).equals('-- ')
        ddlActualSourceLines.remove(0).startsWith('--')
        ddlActualSourceLines.remove(0).equals('-- This file uses the --inlinebatch feature. Batching processes all of the DDL in a single step')
        ddlActualSourceLines.remove(0).equals('-- dramatically reducing the time required to apply the schema compared to processing each')
        ddlActualSourceLines.remove(0).equals('-- command separately.')

        ddlActualSourceLines.remove(0).startsWith('--')
        ddlActualSourceLines.remove(0).equals('-- If the schema declares Java stored procedures, be sure to load the .jar file')
        ddlActualSourceLines.remove(0).equals('-- with the classes before loading the schema. For example:')
        ddlActualSourceLines.remove(0).startsWith('--')
        ddlActualSourceLines.remove(0).equals('-- LOAD CLASSES voltdb-procs.jar;')
        ddlActualSourceLines.remove(0).equals('-- FILE ddl.sql;')

        println("expected" + ddlExpectedSourceLines)
        println("actual" + ddlActualSourceLines)

        println(fileName)
        and: 'DDL Source should match expected text'

        int lengthExpected = ddlExpectedSourceLines.size()
        int lengthActual = ddlActualSourceLines.size()

        if (lengthExpected == lengthActual){
            assert true
        }
//        printAndCompare('DDL Source', fileName, true, ddlExpectedSourceLines, ddlActualSourceLines)
    }

    // Overview Tab

    def OverviewTabCheckSchemaOverview() {
        when: 'go to overview tab'
        page.openSchemaPageOverviewTab()
        then: 'at overview tab'
        at SchemaPageOverviewTab

        when: 'check if Schema Overview present'
        waitFor(waitTime) { page.checkSchemaOverview() }
        then: 'check if text is correct'
        page.schemaOverview.text().equals("Schema Overview")
    }

    def overviewTabCheckGeneratedByVoltdbVersion() {
        when: 'go to overview tab'
        page.openSchemaPageOverviewTab()
        then: 'at overview tab'
        at SchemaPageOverviewTab

        when: 'check if Generated by VoltDB Version present'
        waitFor(waitTime) { page.checkVoltDbVersion() }
        then: 'check if text is correct'
        page.voltDbVersion.text().equals("Generated by VoltDB Version")
    }

    def overviewTabCheckLastSchemaUpdateOn() {
        when: 'go to overview tab'
        page.openSchemaPageOverviewTab()
        then: 'at overview tab'
        at SchemaPageOverviewTab

        when: 'check if Last Schema Update on present'
        waitFor(waitTime) { page.checkLastSchemaUpdate() }
        then: 'check if text is correct'
        page.lastSchemaUpdate.text().equals("Last Schema Update on")
    }

    def overviewTabCheckTableCountOn() {
        when: 'go to overview tab'
        page.openSchemaPageOverviewTab()
        then: 'at overview tab'
        at SchemaPageOverviewTab

        when: 'check if Table Count on present'
        waitFor(waitTime) { page.checkTableCount() }
        then: 'check if text is correct'
        page.tableCount.text().equals("Table Count")
    }

    def overviewTabCheckMaterializedViewCount() {
        when: 'go to overview tab'
        page.openSchemaPageOverviewTab()
        then: 'at overview tab'
        at SchemaPageOverviewTab

        when: 'check if Materialized View Count present'
        waitFor(waitTime) { page.checkMaterializedViewCount() }
        then: 'check if text is correct'
        page.materializedViewCount.text().equals("Materialized View Count")
    }

    def overviewTabCheckIndexCount() {
        when: 'go to overview tab'
        page.openSchemaPageOverviewTab()
        then: 'at overview tab'
        at SchemaPageOverviewTab

        when: 'check if Index Count present'
        waitFor(waitTime) { page.checkIndexCount() }
        then: 'check if text is correct'
        page.indexCount.text().equals("Index Count")
    }

    def overviewTabCheckProcedureCount() {
        when: 'go to overview tab'
        page.openSchemaPageOverviewTab()
        then: 'at overview tab'
        at SchemaPageOverviewTab

        when: 'check if Procedure Count present'
        waitFor(waitTime) { page.checkProcedureCount() }
        then: 'check if text is correct'
        page.procedureCount.text().equals("Procedure Count")
    }

    def overviewTabCheckSqlStatementCount() {
        when: 'go to overview tab'
        page.openSchemaPageOverviewTab()
        then: 'at overview tab'
        at SchemaPageOverviewTab

        when: 'check if SQL Statement Count present'
        waitFor(waitTime) { page.checkSqlStatementCount() }
        then: 'check if text is correct'
        page.sqlStatementCount.text().equals("SQL Statement Count")
    }

    // VALUES

    def overviewTabCheckGeneratedByVoltdbVersionValue() {
        when: 'go to overview tab'
        page.openSchemaPageOverviewTab()
        then: 'at overview tab'
        at SchemaPageOverviewTab

        when: 'check if Generated by VoltDB Version value present'
        waitFor(waitTime) { page.voltDBVersionValue.isDisplayed() }
        then: 'check if text is present'
        if(!page.voltDBVersionValue.text().equals("")) {
            println("Overview Tab:Check Generated by VoltDB Version Value-PASS")
        }
        else {
            println("Overview Tab:Check Generated by VoltDB Version Value-FAIL")
            assert false
        }
        println()
    }

    def overviewTabCheckLastSchemaUpdateOnValue() {
        when: 'go to overview tab'
        page.openSchemaPageOverviewTab()
        then: 'at overview tab'
        at SchemaPageOverviewTab

        when: 'check if Last Schema Update On value present'
        waitFor(waitTime) { page.lastSchemaUpdateValue.isDisplayed() }
        then: 'check if text is present'
        if(!page.lastSchemaUpdateValue.text().equals("")) {
            println("Overview Tab:Check Last Schema Update On Value-PASS")
        }
        else {
            println("Overview Tab:Check Last Schema Update On Value-FAIL")
            assert false
        }
        println()
    }

    def overviewTabCheckTableCountValue() {
        when: 'go to overview tab'
        page.openSchemaPageOverviewTab()
        then: 'at overview tab'
        at SchemaPageOverviewTab

        when: 'check if Table Count value present'
        waitFor(waitTime) { page.tableCountValue.isDisplayed() }
        then: 'check if text is present'
        if(!page.tableCountValue.text().equals("")) {
            println("Overview Tab:Check Table Count Value-PASS")
        }
        else {
            println("Overview Tab:Check Table Count Value-FAIL")
            assert false
        }
        println()
    }

    def overviewTabCheckMaterializedViewCountValue() {
        when: 'go to overview tab'
        page.openSchemaPageOverviewTab()
        then: 'at overview tab'
        at SchemaPageOverviewTab

        when: 'check if Materialized View Count value present'
        waitFor(waitTime) { page.materializedViewCountValue.isDisplayed() }
        then: 'check if text is present'
        if(!page.materializedViewCountValue.text().equals("")) {
            println("Overview Tab:Check Materialized View Count Value-PASS")
        }
        else {
            println("Overview Tab:Check Materialized View Count Value-FAIL")
            assert false
        }
        println()
    }

    def overviewTabCheckIndexCountValue() {
        when: 'go to overview tab'
        page.openSchemaPageOverviewTab()
        then: 'at overview tab'
        at SchemaPageOverviewTab

        when: 'check if Index Count value present'
        waitFor(waitTime) { page.indexCountValue.isDisplayed() }
        then: 'check if text is present'
        if(!page.indexCountValue.text().equals("")) {
            println("Overview Tab:Check Index Count Value-PASS")
        }
        else {
            println("Overview Tab:Check Index Count Value-FAIL")
            assert false
        }
        println()
    }

    def overviewTabCheckProcedureCountValue() {
        when: 'go to overview tab'
        page.openSchemaPageOverviewTab()
        then: 'at overview tab'
        at SchemaPageOverviewTab

        when: 'check if Procedure Count value present'
        waitFor(waitTime) { page.procedureCountValue.isDisplayed() }
        then: 'check if text is present'
        if(!page.procedureCountValue.text().equals("")) {
            println("Overview Tab:Check Procedure Count Value-PASS")
        }
        else {
            println("Overview Tab:Check Procedure Count Value-FAIL")
            assert false
        }
        println()
    }

    def overviewTabCheckSqlStatementCountValue() {
        when: 'go to overview tab'
        page.openSchemaPageOverviewTab()
        then: 'at overview tab'
        at SchemaPageOverviewTab

        when: 'check if SQL Statement Count value present'
        waitFor(waitTime) { page.sqlStatementCountValue.isDisplayed() }
        then: 'check if text is present'
        if(!page.sqlStatementCountValue.text().equals("")) {
            println("Overview Tab:Check SQL Statement Count Value-PASS")
        }
        else {
            println("Overview Tab:Check SQL Statement Count Value-FAIL")
            assert false
        }
        println()
    }

    // Size Worksheet Tab
    def sizeWorksheetTabCheckAscendingDescendingInName() {
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'click name'
        page.name.click()
        then: 'check ascending'
        page.ascending.isDisplayed()

        when: 'click name'
        page.name.click()
        then: 'check descending'
        page.descending.isDisplayed()
    }

    def sizeWorksheetTabCheckAscendingDescendingInType() {
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'click type'
        page.type.click()
        then: 'check ascending'
        page.ascending.isDisplayed()

        when: 'click type'
        page.type.click()
        then: 'check descending'
        page.descending.isDisplayed()
    }

    def sizeWorksheetTabCheckAscendingDescendingInCount() {
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'click count'
        page.count.click()
        then: 'check ascending'
        page.ascending.isDisplayed()

        when: 'click count'
        page.count.click()
        then: 'check descending'
        page.descending.isDisplayed()
    }

    def sizeWorksheetTabCheckAscendingDescendingInRowMin() {
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'click row min'
        page.rowMin.click()
        then: 'check ascending'
        page.ascending.isDisplayed()

        when: 'click row min'
        page.rowMin.click()
        then: 'check descending'
        page.descending.isDisplayed()
    }

    def sizeWorksheetTabCheckAscendingDescendingInRowMax() {
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'click row max'
        page.rowMax.click()
        then: 'check ascending'
        page.ascending.isDisplayed()

        when: 'click row max'
        page.rowMax.click()
        then: 'check descending'
        page.descending.isDisplayed()
    }

    def sizeWorksheetTabCheckAscendingDescendingInIndexMin() {
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'click index min'
        page.indexMin.click()
        then: 'check ascending'
        page.ascending.isDisplayed()

        when: 'click index min'
        page.indexMin.click()
        then: 'check descending'
        page.descending.isDisplayed()
    }

    def sizeWorksheetTabCheckAscendingDescendingInIndexMax() {
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'click index max'
        page.indexMax.click()
        then: 'check ascending'
        page.ascending.isDisplayed()

        when: 'click index max'
        page.indexMax.click()
        then: 'check descending'
        page.descending.isDisplayed()
    }

    def sizeWorksheetTabCheckAscendingDescendingInTableMin() {
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'click table min'
        page.tableMin.click()
        then: 'check ascending'
        page.ascending.isDisplayed()

        when: 'click table min'
        page.tableMin.click()
        then: 'check descending'
        page.descending.isDisplayed()
    }

    def sizeWorksheetTabCheckAscendingDescendingInTableMax() {
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'click table max'
        page.tableMax.click()
        then: 'check ascending'
        page.ascending.isDisplayed()

        when: 'click table max'
        page.tableMax.click()
        then: 'check descending'
        page.descending.isDisplayed()
    }

    def sizeWorksheetTabCheckSizeAnalysisSummaryTitle() {
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'check if size anaysis summary is present'
        page.sizeAnalysisSummary.isDisplayed()
        then: 'check if text is correct'
        if (page.sizeAnalysisSummary.text().equals("Estimate Memory Used by User Data")) {
            println("Size Worksheet Tab:Check Size Analysis Summary title - PASS")
            assert true
        }
        else {
            println("Size Worksheet Tab:Check Size Analysis Summary title - FAIL")
            assert false
        }
        println()
    }

    def sizeWorksheetTabCheckSizeAnalysisSummaryValuesForTables() {
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'check if text is present'
        page.textTable.isDisplayed()
        then: 'check if text is correct'
        if (page.textTable.text().contains("tables whose row data is expected to use")) {
            println("Size Worksheet Tab:Check Size Analysis Summary values for tables-Text Correct")
        }
        else {
            println("Size Worksheet Tab:Text not available or wrong - FAIL")
            assert false
        }

        if (page.sizeTableMin.isDisplayed()) {
            println("Size Worksheet Tab:Check Size Analysis Summary values for tables-Min present")
        }
        else {
            println("Size Worksheet Tab:Size Table Min not present-FAIL")
            assert false
        }

       /* if (page.sizeTableMax.isDisplayed()) {
            println("Size Worksheet Tab:Check Size Analysis Summary values for tables-Max present")
        }
        else {
            println("Size Worksheet Tab:Size Table Max not present-FAIL")
            assert false
        }*/

        println("Size Worksheet Tab:Check Size Analysis Summary values for tables-PASS")
        println()
    }

    def sizeWorksheetTabCheckSizeAnalysisSummaryValuesForViews() {
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'check if text is present'
        page.textTable.isDisplayed()
        then: 'check if text is correct'
        page.textView.text().equals("materialized views whose row data is expected to use about ")
        if(page.textView.text().equals("materialized views whose row data is expected to use about ")) {
            println("Size Worksheet Tab:Check Size Analysis Summary values for views-Text Correct")
        }
        else {
            println("Size Worksheet Tab:Text not available or wrong - FAIL")
            assert false
        }

        /*if (page.sizeViewMin.isDisplayed()) {
            println("Size Worksheet Tab:Check Size Analysis Summary values for views-Min present")
        }
        else {
            println("Size Worksheet Tab:Size Table Min not present-FAIL")
            assert false
        }*/

        println("Size Worksheet Tab:Check Size Analysis Summary values for views-PASS")
        println()
    }

    def sizeWorksheetTabCheckSizeAnalysisSummaryValuesForIndex() {
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'check if text is present'
        page.textTable.isDisplayed()
        then: 'check if text is correct'
        page.textIndex.text().contains("indexes whose key data and overhead is expected to use about ")
        if(page.textIndex.text().contains("indexes whose key data and overhead is expected to use about ")) {
            println("Size Worksheet Tab:Check Size Analysis Summary values for index-Text Correct")
        }
        else {
            println("Size Worksheet Tab:Text not available or wrong - FAIL")
            assert false
        }

        if (page.sizeIndexMin.isDisplayed()) {
            println("Size Worksheet Tab:Check Size Analysis Summary values for index-Min present")
        }
        else {
            println("Size Worksheet Tab:Size Table Min not present-FAIL")
            assert false
        }

        println("Size Worksheet Tab:Check Size Analysis Summary values for index-PASS")
        println()
    }

    // Schema Tab

    def schemaTabCheckAscendingDescendingInName() {
        when: 'go to schema tab'
        page.openSchemaPageSchemaTab()
        then: 'at schema tab'
        at SchemaPageSchemaTab

        when: 'click name'
        page.name.click()
        then: 'check ascending'
        if (page.ascending.isDisplayed()) {
            println("Schema Tab:Ascending Success")
        }
        else {
            println("Schema Tab:Ascending Success")
            assert false
        }

        when: 'click name'
        page.name.click()
        then: 'check descending'
        if(page.descending.isDisplayed()) {
            println("Schema Tab:Descending Success")
        }
        else {
            println("Schema Tab:Descending Success")
            assert false
        }
        println()
    }

    def schemaTabCheckAscendingDescendingInType() {
        when: 'go to schema tab'
        page.openSchemaPageSchemaTab()
        then: 'at schema tab'
        at SchemaPageSchemaTab

        when: 'click name'
        page.type.click()
        then: 'check ascending'
        if (page.ascending.isDisplayed()) {
            println("Schema Tab:Ascending Success")
        }
        else {
            println("Schema Tab:Ascending Success")
            assert false
        }

        when: 'click name'
        page.type.click()
        then: 'check descending'
        if(page.descending.isDisplayed()) {
            println("Schema Tab:Descending Success")
        }
        println()
    }

    def schemaTabCheckAscendingDescendingInPartitioning() {
        when: 'go to schema tab'
        page.openSchemaPageSchemaTab()
        then: 'at schema tab'
        at SchemaPageSchemaTab

        when: 'click name'
        page.partitioning.click()
        then: 'check ascending'
        if (page.ascending.isDisplayed()) {
            println("Schema Tab:Ascending Success")
        }
        else {
            println("Schema Tab:Ascending Success")
            assert false
        }

        when: 'click name'
        page.partitioning.click()
        then: 'check descending'
        if(page.descending.isDisplayed()) {
            println("Schema Tab:Descending Success")
        }
        else {
            println("Schema Tab:Descending Success")
            assert false
        }
        println()
    }

    def schemaTabCheckAscendingDescendingInColumns() {
        when: 'go to schema tab'
        page.openSchemaPageSchemaTab()
        then: 'at schema tab'
        at SchemaPageSchemaTab

        when: 'click name'
        page.columns.click()
        then: 'check ascending'
        if (page.ascending.isDisplayed()) {
            println("Schema Tab:Ascending Success")
        }
        else {
            println("Schema Tab:Ascending Success")
            assert false
        }

        when: 'click name'
        page.columns.click()
        then: 'check descending'
        if(page.descending.isDisplayed()) {
            println("Schema Tab:Descending Success")
        }
        else {
            println("Schema Tab:Descending Success")
            assert false
        }
        println()
    }

    def schemaTabCheckAscendingDescendingInIndexes() {
        when: 'go to schema tab'
        page.openSchemaPageSchemaTab()
        then: 'at schema tab'
        at SchemaPageSchemaTab

        when: 'click name'
        page.indexes.click()
        then: 'check ascending'
        if (page.ascending.isDisplayed()) {
            println("Schema Tab:Ascending Success")
        }
        else {
            println("Schema Tab:Ascending Success")
            assert false
        }

        when: 'click name'
        page.indexes.click()
        then: 'check descending'
        if(page.descending.isDisplayed()) {
            println("Schema Tab:Descending Success")
        }
        else {
            println("Schema Tab:Descending Success")
            assert false
        }
        println()
    }

    def schemaTabCheckAscendingDescendingInPKey() {
        when: 'go to schema tab'
        page.openSchemaPageSchemaTab()
        then: 'at schema tab'
        at SchemaPageSchemaTab

        when: 'click name'
        page.pkey.click()
        then: 'check ascending'
        if (page.ascending.isDisplayed()) {
            println("Schema Tab:Ascending Success")
        }
        else {
            println("Schema Tab:Ascending Success")
            assert false
        }

        when: 'click name'
        page.pkey.click()
        then: 'check descending'
        if(page.descending.isDisplayed()) {
            println("Schema Tab:Descending Success")
        }
        else {
            println("Schema Tab:Descending Success")
            assert false
        }
        println()
    }

    def schemaTabCheckAscendingDescendingInTupleLimit() {
        when: 'go to schema tab'
        page.openSchemaPageSchemaTab()
        then: 'at schema tab'
        at SchemaPageSchemaTab

        when: 'click name'
        page.tuplelimit.click()
        then: 'check ascending'
        if (page.ascending.isDisplayed()) {
            println("Schema Tab:Ascending Success")
        }
        else {
            println("Schema Tab:Ascending Success")
            assert false
        }

        when: 'click name'
        page.tuplelimit.click()
        then: 'check descending'
        if(page.descending.isDisplayed()) {
            println("Schema Tab:Descending Success")
        }
        else {
            println("Schema Tab:Descending Success")
            assert false
        }
        println()
    }

    // Procedures and SQLData

    def procedureAndSqlCheckAscendingDescendingInProcedureName() {
        when: 'go to procedures and sql tab'
        page.openSchemaPageProceduresAndSqlTab()
        then: 'at procedures and sql tab'
        at SchemaPageProceduresAndSqlTab

        when: 'click procedure name'
        page.procedureName.click()
        then: 'check ascending'
        waitFor(waitTime) { page.ascending.isDisplayed() }

        when: 'click procedure name'
        page.procedureName.click()
        then: 'check descending'
        waitFor(waitTime) { page.descending.isDisplayed() }
    }

    def proceduresAndSqlCheckAscendingDescendingInParameters() {
        when: 'go to procedures and sql tab'
        page.openSchemaPageProceduresAndSqlTab()
        then: 'at procedures and sql tab'
        at SchemaPageProceduresAndSqlTab

        when: 'click procedure name'
        page.parameters.click()
        then: 'check ascending'
        waitFor(waitTime) { page.ascending.isDisplayed() }

        when: 'click procedure name'
        page.parameters.click()
        then: 'check descending'
        waitFor(waitTime) { page.descending.isDisplayed() }
    }

    def proceduresAndSqlTabCheckAscendingDescendingInPartitioning() {
        when: 'go to procedures and sql tab'
        page.openSchemaPageProceduresAndSqlTab()
        then: 'at procedures and sql tab'
        at SchemaPageProceduresAndSqlTab

        when: 'click procedure name'
        page.partitioning.click()
        then: 'check ascending'
        waitFor(waitTime) { page.ascending.isDisplayed() }

        when: 'click procedure name'
        page.partitioning.click()
        then: 'check descending'
        waitFor(waitTime) { page.descending.isDisplayed() }
    }

    def proceduresAndSqlTabCheckAscendingDescendingInRw() {
        when: 'go to procedures and sql tab'
        page.openSchemaPageProceduresAndSqlTab()
        then: 'at procedures and sql tab'
        at SchemaPageProceduresAndSqlTab

        when: 'click procedure name'
        page.rw.click()
        then: 'check ascending'
        waitFor(waitTime) { page.ascending.isDisplayed() }

        when: 'click procedure name'
        page.rw.click()
        then: 'check descending'
        waitFor(waitTime) { page.descending.isDisplayed() }
    }

    def proceduresAndSqlTabCheckAscendingDescendingInAccess() {
        when: 'go to procedures and sql tab'
        page.openSchemaPageProceduresAndSqlTab()
        then: 'at procedures and sql tab'
        at SchemaPageProceduresAndSqlTab

        when: 'click procedure name'
        page.access.click()
        then: 'check ascending'
        waitFor(waitTime) { page.ascending.isDisplayed() }

        when: 'click procedure name'
        page.access.click()
        then: 'check descending'
        waitFor(waitTime) { page.descending.isDisplayed() }
    }

    def proceduresAndSqlTabCheckAscendingDescendingInAttributes() {
        when: 'go to procedures and sql tab'
        page.openSchemaPageProceduresAndSqlTab()
        then: 'at procedures and sql tab'
        at SchemaPageProceduresAndSqlTab

        when: 'click procedure name'
        page.attributes.click()
        then: 'check ascending'
        waitFor(waitTime) { page.ascending.isDisplayed() }

        when: 'click procedure name'
        page.attributes.click()
        then: 'check descending'
        waitFor(waitTime) { page.descending.isDisplayed() }
    }

    // DLL Source

    def ddlSourceTabCheckDownloadButton() {
        when: 'go to ddl source tab'
        page.openSchemaPageDdlSourceTab()
        then: 'at ddl source tab'
        at SchemaPageDdlSourceTab

        when: 'check if download button is present'
        waitFor(waitTime) { page.downloadButton.isDisplayed() }
        then: 'check if download button is correct'
        page.downloadButton.text().equals("Download")
    }

    def ddlSourceTabClickDownloadButton() {
        when: 'go to ddl source tab'
        page.openSchemaPageDdlSourceTab()
        then: 'at ddl source tab'
        at SchemaPageDdlSourceTab

        when: 'check if download button is present'
        waitFor(waitTime) { page.downloadButton.isDisplayed() }
        then: 'check if download button is correct'
        page.downloadButton.text().equals("Download")

        when: 'click download button'
        page.downloadButton.click()
        then: 'check download is clicked or not'
        println("Download button is clicked without any error")
    }

    def ddlSourceTabCheckContent() {
        when: 'go to ddl source tab'
        page.openSchemaPageDdlSourceTab()
        then: 'at ddl source tab'
        at SchemaPageDdlSourceTab

        waitFor(waitTime) { page.sourceText.isDisplayed() }
    }

    def schemaTabCheckExpandTextAndCheckBox() {
        when: 'go to schema tab'
        page.openSchemaPageSchemaTab()
        then: 'at schema tab'
        at SchemaPageSchemaTab

        when: 'check expand text and check box'
        waitFor(10) { page.expandallcheck.isDisplayed() }
        then: 'go for expand text and check box'
        page.expandallcheck.text().equals("Expand All")
        println("Expand All text for Schema tab checked")
        page.expandallcheck.click()
        waitFor(5){page.expandedcheck.isDisplayed()}
        println("verified expand check box by double clicking in Schema tab")
    }

    def sqlAndProcedureTabCheckExpandTextAndCheckBox() {
        when: 'go to SQL and Procedure tab'
        page.openSchemaPageProceduresAndSqlTab()
        then: 'at SQL and Procedure tab'
        at SchemaPageProceduresAndSqlTab

        when: 'check expand text and check box'
        waitFor(10) { page.expandallproc.isDisplayed() }
        then: 'go for expand text and check box'
        page.expandallproc.text().equals("Expand All")
        println("Expand All text for SQL and Procedure checked")
        page.expandallproc.click()
        waitFor(5){page.expandedproc.isDisplayed()}
        println("verified expand check box by double clicking in Procedure and SQL")
    }

    // voltdb Documentation link
    def overviewTabCheckVoltdbDocumentationLink() {
        when: 'go to SQL and Procedure tab'
        page.openSchemaPageOverviewTab()
        then: 'at SQL and Procedure tab'
        at SchemaPageOverviewTab

        when: 'check VoltDB Documentaion Link'
        waitFor(10) { page.documentationLink.isDisplayed() }
        then: 'check VoltDB Documentaion Link text'
        page.documentationLink.text().equals("VoltDB Documentation")
        println("text verified for VoltDB Documentation for Overview tab")

    }

    def schemaTabCheckVoltdbDocumentationLink() {
        when: 'go to schema tab'
        page.openSchemaPageSchemaTab()
        then: 'at schema tab'
        at SchemaPageSchemaTab

        when: 'check VoltDB Documentaion Link'
        waitFor(10) { page.documentationLink.isDisplayed() }
        then: 'check VoltDB Documentaion Link text'
        page.documentationLink.text().equals("VoltDB Documentation")
        println("text verified for VoltDB Documentation for Schema tab")
    }

    def sqlAndProcedureTabCheckVoltdbDocumentationLink() {
        when: 'go to SQL and Procedure tab'
        page.openSchemaPageProceduresAndSqlTab()
        then: 'at SQL and Procedure tab'
        at SchemaPageProceduresAndSqlTab

        when: 'check VoltDB Documentaion Link'
        waitFor(10) { page.documentationLink.isDisplayed() }
        then: 'check VoltDB Documentaion Link text'
        page.documentationLink.text().equals("VoltDB Documentation")
        println("text verified for VoltDB Documentation for SQL and Procedure tab")
    }

    def sizeWorksheetTabCheckVoltdbDocumentationLink() {
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'check VoltDB Documentaion Link'
        waitFor(10) { page.documentationLink.isDisplayed() }
        then: 'check VoltDB Documentaion Link text'
        page.documentationLink.text().equals("VoltDB Documentation")
        println("text verified for VoltDB Documentation for Size Worksheet tab")
    }

    def ddlSourceTabCheckVoltdbDocumentationLink() {
        when: 'go to DDL source tab'
        page.openSchemaPageDdlSourceTab()
        then: 'at DDL source tab'
        at SchemaPageDdlSourceTab

        when: 'check VoltDB Documentaion Link'
        waitFor(10) { page.documentationLink.isDisplayed() }
        then: 'check VoltDB Documentaion Link text'
        page.documentationLink.text().equals("VoltDB Documentation")
        println("text verified for VoltDB Documentation for DDL Source tab")
    }

    // generated by text

    def overviewTabCheckGeneratedText() {
        when: 'go to overview tab'
        page.openSchemaPageOverviewTab()
        then: 'at overview tab'
        at SchemaPageOverviewTab

        when: 'check VoltDB generated by text'
        waitFor(10) { page.generatedbytxt.isDisplayed() }
        then: 'verify VoltDB generated by text'
        println(" Generated by text for Overview tab is : " +page.generatedbytxt.text())
    }

    def schemaTabCheckGeneratedText() {
        when: 'go to schema tab'
        page.openSchemaPageSchemaTab()
        then: 'at schema tab'
        at SchemaPageSchemaTab

        when: 'check VoltDB generated by text'
        waitFor(10) { page.generatedbytxt.isDisplayed() }
        then: 'verify VoltDB generated by text'
        println(" Generated by text for Schema tab is : " +page.generatedbytxt.text())
    }

    def sqlAndProcedureTabCheckGeneratedText() {
        when: 'go to SQL and Procedure tab'
        page.openSchemaPageProceduresAndSqlTab()
        then: 'at SQL and Procedure tab'
        at SchemaPageProceduresAndSqlTab

        when: 'check VoltDB generated by text'
        waitFor(10) { page.generatedbytxt.isDisplayed() }
        then: 'verify VoltDB generated by text'
        println(" Generated by text for SQL and Procedure tab is : " +page.generatedbytxt.text())
    }

    def sizeWorksheetTabCheckGeneratedText() {
        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'check VoltDB generated by text'
        waitFor(10) { page.generatedbytxt.isDisplayed() }
        then: 'verify VoltDB generated by text'
        println(" Generated by text for Size Worksheet tab is : " +page.generatedbytxt.text())
    }

    def ddlSourceTabCheckGeneratedText() {
        when: 'go to DDL source tab'
        page.openSchemaPageDdlSourceTab()
        then: 'at DDL source tab'
        at SchemaPageDdlSourceTab

        when: 'check VoltDB generated by text'
        waitFor(10) { page.generatedbytxt.isDisplayed() }
        then: 'verify VoltDB generated by text'
        println(" Generated by text for DDL Source tab is : " +page.generatedbytxt.text())
    }

    def schemaTabViewDdlSourceLink() {
        when: 'go to schema tab'
        page.openSchemaPageSchemaTab()
        then: 'at schema tab'
        at SchemaPageSchemaTab

        when: 'check DDL Source Link'
        waitFor(10) { page.viewDdlSource.isDisplayed() }
        then: 'check DDL Source Link text'
        page.viewDdlSource.text().equals("View the DDL Source")
        println("text verified for DDl SOurce in Schema tab")
        page.viewDdlSource.click()
        println("ddl source link clicked!")
        page.openSchemaPageSchemaTab()
    }

    // expand list inside schema tab

    def schemaTabCheckExpandedList() {
        when: 'go to schema tab'
        page.openSchemaPageSchemaTab()
        then: 'at schema tab'
        at SchemaPageSchemaTab

        when: 'check expanded list'
        waitFor(10) { page.expandedlist.isDisplayed() }
        then: 'verify expanded list'
        page.expandedlist.click()
        println(" expanded list clicked, list that has been clicked: " +page.expandedlist.text())
        if(waitFor(10) { page.expandedlistbox.isDisplayed() }){
        page.expandedlist1.click()
        println("next expanded list clicked! i.e, "+page.expandedlist1.text())}
    }

    def sizeWorksheetTabSizeWorksheetVariations() {
        String createQuery = page.getQueryToCreateTable()
        String createViewQuery = page.getQueryToCreateView()
        String idOfFirstCount = page.returnIdOfRowCount(1)
        String idOfSecondCount = page.returnIdOfRowCount(2)
        int tableMin, newTableMin
        int tableMax, newTableMax
        int viewMin, newViewMin
        int indexMin, newIndexMin
        int totalMin, newTotalMin
        int totalMax, newTotalMax

        when: 'click the SQL Query link (if needed)'
        openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage

        when: 'set create query in the box'
        page.setQueryText(createQuery)
        then: 'run the query'
        page.runQuery()

        when: 'set create view query in the box'
        page.setQueryText(createViewQuery)
        then: 'run the query'
        page.runQuery()

        when: 'check for refresh button'
        waitFor(waitTime) { page.refreshquery.isDisplayed() }
        then: 'click the refresh button'
        page.refreshquery.click()

        when: 'click the Schema (page) link'
        page.openSchemaPage()
        then: 'should be on Schema page'
        at SchemaPage

        when: 'go to size worksheet tab'
        page.openSchemaPageSizeWorksheetTab()
        then: 'at size worksheet tab'
        at SchemaPageSizeWorksheetTab

        when: 'assign values from the page'
        tableMin = Integer.parseInt(removeLastTwoChar(page.sizeTableMin.text().replace(",", "")))
        tableMax = Integer.parseInt(removeLastTwoChar(page.sizeTableMax.text().replace(",", "")))
        viewMin  = Integer.parseInt(removeLastTwoChar(page.sizeViewMin.text().replace(",", "")))
        indexMin = Integer.parseInt(removeLastTwoChar(page.sizeIndexMin.text().replace(",", "")))
        totalMin = Integer.parseInt(removeLastTwoChar(page.sizeTotalMin.text().replace(",", "")))
        totalMax = Integer.parseInt(removeLastTwoChar(page.sizeTotalMax.text().replace(",", "")))
        then: 'check if the values follow the rule or not'
        totalMax == tableMax + viewMin + indexMin
        totalMin == tableMin + viewMin + indexMin

        when: 'row count is changed'
        $(id:idOfFirstCount).value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        $(id:idOfFirstCount).value("100000")
        then: 'click a text'
        page.sizeTableMin.click()

        report 'hello1'
        when: 'assign new values from the page'
        newTableMin = Integer.parseInt(removeLastTwoChar(page.sizeTableMin.text().replace(",", "")))
        newTableMax = Integer.parseInt(removeLastTwoChar(page.sizeTableMax.text().replace(",", "")))
        newViewMin  = Integer.parseInt(removeLastTwoChar(page.sizeViewMin.text().replace(",", "")))
        newIndexMin = Integer.parseInt(removeLastTwoChar(page.sizeIndexMin.text().replace(",", "")))
        newTotalMin = Integer.parseInt(removeLastTwoChar(page.sizeTotalMin.text().replace(",", "")))
        newTotalMax = Integer.parseInt(removeLastTwoChar(page.sizeTotalMax.text().replace(",", "")))
        and: 'compare the new values with old ones'
        newTableMin == 100 * tableMin
        newTableMax == 100 * tableMax
        then: 'check if the new values follow the rule or not'
        newTotalMax == newTableMax + newViewMin + newIndexMin
        newTotalMin == newTableMin + newViewMin + newIndexMin

        when: 'row count is changed'
        $(id:idOfSecondCount).value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        $(id:idOfSecondCount).value("100000")
        then: 'click a text'
        page.sizeTableMin.click()

        report 'hello2'
        when: 'assign new values from the page'
        newTableMin = Integer.parseInt(removeLastTwoChar(page.sizeTableMin.text().replace(",", "")))
        newTableMax = Integer.parseInt(removeLastTwoChar(page.sizeTableMax.text().replace(",", "")))
        newViewMin  = Integer.parseInt(removeLastTwoChar(page.sizeViewMin.text().replace(",", "")))
        newIndexMin = Integer.parseInt(removeLastTwoChar(page.sizeIndexMin.text().replace(",", "")))
        newTotalMin = Integer.parseInt(removeLastTwoChar(page.sizeTotalMin.text().replace(",", "")))
        newTotalMax = Integer.parseInt(removeLastTwoChar(page.sizeTotalMax.text().replace(",", "")))
        and: 'compare the new values with old ones'
        newViewMin  == 100 * viewMin
        newIndexMin == 100 * indexMin
        then: 'check if the new values follow the rule or not'
        newTotalMax == newTableMax + newViewMin + newIndexMin
        newTotalMin == newTableMin + newViewMin + newIndexMin
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
