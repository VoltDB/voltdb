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

import vmcTest.pages.*
import java.io.*;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * This class contains tests of the 'DB Monitor' tab of the VoltDB Management
 * Center (VMC) page, which is the VoltDB (new) web UI.
 */
class DbMonitorTest extends TestBase {
    int count = 0
    def setup() { // called before each test
        // TestBase.setup gets called first (automatically)
        int count = 0

        while(count<numberOfTrials) {
            try {
                when: 'click the DB Monitor link (if needed)'
                page.openDbMonitorPage()
                browser.driver.executeScript("localStorage.removeItem('user-preferences_null')")
                browser.driver.executeScript("localStorage.removeItem('user-preferences_8080')")
                then: 'should be on DB Monitor page'
                at DbMonitorPage
                ///Check all unchecked UserPreferences

//                expect: 'Display Preference button exists'
//                page.displayPreferenceDisplayed()
//
//                when: 'click Display Preference button'
//                page.openDisplayPreference()
//                then: 'display title and save button of preferences'
//                page.preferencesTitleDisplayed()
//                page.savePreferencesBtnDisplayed()
//                page.popupCloseDisplayed()
//
//                when: 'Stored Procedures checkbox is displayed'
//                page.storedProceduresCheckboxDisplayed()
//                page.dataTablesCheckboxDisplayed()
//                page.partitionIdleTimeCheckboxDisplayed()
//               // page.clusterTransactionsCheckboxDisplayed()
//                //page.clusterLatencyCheckboxDisplayed()
//
//
//                then: 'Add all the preferences'
//                page.storedProceduresCheckboxClick()
//                //page.dataTablesCheckboxClick()
//                page.partitionIdleTimeCheckboxClick()
//              //  page.clusterTransactionsCheckboxClick()
//               // page.clusterLatencyCheckboxClick()
//
//
//                when: 'click close button'
//                page.savePreferences()
//                then: 'no Stored Procedures displayed'
//                page.storedProceduresDisplayed()
                break
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                println("ElementNotVisibleException: Unable to Start the test")
                println("Retrying")
            }
        }
    }

    def openAndCloseGraphArea() {
        when: 'ensure the Graph area is open'
        if (!page.isGraphAreaOpen()) {
            page.openGraphArea()
        }
        then: 'Graph area is open (initially)'
        page.isGraphAreaOpen()

        when: 'click Show/Hide Graph (to close)'
        page.closeGraphArea()
        then: 'Graph area is closed'
        !page.isGraphAreaOpen()

        when: 'click Show/Hide Graph (to open)'
        page.openGraphArea()
        then: 'Graph area is open (again)'
        page.isGraphAreaOpen()

        when: 'click Show/Hide Graph (to close again)'
        page.closeGraphArea()
        then: 'Graph area is closed (again)'
        !page.isGraphAreaOpen()
    }

//    def openAndCloseDataArea() {
//        when:
//        if (!page.dataTables.isDisplayed()) {
//
//            when: 'ensure the Data area is open'
//            if (!page.isDataAreaOpen()) {
//                page.openDataArea()
//            }
//            then: 'Data area is open (to start test)'
//            page.isDataAreaOpen()
//
//            when: 'click Show/Hide Data (to close)'
//            page.closeDataArea()
//            then: 'Data area is closed'
//            !page.isDataAreaOpen()
//
//            when: 'click Show/Hide Data (to open again)'
//            page.openDataArea()
//            then: 'Data area is open (again)'
//            page.isDataAreaOpen()
//
//            when: 'click Show/Hide Data (to close again)'
//            page.closeDataArea()
//            then: 'Data area is closed (again)'
//            !page.isDataAreaOpen()
//        }
//        then:
//        println("passed")
//    }

    def checkActiveMissingJoining() {
        expect: '1 Active server (at least)'
        page.getActive() >= 1

        and: '0 Missing servers (initially)'
        page.getMissing() == 0

        and: 'Joining servers not shown (for now)'
        page.getJoining() == -1
    }

    def openAndCloseServerList() {
        boolean result = false
        int count = 0
        when: 'click the Admin link (if needed)'
        page.openAdminPage()
        then: 'should be on Admin page'
        at AdminPage
        try {
            waitFor(waitTime) { page.cluster.resumebutton.isDisplayed() }
            println("Resume button is displayed")
            result = false
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Resume button is not displayed")
            result = true
        }

        if (result == false) {
            println("Resume VMC")

            count = 0
            while(count<numberOfTrials) {
                try {
                    count++
                    page.cluster.resumebutton.click()
                    waitFor(waitTime) { page.cluster.resumeok.isDisplayed() }
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("Error: Resume confirmation was not found")
                    assert false
                }
            }

            count = 0
            while(count<numberOfTrials) {
                try {
                    count++
                    page.cluster.resumeok.click()
                    waitFor(waitTime) { page.cluster.pausebutton.isDisplayed() }
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("Error: Pause button was not found")
                }
            }
        }
        then:
        println()

        when: 'click the DB Monitor link (if needed)'
        page.openDbMonitorPage()
        then: 'should be on DB Monitor page'
        at DbMonitorPage

        expect: 'Server list closed initially'
        !page.isServerListOpen()

        when: 'click Server button (to open list)'
        page.openServerList()
        then: 'Server list is open'
        if(page.isServerListOpen()) {
            page.serverListHeader.isDisplayed()
            page.serverNameHeader.isDisplayed()
            page.serverIpAddressHeader.isDisplayed()
            page.serverMemoryUsageHeader.isDisplayed()
        }

        when: 'click Server button (to close list)'
        page.closeServerList()
        then: 'Server list is closed (again)'
        !page.isServerListOpen()
    }

    def triggerAlert() {
        expect: 'no Alerts shown, initially'
        page.getAlert() == -1

        // TODO: add more testing here, setting threshold
    }

    def checkServerNamesAndMemoryUsage() {
        expect: 'Server list closed initially'
        !page.isServerListOpen()

        // TODO: make this a real test, not just printing values
        List<String> serverNames = page.getServerNames()
        debugPrint "Server Names            : " + serverNames
        debugPrint "Memory Usages           : " + page.getMemoryUsages()
        debugPrint "Memory Usage Percents   : " + page.getMemoryUsagePercents()
        debugPrint "Memory Usage (0)        : " + page.getMemoryUsage(serverNames.get(0))
        debugPrint "Memory Usage Percent (0): " + page.getMemoryUsagePercent(serverNames.get(0))
    }

    //HEADER TESTS

    def headerBannerExists() {
        when:
        at DbMonitorPage
        then:
        waitFor(waitTime) { header.banner.isDisplayed() }
    }

    def headerImageExists() {
        when:
        at DbMonitorPage
        then:
        waitFor(waitTime) { header.image.isDisplayed() }
    }

    def headerUsernameExists() {
        when:
        at DbMonitorPage
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

        when: 'click the DB Monitor link (if needed)'
        page.openDbMonitorPage()
        then: 'should be on DB Monitor page'
        at DbMonitorPage
        if(security=="On") {
            waitFor(30) {  header.logout.isDisplayed() }
        }
    }

//    def "header help exists" () {
//        when:
//        at DbMonitorPage
//        then:
//        waitFor(30) { page.header.help.isDisplayed() }
//        int count = 0
//        while(count<5) {
//            count++
//            try {
//                interact {
//                    moveToElement(page.header.help)
//                }
//                waitFor(30) { page.header.showHelp.isDisplayed() }
//                break
//            } catch (geb.waiting.WaitTimeoutException e) {
//                println("Already tried")
//            }
//        }
//    }

    //HEADER TAB TESTS

    def headerTabDbmonitorExists() {
        when:
        at DbMonitorPage
        then:
        waitFor(30) {
            header.tabDBMonitor.isDisplayed()
            header.tabDBMonitor.text().toLowerCase().equals("DB Monitor".toLowerCase())
        }
    }

    def headerTabAdminExists() {
        when:
        at DbMonitorPage
        then:
        waitFor(30) {
            header.tabAdmin.isDisplayed()
            header.tabAdmin.text().toLowerCase().equals("Admin".toLowerCase())
        }
    }

    def headerTabSchemaExists() {
        when:
        at DbMonitorPage
        then:
        waitFor(30) {
            header.tabSchema.isDisplayed()
            header.tabSchema.text().toLowerCase().equals("Schema".toLowerCase())
        }
    }

    def headerTabSqlQueryExists() {
        when:
        at DbMonitorPage
        then:
        waitFor(30) { header.tabSQLQuery.isDisplayed()
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

        when: 'click the DB Monitor link (if needed)'
        page.openDbMonitorPage()
        then:
        at DbMonitorPage
        String username = page.getUsername()
        if(security=="On") {
            waitFor(30) {  header.usernameInHeader.isDisplayed()
                header.usernameInHeader.text().equals(username) }
        }
    }

    def headerUsernameClickAndClose() {
        when:
        at DbMonitorPage
        then:
        waitFor(30) { header.usernameInHeader.isDisplayed() }
        header.usernameInHeader.click()
        waitFor(30) {
            header.logoutPopupOkButton.isDisplayed()
            header.logoutPopupCancelButton.isDisplayed()
            header.popupClose.isDisplayed()
        }
        header.popupClose.click()
    }

    def headerUsernameClickAndCancel() {
        when:
        at DbMonitorPage
        then:
        waitFor(30) { header.usernameInHeader.isDisplayed() }
        header.usernameInHeader.click()
        waitFor(30) {
            header.logoutPopupOkButton.isDisplayed()
            header.logoutPopupCancelButton.isDisplayed()
            header.popupClose.isDisplayed()
        }
        header.logoutPopupCancelButton.click()
    }

    // LOGOUT TEST

    def logoutButtonTestClose()  {
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
        when: 'click the DB Monitor link (if needed)'
        page.openDbMonitorPage()
        then:
        at DbMonitorPage
        String username = page.getUsername()
        if(security=="On") {
            waitFor(30) { header.logout.isDisplayed() }
            header.logout.click()
            waitFor(30) {
                header.logoutPopupOkButton.isDisplayed()
                header.logoutPopupCancelButton.isDisplayed()
                header.popupClose.isDisplayed()
            }
            header.popupClose.click()
        }
    }

    def logoutButtonTestCancel()  {
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

        when: 'click the DB Monitor link (if needed)'
        page.openDbMonitorPage()
        then:
        at DbMonitorPage
        String username = page.getUsername()
        if(security=="On") {
            waitFor(30) { header.logout.isDisplayed() }
            header.logout.click()
            waitFor(30) {
                header.logoutPopupOkButton.isDisplayed()
                header.logoutPopupCancelButton.isDisplayed()
                header.popupClose.isDisplayed()
            }
            header.logoutPopupCancelButton.click()
        }
    }

    //HELP POPUP TEST

    def helpPopupExistance() {
        when:
        at DbMonitorPage
        then:
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

        when:
        page.header.showHelp.click()
        then:
        waitFor(waitTime) { page.header.popupClose.isDisplayed() }
        waitFor(waitTime) { page.header.popupTitle.text().toLowerCase().contains("help".toLowerCase()) }
    }

    //FOOTER TESTS

    def footerExists() {
        when:
        at DbMonitorPage
        then:
        waitFor(30) { footer.banner.isDisplayed() }
    }

    def footerTextExistsAndValid() {
        when:
        at DbMonitorPage
        then:
        waitFor(30) {
            footer.banner.isDisplayed()
            footer.text.isDisplayed()
            footer.text.text().toLowerCase().contains("Copyright (C) 2008-2022 Volt Active Data Inc. All rights reserved.".toLowerCase())
        }
    }

    def addTableAndCheck() {
        String createQuery = page.getQueryToCreateTable()
        String deleteQuery = page.getQueryToDeleteTableOnly()
        String tablename = page.getTablename()
        String deleteQuery1 = page.getQueryToDeleteTableAndView()
        when:
        println(createQuery)
        println(deleteQuery)
        then:
        println(tablename)

        when: 'sql query tab is clicked'
        page.gotoSqlQuery()
        then: 'at sql query'
        at SqlQueryPage
        report "hello1"

        when: 'set query in the box'
        page.setQueryText(createQuery)
        then: 'run the query'
        page.runQuery()
        report "hello2"

        when: 'Db Monitor tab is clicked'
        page.gotoDbMonitor()
        then: 'at DbMonitor Page'
        at DbMonitorPage
        report "hello3"

        when:
        if(page.dataTablesDisplayed()) {
            when:
            page.searchDatabaseTable(tablename)
            then:
            report "hello4"
            waitFor(30) {
                !page.databaseTableCurrentPage.text().equals("0")
                !page.databaseTableTotalPage.text().equals("0")
            }
            if (!page.databaseTableCurrentPage.text().equals("0") && !page.databaseTableTotalPage.text().equals("0")) {
                println("The table was successfully created")
            } else {
                println("Table not found after creation")
                assert false
            }

            when: 'sql query tab is clicked'
            page.gotoSqlQuery()
            then: 'at sql query'
            at SqlQueryPage

            when: 'set query in the box'
            page.setQueryText(deleteQuery)
            then: 'run the query'
            page.runQuery()
            report "delete"

            when: 'Db Monitor tab is clicked'
            page.gotoDbMonitor()
            then: 'at DbMonitor Page'
            at DbMonitorPage

            when:
            page.searchDatabaseTable(tablename)
            then:
            report "delete1"
            waitFor(30) {
                page.databaseTableCurrentPage.text().equals("0")
                page.databaseTableTotalPage.text().equals("0")
            }
            if (page.databaseTableCurrentPage.text().equals("0") && page.databaseTableTotalPage.text().equals("0")) {
                println("The table was successfully removed")
            } else {
                println("Table found after deletion")
                assert false
            }
        }
        then:
        println("passed")
    }

    def checkIfRowCountIsClickable() {
        String before = ""
        String after  = ""

        when:
        if(page.dataTablesDisplayed()) {
            when: 'click row count'
            page.clickRowcount()
            then: 'check if row count is in ascending'
            if (page.tableInAscendingOrder())
                before = "ascending"
            else
                before = "descending"

            when: 'click row count'
            page.clickRowcount()
            then: 'check if row count is in descending'
            if (page.tableInDescendingOrder())
                after = "descending"
            else
                after = "ascending"

            if (before.equals("ascending") && after.equals("descending"))
                assert true
        }
        then:
        println("passed")
    }

    def checkIfMaxRowsIsClickable() {
        String before = ""
        String after  = ""

        when:
        if(page.dataTablesDisplayed()) {
            when: 'click max rows'
            page.clickMaxRows()
            then: 'check if max rows is in ascending'
            if (page.tableInAscendingOrder())
                before = "ascending"
            else
                before = "descending"

            when: 'click max rows'
            page.clickMaxRows()
            then: 'check if max rows is in descending'
            if (page.tableInDescendingOrder())
                after = "descending"
            else
                after = "ascending"

            if (before.equals("ascending") && after.equals("descending"))
                assert true
        }
        then:
        println("passed")
    }

    def checkIfMinRowsIsClickable() {
        String before = ""
        String after  = ""

        when:
        if(page.dataTablesDisplayed()) {
            when: 'click min rows'
            page.clickMinRows()
            then: 'check if min rows is in ascending'
            if (page.tableInAscendingOrder())
                before = "ascending"
            else
                before = "descending"

            when: 'click min rows'
            page.clickMinRows()
            then: 'check if min rows is in descending'
            if (page.tableInDescendingOrder())
                after = "descending"
            else
                after = "ascending"

            if (before.equals("ascending") && after.equals("descending"))
                assert true
        }
        then:
        println("passed")
    }

    def checkIfAvgRowsIsClickable() {
        String before = ""
        String after  = ""

        when:
        if(page.dataTablesDisplayed()) {
            when: 'click avg rows'
            page.clickAvgRows()
            then: 'check if avg rows is in ascending'
            if (page.tableInAscendingOrder())
                before = "ascending"
            else
                before = "descending"

            when: 'click avg rows'
            page.clickAvgRows()
            then: 'check if avg rows is in descending'
            if (page.tableInDescendingOrder())
                after = "descending"
            else
                after = "ascending"

            if (before.equals("ascending") && after.equals("descending"))
                assert true
        }
        then:
        println("passed")
    }

    def checkIfTypeIsClickable() {
        String before = ""
        String after  = ""

        when:
        if(page.dataTablesDisplayed()) {
            when: 'click type'
            page.clickTabletype()
            then: 'check if type is in ascending'
            if (page.tableInAscendingOrder())
                before = "ascending"
            else
                before = "descending"

            when: 'click type'
            page.clickTabletype()
            then: 'check if type is in descending'
            if (page.tableInDescendingOrder())
                after = "descending"
            else
                after = "ascending"

            if (before.equals("ascending") && after.equals("descending"))
                assert true
        }
        then:
        println("passed")
    }

    // stored procedure ascending descending

    def checkIfStoredProcedureIsClickable() {
        String before = ""
        String after  = ""

        expect: 'Display Preference button exists'
        page.displayPreferenceDisplayed()

        when: 'click Display Preference button'
        page.openDisplayPreference()
        then: 'display title and save button of preferences'
        page.preferencesTitleDisplayed()
        page.savePreferencesBtnDisplayed()
        page.popupCloseDisplayed()

        when: 'Stored Procedure checkbox is displayed'
        page.storedProceduresCheckboxDisplayed()
        then: 'Add Stored Procedure'
        page.storedProceduresCheckboxClick()

        try {
            page.tableInAscendingOrder()
            before = "ascending"
        } catch(geb.error.RequiredPageContentNotPresent e) {
            before = "descending"
        }

        when:
        if(page.storedProceduresDisplayed()) {
            when:
            waitFor(30) { page.clickStoredProcedure() }
            then: 'check if table is in ascending'
            try {
                page.tableInAscendingOrder()
                before = "ascending"
            } catch (geb.error.RequiredPageContentNotPresent e) {
                before = "descending"
            }

            if (!before.equals(after))
                assert true
            else
                assert false
        }
        then:
            println("passed")
    }

    def checkIfInvocationsIsClickable() {
        String before = ""
        String after  = ""
        when:
        if(page.storedProceduresDisplayed()) {
            when: 'click row count'
            page.clickInvocations()
            then: 'check if row count is in ascending'
            if (page.tableInAscendingOrder())
                before = "ascending"
            else
                before = "descending"

            when: 'click row count'
            page.clickInvocations()
            then: 'check if row count is in descending'
            if (page.tableInDescendingOrder())
                after = "descending"
            else
                after = "ascending"

            if (before.equals("ascending") && after.equals("descending"))
                assert true
            else
                assert false
        }
        then:
        println("passed")
    }

    def checkDataInStoredProcedures() {
        expect: 'Display Preference button exists'
        page.displayPreferenceDisplayed()

        when: 'click Display Preference button'
        page.openDisplayPreference()
        then: 'display title and save button of preferences'
        page.preferencesTitleDisplayed()
        page.savePreferencesBtnDisplayed()
        page.popupCloseDisplayed()

        when: 'Stored Procedure checkbox is displayed'
        page.storedProceduresCheckboxDisplayed()
        then: 'Add Stored Procedure'
        page.storedProceduresCheckboxClick()
        page.savePreferencesBtn.click()

        when:
        if(page.storedProceduresDisplayed()) {
            when:
            page.storedProceduresTableDisplayed()
            report 'checkafterdisplay'
            then:
            if (page.storedProceduresMsg.text().equals("No data to be displayed")) {
                println("No data displayed-PASS")
                println()
                assert true
            } else if (!page.storedProceduresMsg.text().equals("")) {
                println("Data displayed-PASS")
                println(page.storedProceduresMsg.text())
                println()
                assert true
            } else {
                println("FAIL")
                println()
                assert false
            }
        }
        then:
        println("passed")
    }

    def checkDataInDatabaseTables() {
        when:
        if(page.dataTablesDisplayed()) {

            when:
            println("dataTable displayed")
            then:
            if (page.databaseTableMsg.text().equals("No data to be displayed")) {
                println("No data displayed-PASS")
                println()
                assert true
            } else if (!page.databaseTableMsg.text().equals("")) {
                println("Data displayed-PASS")
                println(page.databaseTableMsg.text())
                println()
                assert true
            } else {
                println("FAIL")
                println()
                assert false
            }
        }
        then:
        println("passed")
    }

    // ALERT

    def setAlertAndReplaceTriggerAlert() {
        int count = 0

        when: 'set alert threshold to zero'
        page.setAlertThreshold(00)
        then: 'check at least one alert'
        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) { page.alertCount.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }

        int alert = page.getAlert()

        if ( alert != 0 ) {
            println("PASS:There is at least one server on alert")
        }
        else {
            println("FAIL:There are no server on alert")
            assert false
        }

        when: 'set alert threshold to hundred'
        page.setAlertThreshold(100)
        then: 'check no alert'
        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) { !page.alertCount.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
    }

    // server search
    def checkServerSearchOnDbmonitorMatched() {
        when:'clicked server button'
        at DbMonitorPage
        String serverNamevalid = page.getValidPath()  // taking local server valid name from serversearch.txt file ("/src/resources/serversearch.txt")
        page.clusterserverbutton.click()
        waitFor(5) {
            page.serversearch.value(serverNamevalid)
        }
        then:
        at DbMonitorPage
        waitFor(5){page.clusterserverbutton.isDisplayed()}
        page.clusterserverbutton.click()
        println("server searched matched")
    }

    def checkServerSearchOnDbmonitorNotBatched() {
        when:'clicked server button'
        at DbMonitorPage
        String serverNameinvalid = page.getInvalidPath() // taking local server invalid name from serversearch.txt file ("/src/resources/serversearch.txt")
        page.clusterserverbutton.click()
        waitFor(5){page.serversearch.value(serverNameinvalid)}
        then:
        at DbMonitorPage
        waitFor(5){page.clusterserverbutton.isDisplayed()}
        page.clusterserverbutton.click()
        println("server searched unmatched")
    }

    def checkServerTitleOnDbmonitor() {
        when:
        at DbMonitorPage
        waitFor(5){page.clusterserverbutton.isDisplayed()}
        page.clusterserverbutton.click()
        then:
        at DbMonitorPage
        page.checkserverTitle.text().toLowerCase().equals("Servers".toLowerCase())
        page.clusterserverbutton.click()
        println("server title matched");
    }

    //server cpu
    def checkMinAndMaxValueInServerCpuDays() {
        when:
        // This loop is used to gain time.
        for (count=0; count<numberOfTrials; count++) {
            count++
            page.chooseGraphView("Minutes")
            page.chooseGraphView("Days")
            if(graphView.text().equals("")) {
                break
            }
        }
        then:
        String stringMax = ""
        String stringMin = ""

        for(count=0; count<numberOfTrials; count++) {
            count++
            try {
                waitFor(waitTime) {
                    page.servercpumax.isDisplayed()
                }
                stringMax = page.servercpumax.text()
                stringMin = page.servercpumin.text()

                println(stringMax)
                println(stringMin)

                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        when:
        String monthMax = page.changeToMonth(stringMax)
        String monthMin = page.changeToMonth(stringMin)

        String dateMax = page.changeToDate(stringMax)
        String dateMin = page.changeToDate(stringMin)

        int intDateMax = Integer.parseInt(dateMax)
        int intDateMin = Integer.parseInt(dateMin)
        then:
        println(intDateMax)
        println(intDateMin)
        if(monthMax.equals(monthMin)) {
            if(intDateMax > intDateMin) {
                println("The minimum value is " + stringMin + " and the time is in Days")
            }
            else {
                printsln("FAIL: Date of Max is less than that of date of Min for same month")
                assert false
            }
        }
        else {
            if (intDateMax < intDateMin) {
                println("Success")
            }
            else {
                println("FAIL: Date of Max is more than that of date of Min for new month")
                assert false
            }
        }
    }

/*    def checkMaxValueInServerCpuDays(){
        when:
        // This loop is used to gain time.
        for(count=0; count<numberOfTrials; count++) {
            count++
            page.chooseGraphView("Days")
            if(graphView.text().equals("")) {
                break
            }
        }
        count = 0
        then:
        String stringMax = ""
        String stringMin = ""

        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.servercpumax.isDisplayed()
                }
                stringMax = page.servercpumax.text()
                stringMin = page.servercpumin.text()

                println(stringMax)
                println(stringMin)

                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String monthMax = page.changeToMonth(stringMax)
        String monthMin = page.changeToMonth(stringMin)

        String dateMax = page.changeToDate(stringMax)
        String dateMin = page.changeToDate(stringMin)

        int intDateMax = Integer.parseInt(dateMax)
        int intDateMin = Integer.parseInt(dateMin)

        if(monthMax.equals(monthMin)) {
            if(intDateMax > intDateMin) {
                println("The maximum value is " + stringMax + " and the time is in Days")
            }
            else {
                println("FAIL: Date of Max is less than that of date of Min for same month")
                assert false
            }
        }
        else {
            if (intDateMax < intDateMin) {
                println("Success")
            }
            else {
                println("FAIL: Date of Max is more than that of date of Min for new month")
                assert false
            }
        }
    }*/

/*    def "check min value in server cpu minutes"(){
        int count = 0

        when:
        // This loop is used to gain time.
        while(count<numberOfTrials) {
            count++
            page.chooseGraphView("Minutes")
            if(graphView.text().equals("")) {
                break
            }
        }
        count = 0
        then:
        String stringMax
        String stringMin

        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.servercpumax.isDisplayed()
                }
                stringMax = page.servercpumax.text()
                stringMin = page.servercpumin.text()
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String result = page.compareTime(stringMax, stringMin)

        if(result.equals("minutes")) {
            println("The minimum value is " + stringMin + " and the time is in " + result )
            assert true
        }
        else {
            println("FAIL: It is not in minutes")
            assert false
        }
    }*/

    def checkMinAndMaxValueInServerCpuMinutes() {
        when:
        // This loop is used to gain time.
        for(count=0; count<numberOfTrials; count++) {
            count++
            page.chooseGraphView("Minutes")
            if(graphView.text().equals("")) {
                break
            }
        }
        then:
        String stringMax
        String stringMin

        for (count=0; count<numberOfTrials; count++) {
            count++
            try {
                waitFor(waitTime) {
                    page.servercpumax.isDisplayed()
                }
                stringMax = page.servercpumax.text()
                stringMin = page.servercpumin.text()
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String result = page.compareTime(stringMax, stringMin)

        if(result.equals("minutes")) {
            println("The maximum and minimum values are " + stringMax + " and " + stringMin + "respectively and the time is in " + result )
            assert true
        }
        else {
            println("FAIL: It is not in minutes")
            assert false
        }
    }

    def checkMinAndMaxValueInServerCpuSeconds() {
        when:
        // This loop is used to gain time.
        for (count=0; count<numberOfTrials; count++) {
            count++
            page.chooseGraphView("Seconds")
            if(graphView.text().equals("")) {
                break
            }
        }
        then:
        String stringMax
        String stringMin

        for (count=0; count<numberOfTrials; count++) {
            count++
            try {
                waitFor(waitTime) {
                    page.servercpumax.isDisplayed()
                }
                stringMax = page.servercpumax.text()
                stringMin = page.servercpumin.text()
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String result = page.compareTime(stringMax, stringMin)

        if(result.equals("seconds")) {
            println("The minimum and maximum values are " + stringMin + " and " + stringMax + " and the time is in " + result )
            assert true
        }
        else {
            println("FAIL: It is not in seconds")
            assert false
        }
    }

   /* def "check max value in server cpu seconds"(){
        when:
        // This loop is used to gain time.
        for (count=0; count<numberOfTrials; count++) {
            count++
            page.chooseGraphView("Seconds")
            if(graphView.text().equals("")) {
                break
            }
        }
        then:
        String stringMax
        String stringMin

        for(count=0; count<numberOfTrials; count++) {
            count++
            try {
                waitFor(waitTime) {
                    page.servercpumax.isDisplayed()
                }
                stringMax = page.servercpumax.text()
                stringMin = page.servercpumin.text()
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String result = page.compareTime(stringMax, stringMin)

        if(result.equals("seconds")) {
            println("The maximum value is " + stringMax + " and the time is in " + result )
            assert true
        }
        else {
            println("FAIL: It is not in seconds")
            assert false
        }
    }*/

    //for server ram
    def checkMaxAndMinValueInServerRamDays(){
        when:
        // This loop is used to gain time.
        for (count=0; count<numberOfTrials; count++) {
            count++
            page.chooseGraphView("Days")
            if(graphView.text().equals("")) {
                break
            }
        }
        then:
        String stringMax = ""
        String stringMin = ""

        for (count=0; count<numberOfTrials; count++) {
            count++
            try {
                waitFor(waitTime) {
                    page.serverrammax.isDisplayed()
                }
                stringMax = page.serverrammax.text()
                stringMin = page.serverrammin.text()

                println(stringMax)
                println(stringMin)

                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String monthMax = page.changeToMonth(stringMax)
        String monthMin = page.changeToMonth(stringMin)

        String dateMax = page.changeToDate(stringMax)
        String dateMin = page.changeToDate(stringMin)

        int intDateMax = Integer.parseInt(dateMax)
        int intDateMin = Integer.parseInt(dateMin)

        if(monthMax.equals(monthMin)) {
            if(intDateMax > intDateMin) {
                println("The minimum and maximum values are " + stringMin + " and " + stringMax + " and the time is in Days")
            }
            else {
                println("FAIL: Date of Max is less than that of date of Min for same month")
                assert false
            }
        }
        else {
            if (intDateMax < intDateMin) {
                println("Success")
            }
            else {
                println("FAIL: Date of Max is more than that of date of Min for new month")
                assert false
            }
        }
    }

    /*def CheckMaxValueInServerRamDays(){
        int count = 0

        when:
        waitFor(waitTime){ page.chooseGraphView("Minutes")}
        // This loop is used to gain time.
        while(count<numberOfTrials) {
            count++
            page.chooseGraphView("Days")

            if(graphView.text().equals("")) {
                break
            }
        }


        count = 0
        then:
        String stringMax = ""
        String stringMin = ""

        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.serverrammax.isDisplayed()
                }
                stringMax = page.serverrammax.text()
                stringMin = page.serverrammin.text()

                println(stringMax)
                println(stringMin)

                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

//        waitFor(waitTime){ page.chooseGraphView("Minutes")}
//
//        waitFor(waitTime){ page.chooseGraphView("Days")}
//
//        stringMax = page.serverrammax.text()
//        stringMin = page.serverrammin.text()

        String monthMax = waitFor(waitTime){page.changeToMonth(stringMax)}
        String monthMin = page.changeToMonth(stringMin)

        String dateMax = page.changeToDate(stringMax)
        String dateMin = page.changeToDate(stringMin)

        int intDateMax = Integer.parseInt(dateMax)
        int intDateMin = Integer.parseInt(dateMin)

        if(monthMax.equals(monthMin)) {
            if(intDateMax > intDateMin) {
                println("The maximum value is " + stringMax + " and the time is in Days")
            }
            else {
                println("FAIL: Date of Max is less than that of date of Min for same month")
                assert false
            }
        }
        else {
            if (intDateMax < intDateMin) {
                println("Success")
            }
            else {
                println("FAIL: Date of Max is more than that of date of Min for new month")
                assert false
            }
        }
    }*/

    def checkMinAndMinValueInServerRamMinutes() {
        int count = 0

        when:
        // This loop is used to gain time.
        while(count<numberOfTrials) {
            count++
            page.chooseGraphView("Minutes")
            if(graphView.text().equals("")) {
                break
            }
        }
        count = 0
        then:
        String stringMax
        String stringMin

        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.serverrammax.isDisplayed()
                }
                stringMax = page.serverrammax.text()
                stringMin = page.serverrammin.text()
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String result = page.compareTime(stringMax, stringMin)

        if(result.equals("minutes")) {
            println("The minimum and maximum values are " + stringMin + " and " + stringMax + " and the time is in " + result )
            assert true
        }
        else {
            println("FAIL: It is not in minutes")
            assert false
        }
    }

   /* def "check max value in server ram minutes"(){
        int count = 0

        when:
        // This loop is used to gain time.
        while(count<numberOfTrials) {
            count++
            page.chooseGraphView("Minutes")
            if(graphView.text().equals("")) {
                break
            }
        }
        count = 0
        then:
        String stringMax
        String stringMin

        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.serverrammax.isDisplayed()
                }
                stringMax = page.serverrammax.text()
                stringMin = page.serverrammin.text()
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String result = page.compareTime(stringMax, stringMin)

        if(result.equals("minutes")) {
            println("The maximum value is " + stringMax + " and the time is in " + result )
            assert true
        }
        else {
            println("FAIL: It is not in minutes")
            assert false
        }
    }*/

    def checkMinValueInServerRamSeconds() {
        int count = 0

        when:
        // This loop is used to gain time.
        while(count<numberOfTrials) {
            count++
            page.chooseGraphView("Seconds")
            if(graphView.text().equals("")) {
                break
            }
        }
        count = 0
        then:
        String stringMax
        String stringMin

        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.serverrammax.isDisplayed()
                }
                stringMax = page.serverrammax.text()
                stringMin = page.serverrammin.text()
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String result = page.compareTime(stringMax, stringMin)

        if(result.equals("seconds")) {
            println("The minimum value is " + stringMin + " and the time is in " + result )
            assert true
        }
        else {
            println("FAIL: It is not in seconds")
            assert false
        }
    }

    def checkMaxValueInServerRamSeconds() {
        int count = 0

        when:
        // This loop is used to gain time.
        while(count<numberOfTrials) {
            count++
            page.chooseGraphView("Seconds")
            if(graphView.text().equals("")) {
                break
            }
        }
        count = 0
        then:
        String stringMax
        String stringMin

        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.serverrammax.isDisplayed()
                }
                stringMax = page.serverrammax.text()
                stringMin = page.serverrammin.text()
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String result = page.compareTime(stringMax, stringMin)

        if(result.equals("seconds")) {
            println("The maximum value is " + stringMax + " and the time is in " + result )
            assert true
        }
        else {
            println("FAIL: It is not in seconds")
            assert false
        }
    }



    //Test Case relating to Graph
    //cluster latency
    def checkMinValueInClusterLatencyDays() {
        int count = 0

//        when: "Check if Cluster Latency Graph is displayed"
//
//        if(!page.clusterLatencyDisplayed())
//        {
//println("Cluster Latency is not Displayed")
//        }
//
//        then:

        when:
        //if(page.clusterLatencyDisplayed()) {
            // This loop is used to gain time.
            while (count < numberOfTrials) {
                count++
                page.chooseGraphView("Days")
                if (graphView.text().equals("")) {
                    break
                }
            }
//        }
//        else
//        {
//            println("Cluster Latency is not Displayed")
//            assert true
//        }
        count = 0
        then:
        String stringMax = ""
        String stringMin = ""

        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.clusterlatencymax.isDisplayed()
                }
                stringMax = page.clusterlatencymax.text()
                stringMin = page.clusterlatencymin.text()

                println(stringMax)
                println(stringMin)

                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String monthMax = page.changeToMonth(stringMax)
        String monthMin = page.changeToMonth(stringMin)

        String dateMax = page.changeToDate(stringMax)
        String dateMin = page.changeToDate(stringMin)

        int intDateMax = Integer.parseInt(dateMax)
        int intDateMin = Integer.parseInt(dateMin)

        if(monthMax.equals(monthMin)) {
            if(intDateMax > intDateMin) {
                println("The minimum value is " + stringMin + " and the time is in Days")
            }
            else {
                println("FAIL: Date of Max is less than that of date of Min for same month")
                assert false
            }
        }
        else {
            if (intDateMax < intDateMin) {
                println("Success")
            }
            else {
                println("FAIL: Date of Max is more than that of date of Min for new month")
                assert false
            }
        }
    }

    def checkMaxValueInClusterLatencyDays(){
        int count = 0

        when:
        waitFor(waitTime){page.chooseGraphView("Minutes")}
        // This loop is used to gain time.
        while(count<numberOfTrials) {
            count++
            page.chooseGraphView("Days")
            if(graphView.text().equals("")) {
                break
            }
        }
        count = 0
        then:
        String stringMax = ""
        String stringMin = ""

        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.clusterlatencymax.isDisplayed()
                }
                stringMax = page.clusterlatencymax.text()
                stringMin = page.clusterlatencymin.text()

                println(stringMax)
                println(stringMin)

                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String monthMax = page.changeToMonth(stringMax)
        String monthMin = page.changeToMonth(stringMin)

        String dateMax = page.changeToDate(stringMax)
        String dateMin = page.changeToDate(stringMin)

        int intDateMax = Integer.parseInt(dateMax)
        int intDateMin = Integer.parseInt(dateMin)

        if(monthMax.equals(monthMin)) {
            if(intDateMax > intDateMin) {
                println("The maximum value is " + stringMax + " and the time is in Days")
            }
            else {
                println("FAIL: Date of Max is less than that of date of Min for same month")
                assert false
            }
        }
        else {
            if (intDateMax < intDateMin) {
                println("Success")
            }
            else {
                println("FAIL: Date of Max is more than that of date of Min for new month")
                assert false
            }
        }
    }

    def checkMinValueInClusterLatencyMinutes() {
        int count = 0
        when:
        // This loop is used to gain time.
        while(count<numberOfTrials) {
            count++
            page.chooseGraphView("Minutes")
            if(graphView.text().equals("")) {
                break
            }
        }
        count = 0
        then:
        String stringMax
        String stringMin

        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.clusterlatencymax.isDisplayed()
                }
                stringMax = page.clusterlatencymax.text()
                stringMin = page.clusterlatencymin.text()
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String result = page.compareTime(stringMax, stringMin)

        if(result.equals("minutes")) {
            println("The minimum value is " + stringMin + " and the time is in " + result )
            assert true
        }
        else {
            println("FAIL: It is not in minutes")
            assert false
        }
    }

    def checkMaxValueInClusterLatencyMinutes() {
        int count = 0
        when:
        // This loop is used to gain time.
        while(count<numberOfTrials) {
            count++
            page.chooseGraphView("Minutes")
            if(graphView.text().equals("")) {
                break
            }
        }
        count = 0
        then:
        String stringMax
        String stringMin

        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.clusterlatencymax.isDisplayed()
                }
                stringMax = page.clusterlatencymax.text()
                stringMin = page.clusterlatencymin.text()
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String result = page.compareTime(stringMax, stringMin)

        if(result.equals("minutes")) {
            println("The maximum value is " + stringMax + " and the time is in " + result )
            assert true
        }
        else {
            println("FAIL: It is not in minutes")
            assert false
        }
    }

    def checkMinValueInClusterLatencySeconds() {
        int count = 0

        when:
        // This loop is used to gain time.
        while(count<numberOfTrials) {
            count++
            page.chooseGraphView("Seconds")
            if(graphView.text().equals("")) {
                break
            }
        }
        count = 0
        then:
        String stringMax
        String stringMin

        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.clusterlatencymax.isDisplayed()
                }
                stringMax = page.clusterlatencymax.text()
                stringMin = page.clusterlatencymin.text()
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String result = page.compareTime(stringMax, stringMin)

        if(result.equals("seconds")) {
            println("The minimum value is " + stringMin + " and the time is in " + result )
            assert true
        }
        else {
            println("FAIL: It is not in seconds")
            assert false
        }
    }

    def checkMaxValueInClusterLatencySeconds() {
        int count = 0

        when:
        // This loop is used to gain time.
        while(count<numberOfTrials) {
            count++
            page.chooseGraphView("Seconds")
            if(graphView.text().equals("")) {
                break
            }
        }
        count = 0
        then:
        String stringMax
        String stringMin

        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.clusterlatencymax.isDisplayed()
                }
                stringMax = page.clusterlatencymax.text()
                stringMin = page.clusterlatencymin.text()
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String result = page.compareTime(stringMax, stringMin)

        if(result.equals("seconds")) {
            println("The maximum value is " + stringMax + " and the time is in " + result )
            assert true
        }
        else {
            println("FAIL: It is not in seconds")
            assert false
        }
    }

    //cluster transaction
    def checkMinValueInClusterTransactionDays() {
        int count = 0

        when:
        // This loop is used to gain time.
        while(count<numberOfTrials) {
            count++
            page.chooseGraphView("Seconds")
            page.chooseGraphView("Seconds")
            page.chooseGraphView("Days")
            if(graphView.text().equals("")) {
                break
            }
        }
        count = 0
        then:
        String stringMax = ""
        String stringMin = ""

        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.clustertransactionmax.isDisplayed()
                }
                stringMax = page.clustertransactionmax.text()
                stringMin = page.clustertransactionmin.text()

                println(stringMax)
                println(stringMin)

                if(stringMax.length()<10 || stringMax.length()<10) {
                    println("Not fixed")
                    continue
                }

                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String monthMax = page.changeToMonth(stringMax)
        String monthMin = page.changeToMonth(stringMin)

        String dateMax = page.changeToDate(stringMax)
        String dateMin = page.changeToDate(stringMin)

        int intDateMax = Integer.parseInt(dateMax)
        int intDateMin = Integer.parseInt(dateMin)

        if(monthMax.equals(monthMin)) {
            if(intDateMax > intDateMin) {
                println("The minimum value is " + stringMin + " and the time is in Days")
            }
            else {
                println("FAIL: Date of Max is less than that of date of Min for same month")
                assert false
            }
        }
        else {
            if (intDateMax < intDateMin) {
                println("Success")
            }
            else {
                println("FAIL: Date of Max is more than that of date of Min for new month")
                assert false
            }
        }
    }

    def checkMaxValueInClusterTransactionDays() {
        int count = 0
        int smallCount = 0
        when:
        // This loop is used to gain time.

        count = 0
        then:
        String stringMax = ""
        String stringMin = ""

        while(count<numberOfTrials) {
            count++
            while(smallCount<numberOfTrials) {
                smallCount++
                page.chooseGraphView("Seconds")
                page.chooseGraphView("Seconds")
                page.chooseGraphView("Days")
                if(graphView.text().equals("")) {
                    break
                }
            }

            try {
                waitFor(waitTime) {
                    page.clustertransactionmax.isDisplayed()
                }
                stringMax = page.clustertransactionmax.text()
                stringMin = page.clustertransactionmin.text()

                println(stringMax)
                println(stringMin)

                if(stringMax.length()<10 || stringMax.length()<10) {
                    println("Not fixed")
                    continue
                }

                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String monthMax = page.changeToMonth(stringMax)
        String monthMin = page.changeToMonth(stringMin)

        String dateMax = page.changeToDate(stringMax)
        String dateMin = page.changeToDate(stringMin)

        int intDateMax = Integer.parseInt(dateMax)
        int intDateMin = Integer.parseInt(dateMin)

        if(monthMax.equals(monthMin)) {
            if(intDateMax > intDateMin) {
                println("The maximum value is " + stringMax + " and the time is in Days")
            }
            else {
                println("FAIL: Date of Max is less than that of date of Min for same month")
                assert false
            }
        }
        else {
            if (intDateMax < intDateMin) {
                println("Success")
            }
            else {
                println("FAIL: Date of Max is more than that of date of Min for new month")
                assert false
            }
        }
    }

    def checkMinValueInClusterTransactionMinutes() {
        int count = 0

        when:
        // This loop is used to gain time.
        String stringMax
        String stringMin
        int bigCount = 0

        then:
        while(bigCount<numberOfTrials) {
            bigCount++
            while(count<numberOfTrials) {
                count++
                page.chooseGraphView("Seconds")
                page.chooseGraphView("Seconds")
                page.chooseGraphView("Minutes")
                if(graphView.text().equals("")) {
                    break
                }
            }
            count = 0
            while(count<numberOfTrials) {
                count++
                try {
                    waitFor(waitTime) {
                        page.clustertransactionmax.isDisplayed()
                    }
                    stringMax = page.clustertransactionmax.text()
                    stringMin = page.clustertransactionmin.text()
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("WaitTimeoutException")
                }
            }

            String result = page.compareTime(stringMax, stringMin)
            println(result + " " + stringMax + " " + stringMin)
            try {
                waitFor(waitTime) {
                    result.equals("minutes")
                }
                println("The minimum value is " + stringMin + " and the time is in " + result )
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("AJA BHAKOXAINA")
            }

        }
    }

    def checkMaxValueInClusterTransactionMinutes() {
        int count = 0

        when:
        // This loop is used to gain time.
        String stringMax
        String stringMin
        int bigCount = 0

        then:
        while(bigCount<numberOfTrials) {
            while(count<numberOfTrials) {
                count++
                page.chooseGraphView("Seconds")
                page.chooseGraphView("Seconds")
                page.chooseGraphView("Minutes")
                if(graphView.text().equals("")) {
                    break
                }
            }
            count = 0
            bigCount++
            while(count<numberOfTrials) {
                count++
                try {
                    waitFor(waitTime) {
                        page.clustertransactionmax.isDisplayed()
                    }
                    stringMax = page.clustertransactionmax.text()
                    stringMin = page.clustertransactionmin.text()
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("WaitTimeoutException")
                }
            }

            String result = page.compareTime(stringMax, stringMin)
            println(result + " " + stringMax + " " + stringMin)

            try {
                waitFor(waitTime) {
                    result.equals("minutes")
                }
                println("The maximum value is " + stringMax + " and the time is in " + result )
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("AJA BHAKOXAINA")
            }

        }
    }

    def checkMinValueInClusterTransactionSeconds() {
        int count = 0

        when:
        // This loop is used to gain time.
        while(count<numberOfTrials) {
            count++
            page.chooseGraphView("Minutes")
            page.chooseGraphView("Minutes")
            page.chooseGraphView("Seconds")
            if(graphView.text().equals("")) {
                break
            }
        }
        count = 0
        then:
        String stringMax
        String stringMin

        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.clustertransactionmax.isDisplayed()
                }
                stringMax = page.clustertransactionmax.text()
                stringMin = page.clustertransactionmin.text()
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String result = page.compareTime(stringMax, stringMin)

        if(result.equals("seconds")) {
            println("The minimum value is " + stringMin + " and the time is in " + result )
            assert true
        }
        else {
            println("FAIL: It is not in seconds")
            assert false
        }
    }

    def checkMaxValueInClusterTransactionSeconds() {
        int count = 0

        when:
        // This loop is used to gain time.
        while(count<numberOfTrials) {
            count++
            page.chooseGraphView("Minutes")
            page.chooseGraphView("Minutes")
            page.chooseGraphView("Seconds")
            if(graphView.text().equals("")) {
                break
            }
        }
        count = 0
        then:
        String stringMax
        String stringMin

        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.clustertransactionmax.isDisplayed()
                }
                stringMax = page.clustertransactionmax.text()
                stringMin = page.clustertransactionmin.text()
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String result = page.compareTime(stringMax, stringMin)

        if(result.equals("seconds")) {
            println("The maximum value is " + stringMax + " and the time is in " + result )
            assert true
        }
        else {
            println("FAIL: It is not in seconds")
            assert false
        }
    }

    // for partition idle graphs
    def checkMinValueInPartitionIdleGraphWithRespectToSeconds() {
        when: "Open Partition Graph if not open"
        if (!partitiongraphmin.isDisplayed())
            page.openPartitionIdleGraph()

        count = 0
        then:
        String stringMax
        String stringMin

        for(count=0; count<numberOfTrials; count++) {
            count++
            try {
                waitFor(waitTime) {
                    page.partitiongraphmax.isDisplayed()
                }
                stringMax = page.partitiongraphmax.text()
                stringMin = page.partitiongraphmin.text()
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String result = page.compareTime(stringMax, stringMin)

        if(result.equals("seconds")) {
            println("The minimum value is " + stringMin + " and the time is in " + result )
            assert true
        }
        else {
            println("FAIL: It is not in seconds")
            assert false
        }
    }

    def checkMaxValueInPartitionIdleGraphWithRespectToSeconds() {
        when: "Open Partition Graph if not open"
        if (!partitiongraphmin.isDisplayed())
            page.openPartitionIdleGraph()
        count = 0
        then:
        String stringMax
        String stringMin
        report "before"
        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.partitiongraphmax.isDisplayed()
                }
                stringMax = page.partitiongraphmax.text()
                stringMin = page.partitiongraphmin.text()
                println(stringMax)
                println(stringMin)
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String result = page.compareTime(stringMax, stringMin)

        if(result.equals("seconds")) {
            println("The maximum value is " + stringMax + " and the time is in " + result )
            assert true
        }
        else {
            println("FAIL: It is not in seconds")
            assert false
        }

        page.closePartitionIdleGraph()
    }

    def checkMinAndMaxValueInClusterPartitionIdleGraphWithRespectToMinutes(){
        when: "Open Partition Graph if not open"
        if (!partitiongraphmin.isDisplayed())
            page.openPartitionIdleGraph()
        and:
        // This loop is used to gain time.
        waitFor(10){page.chooseGraphView("Days")}
        while(count<numberOfTrials) {
            count++
            page.chooseGraphView("Minutes")
            if(graphView.text().equals("")) {
                break
            }
        }
        count = 0
        then:
        String stringMax
        String stringMin

        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.partitiongraphmax.isDisplayed()
                }
                stringMax = page.partitiongraphmax.text()
                stringMin = page.partitiongraphmin.text()
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String result = page.compareTime(stringMax, stringMin)

        if(result.equals("minutes")) {
            println("The minimum value is " + stringMin + " and the time is in " + result )
            assert true
        }
        else {
            println("FAIL: It is not in minutes")
            assert false
        }
        page.closePartitionIdleGraph()
    }

    /*def checkMaxValueInClusterPartitionIdleGraphWithRespectToMinutes() {
        int count = 0

        when:
        // This loop is used to gain time.
        while(count<numberOfTrials) {
            count++
            page.chooseGraphView("Minutes")
            if(graphView.text().equals("")) {
                break
            }
        }
        count = 0
        then:
        String stringMax
        String stringMin

        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.partitiongraphmax.isDisplayed()
                }
                stringMax = page.partitiongraphmax.text()
                stringMin = page.partitiongraphmin.text()
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String result = page.compareTime(stringMax, stringMin)

        if(result.equals("minutes")) {
            println("The maximum value is " + stringMax + " and the time is in " + result )
            assert true
        }
        else {
            println("FAIL: It is not in minutes")
            assert false
        }
    }*/

    def checkMinAndMaxValueInClusterPartitionIdleGraphWithRespectToDays(){
        when: "Open Partition Graph if not open"
        if (!partitiongraphmin.isDisplayed())
            page.openPartitionIdleGraph()
        waitFor(waitTime){ page.chooseGraphView("Minutes")}
        while(count<numberOfTrials) {
            count++
            page.chooseGraphView("Days")
            if(graphView.text().equals("")) {
                break
            }
        }
        count = 0
        then:
        String stringMax = ""
        String stringMin = ""

        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.partitiongraphmax.isDisplayed()
                }
                stringMax = page.partitiongraphmax.text()
                stringMin = page.partitiongraphmin.text()

                println(stringMax)
                println(stringMin)

                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String monthMax = page.changeToMonth(stringMax)
        String monthMin = page.changeToMonth(stringMin)

        String dateMax = page.changeToDate(stringMax)
        String dateMin = page.changeToDate(stringMin)

        int intDateMax = Integer.parseInt(dateMax)
        int intDateMin = Integer.parseInt(dateMin)

        if(monthMax.equals(monthMin)) {
            if(intDateMax > intDateMin) {
                println("The minimum value is " + stringMin + " and the time is in Days")
            }
            else {
                println("FAIL: Date of Max is less than that of date of Min for same month")
                assert false
            }
        }
        else {
            if (intDateMax < intDateMin) {
                println("Success")
            }
            else {
                println("FAIL: Date of Max is more than that of date of Min for new month")
                assert false
            }
        }

        page.closePartitionIdleGraph()
    }

    /*def checkMaxValueInClusterPartitionIdlegraphWithRespecttoDays(){
        int count = 0

        when:
        waitFor(waitTime){page.chooseGraphView("Minutes")}
        // This loop is used to gain time.
        while(count<numberOfTrials) {
            count++
            page.chooseGraphView("Days")
            if(graphView.text().equals("")) {
                break
            }
        }
        count = 0
        then:
        String stringMax = ""
        String stringMin = ""

        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.partitiongraphmax.isDisplayed()
                }
                stringMax = page.partitiongraphmax.text()
                stringMin = page.partitiongraphmin.text()

                println(stringMax)
                println(stringMin)

                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("WaitTimeoutException")
            }
        }

        String monthMax = page.changeToMonth(stringMax)
        String monthMin = page.changeToMonth(stringMin)

        String dateMax = page.changeToDate(stringMax)
        String dateMin = page.changeToDate(stringMin)

        int intDateMax = Integer.parseInt(dateMax)
        int intDateMin = Integer.parseInt(dateMin)

        println(intDateMax)
        println(intDateMin)

        if(monthMax.equals(monthMin)) {
            if(intDateMax > intDateMin) {
                println("The maximum value is " + stringMax + " and the time is in Days")
            }
            else {
                println("FAIL: Date of Max is less than that of date of Min for same month")
                assert false
            }
        }
        else {
            if (intDateMax < intDateMin) {
                println("Success")
            }
            else {
                println("FAIL: Date of Max is more than that of date of Min for new month")
                assert false
            }
        }
    }*/

    def clickDisplayPreferencesAddAndRemovePartitionIdleTime() {
        expect: 'Display Preference button exists'
        page.displayPreferenceDisplayed()

        when: 'click Display Preference button'
        page.openDisplayPreference()
        then: 'display title and save button of preferences'
        page.preferencesTitleDisplayed()
        page.savePreferencesBtnDisplayed()
        page.popupCloseDisplayed()

        when: 'Partition Idle Time checkbox is displayed'
        page.partitionIdleTimeCheckboxDisplayed()
        then: 'Remove Partition Idle Time'
        page.partitionIdleTimeCheckboxClick()

        when: 'click close button'
        page.savePreferences()
        then: 'Partition Idle Time displayed'
        page.serverCpuDisplayed()
        page.serverRamDisplayed()
        page.clusterLatencyDisplayed()
        page.clusterTransactionsDisplayed()
        page.partitionIdleTimeDisplayed()

        when: 'click Display Preference button'
        page.openDisplayPreference()
        then: 'display title and save button of preferences'
        page.preferencesTitleDisplayed()
        page.savePreferencesBtnDisplayed()
        page.popupCloseDisplayed()

        when: 'Partition Idle Time is displayed'
        page.partitionIdleTimeCheckboxDisplayed()
        then: 'Add Partition Idle Time'
        page.partitionIdleTimeCheckboxClick()

        when: 'click close button'
        page.savePreferences()
        then: 'No Partition Idle Time displayed'
        page.serverCpuDisplayed()
        page.serverRamDisplayed()
        page.clusterLatencyDisplayed()
        page.clusterTransactionsDisplayed()
        !page.partitionIdleTimeDisplayed()
    }

    def checkServerLegendsVisibleInGraphPartitionIdleTime() {
        when: "Open Partition Graph if not open"
        if (!partitiongraphmin.isDisplayed())
            page.openPartitionIdleGraph()
        and: 'server partition legends is visible'
        waitFor(10){    page.localpartition.isDisplayed()
            page.clusterwide.isDisplayed()
            page.multipartition.isDisplayed()
        }
        then: 'check those server partition legends and print them'
        if(page.localpartition.text()=="Local partitions"){
            println("grey partition displayed as: " +page.localpartition.text())}
        if(page.clusterwide.text()=="Cluster-wide Maximum / Minimum"){
            println("Blue partition displayed as: " +page.clusterwide.text())}
        if(page.multipartition.text()=="Multi-partition"){
            println("Orange partition displayed as: " +page.multipartition.text())}
        else {println("No server legends are visible")}

        page.closePartitionIdleGraph()
    }


    def checkIfMinLatencyIsClickable() {
        String before = ""
        String after  = ""

        expect: 'Display Preference button exists'
        page.displayPreferenceDisplayed()

        when: 'click Display Preference button'
        page.openDisplayPreference()
        then: 'display title and save button of preferences'
        page.preferencesTitleDisplayed()
        page.savePreferencesBtnDisplayed()
        page.popupCloseDisplayed()

        when: 'Stored Procedures checkbox is displayed'
        page.storedProceduresCheckboxDisplayed()
        then: 'Remove Stored Procedures'
        page.storedProceduresCheckboxClick()

        report "before_save"
        when: 'click close button'
        page.savePreferences()
        report "after_save"

        if(page.storedProceduresDisplayed()) {
            when:
            println("Stored Procedure table is displayed")
            page.clickMinLatency()

            then: 'check if max rows is in ascending'
            if (page.tableInAscendingOrder())
                before = "ascending"
            else
                before = "descending"
            when: 'click min latency'
            page.clickMinLatency()

            then: 'check if max rows is in descending'
            if (page.tableInDescendingOrder())
                after = "descending"
            else
                after = "ascending"

            if (before.equals("ascending") && after.equals("descending"))
                assert true
            else
                assert false
        }
        then:
        println("passed")
        }

    def checkIfMaxLatencyIsClickable() {
        String before = ""
        String after  = ""

        expect: 'Display Preference button exists'
        page.displayPreferenceDisplayed()

        when: 'click Display Preference button'
        page.openDisplayPreference()
        then: 'display title and save button of preferences'
        page.preferencesTitleDisplayed()
        page.savePreferencesBtnDisplayed()
        page.popupCloseDisplayed()

        when: 'Stored Procedures checkbox is displayed'
        page.storedProceduresCheckboxDisplayed()
        then: 'Remove Stored Procedures'
        page.storedProceduresCheckboxClick()

        when: 'click close button'
        page.savePreferences()

        then:
        println("saved")
        when:
        if(page.storedProceduresDisplayed()) {
            when:
            waitFor(10) { page.clickMaxLatency() }
            then: 'check if min rows is in ascending'
            if (page.tableInAscendingOrder())
                before = "ascending"
            else
                before = "descending"

            when: 'click max latency'
            page.clickMaxLatency()
            then: 'check if min rows is in descending'
            if (page.tableInDescendingOrder())
                after = "descending"
            else
                after = "ascending"

            if (before.equals("ascending") && after.equals("descending"))
                assert true
            else
                assert false
        }
        then:
        println("passed")
    }

    def checkIfAvgLatencyIsClickable() {
        String before = ""
        String after  = ""

        expect: 'Display Preference button exists'
        page.displayPreferenceDisplayed()

        while (!page.storedProcedure.isDisplayed()) {
            when: 'click Display Preference button'
            page.openDisplayPreference()
            then: 'display title and save button of preferences'
            page.preferencesTitleDisplayed()
            page.savePreferencesBtnDisplayed()
            page.popupCloseDisplayed()

            when: 'Stored Procedures checkbox is displayed'
            page.storedProceduresCheckboxDisplayed()
            then: 'Remove Stored Procedures'
            page.storedProceduresCheckboxClick()

            report "before_save"
            when: 'click close button'
            page.savePreferences()
            then: 'check if avg latency is displayed'
            waitFor(waitTime) { page.clickAvgLatency() }
            break
        }

        when:
        if(page.storedProceduresDisplayed()) {
            when: 'click avg latency for first time'
            report "after_save"
            page.clickAvgLatency()
            report "first_click"
            then: 'check if avg rows is in ascending'
            if (page.tableInAscendingOrder())
                before = "ascending"
            else
                before = "descending"

            when: 'click avg latency'
            page.clickAvgLatency()
            report "after_click"
            then: 'check if avg rows is in descending'
            if (page.tableInDescendingOrder())
                after = "descending"
            else
                after = "ascending"

            println(before + " " + after)
            if (before.equals("ascending") && after.equals("descending"))
                assert true
            else
                assert false
        }
        then:
        println("passed")
    }

    def checkIfTimeOfExecutionIsClickable() {
        String before = ""
        String after  = ""

        expect: 'Display Preference button exists'
        page.displayPreferenceDisplayed()

        when: 'click Display Preference button'
        page.openDisplayPreference()
        then: 'display title and save button of preferences'
        page.preferencesTitleDisplayed()
        page.savePreferencesBtnDisplayed()
        page.popupCloseDisplayed()

        when: 'Stored Procedures checkbox is displayed'
        page.storedProceduresCheckboxDisplayed()
        then: 'Remove Stored Procedures'
        page.storedProceduresCheckboxClick()

        when: 'click close button'
        page.savePreferences()

        if(page.storedProceduresDisplayed()) {
            when:
            page.clickTimeOfExecution()
            then: 'check if type is in ascending'
            if (page.tableInAscendingOrder())
                before = "ascending"
            else
                before = "descending"

            when: 'click time of execution'
            page.clickTimeOfExecution()
            then: 'check if type is in descending'
            if (page.tableInDescendingOrder())
                after = "descending"
            else
                after = "ascending"

            if (before.equals("ascending") && after.equals("descending"))
                assert true
            else
                assert false
        }
        then:
        println("passed")
    }


    def clickGraphViewSeconds() {
        expect: 'Graph view button exists'
        page.graphViewDisplayed()

        when: 'choose Seconds in Graph View'
        page.chooseGraphView("Seconds")
        then: 'display'

        DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
        Date date = new Date();
        String stringTwo = dateFormat.format(date)

        String stringOne = timeOne.text()
        int hourOne = page.changeToHour(stringOne)
        int minuteOne = page.changeToMinute(stringOne)

        int hourTwo = page.changeToHour(stringTwo)
        int minuteTwo = page.changeToMinute(stringTwo)

        int diff = minuteTwo - minuteOne

        if ( hourOne == hourTwo && diff < 20 ) {
            assert true
        }
        else if ( hourOne < hourTwo && minuteTwo < 20 ){
            assert true
        }
        else {
            assert false
        }
    }

    def clickGraphViewMinute() {
        int count = 0

        expect: 'Graph view button exists'
        page.graphViewDisplayed()

        when: 'choose Minutes in Graph View'
        // This loop is used to gain time.
        while(count<numberOfTrials) {
            count++
            page.chooseGraphView("Minutes")
            if(graphView.text().equals("")) {
                break
            }
        }
        then: 'display'
        DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
        Date date = new Date();
        String stringTwo = dateFormat.format(date)

        String stringOne = timeOne.text()
        int hourOne = page.changeToHour(stringOne)
        int hourTwo = page.changeToHour(stringTwo)
        int minuteTwo = page.changeToHour(stringTwo)
        int hourDiff = hourTwo - hourOne

        if ( hourDiff == 1 ) {
            assert true
        }
        else if ( hourDiff > 1 && minuteTwo < 30 ){
            assert true
        }
        else {
            assert false
        }
    }

    def clickGraphViewDays() {
        int count = 0

        expect: 'Graph view button exists'
        page.graphViewDisplayed()

        when: 'choose Days in Graph View'
        // This loop is used to gain time.
        while(count<numberOfTrials) {
            count++
            page.chooseGraphView("Days")
            if(graphView.text().equals("")) {
                break
            }
        }
        then: 'display'
        String stringOne = timeOne.text()
        if ( stringOne.length() > 8 ) {
            assert true
        }
        else {
            assert false
        }
    }

    def clickDisplayPreferencesAndClose() {
        expect: 'Display Preference button exists'
        page.displayPreferenceDisplayed()

        when: 'click Display Preference button'
        page.openDisplayPreference()
        then: 'display title and save button of preferences'
        page.preferencesTitleDisplayed()
        page.savePreferencesBtnDisplayed()
        page.popupCloseDisplayed()

        when: 'click close button'
        page.closePreferences()
        then: 'all graph exist'
        page.serverCpuDisplayed()
        page.serverRamDisplayed()
        page.clusterLatencyDisplayed()
        page.clusterTransactionsDisplayed()
        // page.partitionIdleTimeDisplayed()
    }

    def clickDisplayPreferencesRemoveServerCpuAndAddServerCpu() {
        expect: 'Display Preference button exists'
        page.displayPreferenceDisplayed()

        when: 'click Display Preference button'
        page.openDisplayPreference()
        then: 'display title and save button of preferences'
        page.preferencesTitleDisplayed()
        page.savePreferencesBtnDisplayed()
        page.popupCloseDisplayed()

        when: 'Server CPU checkbox is displayed'
        page.serverCpuCheckboxDisplayed()
        then: 'Remove Server CPU'
        page.serverCpuCheckboxClick()

        when: 'click close button'
        page.savePreferences()
        then: 'no Server CPU displayed'
        !page.serverCpuDisplayed()
        page.serverRamDisplayed()
        page.clusterLatencyDisplayed()
        page.clusterTransactionsDisplayed()

        when: 'click Display Preference button'
        page.openDisplayPreference()
        then: 'display title and save button of preferences'
        page.preferencesTitleDisplayed()
        page.savePreferencesBtnDisplayed()
        page.popupCloseDisplayed()

        when: 'Server CPU checkbox is displayed'
        page.serverCpuCheckboxDisplayed()
        then: 'Add Server CPU'
        page.serverCpuCheckboxClick()

        when: 'click close button'
        page.savePreferences()
        then: 'Server CPU displayed along with others'
        page.serverCpuDisplayed()
        page.serverRamDisplayed()
        page.clusterLatencyDisplayed()
        page.clusterTransactionsDisplayed()
    }

    def clickDisplayPreferencesRemoveServerRamAndAddServerRam() {
        expect: 'Display Preference button exists'
        page.displayPreferenceDisplayed()

        when: 'click Display Preference button'
        page.openDisplayPreference()
        then: 'display title and save button of preferences'
        page.preferencesTitleDisplayed()
        page.savePreferencesBtnDisplayed()
        page.popupCloseDisplayed()

        when: 'Server RAM checkbox is displayed'
        page.serverRamCheckboxDisplayed()
        then: 'Remove Server RAM'
        page.serverRamCheckboxClick()

        when: 'click close button'
        waitFor(waitTime){page.savePreferences()}
        then: 'no Server RAM displayed'
        page.serverCpuDisplayed()
        !page.serverRamDisplayed()
        page.clusterLatencyDisplayed()
        page.clusterTransactionsDisplayed()
//        page.partitionIdleTimeDisplayed()
        when: 'click Display Preference button'
        waitFor(waitTime){page.openDisplayPreference()}
        then: 'display title and save button of preferences'
        page.preferencesTitleDisplayed()
        page.savePreferencesBtnDisplayed()
        page.popupCloseDisplayed()

        when: 'Server RAM checkbox is displayed'
        page.serverRamCheckboxDisplayed()
        then: 'Add Server RAM'
        page.serverRamCheckboxClick()

        when: 'click close button'
        page.savePreferences()
        then: 'Server RAM displayed along with others'
        page.serverCpuDisplayed()
        page.serverRamDisplayed()
        page.clusterLatencyDisplayed()
        page.clusterTransactionsDisplayed()
        //page.partitionIdleTimeDisplayed()
    }

    def clickDisplayPreferencesRemoveClusterLatencyAndAddClusterLatency() {
        expect: 'Display Preference button exists'
        page.displayPreferenceDisplayed()

        when: 'click Display Preference button'
        page.openDisplayPreference()
        then: 'display title and save button of preferences'
        page.preferencesTitleDisplayed()
        page.savePreferencesBtnDisplayed()
        page.popupCloseDisplayed()

        when: 'Server Cluster Latency checkbox is displayed'
        page.clusterLatencyCheckboxDisplayed()
        then: 'Remove Cluster Latency'
        page.clusterLatencyCheckboxClick()

        when: 'click close button'
        page.savePreferences()
        then: 'no Cluster Latency displayed'
        page.serverCpuDisplayed()
        page.serverRamDisplayed()
        !page.clusterLatencyDisplayed()
        page.clusterTransactionsDisplayed()
//        page.partitionIdleTimeDisplayed()

        when: 'click Display Preference button'
        page.openDisplayPreference()
        then: 'display title and save button of preferences'
        page.preferencesTitleDisplayed()
        page.savePreferencesBtnDisplayed()
        page.popupCloseDisplayed()

        when: 'Server Cluster Latency is displayed'
        page.clusterLatencyCheckboxDisplayed()
        then: 'Add Cluster Latency'
        page.clusterLatencyCheckboxClick()

        when: 'click close button'
        page.savePreferences()
        then: 'Cluster Latency displayed along with others'
        page.serverCpuDisplayed()
        page.serverRamDisplayed()
        page.clusterLatencyDisplayed()
        page.clusterTransactionsDisplayed()
        // page.partitionIdleTimeDisplayed()
    }

    def clickDisplayPreferencesRemoveClusterTransactionsAndAddClusterTransactions() {
        expect: 'Display Preference button exists'
        page.displayPreferenceDisplayed()

        when: 'click Display Preference button'
        page.openDisplayPreference()
        then: 'display title and save button of preferences'
        page.preferencesTitleDisplayed()
        page.savePreferencesBtnDisplayed()
        page.popupCloseDisplayed()

        when: 'Server Cluster Transactions checkbox is displayed'
        page.clusterTransactionsCheckboxDisplayed()
        then: 'Remove Cluster Transactions'
        page.clusterTransactionsCheckboxClick()

        when: 'click close button'
        page.savePreferences()
        then: 'no Cluster Transactions displayed'
        page.serverCpuDisplayed()
        page.serverRamDisplayed()
        page.clusterLatencyDisplayed()
        !page.clusterTransactionsDisplayed()

        when: 'click Display Preference button'
        page.openDisplayPreference()
        then: 'display title and save button of preferences'
        page.preferencesTitleDisplayed()
        page.savePreferencesBtnDisplayed()
        page.popupCloseDisplayed()

        when: 'Server Cluster Transactions is displayed'
        page.clusterTransactionsCheckboxDisplayed()
        then: 'Add Cluster Transactions'
        page.clusterTransactionsCheckboxClick()

        when: 'click close button'
        page.savePreferences()
        then: 'Cluster Transactions displayed along with others'
        page.serverCpuDisplayed()
        page.serverRamDisplayed()
        page.clusterLatencyDisplayed()
        page.clusterTransactionsDisplayed()
    }

    def clickDisplayPreferencesRemovePartitionIdleTimeAndAgainAddPartitionIdleTime() {
        expect: 'Display Preference button exists'
        page.displayPreferenceDisplayed()

        when: 'click Display Preference button'
        page.openDisplayPreference()
        then: 'display title and save button of preferences'
        page.preferencesTitleDisplayed()
        page.savePreferencesBtnDisplayed()
        page.popupCloseDisplayed()

        when: 'Partition Idle Time checkbox is displayed'
        page.partitionIdleTimeCheckboxDisplayed()
        then: 'Remove Partition Idle Time'
        page.partitionIdleTimeCheckboxClick()

        when: 'click close button'
        page.savePreferences()
        then: 'Partition Idle Time displayed'
        page.serverCpuDisplayed()
        page.serverRamDisplayed()
        page.clusterLatencyDisplayed()
        page.clusterTransactionsDisplayed()
        page.partitionIdleTimeDisplayed()

        when: 'click Display Preference button'
        page.openDisplayPreference()
        then: 'display title and save button of preferences'
        page.preferencesTitleDisplayed()
        page.savePreferencesBtnDisplayed()
        page.popupCloseDisplayed()

        when: 'Partition Idle Time is displayed'
        page.partitionIdleTimeCheckboxDisplayed()
        then: 'Add Partition Idle Time'
        page.partitionIdleTimeCheckboxClick()

        when: 'click close button'
        page.savePreferences()
        then: 'No Partition Idle Time displayed'
        page.serverCpuDisplayed()
        page.serverRamDisplayed()
        page.clusterLatencyDisplayed()
        page.clusterTransactionsDisplayed()
        !page.partitionIdleTimeDisplayed()
    }

    def clickDisplayPreferencesRemoveStoredProceduresAndAgainAddStoredProcedures() {
        expect: 'Display Preference button exists'
        page.displayPreferenceDisplayed()

        when: 'click Display Preference button'
        page.openDisplayPreference()
        then: 'display title and save button of preferences'
        page.preferencesTitleDisplayed()
        page.savePreferencesBtnDisplayed()
        page.popupCloseDisplayed()

        when:
        if(!page.storedProceduresCheckboxDisplayed()) {
            when: 'Stored Procedures checkbox is displayed'
            page.storedProceduresCheckboxDisplayed()
            then: 'Remove Stored Procedures'
            page.storedProceduresCheckboxClick()

            when: 'click close button'
            page.savePreferences()
            then: 'no Stored Procedures displayed'
            page.serverCpuDisplayed()
            page.serverRamDisplayed()
            page.clusterLatencyDisplayed()
            page.clusterTransactionsDisplayed()
            page.storedProceduresDisplayed()
            page.dataTablesDisplayed()

            when: 'click Display Preference button'
            waitFor(10) { page.openDisplayPreference() }
            then: 'display title and save button of preferences'
            page.preferencesTitleDisplayed()
            page.savePreferencesBtnDisplayed()
            page.popupCloseDisplayed()

            when: 'Stored Procedures is displayed'
            page.storedProceduresCheckboxDisplayed()
            then: 'Add Stored Procedures'
            page.storedProceduresCheckboxClick()

            when: 'click close button'
            page.savePreferences()
            then: 'Stored Procedures displayed along with others'
            page.serverCpuDisplayed()
            page.serverRamDisplayed()
            page.clusterLatencyDisplayed()
            page.clusterTransactionsDisplayed()
            page.partitionIdleTimeDisplayed()
            page.storedProceduresDisplayed()
            page.dataTablesDisplayed()
        }
        then:
            println("passed")
    }

    def ClickDisplayPreferencesRemoveDataTablesAndAgainAddDataTables() {
        expect: 'Display Preference button exists'
        page.displayPreferenceDisplayed()

        when: 'click Display Preference button'
        page.openDisplayPreference()
        then: 'display title and save button of preferences'
        page.preferencesTitleDisplayed()
        page.savePreferencesBtnDisplayed()
        page.popupCloseDisplayed()

        when: 'Data Tables checkbox is displayed'
        page.dataTablesCheckboxDisplayed()
        then: 'Remove Data Tables'
        page.dataTablesCheckboxClick()

        when: 'click close button'
        page.savePreferences()
        if(!page.dataTablesDisplayed()) {
            then: 'no Data Tables displayed'
            page.serverCpuDisplayed()
            page.serverRamDisplayed()
            page.clusterLatencyDisplayed()
            page.clusterTransactionsDisplayed()
            page.partitionIdleTimeDisplayed()
            page.storedProceduresDisplayed()
            !page.dataTablesDisplayed()

            when: 'click Display Preference button'
            page.openDisplayPreference()
            then: 'display title and save button of preferences'
            page.preferencesTitleDisplayed()
            page.savePreferencesBtnDisplayed()
            page.popupCloseDisplayed()

            when: 'Data Tables is displayed'
            page.dataTablesCheckboxDisplayed()
            then: 'Add Data Tables'
            page.dataTablesCheckboxClick()

            when: 'click close button'
            page.savePreferences()
            then: 'Data Tables displayed along with others'
            page.serverCpuDisplayed()
            page.serverRamDisplayed()
            page.clusterLatencyDisplayed()
            page.clusterTransactionsDisplayed()
            page.partitionIdleTimeDisplayed()
            page.storedProceduresDisplayed()
            page.dataTablesDisplayed()
        }
        then:
        println("passed")
    }

    def confirmGraphAreaAndDataAreaOpenInitially() {
        expect: 'Graph area open initially'
        page.isGraphAreaOpen()

        and: 'Data area open initially'
        page.isDataAreaOpen()
    }

    /// Test cases relating to Graph end
    /*def cleanupSpec() {
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

        String deleteQuery = page.getQueryToDeleteTableAndView()
        page.setQueryText(deleteQuery)

        page.runQuery()
    }*/
    def addProcedureAndCheck() {
        String createQuery = page.getQueryToCreateTable().replace("\n", "")
        String createStoredProcedure = page.getQueryToCreateStoredProcedure().replace("\n", "")
        String deleteQuery = page.getQueryToDeleteTableOnly().replace("\n", "")
        String dropProcedureQuery = page.getQueryToDropProcedureQuery().replace("\n", "")
        String execProcedureQuery = page.getQueryToExecuteProcedureQuery().replace("\n", "")
        String storedProcedureName = String.valueOf(page.getNameOfStoredProcedure()).replace("\n", "")
        String resultingStoredProcedureName = ""
        boolean createdStatus = false
        boolean deletedStatus = false

        when:
        println(storedProcedureName)
        then:
        println(createStoredProcedure)
        println(execProcedureQuery)
        println(dropProcedureQuery)

        when: 'sql query tab is clicked'
        page.gotoSqlQuery()
        then: 'at sql query'
        at SqlQueryPage

        when: 'set create table query in the box'
        page.setQueryText(createQuery)
        and: 'run the query'
        page.runQuery()
        then: 'refresh the page'
        page.refreshquery.click()

        when: 'set CREATE PROCEDURE query in the box'
        page.setQueryText(createStoredProcedure)
        and: 'run the query'
        page.runQuery()
        then: 'refresh the page'
        page.refreshquery.click()

        when: 'set EXEC PROCEDURE query in the box'
        page.setQueryText(execProcedureQuery)
        and: 'run the query'
        page.runQuery()
        then: 'refresh the page'
        page.refreshquery.click()

        when: 'Db Monitor tab is clicked'
        page.gotoDbMonitor()
        then: 'at DbMonitor Page'
        at DbMonitorPage

        when: 'click Display Preference button'
        page.openDisplayPreference()
        then: 'display title and save button of preferences'
        page.preferencesTitleDisplayed()
        page.savePreferencesBtnDisplayed()
        page.popupCloseDisplayed()

        when: 'Stored Procedure checkbox is displayed'
        page.storedProceduresCheckbox.isDisplayed()
        and: 'Add Stored Procedure'
        page.storedProceduresCheckbox.click()
        then: 'Save Preference'
        page.savePreferencesBtn.click()
        report 'savePreferencesBtn'
        then: 'wait for filter in stored procedure and insert filter term'
        try {
          waitFor(waitTime) { page.filterStoredProcedure.isDisplayed() }
          page.filterStoredProcedure.value(storedProcedureName)
          report "1"
        } catch (geb.waiting.WaitTimeoutException e) {
          try {
            $("#filterSP").isDisplayed()
            $("#filterSP").value(storedProcedureName)
            report "2"
          } catch (geb.waiting.WaitTimeoutException exp) {
          }
        }

        when: 'save the value of resultingStoredProcedureName'
        try {
          waitFor(waitTime) { $("#storeProcedureBody > tr > td:nth-child(1)").isDisplayed() }
          resultingStoredProcedureName = $("#storeProcedureBody > tr > td:nth-child(1)").text()
        } catch(geb.error.RequiredPageContentNotPresent e) {
          waitFor(waitTime) { $("#tblSP > tbody > tr > td.sorting_1").isDisplayed() }
          resultingStoredProcedureName = $("#tblSP > tbody > tr > td:nth-child(1)").text()
        } catch(geb.waiting.WaitTimeoutException e) {
          waitFor(waitTime) { $("#tblSP > tbody > tr > td.sorting_1").isDisplayed() }
          resultingStoredProcedureName = $("#tblSP > tbody > tr > td:nth-child(1)").text()
        }
        then: 'set created status'
        println("The resultingStoredProcedureName is " + resultingStoredProcedureName)
        // if(resultingStoredProcedureName.equals(storedProcedureName)) {
        //     createdStatus = true
        // }
        try {
          waitFor(waitTime) { $("#tblSP > tbody > tr > td:nth-child(1)").isDisplayed() }
          createdStatus = true
        } catch (geb.waiting.WaitTimeoutException e) {
        }
        when: 'sql query tab is clicked'
        page.gotoSqlQuery()
        then: 'at sql query'
        at SqlQueryPage

        when: 'set create table query in the box'
        page.setQueryText(dropProcedureQuery)
        and: 'run the query'
        page.runQuery()
        report 'checking'
        then: 'refresh the page'
        page.refreshquery.click()

        when: 'Db Monitor tab is clicked'
        page.gotoDbMonitor()
        then: 'at DbMonitor Page'
        at DbMonitorPage
        then: 'wait for filter in stored procedure  and insert filter term'
        try {
          waitFor(waitTime) { page.filterStoredProcedure.isDisplayed() }
          page.filterStoredProcedure.value(storedProcedureName)
          report "3"
        } catch (geb.waiting.WaitTimeoutException e) {
          try {
            $("#filterSP").isDisplayed()
            $("#filterSP").value(storedProcedureName)
            report "4"
          } catch (geb.waiting.WaitTimeoutException exp) {
          }
        }

        when: 'check deleted status'
        try {
          waitFor(waitTime) { !$("#storeProcedureBody > tr > td:nth-child(1)").text().equals(storedProcedureName) }
          waitFor(waitTime) { $("#storeProcedureBody > tr > td").isDisplayed() }
          deletedStatus = true
        } catch (geb.waiting.WaitTimeoutException e) {
        }
        try {
          waitFor(waitTime) { !$("#tblSP > tbody > tr > td.sorting_1").text().equals(storedProcedureName) }
          waitFor(waitTime) { $("#tblSP > tbody > tr > td").isDisplayed() }
          deletedStatus = true
        } catch (geb.waiting.WaitTimeoutException e) {
        }
        println(createdStatus)
        println(deletedStatus)
        then: 'display final result'
        if (createdStatus == true && deletedStatus == true) {
          println("addProcedureAndCheck - PASS")
        }
        else {
          println("addProcedureAndCheck - FAIL")
          assert false
        }
    }
}
