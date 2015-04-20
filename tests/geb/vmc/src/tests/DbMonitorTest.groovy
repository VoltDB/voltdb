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

    def setup() { // called before each test
        // TestBase.setup gets called first (automatically)
        when: 'click the DB Monitor link (if needed)'
        page.openDbMonitorPage()
        then: 'should be on DB Monitor page'
        at DbMonitorPage
    }

    def 'confirm Graph area and Data area open initially'() {
        expect: 'Graph area open initially'
        page.isGraphAreaOpen()

        and: 'Data area open initially'
        page.isDataAreaOpen()
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

    def openAndCloseDataArea() {
        when: 'ensure the Data area is open'
        if (!page.isDataAreaOpen()) {
            page.openDataArea()
        }
        then: 'Data area is open (to start test)'
        page.isDataAreaOpen()

        when: 'click Show/Hide Data (to close)'
        page.closeDataArea()
        then: 'Data area is closed'
        !page.isDataAreaOpen()

        when: 'click Show/Hide Data (to open again)'
        page.openDataArea()
        then: 'Data area is open (again)'
        page.isDataAreaOpen()

        when: 'click Show/Hide Data (to close again)'
        page.closeDataArea()
        then: 'Data area is closed (again)'
        !page.isDataAreaOpen()
    }

    def checkActiveMissingJoining() {
        expect: '1 Active server (at least)'
        page.getActive() >= 1

        and: '0 Missing servers (initially)'
        page.getMissing() == 0

        and: 'Joining servers not shown (for now)'
        page.getJoining() == -1
    }

    def openAndCloseServerList() {
        expect: 'Server list closed initially'
        !page.isServerListOpen()
        
        when: 'click Server button (to open list)'
        page.openServerList()
        then: 'Server list is open'
        page.isServerListOpen()

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

    // HEADER TESTS

    def "header banner exists" () {
        when:
        at DbMonitorPage
        then:
        waitFor(30) { header.banner.isDisplayed() }
    }


    def "header image exists" () {
        when:
        at DbMonitorPage
        then:
        waitFor(30) { header.image.isDisplayed() }
    }

    def "header username exists" () {
        when:
        at DbMonitorPage
        then:
        waitFor(30) { header.usernameInHeader.isDisplayed() }
    }

    def "header logout exists" () {
        when:
        at DbMonitorPage
        then:
        waitFor(30) { header.logout.isDisplayed() }
    }

    def "header help exists" () {
        when:
        at DbMonitorPage
        then:
        waitFor(30) { header.help.isDisplayed() }
    }

    // HEADER TAB TESTS

    def "header tab dbmonitor exists" () {
        when:
        at DbMonitorPage
        then:
        waitFor(30) {
            header.tabDBMonitor.isDisplayed()
            header.tabDBMonitor.text().toLowerCase().equals("DB Monitor".toLowerCase())
        }
    }

    def "header tab admin exists" () {
        when:
        at DbMonitorPage
        then:
        waitFor(30) {
            header.tabAdmin.isDisplayed()
            header.tabAdmin.text().toLowerCase().equals("Admin".toLowerCase())
        }
    }

    def "header tab schema exists" () {
        when:
        at DbMonitorPage
        then:
        waitFor(30) {
            header.tabSchema.isDisplayed()
            header.tabSchema.text().toLowerCase().equals("Schema".toLowerCase())

        }
    }

    def "header tab sql query exists" () {
        when:
        at DbMonitorPage
        then:
        waitFor(30) { header.tabSQLQuery.isDisplayed()
            header.tabSQLQuery.text().toLowerCase().equals("SQL Query".toLowerCase())
        }
    }

    def "header username check" () {
        when:
        at DbMonitorPage
        String username = page.getUsername()
        then:
        waitFor(30) {
            header.usernameInHeader.isDisplayed()
            header.usernameInHeader.text().equals(username)
        }
    }


    def "header username click and close" () {
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

    def "header username click and cancel" () {
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

    def "logout button test close" ()  {
        when:
        at DbMonitorPage
        then:
        waitFor(30) { header.logout.isDisplayed() }
        header.logout.click()
        waitFor(30) {
            header.logoutPopupOkButton.isDisplayed()
            header.logoutPopupCancelButton.isDisplayed()
            header.popupClose.isDisplayed()
        }
        header.popupClose.click()

    }

    def "logout button test cancel" ()  {
        when:
        at DbMonitorPage
        then:
        waitFor(30) { header.logout.isDisplayed() }
        header.logout.click()
        waitFor(30) {
            header.logoutPopupOkButton.isDisplayed()
            header.logoutPopupCancelButton.isDisplayed()
            header.popupClose.isDisplayed()
        }
        header.logoutPopupCancelButton.click()
    }

    // HELP POPUP TEST

    def "help popup existance" () {
        when:
        at DbMonitorPage
        waitFor(30) { header.help.isDisplayed() }
        header.help.click()
        then:
        waitFor(30) {
            header.popupTitle.isDisplayed()
            header.popupClose.isDisplayed()
            header.popupTitle.text().toLowerCase().equals("help".toLowerCase());
        }

        header.popupClose.click()
    }

    // FOOTER TESTS

    def "footer exists" () {
        when:
        at DbMonitorPage
        then:
        waitFor(30) { footer.banner.isDisplayed() }
    }

    def "footer text exists and valid"() {
        when:
        at DbMonitorPage
        then:
        waitFor(30) {
            footer.banner.isDisplayed()
            footer.text.isDisplayed()
            footer.text.text().toLowerCase().contains("VoltDB. All rights reserved.".toLowerCase())
        }
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
        expect: 'Graph view button exists'
        page.graphViewDisplayed()

        when: 'choose Seconds in Graph View'
        page.chooseGraphView("Minutes")
        then: 'display'

        DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
        Date date = new Date();
        String stringTwo = dateFormat.format(date)

        String stringOne = timeOne.text()
        int hourOne = page.changeToHour(stringOne)
        int minuteOne = page.changeToMinute(stringOne)

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
        expect: 'Graph view button exists'
        page.graphViewDisplayed()

        when: 'choose Seconds in Graph View'
        page.chooseGraphView("Days")
        then: 'display'

        String stringOne = timeOne.text()
        if ( stringOne.length() > 8 ) {
            assert true
        }
        else {
            assert false
        }
    }

    def "click display preferences and close"() {
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
        page.partitionIdleTimeDisplayed()
    }

    def "click display preferences remove Server Cpu and again add Server Cpu"() {
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
        page.partitionIdleTimeDisplayed()

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
        page.partitionIdleTimeDisplayed()
    }

    def "click display preferences remove Server RAM and again add Server RAM"() {
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
        page.savePreferences()
        then: 'no Server RAM displayed'
        page.serverCpuDisplayed()
        !page.serverRamDisplayed()
        page.clusterLatencyDisplayed()
        page.clusterTransactionsDisplayed()
        page.partitionIdleTimeDisplayed()

        when: 'click Display Preference button'
        page.openDisplayPreference()
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
        page.partitionIdleTimeDisplayed()
    }


    def "click display preferences remove Cluster Latency and again add Cluster Latency"() {
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
        page.partitionIdleTimeDisplayed()

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
        page.partitionIdleTimeDisplayed()
    }

    def "click display preferences remove Cluster Transactions and again add Cluster Transactions"() {
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
        page.partitionIdleTimeDisplayed()

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
        page.partitionIdleTimeDisplayed()
    }

    def "click display preferences remove Partition Idle Time and again add Partition Idle Time"() {
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
        then: 'no Partition Idle Time displayed'
        page.serverCpuDisplayed()
        page.serverRamDisplayed()
        page.clusterLatencyDisplayed()
        page.clusterTransactionsDisplayed()
        !page.partitionIdleTimeDisplayed()

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
        then: 'Partition Idle Time displayed along with others'
        page.serverCpuDisplayed()
        page.serverRamDisplayed()
        page.clusterLatencyDisplayed()
        page.clusterTransactionsDisplayed()
        page.partitionIdleTimeDisplayed()
    }



    def "click display preferences remove Stored Procedures and again add Stored Procedures"() {
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
        then: 'no Stored Procedures displayed'
        page.serverCpuDisplayed()
        page.serverRamDisplayed()
        page.clusterLatencyDisplayed()
        page.clusterTransactionsDisplayed()
        page.partitionIdleTimeDisplayed()
        !page.storedProceduresDisplayed()
        page.dataTablesDisplayed()

        when: 'click Display Preference button'
        page.openDisplayPreference()
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

    def "click display preferences remove Data Tables and again add Data Tables"() {
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


    def "Add a table in Tables and check it"() {

        String createQuery = page.getQueryToCreateTable()
        String deleteQuery = page.getQueryToDeleteTable()
        String tablename = page.getTablename()

        when: 'sql query tab is clicked'
        page.gotoSqlQuery()
        then: 'at sql query'
        at SqlQueryPage

        when: 'set query in the box'
        page.setQueryText(createQuery)
        then: 'run the query'
        page.runQuery()

        if ( page.queryStatus.isDisplayed() ) {
            println("Create query successful")
        }
        else {
            println("Create query unsuccessful")
            assert false
        }

        when: 'Db Monitor tab is clicked'
        page.gotoDbMonitor()
        then: 'at DbMonitor Page'
        at DbMonitorPage

        when:
        page.searchDatabaseTable(tablename)
        then:
        waitFor(30) {
            !page.databaseTableCurrentPage.text().equals("0")
            !page.databaseTableTotalPage.text().equals("0")
        }
        if ( !page.databaseTableCurrentPage.text().equals("0") && !page.databaseTableTotalPage.text().equals("0") ){
            println("The table was successfully created")
        }
        else {
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

        if ( page.queryStatus.isDisplayed() ) {
            println("Delete query successful")
        }
        else {
            println("Delete query unsuccessful")
            assert false
        }

        when: 'Db Monitor tab is clicked'
        page.gotoDbMonitor()
        then: 'at DbMonitor Page'
        at DbMonitorPage

        when:
        page.searchDatabaseTable(tablename)
        then:
        waitFor(30) {
            page.databaseTableCurrentPage.text().equals("0")
            page.databaseTableTotalPage.text().equals("0")
        }
        if ( page.databaseTableCurrentPage.text().equals("0") && page.databaseTableTotalPage.text().equals("0") ) {
            println("The table was successfully removed")
        }
        else {
            println("Table found after deletion")
            assert false
        }
    }


    def "check if Row Count is clickable"() {
        String before = ""
		String after  = ""

		when: 'click row count'
			page.clickRowcount()
        then: 'check if row count is in ascending'
            if ( page.tableInAscendingOrder() )
				before = "ascending"
			else
				before = "descending"

		when: 'click row count'
			page.clickRowcount()
		then: 'check if row count is in descending'
			if ( page.tableInDescendingOrder() )
				after = "descending"
			else
				after = "ascending"

			if ( before.equals("ascending") && after.equals("descending") )
				assert true
			else
				assert false
    }


    def "check if Max Rows is clickable"() {
        String before = ""
		String after  = ""

		when: 'click max rows'
			page.clickMaxRows()
        then: 'check if max rows is in ascending'
            if ( page.tableInAscendingOrder() )
				before = "ascending"
			else
				before = "descending"

		when: 'click max rows'
			page.clickMaxRows()
		then: 'check if max rows is in descending'
			if ( page.tableInDescendingOrder() )
				after = "descending"
			else
				after = "ascending"

			if ( before.equals("ascending") && after.equals("descending") )
				assert true
			else
				assert false
    }

    def "check if Min Rows is clickable"() {
        String before = ""
		String after  = ""

		when: 'click min rows'
			page.clickMinRows()
        then: 'check if min rows is in ascending'
            if ( page.tableInAscendingOrder() )
				before = "ascending"
			else
				before = "descending"

		when: 'click min rows'
			page.clickMinRows()
		then: 'check if min rows is in descending'
			if ( page.tableInDescendingOrder() )
				after = "descending"
			else
				after = "ascending"

			if ( before.equals("ascending") && after.equals("descending") )
				assert true
			else
				assert false
    }

    def "check if Avg Rows is clickable"() {
        String before = ""
		String after  = ""

		when: 'click avg rows'
			page.clickAvgRows()
        then: 'check if avg rows is in ascending'
            if ( page.tableInAscendingOrder() )
				before = "ascending"
			else
				before = "descending"

		when: 'click avg rows'
			page.clickAvgRows()
		then: 'check if avg rows is in descending'
			if ( page.tableInDescendingOrder() )
				after = "descending"
			else
				after = "ascending"

			if ( before.equals("ascending") && after.equals("descending") )
				assert true
			else
				assert false
    }

    def "check if Type is clickable"() {
        String before = ""
		String after  = ""

		when: 'click type'
			page.clickTabletype()
        then: 'check if type is in ascending'
            if ( page.tableInAscendingOrder() )
				before = "ascending"
			else
				before = "descending"

		when: 'click type'
			page.clickTabletype()
		then: 'check if type is in descending'
			if ( page.tableInDescendingOrder() )
				after = "descending"
			else
				after = "ascending"

			if ( before.equals("ascending") && after.equals("descending") )
				assert true
			else
				assert false
    }

	// stored procedure ascending descending
	
	    def "check if stored procedure is clickable"() {
        String before = ""
		String after  = ""

		when: 'click stored procedure'
			page.clickStoredProcedure()
        then: 'check if table is in ascending'
            if ( page.tableInAscendingOrder() )
				before = "ascending"
			else
				before = "descending"

		when: 'click stored procedure'
			page.clickStoredProcedure()
		then: 'check if table is in descending'
			if ( page.tableInDescendingOrder() )
				after = "descending"
			else
				after = "ascending"

			if ( before.equals("ascending") && after.equals("descending") )
				assert true
			else
				assert false
    }


    def "check if Invocations is clickable"() {
        String before = ""
		String after  = ""

		when: 'click row count'
			page.clickInvocations()
        then: 'check if row count is in ascending'
            if ( page.tableInAscendingOrder() )
				before = "ascending"
			else
				before = "descending"

		when: 'click row count'
			page.clickInvocations()
		then: 'check if row count is in descending'
			if ( page.tableInDescendingOrder() )
				after = "descending"
			else
				after = "ascending"

			if ( before.equals("ascending") && after.equals("descending") )
				assert true
			else
				assert false
    }


    def "check if Min Latency is clickable"() {
        String before = ""
		String after  = ""

		when: 'click min latency'
			page.clickMinLatency()
        then: 'check if max rows is in ascending'
            if ( page.tableInAscendingOrder() )
				before = "ascending"
			else
				before = "descending"

		when: 'click min latency'
			page.clickMinLatency()
		then: 'check if max rows is in descending'
			if ( page.tableInDescendingOrder() )
				after = "descending"
			else
				after = "ascending"

			if ( before.equals("ascending") && after.equals("descending") )
				assert true
			else
				assert false
    }

    def "check if Max Latency is clickable"() {
        String before = ""
		String after  = ""

		when: 'click max latency'
			page.clickMaxLatency()
        then: 'check if min rows is in ascending'
            if ( page.tableInAscendingOrder() )
				before = "ascending"
			else
				before = "descending"

		when: 'click max latency'
			page.clickMaxLatency()
		then: 'check if min rows is in descending'
			if ( page.tableInDescendingOrder() )
				after = "descending"
			else
				after = "ascending"

			if ( before.equals("ascending") && after.equals("descending") )
				assert true
			else
				assert false
    }

    def "check if Avg Latency is clickable"() {
        String before = ""
		String after  = ""

		when: 'click avg latency'
			page.clickAvgLatency()
        then: 'check if avg rows is in ascending'
            if ( page.tableInAscendingOrder() )
				before = "ascending"
			else
				before = "descending"

		when: 'click avg latency'
			page.clickAvgLatency()
		then: 'check if avg rows is in descending'
			if ( page.tableInDescendingOrder() )
				after = "descending"
			else
				after = "ascending"

			if ( before.equals("ascending") && after.equals("descending") )
				assert true
			else
				assert false
    }

    def "check if Time of Execution is clickable"() {
        String before = ""
		String after  = ""

		when: 'click time of execution'
			page.clickTimeOfExecution()
        then: 'check if type is in ascending'
            if ( page.tableInAscendingOrder() )
				before = "ascending"
			else
				before = "descending"

		when: 'click time of execution'
			page.clickTimeOfExecution()
		then: 'check if type is in descending'
			if ( page.tableInDescendingOrder() )
				after = "descending"
			else
				after = "ascending"

			if ( before.equals("ascending") && after.equals("descending") )
				assert true
			else
				assert false
    }

    def "Check Data in Stored Procedures"() {
        when:
        page.storedProceduresTableDisplayed()
        then:
        if(page.storedProceduresMsg.text().equals("No data to be displayed")) {
            println("No data displayed-PASS")
            println()
            assert true
        }
        else if(!page.storedProceduresMsg.text().equals("")) {
            println("Data displayed-PASS")
            println(page.storedProceduresMsg.text())
            println()
            assert true
        }
        else {
            println("FAIL")
            println()
            assert false
        }
    }

    def "Check Data in Database Tables"() {
        when:
        page.databaseTableDisplayed()
        then:
        if(page.databaseTableMsg.text().equals("No data to be displayed")) {
            println("No data displayed-PASS")
            println()
            assert true
        }
        else if(!page.databaseTableMsg.text().equals("")) {
            println("Data displayed-PASS")
            println(page.databaseTableMsg.text())
            println()
            assert true
        }
        else {
            println("FAIL")
            println()
            assert false
        }
    }

    // ALERT

	def "set alert and replace trigger alert"() {
		when: 'set alert threshold to zero'
			page.setAlertThreshold(00)
		then: 'check at least one alert'
			waitFor(40, 2) { page.alertCount.isDisplayed() }
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
			waitFor(40,20) { !page.alertCount.isDisplayed() }
	}


    // server search
    def "check server search on dbmonitor matched"(){

        when:'clicked server button'
        at DbMonitorPage
        String serverNamevalid = page.getValidPath()  // taking local server valid name from serversearch.txt file ("/src/resources/serversearch.txt")
        page.clusterserverbutton.click()
        waitFor(5){page.serversearch.value(serverNamevalid)
        }

        then:
        at DbMonitorPage
        waitFor(5){page.clusterserverbutton.isDisplayed()}
        page.clusterserverbutton.click()
        println("server searched matched")
    }


    def "check server search on dbmonitor not matched"(){

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


    def "check server title on dbmonitor"(){
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


    //dbmonitor graph part
    def "check min value in server cpu days"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypedays.click()
        
		then:
        if(page.servercpudaysmax.text()-page.servercpudaysmin.text()<="7:00:00") {
            println("Min Time for cpu days is:"+page.servercpudaysmin.text());
        }

        else {
            println("doesn't match")
        }
    }

    def "check max value in server cpu days"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypedays.click()

        then:
        if(page.servercpudaysmax.text()-page.servercpudaysmin.text()<="7:00:00") {
            println("Max Time  for cpu days is:"+page.servercpudaysmax.text());
        }

        else {
            println("doesn't match")
        }
    }


    def "check min value in server cpu minutes"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypemin.click()

        then:
        if(page.servercpuminutemax.text()-page.servercpuminutesmin.text()<="1:00:06") {
            println("Min Time for cpu minutes is:"+page.servercpuminutesmin.text());
        }

        else {
            println("doesn't match")
        }
    }

    def "check max value in server cpu minutes"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypemin.click()
    
        then:
        if(page.servercpuminutemax.text()-page.servercpuminutesmin.text()<="1:00:06") {
            println("Max Time for cpu minutes is:"+page.servercpuminutemax.text());
        }

        else {
            println("doesn't match")
        }
    }


    def "check min value in server cpu seconds"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypesec.click()

        then:
        if(page.servercpusecondmax.text()-page.servercpusecondmin.text()<="00:11:06") {
            println("Min Time for cpu seconds is:"+page.servercpusecondmin.text());
        }

        else {
            println("doesn't match")
        }
    }


    def "check max value in server cpu seconds"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypesec.click()

        then:
        if(page.servercpusecondmax.text()-page.servercpusecondmin.text()<="00:11:06") {
            println("Max Time for cpu seconds is:"+page.servercpusecondmax.text());
        }

        else {
            println("doesn't match")
        }
    }


    def "check min value in server ram days"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypedays.click()

        then:
        if(page.serverramdaysmax.text()-page.serverramdaysmin.text()<="7:00:00") {
            println("Min Time for RAM days is:"+page.serverramdaysmin.text());
        }
        else {
            println("doesn't match")
        }
    }


   def "check max value in server ram days"(){
       when:
       page.selecttypeindrop.click()
       page.selecttypedays.click()

       then:
       if(page.serverramdaysmax.text()-page.serverramdaysmin.text()<="7:00:00") {
          println("Max Time for RAM days is:"+page.serverramdaysmax.text());
       }
       else {
           println("doesn't match")
       }
   }


    def "check min value in server ram minutes"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypemin.click()

        then:
        if(page.serverramminutesmax.text()-page.serverramminutesmin.text()<="1:00:06") {
            println("Min Time for RAM minutes is:"+page.serverramminutesmin.text());
        }
        else {
            println("doesn't match")
        }
    }


    def "check max value in server ram minutes"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypemin.click()

        then:
        if(page.serverramminutesmax.text()-page.serverramminutesmin.text()<="1:00:06") {
            println("Max Time for RAM minutes is:"+page.serverramminutesmax.text());
        }
        else {
            println("doesn't match")
        }
    }



    def "check min value in server ram seconds"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypesec.click()

        then:
        if(page.serverramsecondmax.text()-page.serverramsecondmin.text()<="00:11:06") {
            println("Min Time for RAM seconds is:"+page.serverramsecondmin.text());
        }
        else {
            println("doesn't match")
        }
    }


    def "check max value in server ram seconds"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypesec.click()

        then:
        if(page.serverramsecondmax.text()-page.serverramsecondmin.text()<="00:11:06") {
            println("Max Time for RAM seconds is:"+page.serverramsecondmax.text());
        }
        else {
            println("doesn't match")
        }
    }


    def "check min value in cluster latency days"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypedays.click()

        then:
        if(page.clusterlatencydaysmax.text()-page.clusterlatencydaysmin.text()<="7:00:00") {
            println("Min Time for cluster latency days is:"+page.clusterlatencydaysmin.text());
        }
        else {
            println("doesn't match")
        }
    }


    def "check max value in cluster latency days"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypedays.click()

        then:
        if(page.clusterlatencydaysmax.text()-page.clusterlatencydaysmin.text()<="7:00:00") {
            println("Max Time for cluster latency days is:"+page.clusterlatencydaysmax.text());
        }
        else {
            println("doesn't match")
        }
    }


    def "check min value in cluster latency minutes"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypemin.click()

        then:
        if(page.clusterlatencyminutesmax.text()-page.clusterlatencyminutesmin.text()<="1:00:06") {
            println("Min Time for cluster latency minutes is:"+page.clusterlatencyminutesmin.text());
        }
        else {
            println("doesn't match")
        }
    }


    def "check max value in cluster latency minutes"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypemin.click()

        then:
        if(page.clusterlatencyminutesmax.text()-page.clusterlatencyminutesmin.text()<="1:00:06") {
            println("Max Time for cluster latency minutes is:"+page.clusterlatencyminutesmax.text());
        }
        else {
            println("doesn't match")
        }
    }


    def "check min value in cluster latency seconds"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypesec.click()

        then:
        if(page.clusterlatencysecondmax.text()-page.clusterlatencysecondmin.text()<="00:11:06") {
            println("Min Time for cluster latency seconds is:"+page.clusterlatencysecondmin.text());
        }
        else {
            println("doesn't match")
        }
    }


    def "check max value in cluster latency seconds"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypesec.click()

        then:
        if(page.clusterlatencysecondmax.text()-page.clusterlatencysecondmin.text()<="00:11:06") {
            println("Max Time for cluster latency seconds is:"+page.clusterlatencysecondmax.text());
        }
        else {
            println("doesn't match")
        }
    }



    def "check min value in cluster transaction days"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypedays.click()

        then:
        if(page.clustertransactiondaysmax.text()-page.clustertransactiondaysmin.text()<="7:00:00") {
            println("Min Time for cluster transaction days is:"+page.clustertransactiondaysmin.text());
        }
        else {
            println("doesn't match")
        }
    }


    def "check max value in cluster transaction days"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypedays.click()

        then:
        if(page.clustertransactiondaysmax.text()-page.clustertransactiondaysmin.text()<="7:00:00") {
            println("Max Time for cluster transaction days is:"+page.clustertransactiondaysmax.text());
        }
        else {
            println("doesn't match")
        }
    }


    def "check min value in cluster transaction minutes"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypemin.click()

        then:
        if(page.clustertransactionminutesmax.text()-page.clustertransactionminutesmin.text()<="1:00:06") {
            println("Min Time for cluster transaction minutes is:"+page.clustertransactionminutesmin.text());
        }
        else {
            println("doesn't match")
        }
    }


    def "check max value in cluster transaction minutes"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypemin.click()

        then:
        if(page.clustertransactionminutesmax.text()-page.clustertransactionminutesmin.text()<="1:00:06") {
            println("Max Time for cluster transaction minutes is:"+page.clustertransactionminutesmax.text());
        }
        else {
            println("doesn't match")
        }
    }


    def "check min value in cluster transaction seconds"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypesec.click()

        then:
        if(page.clustertransactionsecondmax.text()-page.clustertransactionsecondmin.text()<="00:11:06") {
            println("Min Time for cluster transaction seconds is:"+page.clustertransactionsecondmin.text());
        }
        else {
            println("doesn't match")
        }
    }

    def "check max value in cluster transaction seconds"(){
        when:
        page.selecttypeindrop.click()
        page.selecttypesec.click()

        then:
        if(page.clustertransactionsecondmax.text()-page.clustertransactionsecondmin.text()<="00:11:06") {
            println("Max Time for cluster transaction seconds is:"+page.clustertransactionsecondmax.text());
        }
        else {
            println("doesn't match")
        }
    }

// for partition idle graph

    def "check min value in cluster Partition Idle graph with respect to seconds"(){

        when:'select the type as second in dropdown list'
        page.selecttypeindrop.click()
        page.selecttypesec.click()

        then:'check minimum value in partition idle time graph'
        waitFor(10){	page.partitiongraphmin.isDisplayed()
                         page.partitiongraphmax.isDisplayed()
        }

        if(page.partitiongraphmax.text() - page.partitiongraphmin.text()<= "15:06") {
                        println("Min Time for cluster transaction seconds for partition Idle graph is:"+page.partitiongraphmin.text());
        }
        else {
                        println("doesn't match")
        }
    }

    def "check max value in cluster Partition Idle graph with respect to seconds"(){

        when:'select the type as second in dropdown list'
        page.selecttypeindrop.click()
        page.selecttypesec.click()

        then: 'check maximum value in partition idle time graph'
        waitFor(10){	page.partitiongraphmin.isDisplayed()
                        page.partitiongraphmax.isDisplayed()}
        if(page.partitiongraphmax.text() - page.partitiongraphmin.text()<= "15:06") {
                        println("Max Time for cluster transaction seconds for Partition Idle graph is:"+page.partitiongraphmax.text());
        }
        else {
                        println("doesn't match")
        }
    }


    def "check min value in cluster Partition Idle graph with respect to days"(){

        when:'select the type as days in dropdown list'
        page.selecttypeindrop.click()
        page.selecttypedays.click()

        then:'check minimum value in partition idle time graph'
        waitFor(10){	page.partitiongraphdaysmin.isDisplayed()
                        page.partitiongraphdaysmax.isDisplayed()}

        if(page.partitiongraphdaysmax.text() - page.partitiongraphdaysmin.text()<= "08:00:00") {
                        println("Min Time for cluster transaction days for partition Idle graph is:"+page.partitiongraphdaysmin.text());
        }
        else {
                        println("doesn't match")
        }
    }

    def "check max value in cluster Partition Idle graph with respect to days"(){

        when:'select the type as days in dropdown list'
        page.selecttypeindrop.click()
        page.selecttypedays.click()

        then:'check maximum value in partition idle time graph'
        waitFor(10){	page.partitiongraphdaysmin.isDisplayed()
                        page.partitiongraphdaysmax.isDisplayed()}
        if(page.partitiongraphdaysmax.text() - page.partitiongraphdaysmin.text()<= "08:00:00") {
                        println("Max Time for cluster transaction days for Partition Idle graph is:"+page.partitiongraphdaysmax.text());
        }
        else {
                        println("doesn't match")
        }
    }

    def "check min value in cluster Partition Idle graph with respect to minutes"(){

        when:'select the type as minutes in dropdown list'

        waitFor(5){page.selecttypeindrop.click()}
        page.selecttypemin.click()
        page.selecttypemin.click()

        then:'check minimum value in partition idle time graph'
        waitFor(10){	page.partitiongraphminutmin.isDisplayed()
                        page.partitiongraphminutmax.isDisplayed()}
        if(page.partitiongraphminutmax.text() - page.partitiongraphminutmin.text() <= "20:00") {
                        println("Min Time for cluster transaction minutes for partition Idle graph is:"+page.partitiongraphminutmin.text());
        }
        else {
            println("doesn't match");
        }
    }

    def "check max value in cluster Partition Idle graph with respect to minutes"(){

        when:'select the type as minutes in dropdown list'

        waitFor(5){page.selecttypeindrop.click()}
        page.selecttypemin.click()
        page.selecttypemin.click()

        then:'check maximum value in partition idle time graph'
        waitFor(10){	page.partitiongraphminutmin.isDisplayed()
                        page.partitiongraphminutmax.isDisplayed()}
        if(page.partitiongraphminutmax.text() - page.partitiongraphminutmin.text() <= "20:00") {
                        println("Max Time for cluster transaction minutes for Partition Idle graph is:"+page.partitiongraphminutmax.text());
        }
        else {
                        println("doesn't match");
        }
    }


    def "Click display preferences remove Partition Idle Time and again Add Partition Idle Time"() {
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
        then: 'no Partition Idle Time displayed'
        page.serverCpuDisplayed()
        page.serverRamDisplayed()
        page.clusterLatencyDisplayed()
        page.clusterTransactionsDisplayed()
        !page.partitionIdleTimeDisplayed()

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
        then: 'Partition Idle Time displayed along with others'
        page.serverCpuDisplayed()
        page.serverRamDisplayed()
        page.clusterLatencyDisplayed()
        page.clusterTransactionsDisplayed()
        page.partitionIdleTimeDisplayed()
    }

    def "Check server legends visible in Graph Partition Idle Time"(){

        // for testing , 4 server legends are visible
        
        when: 'server partition legends is visible'
        waitFor(10){	page.localpartition.isDisplayed()
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
