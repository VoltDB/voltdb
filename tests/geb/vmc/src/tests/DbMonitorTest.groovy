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
        int count = 0
		
		while(count<numberOfTrials) {
			try {
			    when: 'click the DB Monitor link (if needed)'
                page.openDbMonitorPage()
                then: 'should be on DB Monitor page'
                at DbMonitorPage
            break
			} catch (org.openqa.selenium.ElementNotVisibleException e) {
				println("ElementNotVisibleException: Unable to Start the test")
				println("Retrying")
			}
		}
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
		    try {
                page.tableInAscendingOrder()
                before = "ascending"
            } catch(geb.error.RequiredPageContentNotPresent e) {
				before = "descending"
		    }
			waitFor(30) { page.clickStoredProcedure() }
        then: 'check if table is in ascending'
            try {
                page.tableInAscendingOrder()
                before = "ascending"
            } catch(geb.error.RequiredPageContentNotPresent e) {
				before = "descending"
		    }

			if ( !before.equals(after)  )
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

    //server cpu
    def "check min value in server cpu days"(){
        int count = 0
        
        when:
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
		
		if(monthMax.equals(monthMin)) {
		    if(dateMax > dateMin) {
		        println("The minimum value is " + stringMin + " and the time is in Days")
		    }
		    else {
		        println("FAIL: Date of Max is less than that of date of Min for same month")
		        assert false
		    }
		}
		else {
		    if (dateMax < dateMin) {
		        println("Success")
		    }
		    else {
		        println("FAIL: Date of Max is more than that of date of Min for new month")
		        assert false
		    }
		}
    }
    
    def "check max value in server cpu days"(){
        int count = 0
        
        when:
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
		
		if(monthMax.equals(monthMin)) {
		    if(dateMax > dateMin) {
		        println("The maximum value is " + stringMax + " and the time is in Days")
		    }
		    else {
		        println("FAIL: Date of Max is less than that of date of Min for same month")
		        assert false
		    }
		}
		else {
		    if (dateMax < dateMin) {
		        println("Success")
		    }
		    else {
		        println("FAIL: Date of Max is more than that of date of Min for new month")
		        assert false
		    }
		}
    }
    
    def "check min value in server cpu minutes"(){
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
    }


    def "check max value in server cpu minutes"(){
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
		    println("The maximum value is " + stringMax + " and the time is in " + result )
		    assert true
		}
		else {
		    println("FAIL: It is not in minutes")
		    assert false
		}
    }

    def "check min value in server cpu seconds"(){
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
		    println("The minimum value is " + stringMin + " and the time is in " + result )
		    assert true
		}
		else {
		    println("FAIL: It is not in seconds")
		    assert false
		}
    }

    def "check max value in server cpu seconds"(){
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
    }
    
    //for server ram
    def "check min value in server ram days"(){
        int count = 0
        
        when:
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
		
		String monthMax = page.changeToMonth(stringMax)
		String monthMin = page.changeToMonth(stringMin)
		
		String dateMax = page.changeToDate(stringMax)
		String dateMin = page.changeToDate(stringMin)
		
		if(monthMax.equals(monthMin)) {
		    if(dateMax > dateMin) {
		        println("The minimum value is " + stringMin + " and the time is in Days")
		    }
		    else {
		        println("FAIL: Date of Max is less than that of date of Min for same month")
		        assert false
		    }
		}
		else {
		    if (dateMax < dateMin) {
		        println("Success")
		    }
		    else {
		        println("FAIL: Date of Max is more than that of date of Min for new month")
		        assert false
		    }
		}
    }
    
    def "check max value in server ram days"(){
        int count = 0
        
        when:
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
		
		String monthMax = page.changeToMonth(stringMax)
		String monthMin = page.changeToMonth(stringMin)
		
		String dateMax = page.changeToDate(stringMax)
		String dateMin = page.changeToDate(stringMin)
		
		if(monthMax.equals(monthMin)) {
		    if(dateMax > dateMin) {
		        println("The maximum value is " + stringMax + " and the time is in Days")
		    }
		    else {
		        println("FAIL: Date of Max is less than that of date of Min for same month")
		        assert false
		    }
		}
		else {
		    if (dateMax < dateMin) {
		        println("Success")
		    }
		    else {
		        println("FAIL: Date of Max is more than that of date of Min for new month")
		        assert false
		    }
		}
    }
    
    def "check min value in server ram minutes"(){
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
		    println("The minimum value is " + stringMin + " and the time is in " + result )
		    assert true
		}
		else {
		    println("FAIL: It is not in minutes")
		    assert false
		}
    }


    def "check max value in server ram minutes"(){
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
    }

    def "check min value in server ram seconds"(){
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

    def "check max value in server ram seconds"(){
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

    //cluster latency
    def "check min value in cluster latency days"(){
        int count = 0
        
        when:
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
		
		if(monthMax.equals(monthMin)) {
		    if(dateMax > dateMin) {
		        println("The minimum value is " + stringMin + " and the time is in Days")
		    }
		    else {
		        println("FAIL: Date of Max is less than that of date of Min for same month")
		        assert false
		    }
		}
		else {
		    if (dateMax < dateMin) {
		        println("Success")
		    }
		    else {
		        println("FAIL: Date of Max is more than that of date of Min for new month")
		        assert false
		    }
		}
    }

    def "check max value in cluster latency days"(){
        int count = 0
        
        when:
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
		
		if(monthMax.equals(monthMin)) {
		    if(dateMax > dateMin) {
		        println("The maximum value is " + stringMax + " and the time is in Days")
		    }
		    else {
		        println("FAIL: Date of Max is less than that of date of Min for same month")
		        assert false
		    }
		}
		else {
		    if (dateMax < dateMin) {
		        println("Success")
		    }
		    else {
		        println("FAIL: Date of Max is more than that of date of Min for new month")
		        assert false
		    }
		}
    }

    def "check min value in cluster latency minutes"(){
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

    def "check max value in cluster latency minutes"(){
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

    def "check min value in cluster latency seconds"(){
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

    def "check max value in cluster latency seconds"(){
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
    def "check min value in cluster transaction days"(){
        int count = 0
        
        when:
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
		            page.clustertransactionmax.isDisplayed()
		        }
		        stringMax = page.clustertransactionmax.text()
		        stringMin = page.clustertransactionmin.text()
		        
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
		
		if(monthMax.equals(monthMin)) {
		    if(dateMax > dateMin) {
		        println("The minimum value is " + stringMin + " and the time is in Days")
		    }
		    else {
		        println("FAIL: Date of Max is less than that of date of Min for same month")
		        assert false
		    }
		}
		else {
		    if (dateMax < dateMin) {
		        println("Success")
		    }
		    else {
		        println("FAIL: Date of Max is more than that of date of Min for new month")
		        assert false
		    }
		}
    }

    def "check max value in cluster transaction days"(){
        int count = 0
        
        when:
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
		            page.clustertransactionmax.isDisplayed()
		        }
		        stringMax = page.clustertransactionmax.text()
		        stringMin = page.clustertransactionmin.text()
		        
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
		
		if(monthMax.equals(monthMin)) {
		    if(dateMax > dateMin) {
		        println("The maximum value is " + stringMax + " and the time is in Days")
		    }
		    else {
		        println("FAIL: Date of Max is less than that of date of Min for same month")
		        assert false
		    }
		}
		else {
		    if (dateMax < dateMin) {
		        println("Success")
		    }
		    else {
		        println("FAIL: Date of Max is more than that of date of Min for new month")
		        assert false
		    }
		}
    }

    def "check min value in cluster transaction minutes"(){
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
		
		if(result.equals("minutes")) {
		    println("The minimum value is " + stringMin + " and the time is in " + result )
		    assert true
		}
		else {
		    println("FAIL: It is not in minutes")
		    assert false
		}
    }

    def "check max value in cluster transaction minutes"(){
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
		
		if(result.equals("minutes")) {
		    println("The maximum value is " + stringMax + " and the time is in " + result )
		    assert true
		}
		else {
		    println("FAIL: It is not in minutes")
		    assert false
		}
    }

    def "check min value in cluster transaction seconds"(){
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

    def "check max value in cluster transaction seconds"(){
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

    // for partition idle graph
    def "check min value in Partition Idle graph with respect to seconds"() {
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

    def "check max value in Partition Idle graph with respect to seconds"(){
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
    }

    def "check min value in cluster Partition Idle graph with respect to minutes"(){
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
		    println("The minimum value is " + stringMin + " and the time is in " + result )
		    assert true
		}
		else {
		    println("FAIL: It is not in minutes")
		    assert false
		}
    }

    def "check max value in cluster Partition Idle graph with respect to minutes"(){
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
    }
    
    def "check min value in cluster Partition Idle graph with respect to days"(){
        int count = 0
        
        when:
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
		
		if(monthMax.equals(monthMin)) {
		    if(dateMax > dateMin) {
		        println("The minimum value is " + stringMin + " and the time is in Days")
		    }
		    else {
		        println("FAIL: Date of Max is less than that of date of Min for same month")
		        assert false
		    }
		}
		else {
		    if (dateMax < dateMin) {
		        println("Success")
		    }
		    else {
		        println("FAIL: Date of Max is more than that of date of Min for new month")
		        assert false
		    }
		}
    }

    def "check max value in cluster Partition Idle graph with respect to days"(){
        int count = 0
        
        when:
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
		
		
		if(monthMax.equals(monthMin)) {
		    if(dateMax > dateMin) {
		        println("The maximum value is " + stringMax + " and the time is in Days")
		    }
		    else {
		        println("FAIL: Date of Max is less than that of date of Min for same month")
		        assert false
		    }
		}
		else {
		    if (dateMax < dateMin) {
		        println("Success")
		    }
		    else {
		        println("FAIL: Date of Max is more than that of date of Min for new month")
		        assert false
		    }
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
