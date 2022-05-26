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

package vmcTest.pages

import geb.navigator.Navigator
import java.io.*
import java.util.Date
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * This class represents the 'DB Monitor' tab of the VoltDB Management Center
 * (VMC) page, which is the VoltDB web UI (replacing the old Management Center).
 */
class DbMonitorPage extends VoltDBManagementCenterPage {
    static content = {
        activeIcon                      { $('.activeIcon') }
        activeCount                     { activeIcon.find('#activeCount') }
        missingIcon                     { $('.missingIcon') }
        missingCount                    { missingIcon.find('#missingCount') }
        alertIcon                       (required: false) { $('.alertIcon') }
        alertCount                      (required: false) { alertIcon.find('span') }
        joiningIcon                     (required: false) { $('.joiningIcon') }
        joiningCount                    (required: false) { joiningIcon.find('span') }
        serverButton                    { $('#btnPopServerList') }
        serverList                      { $('#popServerList') }
        servers                         { serverList.find('.active') }
        servName                        { servers.find('a') }
        servMemory                      { servers.next().next().find('.memory-status') }
        showHideGraph                   { $('#showHideGraphBlock') }
        graphsArea                      { $('#graphChart') }
        showHideData                    { $('#ShowHideBlock') }
        dataArea                        { $('.menu_body') }

        serverCpu                       { $("#chartServerCPU") }
        serverRam                       { $("#chartServerRAM") }
        clusterLatency                  { $("#chartClusterLatency") }
        clusterTransactions             { $("#chartClusterTransactions") }
        partitionIdleTime               { $("#chartPartitionIdleTime") }
        commandLogStatistics            { $("#chartCommandLogging") }
        databaseReplication             { $("#ChartDrReplicationRate") }
        storedProcedures                { $("#divSPSection") }
        dataTables                      { $("#tblDT") }

        serverCpuCheckbox               { $("#ServerCPU") }
        serverRamCheckbox               { $("#ServerRAM") }
        clusterLatencyCheckbox          { $("#ClusterLatency") }
        clusterTransactionsCheckbox     { $("#ClusterTransactions") }
        partitionIdleTimeCheckbox       { $("#PartitionIdleTime") }
        commandLogStatisticsCheckbox    { $("#CommandLogStat") }
        databaseReplicationCheckbox     { $("#DrReplicationRate") }
        storedProceduresCheckbox        { $("#StoredProcedures") }
        dataTablesCheckbox              { $("#DatabaseTables") }

        filterStoredProcedure           { $("#filterStoredProc") }
        filterDatabaseTable             { $("#filterDT") }

        databaseTableCurrentPage        { $("#tblDT_paginate > div > span.pageIndex") }
        databaseTableTotalPage          { $("#tblDT_paginate > div > span.totalPages") }

        displayPreference               { $("#showMyPreference") }
        graphView                       { $('#graphView') }
        timeOne                         { $(class:"nv-axisMaxMin", transform:"translate(0,0)") }

        table                           { $("#TABLE_NAME") }
        rowcount                        { $("#TUPLE_COUNT") }
        maxrows                         { $("#MAX_ROWS") }
        minrows                         { $("#MIN_ROWS") }
        avgrows                         { $("#AVG_ROWS") }
        tabletype                       { $("#TABLE_TYPE") }

        ascending                       (required: false) { $(class:"sorttable_sorted") }
        descending                      (required: false) { $(class:"sorttable_sorted_reverse") }

        alertThreshold                  { $("#threshold") }
        saveThreshold                   { $("#saveThreshold") }

        storedProceduresNodataMsg       { $("html body div.page-wrap div#wrapper div.contents div#containerMain.container div.data div#firstpane.menu_list div.menu_body div#tblStoredProcedures.storedProcWrapper div.tblScroll table.storeTbl tbody#storeProcedureBody tr td") }

        databasetableNoDataMsg          { $("html body div.page-wrap div#wrapper div.contents div#containerMain.container div.data div#firstpane.menu_list div.menu_body div#tblDataTables.dataTablesWrapper div.tblScroll table.storeTbl tbody#tablesBody tr td") }

        preferencesTitle                { $(class:"overlay-title", text:"Graph/Data Preferences") }
        savePreferencesBtn              { $("#savePreference") }
        popupClose                      { $(class:"popup_close") }

        serverbutton                    { $("#serverName") }
        serverconfirmation              { $("#serverConfigAdmin > div > div.slide-pop-title > div.icon-server.searchLeft.searchLeftAdmin") }

        deerwalkserver3stopbutton       { $("#stopServer_deerwalk3")}
        deeerwalkservercancelbutton     { $("#StopConfirmCancel")}
        deerwalkserverstopok            { $("#StopConfirmOK")}

        deerwalkserver4stopbutton       { $("#stopServer_deerwalk4")}

        storedProcedure                 { $("#PROCEDURE") }
        invocations                     { $("#INVOCATIONS") }
        minLatency                      { $("#MIN_LATENCY") }
        maxLatency                      { $("#MAX_LATENCY") }
        avgLatency                      { $("#AVG_LATENCY") }
        timeOfExecution                 { $("#PERC_EXECUTION") }

        //DBmonitor part for server
        dbmonitorbutton                 { $("#navDbmonitor > a")}
        clusterserverbutton             { $("#btnPopServerList")}
        servernamefourthbtn             { $("#serversList > li:nth-child(1) > a")}
        servernamesecondbtn             { $("#serversList > li:nth-child(2) > a")}
        servernamethirdbtn              { $("#serversList > li:nth-child(3) > a")}
        serveractivechk                 { $("#serversList > li.active.monitoring > a")}
        serversearch                    { $("input", type: "text", id: "popServerSearch")}
        checkserverTitle                { $("#popServerList > div > div.slide-pop-title > div.icon-server.searchLeft")}
        setthreshhold                   { $("#threshold")}
        clickthreshholdset              { $("#saveThreshold")}

        // dbmonitor graph
        servercpumin                { $("#visualisationCpu > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text")}
        servercpumax                { $("#visualisationCpu > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text")}

        selecttypeindrop                { $("#graphView")}
        selecttypedays                  { $("#graphView > option:nth-child(3)")}
        selecttypemin                   { $("#graphView > option:nth-child(2)")}
        selecttypesec                   { $("#graphView > option:nth-child(1)")}

        serverrammin                { $("#visualisationRam > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text")}
        serverrammax                { $("#visualisationRam > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text")}

        clusterlatencymin           { $("#visualisationLatency > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text")}
        clusterlatencymax           { $("#visualisationLatency > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text")}

        clustertransactionmin       { $("#visualisationTransaction > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text")}
        clustertransactionmax       { $("#visualisationTransaction > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text")}

        //partition idle graph
        partitiongraphmin               (required: false) { $("#visualisationPartitionIdleTime > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text")}
        partitiongraphmax               { $("#visualisationPartitionIdleTime > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text")}

        //command log statistics graph
        commandLogStatisticsMin         { $("#visualisationCommandLog > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text") }
        commandLogStatisticsMax         { $("#visualisationCommandLog > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text") }

        //database replication graph
        databaseReplicationMin          { $("#visualizationDrReplicationRate > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text") }
        databaseReplicationMax          { $("#visualizationDrReplicationRate > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text") }

        partitionstatus                 { $("#visualisationPartitionIdleTime > g > g > g.nv-y.nv-axis > g > g:nth-child(1) > g:nth-child(2) > text")}
        localpartition                  { $("#chartPartitionIdleTime > div.legend > ul > li:nth-child(1)")}
        clusterwide                     { $("#chartPartitionIdleTime > div.legend > ul > li:nth-child(2)")}
        multipartition                  { $("#chartPartitionIdleTime > div.legend > ul > li:nth-child(3)")}

        storedProceduresMsg             { $("#tblSP > tbody > tr > td") }
        databaseTableMsg                { $("#tblDT > tbody > tr > td") }

        header                          { module Header }
        footer                          { module Footer }

        //DR Section

        showHideDrBlock {$("#showHideDrBlock")}
        drSection {$("#drSection")}

        //DR Master Columns

        partitionID {$("#partitionID")}

        status {$("#status")}

        ascendingDT                 { $(class:"sorting_asc") }
        descendingDT                { $(class:"sorting_desc") }

        dbDrMode {$("#dbDrMode")}

        filterPartitionId {$("#filterPartitionId")}

        partitionIdRows{$("#tblDrMAster").find(class:"sorting_1")}

        //Dr Replica Section

        replicaStatus{$("#replicaStatus")}
        replicationRate1 {$("#replicationRate1")}
        replicationRate5 {$("#replicationRate5")}

        filterReplicaServerRows{$("#tblDrReplica").find(class:"sorting_1")}

        filterHostID {($("#filterHostID"))}

        // Command Log Table

        showHideCLPBlock (required:false) {$("#showHideCLPBlock")}
        clpSection {$("#clpSection")}

        cmdServer {$("#cmdServer")}
        cmdPendingBytes {$("#cmdPendingBytes")}
        cmdPendingTrans {$("#cmdPendingTrans")}
        cmdTotalSegments {$("#cmdTotalSegments")}
        cmdSegmentsInUse {$("#cmdSegmentsInUse")}
        cmdFsyncInterval {$("#cmdFsyncInterval")}

        drMasterTitle {$("#drMasterTitle")}
        drReplicaTitle {$("#drReplicaTitle")}
        drCLPTitle {$("#drCLPTitle")}

        filterServer {$("#filterServer")}

        clpServerRows {$("#tblCmdLog").find(class:"sorting_1")}

        // UAT
        drTableModeTypeText         { $("#dbDrMode") }
        drTableBlock                { $("#showHideDrBlock") }
        drTableCurrentPageReplica   { $("#tblDrReplica_paginate > div > span.pageIndex") }
        drTableTotalPagesReplica    { $("#tblDrReplica_paginate > div > span.totalPages") }
        drTableNextReplicaDisabled  { $("#tblDrReplica_paginate > span.paginate_disabled_next.paginate_button") }
        drTablePrevReplicaDisabled  { $("#tblDrReplica_paginate > span.paginate_disabled_previous.paginate_button") }
        drTableNextReplicaEnabled   { $("#tblDrReplica_paginate > span.paginate_enabled_next.paginate_button") }
        drTablePrevReplicaEnabled   { $("#tblDrReplica_paginate > span.paginate_enabled_previous.paginate_button") }

        drTableCurrentPageMaster    { $("#tblDrMAster_paginate > div > span.pageIndex") }
        drTableTotalPagesMaster     { $("#tblDrMAster_paginate > div > span.totalPages") }
        drTableNextMasterEnabled    { $("#tblDrMAster_paginate > span.paginate_enabled_next.paginate_button") }
        drTablePrevMasterEnabled    { $("#tblDrMAster_paginate > span.paginate_enabled_previous.paginate_button") }
        drTableNextMasterDisabled   { $("#tblDrMAster_paginate > span.paginate_disabled_next.paginate_button") }
        drTablePrevMasterDisabled   { $("#tblDrMAster_paginate > span.paginate_disabled_previous.paginate_button") }

        //
        clpPrevDisabled             { $("#tblCmdLog_paginate > span.paginate_disabled_previous.paginate_button") }
        clpNextDisabled             { $("#tblCmdLog_paginate > span.paginate_disabled_next.paginate_button") }
        clpPrevEnabled              { $("#tblCmdLog_paginate > span.paginate_enabled_previous.paginate_button") }
        clpNextEnabled              { $("#tblCmdLog_paginate > span.paginate_enabled_next.paginate_button") }

        clpCurrentPage              { $("#tblCmdLog_paginate > div > span.pageIndex") }
        clpTotalPages               { $("#tblCmdLog_paginate > div > span.totalPages") }

        //
        storedProcPrev              { $("#previousProcedures") }
        storedProcNext              { $("#nextProcedures") }
        storedProcCurrentPage       { $("#lblPrevious") }
        storedProcTotalPages        { $("#lblTotalPages") }

        //
        tablesPrev                  { $("#previousTables") }
        tablesNext                  { $("#nextTables") }

        serverListHeader            { $("#tblServerListHeader") }
        serverNameHeader            { $("#thServerName") }
        serverIpAddressHeader       { $("#thIpAddress") }
        serverMemoryUsageHeader     { $("#thMemoryUsage") }
    }

    static at = {
        dbMonitorTab.displayed
        dbMonitorTab.attr('class') == 'active'
        // showHideGraph.displayed
        // showHideData.displayed
    }

    /**
     * Returns the count, as displayed (possibly in parentheses) in the
     * specified Navigator element; e.g., if "(3)" is displayed, then 3 is
     * returned (as an int).
     * <p>Note: if the specified Navigator element is not displayed on the
     * page, then -1 is returned; if the text in parentheses cannot be parsed
     * as an int, then -2 is returned.
     * @param nav - a Navigator specifying an element whose (parenthetical)
     * count is to be returned.
     * @return the displayed (parenthetical) count (or -1 if not shown, or
     * -2 if not parseable).
     */

    private int getCount(Navigator nav) {
        if (!nav.displayed) {
            return -1
        }
        String str = nav.text()
        try {
            return Integer.parseInt(str.replaceAll("[\\(\\)]", ""))
        } catch (NumberFormatException e) {
            System.err.println("Cannot parse '" + str + "' as an int.")
            e.printStackTrace()
            return -2
        }
    }

    /**
     * Returns the number of Active servers, as displayed in parentheses near
     * the top of the DB Monitor page; e.g., if "Active (3)" is displayed,
     * then 3 is returned (as an int).
     * <p>Note: if the Active server count is not displayed on the page, then
     * -1 is returned; if the text in parentheses cannot be parsed as an int,
     * then -2 is returned.
     * @return the number of Active servers, as shown on the page (or -1 if
     * not shown, or -2 if not parseable).
     */
    def int getActive() {
        return getCount(activeCount)
    }

    /**
     * Returns the number of Missing servers, as displayed in parentheses near
     * the top of the DB Monitor page; e.g., if "Missing (1)" is displayed,
     * then 1 is returned (as an int).
     * <p>Note: if the Missing server count is not displayed on the page, then
     * -1 is returned; if the text in parentheses cannot be parsed as an int,
     * then -2 is returned.
     * @return the number of Missing servers, as shown on the page (or -1 if
     * not shown, or -2 if not parseable).
     */
    def int getMissing() {
        return getCount(missingCount)
    }

    /**
     * Returns the number of server Alerts, as displayed in parentheses near
     * the top of the DB Monitor page; e.g., if "Alert (2)" is displayed,
     * then 2 is returned (as an int).
     * <p>Note: if the server Alert count is not displayed on the page, then
     * -1 is returned; if the text in parentheses cannot be parsed as an int,
     * then -2 is returned.
     * @return the number of server Alerts, as shown on the page (or -1 if
     * not shown, or -2 if not parseable).
     */
    def int getAlert() {
        return getCount(alertCount)
    }

    /**
     * Returns the number of Joining servers, as displayed in parentheses near
     * the top of the DB Monitor page; e.g., if "Joining (0)" is displayed,
     * then 0 is returned (as an int).
     * <p>Note: if the Joining server count is not displayed on the page, then
     * -1 is returned; if the text in parentheses cannot be parsed as an int,
     * then -2 is returned.
     * @return the number of Joining servers, as shown on the page (or -1 if
     * not shown, or -2 if not parseable).
     */
    def int getJoining() {
        return getCount(joiningCount)
    }

    /**
     * Returns true if the Server list is currently open (displayed).
     * @return true if the Server list is currently open (displayed).
     */
    def boolean isServerListOpen() {
        return serverList.displayed
    }

    /**
     * Opens the Server list, by clicking on the "Server" button (if it's not
     * open already).
     */
    def boolean openServerList() {
        clickToDisplay(serverButton, serverList)
        waitFor { servName.last().displayed }
        waitFor { servMemory.last().displayed }
        return true
    }

    def boolean isDrSectionOpen(){
        return divDrReplication.displayed
    }

    /**
     * Closes the Server list, by clicking on the "Server" button (if it's not
     * closed already).
     */
    def boolean closeServerList() {
        clickToNotDisplay(serverButton, serverList)
        waitFor { !servName.first().displayed }
        waitFor { !servMemory.first().displayed }
        return true
    }

    /**
     * Returns the Server Names, as displayed on the Server list of the DB
     * Monitor page after clicking the "Server" button.
     * <p>Note: as a side effect, opens (if needed) and then closes the Server
     * Name list, by clicking the "Server" button.
     * @return the list of Server Names.
     */
    def List<String> getServerNames() {
        def names = []
        openServerList()
        servName.each { names.add(it.text()) }
        closeServerList()
        return names
    }

    /**
     * Returns the Memory Usage percentages, as displayed on the Server list
     * of the DB Monitor page after clicking the "Server" button - as Strings,
     * including the % signs.
     * <p>Note: as a side effect, opens (if needed) and then closes the Server
     * Name list, by clicking the "Server" button.
     * @return the list of Memory Usages (as Strings).
     */
    def List<String> getMemoryUsages() {
        def memUsages = []
        openServerList()
        servMemory.each { memUsages.add(it.text()) }
        closeServerList()
        return memUsages
    }

    /**
     * Returns the Memory Usage percentages, as displayed on the Server list
     * of the DB Monitor page after clicking the "Server" button - as Floats,
     * without the % signs.
     * <p>Note: as a side effect, opens (if needed) and then closes the Server
     * Name list, by clicking the "Server" button.
     * @return the list of Memory Usages (as Floats).
     */
    def List<Float> getMemoryUsagePercents() {
        def percents = []
        getMemoryUsages().each { percents.add(Float.parseFloat(it.replace("%", ""))) }
        return percents
    }

    /**
     * Returns the Memory Usage percentage of the specified server, as
     * displayed on the Server list of the DB Monitor page after clicking
     * the "Server" button - as a String, including the % sign.
     * <p>Note: as a side effect, opens (if needed) and then closes the Server
     * Name list, by clicking the "Server" button.
     * @param serverName - the name of the server whose Memory Usage is wanted.
     * @return the specified Memory Usage percentage (as String).
     */
    def String getMemoryUsage(String serverName) {
        openServerList()
        waitFor { servName.filter(text: serverName).parent().next().next().find('.memory-status').displayed }
        String text = servName.filter(text: serverName).parent().next().next().find('.memory-status').text()
        closeServerList()
        return text
    }

    /**
     * Returns the Memory Usage percentage of the specified server, as
     * displayed on the Server list of the DB Monitor page after clicking
     * the "Server" button - as a float, without the % signs.
     * <p>Note: as a side effect, opens (if needed) and then closes the Server
     * Name list, by clicking the "Server" button.
     * @param serverName - the name of the server whose Memory Usage is wanted.
     * @return the specified Memory Usage percentage (as float).
     */
    def float getMemoryUsagePercent(String serverName) {
        return Float.parseFloat(getMemoryUsage(serverName).replace("%", ""))
    }

    /**
     * Returns true if the "Graph" area (containing up to four graphs) is
     * currently open (displayed).
     * @return true if the "Graph" area is currently open.
     */
    def boolean isGraphAreaOpen() {
        return graphsArea.displayed
    }

    /**
     * Opens the "Graph" area (containing up to four graphs), by clicking on
     * "Show/Hide Graph" (if it's not open already).
     */
    def boolean openGraphArea() {
        clickToDisplay(showHideGraph, graphsArea)
        return true
    }

    /**
     * Closes the "Graph" area (containing up to four graphs), by clicking on
     * "Show/Hide Graph" (if it's not closed already).
     */
    def boolean closeGraphArea() {
        clickToNotDisplay(showHideGraph, graphsArea)
        return true
    }



    /**
     * Returns true if the "Data" area (containing Stored Procedures and
     * Database Tables info) is currently open (displayed).
     * @return true if the "Data" area is currently open.
     */
    def boolean isDataAreaOpen() {
        return dataArea.displayed
    }

    def boolean isDrAreaOpen() {
        return drSection.displayed
    }

    def boolean isCmdLogSectionOpen() {
        return showHideCLPBlock.displayed
    }

    def boolean isCLPAreaOpen() {
        return clpSection.displayed
    }

    /**
     * Opens the "Data" area (containing Stored Procedures and Database Tables
     * info), by clicking on "Show/Hide Data" (if it's not open already).
     */
    def boolean openDataArea() {
        clickToDisplay(showHideData, dataArea)
        return true
    }

    def boolean openDrArea(){
        clickToDisplay(showHideDrBlock, drSection)
        return true
    }

    def boolean openCLPArea(){
        clickToDisplay(showHideCLPBlock, clpSection)
        return true
    }

    /**
     * Closes the "Data" area (containing Stored Procedures and Database Tables
     * info), by clicking on "Show/Hide Data" (if it's not closed already).
     */
    def boolean closeDataArea() {
        clickToNotDisplay(showHideData, dataArea)
        return true
    }

    def boolean closeDrArea() {
        clickToNotDisplay(showHideDrBlock, drSection)
        return true
    }

    def boolean closeCLPArea() {
        clickToNotDisplay(showHideCLPBlock, clpSection)
        return true
    }

    /**
     *  Edits from here
     */

    /**
     * Check if preference button is displayed
     */
    def boolean displayPreferenceDisplayed() {
        return displayPreference.displayed
    }

    /**
     * Check if graph view button is displayed
     */
    def boolean graphViewDisplayed() {
        return graphView.displayed
    }

    /**
     * Check if Title of Preferences is displayed
     */
    def boolean preferencesTitleDisplayed() {
        return preferencesTitle.displayed
    }

    def boolean drMasterTitleDisplayed(){
        return drMasterTitle.displayed
    }

    def boolean drReplicaTitleDisplayed(){
        return drReplicaTitle.displayed
    }

    def boolean drCLPTitleDisplayed(){
        return drCLPTitle.displayed
    }

    /**
     * Check if Save button of Preferences is displayed
     */
    def boolean savePreferencesBtnDisplayed() {
        return savePreferencesBtn.displayed
    }

    /**
     * Check if Popup Close is displayed
     */
    def boolean popupCloseDisplayed() {
        return popupClose.displayed
    }

    /**
     * Check if Server CPU is displayed
     */
    def boolean serverCpuDisplayed() {
        return serverCpu.displayed
    }

    /**
     * Check if Server RAM is displayed
     */
    def boolean serverRamDisplayed() {
        return serverRam.displayed
    }

    /**
     * Check if Cluster Latency is displayed
     */
    def boolean clusterLatencyDisplayed() {
        return clusterLatency.displayed
    }

    /**
     * Check if Cluster Transactions is displayed
     */
    def boolean clusterTransactionsDisplayed() {
        return clusterTransactions.displayed
    }

    /**
     * Check if Partition Idle Time is displayed
     */
    def boolean partitionIdleTimeDisplayed() {
        return partitionIdleTime.displayed
    }

    /**
     * Check if Stored Procedures is displayed
     */
    def boolean storedProceduresDisplayed() {
        return storedProcedures.displayed
    }

    /**
     * Check if Data Tables is displayed
     */
    def boolean dataTablesDisplayed() {
        return dataTables.displayed
    }

    /*
     *  Returns true if Checkbox for Server CPU in preferences
     */
    def boolean serverCpuCheckboxDisplayed() {
        return serverCpuCheckbox.displayed
    }

    /*
     *  Returns true if Checkbox for Server RAM in preferences
     */
    def boolean serverRamCheckboxDisplayed() {
        return serverRamCheckbox.displayed
    }

    /*
     *  Returns true if Checkbox for Cluster Latency in preferences
     */
    def boolean clusterLatencyCheckboxDisplayed() {
        return clusterLatencyCheckbox.displayed
    }

    /*
     *  Returns true if Checkbox for Cluster Transactions in preferences
     */
    def boolean clusterTransactionsCheckboxDisplayed() {
        return clusterTransactionsCheckbox.displayed
    }

    /*
     *  Returns true if Checkbox for Partition Idle Time in preferences
     */
    def boolean partitionIdleTimeCheckboxDisplayed() {
        return partitionIdleTimeCheckbox.displayed
    }

    /*
     *  Returns true if Checkbox for Stored Procedures in preferences
     */
    def boolean storedProceduresCheckboxDisplayed() {
        return storedProceduresCheckbox.displayed
    }

    /*
     *  Returns true if Checkbox for Data Tables in preferences
     */
    def boolean dataTablesCheckboxDisplayed() {
        return dataTablesCheckbox.displayed
    }

    /*
     *  Returns true if display preferences is clicked
     */
    def boolean openDisplayPreference() {
        displayPreference.click()
    }

    /*
     *  Returns true if save button of preferences is clicked
     */
    def boolean savePreferences() {
        savePreferencesBtn.click()
    }

    /*
     *  Returns true if close popup button is clicked
     */
    def boolean closePreferences() {
        popupClose.click()
    }

    /*
     *  Click the check in Server CPU Checkbox
     */
    def boolean serverCpuCheckboxClick() {
        serverCpuCheckbox.click()
    }

    /*
     *  Click the check in Server RAM Checkbox
     */
    def boolean serverRamCheckboxClick() {
        serverRamCheckbox.click()
    }

    /*
     *  Click the check in Cluster Latency Checkbox
     */
    def boolean clusterLatencyCheckboxClick() {
        clusterLatencyCheckbox.click()
    }

    /*
     *  Click the check in Cluster Transactions Checkbox
     */
    def boolean clusterTransactionsCheckboxClick() {
        clusterTransactionsCheckbox.click()
    }

    /*
     *  Click the check in Partition Idle Time Checkbox
     */
    def boolean partitionIdleTimeCheckboxClick() {
        partitionIdleTimeCheckbox.click()
    }

    /*
     *  Click the check in Stored Procedures Checkbox
     */
    def boolean storedProceduresCheckboxClick() {
        storedProceduresCheckbox.click()
    }

    /*
     *  Click the check in Data Tables Checkbox
     */
    def boolean dataTablesCheckboxClick() {
        dataTablesCheckbox.click()
    }

    def boolean chooseGraphView( String choice ) {
        graphView.value(choice)
    }

    def int changeToMinute( String string ) {
        String minute = string.substring(3, string.length()-3)
        int minuteInt = Integer.parseInt(minute)
        return minuteInt
    }

    def int changeToHour(String string) {
        String hour = string.substring(0, string.length()-6)
        int hourInt = Integer.parseInt(hour)
        return hourInt
    }

    def int changeToDate(String string) {
        String date = string.substring(0, 2)
        int dateInt = Integer.parseInt(date)
        return dateInt
    }

    def String changeToMonth(String string) {
        String date = string.substring(3, string.length()-9)
        return date
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
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryDbMonitor.txt"));
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
    def String getQueryToDeleteTableAndView() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryDbMonitor.txt"));
        String line;
        String query = ""

        while((line = br.readLine()) != "#delete") {
        }

        while ((line = br.readLine()) != "#deleteOnly") {
            // process the line.
            query = query + line + "\n"
        }

        return query
    }

    /*

     */
    def String getQueryToDeleteTableOnly() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryDbMonitor.txt"));
        String line;
        String query = ""

        while((line = br.readLine()) != "#deleteOnly") {
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
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryDbMonitor.txt"));
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
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryDbMonitor.txt"));
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
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryDbMonitor.txt"));
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
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryDbMonitor.txt"));
        String line;
        String query = ""

        while((line = br.readLine()) != "#procedureName") {
        }

        while ((line = br.readLine()) != null) {
            // process the line.
            query = query + line + "\n"
        }

        return query
    }

    /*
     * get tablename that is created and deleted
     */
    def String getTablename() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryDbMonitor.txt"));
        String line;
        String query = ""

        while((line = br.readLine()) != "#name") {
        }

        /*while ((line = br.readLine()) != null) {
            query = query + line + "\n"
        }*/

        query = br.readLine()

        return query
    }

    //server search

    def String getValidPath() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/serversearch.txt"))
        String validPath

        while((validPath = br.readLine()) != "#servername") {
        }

        validPath = br.readLine()

        return validPath
    }

    def String getInvalidPath() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/serversearch.txt"))
        String invalidPath

        while((invalidPath = br.readLine()) != "#invalidservername") {
        }

        invalidPath = br.readLine()

        return invalidPath
    }

    def String getEmptyPath() {
        return ""
    }

    /*
     *  search the tablename in Database Tables
     */
    def boolean searchDatabaseTable(String tablename) {
        filterDatabaseTable.value(tablename)
    }

    /*
     *  return true if table is in ascending order
     *  to check ascending order, check the class "sorttable_sorted" displayed
     */
    def boolean tableInAscendingOrder() {
        if ( ascendingDT.displayed )
            return true
        else
            return false
    }

    /*
     *  return true if table is in ascending order
     *  to check ascending order, check the class "sorttable_sorted" displayed
     */
    def boolean tableInDescendingOrder() {
        if ( descendingDT.displayed )
            return true
        else
            return false
    }

    /*
     *  return true if table is in descending order
     *  to check descending order, check the class "sorting_desc" displayed
     */
    def boolean tableInDescendingOrderDT() {
        if ( descendingDT.displayed )
            return true
        else
            return false
    }

    /*
     *  click the table column in database table
     */
    def boolean clickTable() {
        table.click()
    }

    /*
     *  click the row count column in database table
     */
    def boolean clickRowcount() {
        rowcount.click()
    }

    /*
     *  click the max rows column in database table
     */
    def boolean clickMaxRows() {
        maxrows.click()
    }

    /*
     *  click the partitionID column in database table
     */
    def boolean clickPartitionID() {
        partitionID.click()
    }

    /*
     *  click the status column in DR MAster table
     */
    def boolean clickStatus() {
        status.click()
    }

    /*
     *  click the status column in DR Replica table
     */
    def boolean clickReplicaStatus() {
        replicaStatus.click()
    }

    /*
     *  click the status column in Replication Rate 1min table
     */
    def boolean clickReplicationRate1() {
        replicationRate1.click()
    }

    /*
     *  click the ReplicationRate5 column in DR Replica table
     */
    def boolean clickReplicationRate5() {
        replicationRate5.click()
    }

    /*
     *  click the cmdServer column in  Command log table
     */
    def boolean clickCmdServer() {
        cmdServer.click()
    }

    /*
     *  click the CmdPendingBytes column in  Command log table
     */
    def boolean clickCmdPendingBytes() {
        cmdPendingBytes.click()
    }

    /*
     *  click the CmdPendingTrans column in  Command log table
     */
    def boolean clickCmdPendingTrans() {
        cmdPendingTrans.click()
    }

    /*
     *  click the TotalSegments column in  Command log table
     */
    def boolean clickCmdTotalSegments() {
        cmdTotalSegments.click()
    }

    /*
   *    click the SegmentsInUse column in  Command log table
   */
    def boolean clickCmdSegmentsInUse() {
        cmdSegmentsInUse.click()
    }

    /*
     *    click the FsyncInterval column in  Command log table
     */
    def boolean clickCmdFsyncInterval() {
        cmdFsyncInterval.click()
    }

    /*
     *  click the min rows column in database table
     */
    def boolean clickMinRows() {
        minrows.click()
    }

    /*
     *  click the avg rows column in database table
     */
    def boolean clickAvgRows() {
        avgrows.click()
    }

    /*
     *  click the type column in database table
     */
    def boolean clickTabletype() {
        tabletype.click()
    }

    // for stored procedure

    /*
     *  return true if stored procedures table is displayed
     */
    def boolean storedProceduresTableDisplayed() {
        waitFor     { storedProcedures.displayed }
    }

    /*
     *  return true if data in stored procedures table is displayed
     */
    def boolean storedProceduresDataDisplayed() {
        waitFor(20) { storedProceduresNodataMsg.displayed }
    }

    /*
     *  return true if stored procedures table is displayed
     */
    def boolean databaseTableDisplayed() {
        waitFor     { dataTables.displayed }
    }

    /*
     *  return true if data in stored procedures table is displayed
     */
    def boolean sdatabaseTableDisplayed() {
        waitFor(20) { databasetableNoDataMsg.displayed }
    }

    // for ascending descending in the stored procedures

    /*
     *  click the stored procedure in database table
     */
    def boolean clickStoredProcedure() {
        storedProcedure.click()
    }

    /*
     *  click the row count column in database table
     */
    def boolean clickInvocations() {
        invocations.click()
    }

    /*
     *  click the max rows column in database table
     */
    def boolean clickMinLatency() {
        minLatency.click()
    }

    /*
     *  click the min rows column in database table
     */
    def boolean clickMaxLatency() {
        maxLatency.click()
    }

    /*
     *   click the avg rows column in database table
     */
    def boolean clickAvgLatency() {
        avgLatency.click()
    }

    /*
     *  click the type column in database table
     */
    def boolean clickTimeOfExecution() {
        timeOfExecution.click()
    }

    /*
     *  set value in alert threshold and save
     */
    def boolean setAlertThreshold(int threshold) {
        serverButton.click()

        if (threshold < 0 && threshold >100) {
            println("the set value for threshold is not valid")
            return false
        }

        waitFor     { alertThreshold.displayed }
        waitFor     { saveThreshold.displayed }

        alertThreshold.value(threshold)
        saveThreshold.click()
    }

    /*
     *
     */
    def String compareTime(String stringTwo, String stringOne) {
        int hourOne = changeToHour(stringOne)
        int hourTwo = changeToHour(stringTwo)
        int minuteOne = changeToMinute(stringOne)
        int minuteTwo = changeToMinute(stringTwo)

        String result = ""

        if(hourTwo-hourOne == 0) {
            result = "seconds"
        }
        else {
            if((minuteOne - minuteTwo) > 20 ) {
                result = "seconds"
            }
            else {
                result = "minutes"
            }
        }

        return result
    }

    /*
     *  openPartitionIdleGraph opens and checks the
     *
     */
    def boolean openPartitionIdleGraph() {
        openDisplayPreference()
        preferencesTitleDisplayed()
        savePreferencesBtnDisplayed()
        popupCloseDisplayed()

        partitionIdleTimeCheckboxDisplayed()
        partitionIdleTimeCheckboxClick()
        savePreferences()

        if (partitionIdleTimeDisplayed())
            return true
        else
            return false
    }

    def boolean closePartitionIdleGraph() {
        openDisplayPreference()
        preferencesTitleDisplayed()
        savePreferencesBtnDisplayed()
        popupCloseDisplayed()

        partitionIdleTimeCheckboxDisplayed()
        partitionIdleTimeCheckboxClick()
        savePreferences()

        if (partitionIdleTimeDisplayed())
            return false
        else
            return true
    }
}
