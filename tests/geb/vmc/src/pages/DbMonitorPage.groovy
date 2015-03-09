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

package vmcTest.pages

import geb.navigator.Navigator

/**
 * This class represents the 'DB Monitor' tab of the VoltDB Management Center
 * (VMC) page, which is the VoltDB web UI (replacing the old Management Center).
 */
class DbMonitorPage extends VoltDBManagementCenterPage {
    static content = {
        activeIcon      { $('.activeIcon') }
        activeCount     { activeIcon.find('#activeCount') }
        missingIcon     { $('.missingIcon') }
        missingCount    { missingIcon.find('#missingCount') }
        alertIcon       (required: false) { $('.alertIcon') }
        alertCount      (required: false) { alertIcon.find('span') }
        joiningIcon     (required: false) { $('.joiningIcon') }
        joiningCount    (required: false) { joiningIcon.find('span') }
        serverButton    { $('#btnPopServerList') }
        serverList      { $('#popServerList') }
        servers         { serverList.find('.active') }
        servName        { servers.find('a') }
        servMemory      { servers.find('.memory-status') }
        showHideGraph   { $('#showHideGraphBlock') }
        graphsArea      { $('#graphChart') }
        showHideData    { $('#ShowHideBlock') }
        dataArea        { $('.menu_body') }
    }
    static at = {
        dbMonitorTab.displayed
        dbMonitorTab.attr('class') == 'active'
        showHideGraph.displayed
        showHideData.displayed
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
        waitFor { servName.filter(text: serverName).next('.memory-status').displayed }
        String text = servName.filter(text: serverName).next('.memory-status').text()
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

    /**
     * Opens the "Data" area (containing Stored Procedures and Database Tables
     * info), by clicking on "Show/Hide Data" (if it's not open already).
     */
    def boolean openDataArea() {
        clickToDisplay(showHideData, dataArea)
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
}
