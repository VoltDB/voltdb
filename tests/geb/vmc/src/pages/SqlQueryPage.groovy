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

import geb.error.RequiredPageContentNotPresent
import geb.navigator.Navigator
import geb.waiting.WaitTimeoutException

import org.openqa.selenium.support.ui.Select
import org.openqa.selenium.WebElement

/**
 * This class represents the 'SQL Query' tab of the VoltDB Management Center
 * page, which is the VoltDB web UI (replacing the old Web Studio).
 */
class SqlQueryPage extends VoltDBManagementCenterPage {
    static content = {
        // Tables, Streams, Views & Stored Procedures elements
        tabArea     { $('#tabMain') }
        tabControls { tabArea.find('.tabs') }
        tablesTab   { tabControls.find("a[href='#tab1']") }
        streamsTab  { tabControls.find("a[href='#tab2']") }
        viewsTab    { tabControls.find("a[href='#tab3']") }
        storedProcsTab  { tabControls.find("a[href='#tab4']") }
        listsArea   { tabArea.find('#tabScroller') }
        tablesNames { listsArea.find('#accordionTable').find('h3') }
        streamsNames{ listsArea.find('#accordionStreamedTable').find('h3') }
        viewsNames  { listsArea.find('#accordionViews').find('h3') }
        storedProcs { listsArea.find('#accordionProcedures') }
        systemStoredProcsHeader  { storedProcs.find('.systemHeader').first() }
        defaultStoredProcsHeader { systemStoredProcsHeader.next('.systemHeader') }
        userStoredProcsHeader    { storedProcs.find('.systemHeader').last() }
        systemStoredProcs   { storedProcs.find('#systemProcedure').find('h3') }
        defaultStoredProcs  { storedProcs.find('#defaultProcedure').find('h3') }
        userStoredProcs     { storedProcs.find('#userProcedure').find('h3') }
        allStoredProcs  { storedProcs.find('h3') }

        // Query elements
        queryInput  { $('#querybox-1') }
        runButton   { $('#runBTn-1') }
        clearButton { $('#clearQuery-1') }
        qrFormatDropDown    { $('#exportType-1') }
        qrfddOptions    { qrFormatDropDown.find('option') }
        qrfddSelected   { qrFormatDropDown.find('option', selected: "selected") }
        queryRes        { $('.queryResult-1') }
        queryResHtml    { queryRes.find('#resultHtml-1') }
        queryTables     (required: false) { queryResHtml.find('table') }
        queryErrHtml    (required: false) { queryResHtml.find('.errorValue') }
        queryDur        { $('#queryResults-1') }


        //popup query ok and cancel
        cancelpopupquery        { $("#btnQueryDatabasePausedErrorCancel") }
        okpopupquery            { $("#btnQueryDatabasePausedErrorOk") }
        switchadminport         { $("#queryDatabasePausedInnerErrorPopup > div.overlay-contentError.errorQueryDbPause > p:nth-child(3) > span")}
        queryexecutionerror     { $("#queryDatabasePausedInnerErrorPopup > div.overlay-title", text:"Query Execution Error")}
        queryerrortxt           { $("#queryDatabasePausedInnerErrorPopup > div.overlay-contentError.errorQueryDbPause > p:nth-child(1)")}

        htmltableresult         { $("#table_r0_html_0")}
        createerrorresult       { $("#resultHtml-1 > span")}
        htmlresultallcolumns    { $("#table_r0_html_0 > thead")}

        htmlresultselect        { $("#table_r0_html_0 > thead > tr")}
        refreshquery            { $("#tabMain > button", text:"Refresh")}

        //options
        htmlOptions             { $("option", text:"HTML") }
        csvOptions              { $("option", text:"CSV") }
        monospaceOptions        { $("option", text:"Monospace") }

        // for view
        checkview       { $("#tabMain > ul > li.active > a")}

        //result
        resultHtml      { $("#resultHtml-1") }
        resultCsv       { $("#resultCsv-1") }
        resultMonospace { $("#resultMonospace-1") }

        errorObjectNameAlreadyExist     { $("span", class:"errorValue") }

        // Query Box
        addQueryTab             { $("#new-query > span") }
        saveTabPopupOk          { $("#btnSaveQueryOk") }
        saveTabPopupTextField   { $("#txtQueryName") }

        deleteTabOk             { $("#btnCloseTabOk") }
        deleteTabCancel         { $("#btnCloseTabCancel") }

        // SqlQueriesTextBox
        queryResultBox          { $("#table_r0_html_0 > tbody") }
        queryResultBoxTd        { queryResultBox.find("td") }
    }
    static at = {
        sqlQueryTab.displayed
        sqlQueryTab.attr('class') == 'active'
        tablesTab.displayed
        streamsTab.displayed
        viewsTab.displayed
        storedProcsTab.displayed
        listsArea.displayed
//        queryInput.displayed
//        queryRes.displayed
//        queryDur.displayed
    }
    boolean textHasChanged = false

    /**
     * Displays the list of Tables (by clicking the "Tables" tab).
     */
    def showTables() {
        clickToDisplay(tablesTab, tablesNames)
    }

    /**
     * Displays the list of Streams (by clicking the "Streams" tab).
     */
    def showStreams() {
        clickToDisplay(streamsTab, streamsNames)
    }

    /**
     * Displays the list of Views (by clicking the "Views" tab).
     */
    def showViews() {
        clickToDisplay(viewsTab, viewsNames)
    }

    /**
     * Displays the list of Stored Procedures (by clicking the "Stored Procedures" tab).
     */
    def showStoredProcedures() {
        clickToDisplay(storedProcsTab, allStoredProcs)
    }

    /**
     * Returns the list of Tables (as displayed on the "Tables" tab).<p>
     * Note: as a side effect, the "Tables" tab is opened.
     * @return the list of Table names.
     */
    def List<String> getTableNames() {
        def names = []
        showTables()
        tablesNames.each {
            scrollIntoView(it);
            names.add(it.text())
        }
        return names
    }

    /**
     * Returns the list of Streams (as displayed on the "Streams" tab).<p>
     * Note: as a side effect, the "Streams" tab is opened.
     * @return the list of Stream names.
     */
    def List<String> getStreamNames() {
        def names = []
        showStreams()
        streamsNames.each {
            scrollIntoView(it);
            names.add(it.text())
        }
        return names
    }

    /**
     * Returns the list of Views (as displayed on the "Views" tab).<p>
     * Note: as a side effect, the "Views" tab is opened.
     * @return the list of View names.
     */
    def List<String> getViewNames() {
        def names = []
        showViews()
        viewsNames.each {
            scrollIntoView(it);
            names.add(it.text())
        }
        return names
    }

    /**
     * Given two Navigators, for a specific category of Stored Procedures
     * (System, Default or User), returns the list of that category of Stored
     * Procedures (as displayed on the "Stored Procedures" tab, under the
     * specified heading).<p>
     * Note: as a side effect, the "Stored Procedures" tab is opened (if
     * needed), and the specfied list of Stored Procedures  is opened (if
     * needed), and then closed.
     * @param storedProcsHeaderNav - a Navigator specifiying the header for
     * the desired category (System, Default or User) of Stored Procedures.
     * @param storedProcsNav - a Navigator specifiying each of the Stored
     * Procedures the desired category (System, Default or User).
     * @return the list of Default Stored Procedure names.
     */
    private List<String> getSpecifiedStoredProcedures(Navigator storedProcsHeaderNav,
                                                      Navigator storedProcsNav) {
        def storedProcs = []
        try {
            showStoredProcedures()
            clickToDisplay(storedProcsHeaderNav, storedProcsNav)
            storedProcsNav.each {
                scrollIntoView(it)
                storedProcs.add(it.text())
            }
            clickToNotDisplay(storedProcsHeaderNav, storedProcsNav)
        } catch (RequiredPageContentNotPresent e) {
            // do nothing: empty list will be returned
        }
        return storedProcs
    }

    /**
     * Returns the list of System Stored Procedures (as displayed on the
     * "Stored Procedures" tab, under the "System Stored Procedures" heading).<p>
     * Note: as a side effect, the "Stored Procedures" tab is opened (if
     * needed), and the "System Stored Procedures" list is opened (if needed),
     * and then closed.
     * @return the list of System Stored Procedure names.
     */
    def List<String> getSystemStoredProcedures() {
        return getSpecifiedStoredProcedures(systemStoredProcsHeader, systemStoredProcs)
    }

    /**
     * Returns the list of Default Stored Procedures (as displayed on the
     * "Stored Procedures" tab, under the "Default Stored Procedures" heading).<p>
     * Note: as a side effect, the "Stored Procedures" tab is opened (if
     * needed), and the "Default Stored Procedures" list is opened (if needed),
     * and then closed.
     * @return the list of Default Stored Procedure names.
     */
    def List<String> getDefaultStoredProcedures() { // defaultStoredProcsHeader
        return getSpecifiedStoredProcedures(defaultStoredProcsHeader, defaultStoredProcs)
    }

    /**
     * Returns the list of User-defined Stored Procedures (as displayed on
     * the "Stored Procedures" tab, after the "System Stored Procedures" and
     * "Default Stored Procedures").<p>
     * Note: as a side effect, the "Stored Procedures" tab is opened (if
     * needed).
     * @return the list of User-defined Stored Procedure names.
     */
    def List<String> getUserStoredProcedures() {
        def storedProcs = []
        try {
            showStoredProcedures()
            clickToDisplay(userStoredProcsHeader, userStoredProcs)
            userStoredProcs.each {
                scrollIntoView(it)
                storedProcs.add(it.text())
            }
            clickToNotDisplay(userStoredProcsHeader, userStoredProcs)
        } catch (RequiredPageContentNotPresent e) {
            // do nothing: empty list will be returned
        }
        return storedProcs
    }

    /**
     * Returns the complete list of all Stored Procedures (as displayed on
     * the "Stored Procedures" tab), including the System, Default, and
     * User-defined Stored Procedures.<p>
     * Note: as a side effect, the "Stored Procedures" tab is opened (if
     * needed), and the "System Stored Procedures" and "Default Stored
     * Procedures" lists are opened (if needed), and then closed.
     * @return the list of all Stored Procedure names.
     */
    def List<String> getAllStoredProcedures() {
        def names = getSystemStoredProcedures()
        names.addAll(getDefaultStoredProcedures())
        names.addAll(getUserStoredProcedures())
        return names
    }

    /**
     * Returns the list of Columns (as displayed on the "Tables", "Streams",
     * or "Views" tab), for the specified table or view; each column returned
     * includes both the column name and type, e.g. "ROWID (bigint)".
     * <p>
     * Note: as a side effect, the "Tables", "Streams", or "Views" tab is opened
     * (if needed), and the specified table, stream, or view is opened (if
     * needed), and then closed.
     * @param name - the name of the table, stream, or view whose columns are
     * to be returned.
     * @param type - the type of object whose columns are to be returned; should
     * be 'table' (default value), 'stream', or 'view'
     * @return the list of (table, stream, or view) Column names and data types.
     */
    private def List<String> getColumns(String name, String type='table') {
        def names = null
        if ("stream".equalsIgnoreCase(type)) {
            showStreams()
            names = streamsNames
        } else if ("view".equalsIgnoreCase(type)) {
            showViews()
            names = viewsNames
        } else {
            showTables()
            names = tablesNames
        }
        def columns = []
        names.each {
            scrollIntoView(it)
            if (it.text() == name) {
                def columnList = it.next()
                clickToDisplay(it, columnList)
                columnList.find('ul').find('li').each {
                    scrollIntoView(it)
                    columns.add(it.text())
                }
                clickToNotDisplay(it, columnList)
            }
        }
        return columns
    }

    /**
     * Returns the list of Columns (as displayed on the "Tables" tab), for the
     * specified table; each column returned includes both the column name and
     * type, e.g. "ROWID (bigint)".<p>
     * Note: as a side effect, the "Tables" tab is opened (if needed), and the
     * specified table is opened (if needed), and then closed.
     * @param tableName - the name of the table whose columns are to be returned.
     * @return the list of table Column names and data types.
     */
    def List<String> getTableColumns(String tableName) {
        return getColumns(tableName)
    }

    /**
     * Returns the list of Columns (as displayed on the "Streams" tab), for the
     * specified stream; each column returned includes both the column name and
     * type, e.g. "ROWID (bigint)".<p>
     * Note: as a side effect, the "Streams" tab is opened (if needed), and the
     * specified stream is opened (if needed), and then closed.
     * @param streamName - the name of the stream whose columns are to be returned.
     * @return the list of stream Column names and data types.
     */
    def List<String> getStreamColumns(String streamName) {
        return getColumns(streamName, 'stream')
    }

    /**
     * Returns the list of Columns (as displayed on the "Views" tab), for the
     * specified view; each column returned includes both the column name and
     * type, e.g. "RECORD_COUNT (integer)".<p>
     * Note: as a side effect, the "Views" tab is opened (if needed), and the
     * specified view is opened (if needed), and then closed.
     * @param viewName - the name of the view whose columns are to be returned.
     * @return the list of view Column names and data types.
     */
    def List<String> getViewColumns(String viewName) {
        return getColumns(viewName, 'view')
    }

    /**
     * Returns the list of Column names (as displayed on the "Tables" tab), for
     * the specified table.<p>
     * Note: as a side effect, the "Tables" tab is opened (if needed), and the
     * specified table is opened (if needed), and then closed.
     * @param tableName - the name of the table whose column names are to be
     * returned.
     * @return the list of table Column names.
     */
    def List<String> getTableColumnNames(String tableName) {
        def columns = getTableColumns(tableName)
        columns = columns.collect { it.substring(0, it.indexOf('(')).trim() }
        return columns
    }

    /**
     * Returns the list of Column data types (as displayed on the "Tables"
     * tab), for the specified table.<p>
     * Note: as a side effect, the "Tables" tab is opened (if needed), and the
     * specified table is opened (if needed), and then closed.
     * @param tableName - the name of the table whose column types are to be
     * returned.
     * @return the list of table Column data types.
     */
    def List<String> getTableColumnTypes(String tableName) {
        def columns = getTableColumns(tableName)
        columns = columns.collect { it.substring(it.indexOf('(')+1).replace(")", "").trim() }
        return columns
    }

    /**
     * Returns the list of Column names (as displayed on the "Streams" tab), for
     * the specified stream.<p>
     * Note: as a side effect, the "Streams" tab is opened (if needed), and the
     * specified stream is opened (if needed), and then closed.
     * @param streamName - the name of the stream whose column names are to be
     * returned.
     * @return the list of stream Column names.
     */
    def List<String> getStreamColumnNames(String streamName) {
        def columns = getStreamColumns(streamName)
        columns = columns.collect { it.substring(0, it.indexOf('(')).trim() }
        return columns
    }

    /**
     * Returns the list of Column data types (as displayed on the "Streams"
     * tab), for the specified stream.<p>
     * Note: as a side effect, the "Streams" tab is opened (if needed), and the
     * specified stream is opened (if needed), and then closed.
     * @param streamName - the name of the stream whose column types are to be
     * returned.
     * @return the list of stream Column data types.
     */
    def List<String> getStreamColumnTypes(String streamName) {
        def columns = getStreamColumns(streamName)
        columns = columns.collect { it.substring(it.indexOf('(')+1).replace(")", "").trim() }
        return columns
    }

    /**
     * Returns the list of Column names (as displayed on the "Views" tab), for
     * the specified view.<p>
     * Note: as a side effect, the "Views" tab is opened (if needed), and the
     * specified view is opened (if needed), and then closed.
     * @param viewName - the name of the view whose column names are to be
     * returned.
     * @return the list of view Column names.
     */
    def List<String> getViewColumnNames(String viewName) {
        def columns = getViewColumns(viewName)
        columns = columns.collect { it.substring(0, it.indexOf('(')).trim() }
        return columns
    }

    /**
     * Returns the list of Column data types (as displayed on the "Views"
     * tab), for the specified view.<p>
     * Note: as a side effect, the "Views" tab is opened (if needed), and the
     * specified view is opened (if needed), and then closed.
     * @param viewName - the name of the view whose column types are to be
     * returned.
     * @return the list of view Column data types.
     */
    def List<String> getViewColumnTypes(String viewName) {
        def columns = getViewColumns(viewName)
        columns = columns.collect { it.substring(it.indexOf('(')+1).replace(")", "").trim() }
        return columns
    }

    /**
     * Enters the specified query text into the Query textarea.
     * @param queryText - the text to be entered into the Query textarea.
     */
    def setQueryText(def queryText) {
        queryInput.jquery.html(queryText)
    }

    /**
     * Returns the current contents of the Query textarea.
     * @return the current contents of the Query textarea.
     */
    def String getQueryText() {
        return queryInput.text()
    }

    /**
     * Clears the Query text (by clicking the "Clear" button).
     */
    def clearQuery() {
        clearButton.click()
    }

    /**
     * Returns true if the text of the query duration element has changed to a
     * different value - even if it subsequently changed back to the same value.
     * @param navDurElem - a Navigator specifying the query duration element
     * to be checked for having changed.
     * @param initDurText - the original text of the query duration element,
     * before running a new query.
     * @return true if the query duration text has changed.
     */
    private boolean hasChanged(Navigator navDurElem, String initDurText) {
        if (textHasChanged) {
            return true
        }
        if (navDurElem.text() != initDurText ) {
            textHasChanged = true
        }
        return textHasChanged
    }

    /**
     * Runs whatever query is currently listed in the Query text
     * (by clicking the "Run" button).
     */
    def runQuery() {
        String initQueryDurationText = queryDur.text()
        runButton.click()

        // Wait for both the query result and duration to be displayed, with
        // (non-null) non-empty text; and for the latter to have changed
        try {
            textHasChanged = false
            waitFor() {
                hasChanged(queryDur, initQueryDurationText) &&
                        isDisplayed(queryRes) && queryRes.text() != null && !queryRes.text().isEmpty() &&
                        isDisplayed(queryDur) && queryDur.text() != null && !queryDur.text().isEmpty()
            }
        } catch (WaitTimeoutException e) {
            String message = '\nIn SqlQueryPage.runQuery(), caught WaitTimeoutException; this is probably nothing to worry about'
            println message + '.'
            println 'See Standard error for stack trace.'
            println 'Previous Duration text: ' + initQueryDurationText
            println 'Current  Duration text: ' + queryDur.text()
            println 'Duration text changed : ' + hasChanged(queryDur, initQueryDurationText)
            println 'Current  Result   text:\n' + queryRes.text()
            System.err.println message + ':'
            e.printStackTrace()
        }
        return this
    }

    /**
     * Enters the specified query text into the Query textarea, and then runs
     * that query (by clicking the "Run" button).
     * @param queryText - the text of the query to be entered and run.
     */
    def runQuery(String queryText) {
        setQueryText(queryText)
        runQuery()
    }

    /**
     * Returns a list of (the text of) the options available on query result
     * format drop-down menu. (Typically, these are "HTML", "CSV" and
     * "Monospace".)
     */
    def List<String> getQueryResultFormatOptions() {
        List<String> options = []
        qrfddOptions.each { options.add(it.text()) }
        return options
    }

    /**
     * Returns a list of the values of the options available on query result
     * format drop-down menu. (Typically, these are the same as the text
     * values, i.e., normally "HTML", "CSV" and "Monospace".)
     */
    def List<String> getQueryResultFormatOptionValues() {
        List<String> values = []
        qrfddOptions.each { values.add(it.value()) }
        return values
    }

    /**
     * Returns the value of the currently selected option of the query result
     * format drop-down menu. (Typically, "HTML", "CSV" or "Monospace".)
     */
    def String getSelectedQueryResultFormat() {
        return qrFormatDropDown.value()
    }

    /**
     * Sets the query result format drop-down menu to the specified value.
     * @param format - the value to which the menu should be set (typically
     * "HTML", "CSV" or "Monospace").
     */
    def selectQueryResultFormat(String format) {
        qrFormatDropDown.value(format)
    }

    /**
     * Returns the contents of every "table" element in the "Query Result" area,
     * in the form of a List (for each table) of Maps, with each Map element a
     * List of Strings; each Key of the Map is a column header of the table,
     * and its List contains the displayed text of that column.
     * @param colHeaderFormat - the case in which you want each table's column
     * headers returned: converted to lower case, to upper case, or as-is.
     * @return a List of Maps representing the contents of every table.
     */
    def List<Map<String,List<String>>> getQueryResults(
            ColumnHeaderCase colHeaderFormat=ColumnHeaderCase.TO_LOWER_CASE) {
        def results = []
        queryTables.each { results.add(getTableByColumn(it, colHeaderFormat)) }
        return results
    }

    /**
     * Returns the contents of the "table" element, in the "Query Result" area,
     * with the specified index, in the form of a Map, with each element a List
     * of Strings; each Key of the Map is a column header of the table, and its
     * List contains the displayed text of that column. For example, calling
     * this method with index 0 will return the first table.
     * @param index - the index (0-based) of the "table" element whose contents
     * are to be returned.
     * @param colHeaderFormat - the case in which you want the table's column
     * headers returned: converted to lower case, to upper case, or as-is.
     * @return a Map representing the contents of the specified table.
     */
    def Map<String,List<String>> getQueryResult(int index,
                                                ColumnHeaderCase colHeaderFormat=ColumnHeaderCase.TO_LOWER_CASE) {
        return getQueryResults(colHeaderFormat).get(index)
    }

    /**
     * Returns the contents of the <i>last</i> "table" element, in the "Query
     * Result" area, in the form of a Map, with each element a List of Strings;
     * each Key of the Map is a column header of the table, and its List
     * contains the displayed text of that column.
     * @param colHeaderFormat - the case in which you want the table's column
     * headers returned: converted to lower case, to upper case, or as-is.
     * @return a Map representing the contents of the <i>last</i> table.
     */
    def Map<String,List<String>> getQueryResult(
            ColumnHeaderCase colHeaderFormat=ColumnHeaderCase.TO_LOWER_CASE) {
        return getTableByColumn(queryTables.last(), colHeaderFormat)
    }

    /**
     * Returns the text of whatever is shown in the "Query Result" area, in its
     * entirety; normally, this would include query results and/or errors, but
     * at times it could also be "Connect to a datasource first.", which would
     * not show up as either a result nor an error message.
     * @return the text of whatever is shown in the "Query Result" area.
     */
    def String getQueryResultText() {
        return queryRes.text()
    }

    /**
     * Returns the text of an error message, if one is displayed; or null,
     * if no error message appears.
     * @return the text of any error message; or null.
     */
    def String getQueryError() {
        return queryErrHtml.text()
    }

    /**
     * Returns the text of the "Query Duration" message (e.g., "Query Duration:
     * 0.012s"), if one is displayed; or null, if none appears.
     * @return the text of any "Query Duration" message; or null.
     */
    def String getQueryDuration() {
        return queryDur.text()
    }

    /*
     * click DbMonitor tab to go to Db Monitor
     */
    def boolean gotoDbMonitor() {
        header.tabDBMonitor.click()
    }

    /*
     * click DbMonitor tab to go to Db Monitor
     */
    def boolean gotoSchema() {
        header.tabSchema.click()
    }

    /*
     * click DbMonitor tab to go to Db Monitor
     */
    def boolean gotoAnalysis() {
        header.tabAnalysis.click()
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
     *  Get delete query
     */
    def String getQueryToDeleteTable() {
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
    * get query to delete a table only
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
     *
     */
    def String getQueryToDropTableAndProcedureQuery() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryAnalysis.txt"));
        String line;
        String query = ""

        while((line = br.readLine()) != "#dropTableAndProcedure") {
        }

        while ((line = br.readLine()) != "#insertQuery") {
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

        while ((line = br.readLine()) != "#index") {
            query = query + line + "\n"
        }

        return query
    }

    //for view

    /*
     * get query to create a view
     */
    def String getQueryToCreateView() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/viewtable.txt"));
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
     *  Get query to delete a view
     */
    def String getQueryToDeleteView() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/viewtable.txt"));
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
     * get viewname that is created and deleted
     */
    def String getViewname() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/viewtable.txt"));
        String line;
        String query = ""

        while((line = br.readLine()) != "#name") {
        }

        while ((line = br.readLine()) != null) {
            query = query + line + "\n"
        }

        return query
    }

    def String getCssPathOfTab(int index) {
        return "#qTab-" + String.valueOf(index) +" > a"
    }

    def String getIdOfQueryBox(int index) {
        return "#querybox-" + String.valueOf(index)
    }

    def String getIdOfSaveButton(int index) {
        return "#querySaveBtn-" + String.valueOf(index)
    }

    def String getIdOfDeleteTab(int index) {
        return "close-tab-" + String.valueOf(index)
    }

    def String getCreateQueryForSqlQueriesTextBoxTest() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueriesTextBox.txt"));
        String line = null;
        String query = "";
        int count = 0;
        while((line = br.readLine()) != null && count <= 2) {
            if (count == 2)
                query = line;
            count++;
        }
        return query;
    }

    def String getDeleteQueryForSqlQueriesTextBoxTest() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueriesTextBox.txt"));
        String line = null;
        String query = "";
        int count = 0;
        while((line = br.readLine()) != null && count <= 1) {
            if (count == 1)
                query = line;
            count++;
        }
        return query;
    }

    def String getInsertQueryForSqlQueriesTextBoxTest() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueriesTextBox.txt"));
        String line = null;
        String query = "";
        int count = 0;
        while((line = br.readLine()) != null && count <= 4) {
            if (count == 4)
                query = line;
            count++;
        }
        return query;
    }

    def String getSelectQueryForSqlQueriesTextBoxTest() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueriesTextBox.txt"));
        String line = null;
        String query = "";
        int count = 0;
        while((line = br.readLine()) != null && count <= 3) {
            if (count == 3)
                query = line;
            count++;
        }
        return query;
    }

    def String getInsertQueryWithSpecialCharactersForSqlQueriesTextBoxTest() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueriesTextBox.txt"));
        String line = null;
        String query = "";
        int count = 0;
        while((line = br.readLine()) != null && count <= 5) {
            if (count == 5)
                query = line;
            count++;
        }
        return query;
    }

    def String getUpdateQueryWithSpecialCharactersForSqlQueriesTextBoxTest() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueriesTextBox.txt"));
        String line = null;
        String query = "";
        int count = 0;
        while((line = br.readLine()) != null && count <= 6) {
            if (count == 6)
                query = line;
            count++;
        }
        return query;
    }

    def String getInsertQueryWithSpacesForSqlQueriesTextBoxTest() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueriesTextBox.txt"));
        String line = null;
        String query = "";
        int count = 0;
        while((line = br.readLine()) != null && count <= 7) {
            if (count == 7)
                query = line;
            count++;
        }
        return query;
    }
}
