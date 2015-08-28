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

import geb.error.RequiredPageContentNotPresent
import geb.navigator.Navigator
import geb.waiting.WaitTimeoutException
import org.openqa.selenium.support.ui.Select

/**
 * This class represents the 'SQL Query' tab of the VoltDB Management Center
 * page, which is the VoltDB web UI (replacing the old Web Studio).
 */
class SqlQueryPage extends VoltDBManagementCenterPage {
    static content = {
        // Tables, Views & Stored Procedures elements
        tabArea     { $('#tabMain') }
        tabControls { tabArea.find('.tabs') }
        tablesTab   { tabControls.find("a[href='#tab1']") }
        viewsTab    { tabControls.find("a[href='#tab2']") }
        storedProcsTab  { tabControls.find("a[href='#tab3']") }
        listsArea   { tabArea.find('#tabScroller') }
        tablesNames { listsArea.find('#accordionTable').find('h3') }
        viewsNames  { listsArea.find('#accordionViews').find('h3') }
        storedProcs { listsArea.find('#accordionProcedures') }
        systemStoredProcsHeader  { storedProcs.find('.systemHeader').first() }
        defaultStoredProcsHeader { systemStoredProcsHeader.next('.systemHeader') }
        userStoredProcsHeader    { storedProcs.find('.systemHeader').last() }
        systemStoredProcs   { storedProcs.find('#systemProcedure').find('h3') }
        defaultStoredProcs  { storedProcs.find('#defaultProcedure').find('h3') }
        userStoredProcs     { storedProcs.find('#userProcedure').find('h3') }
        allStoredProcs  { storedProcs.find('h3') }

        queryStatus			{ $("th", text:"STATUS") }

        // Query elements
        queryInput  { $('#theQueryText') }
        runButton   { $('#runBTn') }
        clearButton { $('#clearQuery') }
        qrFormatDropDown    { $('#exportType') }
        qrfddOptions    { qrFormatDropDown.find('option') }
        qrfddSelected   { qrFormatDropDown.find('option', selected: "selected") }
        queryResHtml    { $('#resultHtml') }
        queryTables     (required: false) { queryResHtml.find('table') }
        queryErrHtml    (required: false) { queryResHtml.find('span') }
        queryDurHtml    { $('#queryResults') }


        //popup query ok and cancel
        cancelpopupquery        { $("#btnQueryDatabasePausedErrorCancel", text:"Cancel")}
        okpopupquery            { $("#btnQueryDatabasePausedErrorOk", text:"Ok")}
        switchadminport         { $("#queryDatabasePausedInnerErrorPopup > div.overlay-contentError.errorQueryDbPause > p:nth-child(3) > span")}
        queryexecutionerror     { $("#queryDatabasePausedInnerErrorPopup > div.overlay-title", text:"Query Execution Error")}
        queryerrortxt           { $("#queryDatabasePausedInnerErrorPopup > div.overlay-contentError.errorQueryDbPause > p:nth-child(1)")}

        htmltableresult 	    { $("#table_r0_html_0")}
        createerrorresult	    { $("#resultHtml > span")}
        htmlresultallcolumns	{ $("#table_r0_html_0 > thead")}

        htmlresultselect	    { $("#table_r0_html_0 > thead > tr")}
        refreshquery		    { $("#tabMain > button", text:"Refresh")}

        //options
        htmlOptions				{ $("option", text:"HTML") }
        csvOptions				{ $("option", text:"CSV") }
        monospaceOptions		{ $("option", text:"Monospace") }

        // for view
        checkview		{ $("#tabMain > ul > li.active > a")}

        //result
        resultHtml		{ $("#resultHtml") }
        resultCsv		{ $("#resultCsv") }
        resultMonospace	{ $("#resultMonospace") }
        
        errorObjectNameAlreadyExist     { $("span", class:"errorValue") }
    }
    static at = {
        sqlQueryTab.displayed
        sqlQueryTab.attr('class') == 'active'
        tablesTab.displayed
        viewsTab.displayed
        storedProcsTab.displayed
        listsArea.displayed
        queryInput.displayed
        queryResHtml.displayed
    }

    /**
     * Displays the list of Tables (by clicking the "Tables" tab).
     */

    def showTables() {
        clickToDisplay(tablesTab, tablesNames)
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
     * Returns the list of Columns (as displayed on the "Tables" or "Views"
     * tab), for the specified table or view; each column returned includes
     * both the column name and type, e.g. "CONTESTANT_NUMBER (integer)".<p>
     * Note: as a side effect, the "Tables" or "Views" tab is opened (if
     * needed), and the specified table or view is opened (if needed), and
     * then closed.
     * @param tableOrViewName - the name of the table or view whose columns
     * are to be returned.
     * @param getViewColumns - if true, get columns for the specified view,
     * rather than table.
     * @return the list of (table or view) Column names and data types.
     */
    private def List<String> getColumns(String tableOrViewName, boolean getViewColumns) {
        def names = null
        if (getViewColumns) {
            showViews()
            names = viewsNames
        } else {
            showTables()
            names = tablesNames
        }
        def columns = []
        names.each {
            scrollIntoView(it)
            if (it.text() == tableOrViewName) {
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
     * type, e.g. "CONTESTANT_NUMBER (integer)".<p>
     * Note: as a side effect, the "Tables" tab is opened (if needed), and the
     * specified table is opened (if needed), and then closed.
     * @param tableName - the name of the table whose columns are to be returned.
     * @return the list of table Column names and data types.
     */
    def List<String> getTableColumns(String tableName) {
        return getColumns(tableName, false)
    }

    /**
     * Returns the list of Columns (as displayed on the "Views" tab), for the
     * specified view; each column returned includes both the column name and
     * type, e.g. "CONTESTANT_NUMBER (integer)".<p>
     * Note: as a side effect, the "Views" tab is opened (if needed), and the
     * specified view is opened (if needed), and then closed.
     * @param viewName - the name of the view whose columns are to be returned.
     * @return the list of view Column names and data types.
     */
    def List<String> getViewColumns(String viewName) {
        return getColumns(viewName, true)
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
        queryInput.value(queryText)
    }

    /**
     * Returns the current contents of the Query textarea.
     * @return the current contents of the Query textarea.
     */
    def String getQueryText() {
        return queryInput.value()
    }

    /**
     * Clears the Query text (by clicking the "Clear" button).
     */
    def clearQuery() {
        clearButton.click()
    }

    /**
     * Runs whatever query is currently listed in the Query text
     * (by clicking the "Run" button).
     */
    def runQuery() {
        String initQueryResultText = queryResHtml.text()
        String initQueryDurationText = queryDurHtml.text()
        runButton.click()
        // TODO: improve this wait, so that it waits for the old element(s) to
        // become "stale", rather than relying on the text to change (which it
        // sometimes does not, which is why we have to catch a WaitTimeoutException
        try {
            waitFor() {
                queryResHtml.text() != null && !queryResHtml.text().isEmpty() &&
                        queryDurHtml.text() != null && !queryDurHtml.text().isEmpty() &&
                        (queryResHtml.text() != initQueryResultText || queryDurHtml.text() != initQueryDurationText)
            }
        } catch (WaitTimeoutException e) {
            String message = '\nIn SqlQueryPage.runQuery(), caught WaitTimeoutException; this is probably nothing to worry about'
            println message + '.'
            System.err.println message + ':'
            e.printStackTrace()
            println 'See Standard error for stack trace.\n'
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
        return queryResHtml.text()
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
        return queryDurHtml.text()
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
	 *	Get delete query
	 */
    def String getQueryToDeleteTable() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryDbMonitor.txt"));
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
     * get tablename that is created and deleted
     */
    def String getTablename() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryDbMonitor.txt"));
        String line;
        String query = ""

        while((line = br.readLine()) != "#name") {
        }

        while ((line = br.readLine()) != null) {
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
	 *	Get delete query
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
}
