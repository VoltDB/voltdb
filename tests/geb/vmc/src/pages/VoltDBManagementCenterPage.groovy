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

import java.util.List;
import java.util.Map;

import geb.*
import geb.navigator.Navigator
import geb.waiting.WaitTimeoutException

import org.openqa.selenium.JavascriptExecutor

/**
 * This class represents a generic VoltDB Management Center page (without
 * specifying which tab you are on), which is the main page of the web UI
 * - the new UI for version 4.9 (November 2014), replacing the old Web Studio
 * (& DB Monitor Catalog Report, etc.).
 */
class VoltDBManagementCenterPage extends Page {
    static Boolean securityEnabled = null;

    static url = '/'  // relative to the baseUrl
    static content = {
        navTabs { $('#nav') }
        dbMonitorTab { navTabs.find('#navDbmonitor') }
        schemaTab    { navTabs.find('#navSchema') }
        sqlQueryTab  { navTabs.find('#navSqlQuery') }
        dbMonitorLink(to: DbMonitorPage) { dbMonitorTab.find('a') }
        schemaLink   (to: SchemaPage)    { schemaTab.find('a') }
        sqlQueryLink (to: SqlQueryPage)  { sqlQueryTab.find('a') }
        loginDialog  (required: false) { $('#loginBox') }
        usernameInput   (required: false) { loginDialog.find('input#username') }
        passwordInput   (required: false) { loginDialog.find('input#password') }
        loginButton  (required: false) { loginDialog.find('#LoginBtn') }
    }
    static at = {
        title == 'VoltDB Management Center'
        dbMonitorLink.displayed
        schemaLink.displayed
        sqlQueryLink.displayed
    }

    /**
     * Returns true if at least one of the specified elements is displayed
     * (i.e., visible on the page).
     * @param navElements - a Navigator specifying the element(s) to be checked
     * for visibility.
     * @return true if at least one of the specified elements is displayed.
     */
    protected boolean atLeastOneIsDisplayed(Navigator navElements) {
        for (navElem in navElements.findAll()) {
            if (navElem.displayed) {
                return true
            }
        }
        return false
    }

    /**
     * Scrolls the specified element into view on the page; if it is already
     * visible, no action is taken.
     * @param navElement - a Navigator specifying the element to be scrolled
     * into view.
     */
    protected void scrollIntoView(Navigator navElement) {
        if (!navElement.displayed) {
            JavascriptExecutor executor = (JavascriptExecutor) driver;
            executor.executeScript("arguments[0].scrollIntoView(true);", navElement.firstElement());
            waitFor { navElement.displayed }
        }
    }

    /**
     * Clicks the specified "click" element, in order to make at least one of
     * the specified "display" elements visible; if one or more is already
     * visible, no action is taken.
     * @param clickElement - a Navigator specifying the element to be clicked.
     * @param displayElements - a Navigator specifying the element(s) that
     * should become visible.
     */
    protected void clickToDisplay(Navigator clickElement, Navigator displayElements) {
        if (!atLeastOneIsDisplayed(displayElements)) {
            scrollIntoView(clickElement)
            clickElement.click()
            scrollIntoView(displayElements.first())
            waitFor { atLeastOneIsDisplayed(displayElements) }
        }
    }

    /**
     * Clicks the specified "click" element, in order to make all of the
     * specified "display" elements invisible; if all are already invisible,
     * no action is taken.
     * @param clickElement - a Navigator specifying the element to be clicked.
     * @param displayElements - a Navigator specifying the element(s) that
     * should become invisible.
     */
    protected void clickToNotDisplay(Navigator clickElement, Navigator displayElements) {
        if (atLeastOneIsDisplayed(displayElements)) {
            scrollIntoView(clickElement)
            clickElement.click()
            waitFor { !atLeastOneIsDisplayed(displayElements) }
        }
    }

    /**
     * Used to specify whether you want a table's contents returned with the
     * column headers as displayed, converted to lower case, or converted to
     * upper case.
     */
    enum ColumnHeaderCase {
        AS_IS,
        TO_LOWER_CASE,
        TO_UPPER_CASE
    }

    /**
     * Returns the contents of the element specified by a Navigator, which
     * should refer to a "table" HTML element. If columnWise is true, the table
     * contents are returned in the form of a Map, with each element a List of
     * Strings; each Key of the Map is a column header of the table, and its
     * List contains the displayed text of that column. If columnWise is false,
     * the table contents are returned in the form of a List of List of String;
     * each List element represents a row of the table, containing a List of
     * all the elements in that row.
     * @param tableElement - a Navigator specifying the "table" element whose
     * contents are to be returned.
     * @param colHeaderFormat - the case in which you want the table's column
     * headers returned: converted to lower case, to upper case, or as-is.
     * @param columnWise - if true, the table contents are returned column-wise,
     * as a Map<String,List<String>>; if false, they're returned row-wise, as a
     * List<List<String>>.
     * @return a List or Map representing the contents of the specified table
     * element.
     */
    private def getTableContents(Navigator tableElement,
                                 ColumnHeaderCase colHeaderFormat,
                                 boolean columnWise) {
        def result = []
        def columnHeaders = tableElement.find('thead').first().find('th')*.text()
        if (ColumnHeaderCase.TO_LOWER_CASE.equals(colHeaderFormat)) {
            columnHeaders = columnHeaders.collect { it.toLowerCase() }
        } else if (ColumnHeaderCase.TO_UPPER_CASE.equals(colHeaderFormat)) {
            columnHeaders = columnHeaders.collect { it.toUpperCase() }
        }
        def rows = tableElement.find('tbody').find('tr')
        // Remove "empty" (or hidden) rows (those with no visible text)
        for (int i=rows.size()-1; i>= 0; i--) {
            String rowText = rows.getAt(i).text()
            if (rowText == null || rowText.isEmpty()) {
                rows = rows.remove(i);
            }
        }
        if (columnWise) {
            result = [:]
            def makeColumn = { index,rowset -> rowset.collect { row -> row.find('td',index).text() } }
            def colNum = 0
            columnHeaders.each { result.put(it, makeColumn(colNum++, rows)) }
            return result
        } else {
            result.add(columnHeaders)
            rows.each { result.add(it.find('td')*.text()) }
        }
        return result
    }

    /**
     * Returns the contents of the element specified by a Navigator, which
     * should refer to a "table" HTML element. The table contents are returned
     * in the form of a Map, with each element a List of Strings; each Key of
     * the Map is a column header of the table, and its List contains the
     * displayed text of that column. 
     * @param tableElement - a Navigator specifying the "table" element whose
     * contents are to be returned.
     * @param colHeaderFormat - the case in which you want the table's column
     * headers returned: converted to lower case, to upper case, or as-is.
     * @return a Map representing the contents of the specified table element.
     */
    protected Map<String,List<String>> getTableByColumn(Navigator tableElement,
                ColumnHeaderCase colHeaderFormat=ColumnHeaderCase.AS_IS) {
        return getTableContents(tableElement, colHeaderFormat, true)
    }

    /**
     * Returns the contents of the element specified by a Navigator, which
     * should refer to a "table" HTML element. The table contents are returned
     * in the form of a List of List of String; each List element represents a
     * row of the table, containing a List of all the elements in that row.
     * @param tableElement - a Navigator specifying the "table" element whose
     * contents are to be returned.
     * @param colHeaderFormat - the case in which you want the table's column
     * headers returned: converted to lower case, to upper case, or as-is.
     * @return a List representing the contents of the specified table element.
     */
    protected List<List<String>> getTableByRow(Navigator tableElement,
                ColumnHeaderCase colHeaderFormat=ColumnHeaderCase.AS_IS) {
        return getTableContents(tableElement, colHeaderFormat, false)
    }

    /**
     * Returns true if the current page is a DbMonitorPage (i.e., the "DB Monitor"
     * tab of the VoltDB Management Center page is currently open).
     * @return true if a DbMonitorPage is currently open.
     */
    def boolean isDbMonitorPageOpen() {
        if (dbMonitorTab.attr('class') == 'active') {
            return true
        } else {
            return false
        }
    }

    /**
     * Returns true if the current page is a SchemaPage (i.e., the "Schema"
     * tab of the VoltDB Management Center page is currently open).
     * @return true if a SchemaPage is currently open.
     */
    def boolean isSchemaPageOpen() {
        if (schemaTab.attr('class') == 'active') {
            return true
        } else {
            return false
        }
    }

    /**
     * Returns true if the current page is a SqlQueryPage (i.e., the "SQL Query"
     * tab of the VoltDB Management Center page is currently open).
     * @return true if a SqlQueryPage is currently open.
     */
    def boolean isSqlQueryPageOpen() {
        if (sqlQueryTab.attr('class') == 'active') {
            return true
        } else {
            return false
        }
    }

    /**
     * Clicks the "DB Monitor" link, opening the "DB Monitor" page (or tab);
     * if the "DB Monitor" page is already open, no action is taken.
     */
    def void openDbMonitorPage() {
        if (!isDbMonitorPageOpen()) {
            dbMonitorLink.click()
        }
    }

    /**
     * Clicks the "Schema" link, opening the "Schema" page (or tab);
     * if the "Schema" page is already open, no action is taken.
     */
    def void openSchemaPage() {
        if (!isSchemaPageOpen()) {
            schemaLink.click()
        }
    }

    /**
     * Clicks the "Sql Query" link, opening the "Sql Query" page (or tab);
     * if the "Sql Query" page is already open, no action is taken.
     */
    def void openSqlQueryPage() {
        if (!isSqlQueryPageOpen()) {
            sqlQueryLink.click()
            waitFor() { !$('#tabMain').find('#tabScroller').text().isEmpty() }
        }
    }

    /**
     * Returns whether or not a Login dialog is currently open.
     * @return true if a Login dialog is currently open.
     */
    def boolean isLoginDialogOpen() {
        if (securityEnabled == null) {
            try {
                waitFor() { loginDialog.displayed }
                securityEnabled = true
            } catch (WaitTimeoutException e) {
                securityEnabled = false
            }
        }
        if (securityEnabled) {
            try {
                waitFor() { loginDialog.displayed }
            } catch (WaitTimeoutException e) {
                // do nothing
            }
        }
        if (loginDialog.displayed) {
            return true
        } else {
            return false
        }
    }

    /**
     * Logs in, via the Login dialog (which is assumed to be open on the
     * current page), using the specified username and password (or their
     * default values, if not specified).
     * @param username - the username to be used for login.
     * @param password - the password to be used for login.
     */
    def void login(username="admin", password="voltdb") {
        usernameInput = username
        passwordInput = password
        loginButton.click()
        waitFor() { !loginDialog.displayed }
    }

    /**
     * Checks whether a Login dialog is open on the current page; and if so,
     * logs in, using the specified username and password (or their default
     * values, if not specified).
     * @param username - the username to be used for login.
     * @param password - the password to be used for login.
     */
    def boolean loginIfNeeded(username="admin", password="voltdb") {
        if (isLoginDialogOpen()) {
            println "Login dialog is open; will attempt to login."
            login(username, password)
            return true
        } else {
            return false
        }
    }

}
