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

import java.util.List;
import java.util.Map;

import geb.*
import geb.navigator.Navigator
import geb.waiting.WaitTimeoutException

import org.openqa.selenium.JavascriptExecutor
import org.openqa.selenium.StaleElementReferenceException
import org.openqa.selenium.WebElement

/**
 * This class represents a generic VoltDB Management Center page (without
 * specifying which tab you are on), which is the main page of the web UI
 * - the new UI for version 4.9 (November 2014), replacing the old Web Studio
 * (& DB Monitor Catalog Report, etc.).
 */
class VoltDBManagementCenterPage extends Page {
    static Boolean securityEnabled = null;

    static url = ''  // relative to the baseUrl
    static content = {
        navTabs                             { $('#nav') }
        dbMonitorTab                        { navTabs.find('#navDbmonitor') }
        analysisTab                         { navTabs.find('#navAnalysis') }
        adminTab                            { navTabs.find('#navAdmin') }
        schemaTab                           { navTabs.find('#navSchema') }
        sqlQueryTab                         { navTabs.find('#navSqlQuery') }
        drTab                               { navTabs.find('#navDR') }
        importerTab                         { navTabs.find('#navImporter') }
        dbMonitorLink (to: DbMonitorPage)   { dbMonitorTab.find('a') }
        analysisLink  (to: AnalysisPage)    { analysisTab.find('a') }
        adminLink     (to: AdminPage)       { adminTab.find('a') }
        schemaLink    (to: SchemaPage)      { schemaTab.find('a') }
        sqlQueryLink  (to: SqlQueryPage)    { sqlQueryTab.find('a') }
        drLink        (to: DrPage)          { $('#navDR > a') }
        importerLink  (to: ImporterPage)    { $('#navImporter > a') }
        loginDialog   (required: false)     { $('#loginBox') }
        usernameInput (required: false)     { loginDialog.find('input#username') }
        passwordInput (required: false)     { loginDialog.find('input#password') }
        loginButton   (required: false)     { loginDialog.find('#LoginBtn') }

        header { module Header }
        footer { module Footer }
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
    protected boolean isDisplayed(Navigator navElements) {
        for (navElem in navElements.findAll()) {
            if (navElem.displayed) {
                return true
            }
        }
        return false
    }

    /**
     * Returns true if the specified element is stale, i.e., no longer present
     * in the DOM.
     * @param webElement - a WebElement whose staleness is to be checked.
     * @return true if the specified WebElement is stale.
     */
    protected boolean isStale(WebElement webElement) {
        try {
            webElement.isDisplayed()
        } catch (StaleElementReferenceException e) {
            return true
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
        if (!isDisplayed(displayElements)) {
            scrollIntoView(clickElement)
            clickElement.click()
            scrollIntoView(displayElements.first())
            waitFor { isDisplayed(displayElements) }
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
        if (isDisplayed(displayElements)) {
            scrollIntoView(clickElement)
            clickElement.click()
            waitFor { !isDisplayed(displayElements) }
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
            // Note: need to use '@innerHTML' rather than 'text()' here,
            // because the latter removes leading and trailing whitespace
            def makeColumn = { index,rowset -> rowset.collect { row -> row.find('td',index).@innerHTML } }
            def colNum = 0
            columnHeaders.each { result.put(it, makeColumn(colNum++, rows)) }
            return result
        } else {
            result.add(columnHeaders)
            // Same comment as above (use '@innerHTML', not 'text()')
            rows.each { result.add(it.find('td')*.@innerHTML) }
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
     * Returns true if the "DB Monitor" link, i.e. the link to the "DB Monitor"
     * page (i.e., the "DB Monitor" tab of the VoltDB Management Center page),
     * is present and visible. (This should almost always be true.)
     * @return true if a dbMonitorLink is currently visible.
     */
    def boolean isDbMonitorLinkVisible() {
        return dbMonitorLink.displayed
    }

    /**
     * Returns true if the "Analysis" link, i.e. the link to the "Analysis"
     * page (i.e., the "Analysis" tab of the VoltDB Management Center page),
     * is present and visible. (This should almost always be true.)
     * @return true if an analysisLink is currently visible.
     */
    def boolean isAnalysisLinkVisible() {
        return analysisLink.displayed
    }

    /**
     * Returns true if the "Admin" link, i.e. the link to the "Admin"
     * page (i.e., the "Admin" tab of the VoltDB Management Center page),
     * is present and visible. (This should almost always be true.)
     * @return true if an adminLink is currently visible.
     */
    def boolean isAdminLinkVisible() {
        return adminLink.displayed
    }

    /**
     * Returns true if the "Schema" link, i.e. the link to the "Schema"
     * page (i.e., the "Schema" tab of the VoltDB Management Center page),
     * is present and visible. (This should almost always be true.)
     * @return true if a schemaLink is currently visible.
     */
    def boolean isSchemaLinkVisible() {
        return schemaLink.displayed
    }

    /**
     * Returns true if the "SQL Query" link, i.e. the link to the "SQL Query"
     * page (i.e., the "SQL Query" tab of the VoltDB Management Center page),
     * is present and visible. (This should almost always be true.)
     * @return true if a sqlQueryLink is currently visible.
     */
    def boolean isSqlQueryLinkVisible() {
        return sqlQueryLink.displayed
    }

    /**
     * Returns true if the "DR" link, i.e. the link to the "DR"
     * page (i.e., the "DR" tab of the VoltDB Management Center page),
     * is present and visible. (This should be true only when running
     * "pro", and a <dr> tag is present in the deployment file.)
     * @return true if a drLink is currently visible.
     */
    def boolean isDrLinkVisible() {
        return drLink.displayed
    }

    /**
     * Returns true if the "Importer" link, i.e. the link to the "Importer"
     * page (i.e., the "Importer" tab of the VoltDB Management Center page),
     * is present and visible. (This should be true only when running "pro",
     * and an <importer> tag is present in the deployment file.)
     * @return true if an importerLink is currently visible.
     */
    def boolean isImporterLinkVisible() {
        return importerLink.displayed
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
     * Returns true if the current page is a AnalysisPage (i.e., the "Analysis"
     * tab of the VoltDB Management Center page is currently open).
     * @return true if a AnalysisPage is currently open.
     */
    def boolean isAnalysisPageOpen() {
        if (analysisTab.attr('class') == 'active') {
            return true
        } else {
            return false
        }
    }

    /**
     * Returns true if the current page is a AdminPage (i.e., the "Admin"
     * tab of the VoltDB Management Center page is currently open).
     * @return true if a AdminPage is currently open.
     */
    def boolean isAdminPageOpen() {
        if (adminTab.attr('class') == 'active') {
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
     * Returns true if the current page is a DrPage (i.e., the "Dr"
     * tab of the VoltDB Management Center page is currently open).
     * @return true if a DrPage is currently open.
     */
    def boolean isDrPageOpen() {
        if (drTab.attr('class') == 'active') {
            return true
        } else {
            return false
        }
    }

    /**
     * Returns true if the current page is an ImporterPage (i.e., the "Importer"
     * tab of the VoltDB Management Center page is currently open).
     * @return true if a ImporterPage is currently open.
     */
    def boolean isImporterPageOpen() {
        if (importerTab.attr('class') == 'active') {
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
     * Clicks the "Analysis" link, opening the "Analysis" page (or tab);
     * if the "Analysis" page is already open, no action is taken.
     */
    def void openAnalysisPage() {
        if (!isAnalysisPageOpen()) {
            analysisLink.click()
        }
    }

    /**
     * Clicks the "Admin" link, opening the "Admin" page (or tab);
     * if the "Admin" page is already open, no action is taken.
     */
    def void openAdminPage() {
        if (!isAdminPageOpen()) {
            adminLink.click()
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
     * Clicks the "Dr" link, opening the "Dr" page (or tab);
     * if the "Dr" page is already open, no action is taken.
     */
    def void openDrPage() {
        if (!isDrPageOpen()) {
            drLink.click()
        }
    }

    /**
     * Clicks the "Importer" link, opening the "Importer" page (or tab);
     * if the "Importer" page is already open, no action is taken.
     */
    def void openImporterPage() {
        if (!isImporterPageOpen()) {
            importerLink.click()
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

    String user = getUsername()
    String pass = getPassword()

    def void loginValid(username = user, password = pass) {
        usernameInput = username
        passwordInput = password
        loginButton.click()
        waitFor() { !loginDialog.displayed }

    }

    def void loginEmpty(username = "", password = "") {
        usernameInput = username
        passwordInput = password
        loginButton.click()
        waitFor() { !loginDialog.displayed }

    }

    def void loginInvalid(username = "invalid", password = "invalid") {
        usernameInput = username
        passwordInput = password
        loginButton.click()
        waitFor() { !loginDialog.displayed }

    }

    def String getUsername() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/users.txt"))
        String username

        while((username = br.readLine()) != "#username") {
        }

        username = br.readLine()

        return username
    }

    def String getPassword() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/users.txt"))
        String password

        while((password = br.readLine()) != "#password") {
        }

        password = br.readLine()

        return password
    }

}
