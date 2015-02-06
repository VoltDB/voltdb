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

import geb.*
import geb.navigator.Navigator
import org.openqa.selenium.JavascriptExecutor

/**
 * This class represents a generic VoltDB Management Center page (without
 * specifying which tab you are on), which is the main page of the web UI
 * - the new UI for version 4.9 (November 2014), replacing the old Web Studio
 * (& DB Monitor Catalog Report, etc.).
 */
class VoltDBManagementCenterPage extends Page {

    static url = 'http://localhost:8080/'
    static content = {
        navTabs { $('#nav') }
        dbMonitorTab { navTabs.find('#navDbmonitor') }
        schemaTab    { navTabs.find('#navSchema') }
        sqlQueryTab  { navTabs.find('#navSqlQuery') }
        dbMonitorLink(to: DbMonitorPage) { dbMonitorTab.find('a') }
        schemaLink   (to: SchemaPage)    { schemaTab.find('a') }
        sqlQueryLink (to: SqlQueryPage)  { sqlQueryTab.find('a') }
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
        }
    }
}
