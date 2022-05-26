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

import geb.*
import groovy.json.*
import java.util.List;
import java.util.Map;
import spock.lang.*
import vmcTest.pages.*

/**
 * This class contains tests of the 'SQL Query' tab of the VoltDB Management
 * Center (VMC) page, which is the VoltDB (new) web UI.
 */

class SqlQueriesTabTest extends SqlQueriesTestBase {
    def setup() {
        when: 'click the SQL Query link (if needed)'
        openSqlQueryPage()
        then: 'should be on SQL Query page'
        at SqlQueryPage
    }

    def createSaveAndDeleteTabForQueryBox() {
        String gibberishQuery = "asdf"
        String tabName = "thisissparta"

        browser.driver.executeScript("localStorage.clear()")
        when: 'Check if Query1 tab exists'
            if (!$("#qTab-1").isDisplayed()) {
                page.addQueryTab.click()
            }
        then:
        report "Query 1"
        waitFor(10){$("#qTab-1").isDisplayed()}

        when: 'Click on Add Tab to create second tab'
        page.addQueryTab.click()
        then: 'Confirm  the second tab is created'
        waitFor { $(page.getCssPathOfTab(2)).isDisplayed() }
        //waitFor { $(page.getIdOfQueryBox(2)).isDisplayed() }
        waitFor { $(page.getCssPathOfTab(2)).text().equals("Query2") }

       // println(page.getIdOfQueryBox(2))
        when: 'Insert some gibberish in the Query box'
        $(page.getIdOfQueryBox(2)).jquery.html(gibberishQuery)
        then: 'Click on Save button'
        $(page.getIdOfSaveButton(2)).click()

        when: 'Check if the popup with its elements are displayed'
        waitFor(10) { page.saveTabPopupOk.isDisplayed() }
        page.saveTabPopupTextField.isDisplayed()
        then: 'Provide value for the text field'
        page.saveTabPopupTextField.value(tabName)

        when: 'Click the save button'
        $("#btnSaveQueryOk").click()
        then: 'Assert the tab name'
        assert waitFor(20) { $(page.getCssPathOfTab(2)).text() == tabName }

        when: 'Click on the delete button of second tab'
        waitFor(10){$(id:page.getIdOfDeleteTab(2)).click()}
        then: 'Check the delete popup with its element is displayed'
        waitFor { page.deleteTabOk.isDisplayed() }
        waitFor { page.deleteTabCancel.isDisplayed() }

        when: 'Click Ok in confirmation popup'
        page.deleteTabOk.click()
        then: 'Assert that the second tab is deleted'
        $(id:page.getIdOfDeleteTab(1)).isDisplayed()
    }
}
