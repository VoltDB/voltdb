/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

import geb.spock.GebReportingSpec
import org.openqa.selenium.WebElement
import geb.navigator.Navigator

class ClusterSettingsTest extends TestBase {
    def setup() { // called before each test
        count = 0

        while(count<numberOfTrials) {
            count ++
            try {
                setup: 'Open Cluster Settings page'
                to ClusterSettingsPage
                expect: 'to be on Cluster Settings page'
                at ClusterSettingsPage

                browser.driver.executeScript("VdmUI.isTestServer = true;",1)

                break
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                println("ElementNotVisibleException: Unable to Start the test")
                println("Retrying")
            }
        }
    }

    def createEditDeleteServer() {
        int value = 0
        when: 'Create database'
        for(count=0; count<numberOfTrials; count++) {
            try {
                indexOfNewDatabase = createNewDatabase(create_DatabaseTest_File)
                println("The index of database " + indexOfNewDatabase)
                break
            } catch(Exception e) {
                //deleteDatabase(create_DatabaseTest_File)
            } catch(org.codehaus.groovy.runtime.powerassert.PowerAssertionError e) {
                //deleteDatabase(create_DatabaseTest_File)
            }
        }
        then: 'Choose new database'
        println("The index of database " + indexOfNewDatabase)
        chooseDatabase(indexOfNewDatabase, "name_src")

        when: 'Get the count for next server'
        count = 1
        while(count<numberOfTrials) {
            try {
                waitFor { $(getCssPathOfServer(count)).isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                break
            }
            count++
        }
        value = count
        println("The count for test " + count)
        and: 'Set the id for new server'
        String deleteId = getCssPathOfDeleteServer(1)
        String editId   = getCssPathOfEditServer(1)
        String serverId = getCssPathOfServer(1)
        println("this " + " " + serverId + " " + editId)
        then: 'Click Add Server button to open popup'
        for(count=0; count<numberOfTrials; count++) {
            try {
                page.btnAddServerOption.click()
                page.buttonAddServer.click()
                waitFor { page.popupAddServer.isDisplayed() }
                println("Add Server popup found")
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to find Add Server popup - Retrying")
            } catch(geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find Add Server button - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }

        when: 'Provide values for new server'
        for(count=0; count<numberOfTrials; count++) {
            try {
                waitFor { popupAddServerNameField.value("new_server") }
                waitFor { popupAddServerHostNameField.value("new_host") }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to provide value to the text fields - Retrying")
            }
        }
        count = 0
        and: 'Click Ok to save the Server'
        for(count=0; count<numberOfTrials; count++) {
            try {
                popupAddServerButtonOk.click()
                waitFor { !popupAddServerButtonOk.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to Close Popup - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException e) {
                try {
                    waitFor { !popupAddServerButtonOk.isDisplayed() }
                } catch(geb.waiting.WaitTimeoutException f) {
                    println("Popup Closed")
                    break
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        then: 'Check if the server is created or not'
        try {
            waitFor { $(serverId).text() == "new_server" }
            println($(serverId).text() + " get Id of server")
            println("The new server was created")
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Test Fail: The new server was not created")
            assert false
        }

        try {
            waitFor(10) { 1==0 }
        } catch(geb.waiting.WaitTimeoutException exception) {
            println("waited")
        }

        when: 'Click edit button to open the edit popup'
        for(count=1; count<numberOfTrials; count++) {
            try {
                waitFor { $(editId).click() }
                waitFor { popupAddServerButtonOk.isDisplayed() }
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find edit button - Retrying")
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to find the edit popup - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        and: 'provide value for edit'
        for(count=0; count<numberOfTrials; count++) {
            try {
                waitFor { popupAddServerNameField.value("new_edited_server") }
                waitFor { popupAddServerHostNameField.value("new_edited_host") }
                break
            } catch (geb.waiting.WaitTimeoutException e) {
                println("Unable to provide value to text fields - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        then: 'Click ok and confirm the edit'
        for(count=1; count<numberOfTrials; count++) {
            try {
                popupAddServerButtonOk.click()
                waitFor { $(serverId).isDisplayed() }
                waitFor { $(serverId).text().equals("new_edited_host") }
                println("The server was edited")
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find edit button - Retrying")
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to find edited server - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException e) {
                try {
                    waitFor { $(serverId).isDisplayed() }
                    waitFor { $(serverId).text().equals("new_edited_host") }
                    println("The server was edited")
                    break
                } catch(geb.waiting.WaitTimeoutException exc) {
                    println("Unable to find edited server - Retrying")
                }
            }
        }
    }

    def ensureServerNameAndHostNameIsNotEmpty() {
        println("Test: ensureServerNameAndHostNameIsNotEmpty")
        when:"Click Add Server button"
        try {
            page.btnAddServerOption.click()
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Unable to find Add Server button or popup - Retrying")
        }

        then: "Check popup is displayed"
        waitFor{page.popupAddServer.isDisplayed()}
        when: "click the Ok button"
        page.popupAddServerButtonOk.click()

        then: "Check validation for server name and host name exists"
        errorHostName.isDisplayed()
        page.popupAddServerButtonCancel.click()

        try {
            waitFor { 1==0 }
        } catch(geb.waiting.WaitTimeoutException exception) {

        }
    }

    def ensureInternalInterfaceIsValid(){
        println("Test: ensureInternalInterfaceIsValid")
        boolean status = false
        int newValue = 1

        int value = 0
        when: 'Create database'
        for(count=0; count<numberOfTrials; count++) {
            try {
                indexOfNewDatabase = createNewDatabase(create_DatabaseTest_File)
                println("The index of database " + indexOfNewDatabase)
                break
            } catch(Exception e) {
                //deleteDatabase(create_DatabaseTest_File)
            } catch(org.codehaus.groovy.runtime.powerassert.PowerAssertionError e) {
                //deleteDatabase(create_DatabaseTest_File)
            }
        }
        then: 'Choose new database'
        println("The index of database " + indexOfNewDatabase)
        chooseDatabase(indexOfNewDatabase, "name_src")

        when:
        count=1
        while(count<numberOfTrials) {
            count++
            try {
                waitFor { $(id:page.getIdofDeleteButton(count)).isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                break
            }
        }
        newValue = count
        then:
        println("The count is " + newValue)

        when:
        for(count=0; count<numberOfTrials; count++) {
            try {
                waitFor { page.btnAddServerOption.click() }
                waitFor { page.buttonAddServer.click() }
                waitFor { page.popupAddServer.isDisplayed() }
                break
            } catch (geb.waiting.WaitTimeoutException e) {
                println("Unable to find Add Server button or popup - Retrying")
            }
        }

        for(count=0; count<numberOfTrials; count++) {
            try {
                waitFor { page.popupAddServerDetails.click() }
                waitFor { page.popupAddServerInternalInterfaceField.isDisplayed() }
                break
            } catch (geb.waiting.WaitTimeoutException e) {
                println("Unable to expand details - Retrying")
            }
        }
        println("This " + page.popupAddServerDetails.text())
        and:
        for(count=0; count<numberOfTrials; count++) {
            try {
                waitFor { popupAddServerNameField.value("new_server") }
                waitFor { popupAddServerHostNameField.value("new_host") }
                waitFor { popupAddServerDescriptionField.value("") }
                waitFor { popupAddServerInternalInterfaceField.value("sfdsaf12321") }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to provide value to the text fields - Retrying")
            }
        }
        count = 0
        then:
        for(count=0; count<numberOfTrials; count++) {
            try {
                popupAddServerButtonOk.click()
                //waitFor { !popupAddServerButtonOk.isDisplayed() }
                errorInternalInterface.isDisplayed()
                errorInternalInterface.text().equals("Please enter a valid IP address.")
                status = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to Close Popup - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException e) {
                try {
                    waitFor { !popupAddServerButtonOk.isDisplayed() }
                } catch(geb.waiting.WaitTimeoutException f) {
                    println("Popup Closed")
                    break
                }
            }
        }

        if(status==true) {
            println("Test Pass")
        }
        else if(status==false) {
            println("Test Fail")
            assert false
        }
        else {
            println("Unknown Error")
            assert false
        }
        println()
    }


    def ensureClientListenerIsValid(){
        println("Test: ensureInternalInterfaceIsValid123")
        boolean status = false
        int newValue = 1

        int value = 0
        when: 'Create database'
        for(count=0; count<numberOfTrials; count++) {
            try {
                indexOfNewDatabase = createNewDatabase(create_DatabaseTest_File)
                println("The index of database " + indexOfNewDatabase)
                break
            } catch(Exception e) {
                //deleteDatabase(create_DatabaseTest_File)
            } catch(org.codehaus.groovy.runtime.powerassert.PowerAssertionError e) {
                //deleteDatabase(create_DatabaseTest_File)
            }
        }
        then: 'Choose new database'
        println("The index of database " + indexOfNewDatabase)
        chooseDatabase(indexOfNewDatabase, "name_src")

        when:
        count=1
        while(count<numberOfTrials) {
            count++
            try {
                waitFor { $(id:page.getIdofDeleteButton(count)).isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                break
            }
        }
        newValue = count
        then:
        println("The count is " + newValue)

        when:
        for(count=0; count<numberOfTrials; count++) {
            try {
                waitFor { page.btnAddServerOption.click() }
                waitFor { page.buttonAddServer.click() }
                waitFor { page.popupAddServer.isDisplayed() }
                break
            } catch (geb.waiting.WaitTimeoutException e) {
                println("Unable to find Add Server button or popup - Retrying")
            }
        }

        for(count=0; count<numberOfTrials; count++) {
            try {
                waitFor { page.popupAddServerDetails.click() }
                waitFor { page.popupAddServerClientListenerField.isDisplayed() }
                break
            } catch (geb.waiting.WaitTimeoutException e) {
                println("Unable to expand details - Retrying")
            }
        }
        println("This " + page.popupAddServerDetails.text())
        and:
        for(count=0; count<numberOfTrials; count++) {
            try {
                waitFor { popupAddServerNameField.value("new_server") }
                waitFor { popupAddServerHostNameField.value("new_host") }
                waitFor { popupAddServerDescriptionField.value("") }
                waitFor { popupAddServerClientListenerField.value("sfdsaf12321") }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to provide value to the text fields - Retrying")
            }
        }
        count = 0
        then:
        for(count=0; count<numberOfTrials; count++) {
            try {
                popupAddServerButtonOk.click()
                //waitFor { !popupAddServerButtonOk.isDisplayed() }
                errorInternalInterface.isDisplayed()
                errorInternalInterface.text().equals("Please enter a valid value.(e.g, 127.0.0.1:8000 or 8000(1-65535))")
                status = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to Close Popup - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException e) {
                try {
                    waitFor { !popupAddServerButtonOk.isDisplayed() }
                } catch(geb.waiting.WaitTimeoutException f) {
                    println("Popup Closed")
                    break
                }
            }
        }

        if(status==true) {
            println("Test Pass")
        }
        else if(status==false) {
            println("Test Fail")
            assert false
        }
        else {
            println("Unknown Error")
            assert false
        }
        println()
    }

    def verifyDuplicateNameAndPortNotCreated() {
        int count
        when: 'Create database'
        for(count=0; count<numberOfTrials; count++) {
            try {
                indexOfNewDatabase = createNewDatabase(create_DatabaseTest_File)
                println("The index of database " + indexOfNewDatabase)
                break
            } catch(Exception e) {
            } catch(org.codehaus.groovy.runtime.powerassert.PowerAssertionError e) {
            }
        }
        then: 'Choose new database'
        println("The index of database " + indexOfNewDatabase)
        chooseDatabase(indexOfNewDatabase, "name_src")

        when: 'Set the id for new server'
        String serverId = getCssPathOfServer(1)
        then: 'Click Add Server button to open popup'
        for(count=0; count<numberOfTrials; count++) {
            try {
                page.btnAddServerOption.click()
                page.buttonAddServer.click()
                waitFor { page.popupAddServer.isDisplayed() }
                println("Add Server popup found")
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to find Add Server popup - Retrying")
            } catch(geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find Add Server button - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }

        when: 'Provide values for new server'
        for(count=0; count<numberOfTrials; count++) {
            try {
                waitFor { popupAddServerNameField.value("new_server") }
                waitFor { popupAddServerHostNameField.value("new_host") }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to provide value to the text fields - Retrying")
            }
        }
        count = 0
        and: 'Click Ok to save the Server'
        for(count=0; count<numberOfTrials; count++) {
            try {
                popupAddServerButtonOk.click()
                waitFor { !popupAddServerButtonOk.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to Close Popup - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException e) {
                try {
                    waitFor { !popupAddServerButtonOk.isDisplayed() }
                } catch(geb.waiting.WaitTimeoutException f) {
                    println("Popup Closed")
                    break
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        then: 'Check if the server is created or not'
        try {
            waitFor { $(serverId).text() == "new_server" }
            println($(serverId).text() + " get Id of server")
            println("The new server was created")
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Test Fail: The new server was not created")
            assert false
        }

        when: 'Click Add Server button to open popup'
        for(count=0; count<numberOfTrials; count++) {
            try {
                page.btnAddServerOption.click()
                page.buttonAddServer.click()
                waitFor { page.popupAddServer.isDisplayed() }
                println("Add Server popup found")
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to find Add Server popup - Retrying")
            } catch(geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find Add Server button - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        then: 'Provide values for new server'
        for(count=0; count<numberOfTrials; count++) {
            try {
                waitFor { popupAddServerNameField.value("new_server") }
                waitFor { popupAddServerHostNameField.value("new_host") }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to provide value to the text fields - Retrying")
            }
        }

        when: 'Click Ok to save the Server'
        for(count=0; count<numberOfTrials; count++) {
            try {
                popupAddServerButtonOk.click()
                waitFor { !popupAddServerButtonOk.isDisplayed() }
                break
            } catch (geb.waiting.WaitTimeoutException e) {
                println("Unable to Close Popup - Retrying")
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                try {
                    waitFor { !popupAddServerButtonOk.isDisplayed() }
                } catch (geb.waiting.WaitTimeoutException f) {
                    println("Popup Closed")
                    break
                }
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        then:
        try {
            waitFor { page.errorClientPort.text().equals("Default port already exists") }
        } catch(geb.waiting.WaitTimeoutException exception) {
            println("Test Fail: The Error Message is not displayed")
            assert false
        }
    }

    def verifyDuplicateNameButDifferentPortCreated() {
        int count
        when: 'Create database'
        for(count=0; count<numberOfTrials; count++) {
            try {
                indexOfNewDatabase = createNewDatabase(create_DatabaseTest_File)
                println("The index of database " + indexOfNewDatabase)
                break
            } catch(Exception e) {
            } catch(org.codehaus.groovy.runtime.powerassert.PowerAssertionError e) {
            }
        }
        then: 'Choose new database'
        println("The index of database " + indexOfNewDatabase)
        chooseDatabase(indexOfNewDatabase, "name_src")

        when: 'Set the id for new server'
        String serverId = getCssPathOfServer(1)
        String serverIdNext = getCssPathOfServer(2)
        then: 'Click Add Server button to open popup'
        for(count=0; count<numberOfTrials; count++) {
            try {
                page.btnAddServerOption.click()
                page.buttonAddServer.click()
                waitFor { page.popupAddServer.isDisplayed() }
                println("Add Server popup found")
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to find Add Server popup - Retrying")
            } catch(geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find Add Server button - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }

        when: 'Provide values for new server'
        for(count=0; count<numberOfTrials; count++) {
            try {
                waitFor { popupAddServerNameField.value("new_server") }
                waitFor { popupAddServerHostNameField.value("new_host") }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to provide value to the text fields - Retrying")
            }
        }
        count = 0
        and: 'Click Ok to save the Server'
        for(count=0; count<numberOfTrials; count++) {
            try {
                popupAddServerButtonOk.click()
                waitFor { !popupAddServerButtonOk.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to Close Popup - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException e) {
                try {
                    waitFor { !popupAddServerButtonOk.isDisplayed() }
                } catch(geb.waiting.WaitTimeoutException f) {
                    println("Popup Closed")
                    break
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        then: 'Check if the server is created or not'
        try {
            waitFor { $(serverId).text() == "new_server" }
            println($(serverId).text() + " get Id of server")
            println("The new server was created")
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Test Fail: The new server was not created")
            assert false
        }

        when: 'Click Add Server button to open popup'
        for(count=0; count<numberOfTrials; count++) {
            try {
                page.btnAddServerOption.click()
                page.buttonAddServer.click()
                waitFor { page.popupAddServer.isDisplayed() }
                println("Add Server popup found")
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to find Add Server popup - Retrying")
            } catch(geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find Add Server button - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        and: 'Click on the Details'
        for(count=0; count<numberOfTrials; count++) {
            try {
                page.popupAddServerDetails.click()
                waitFor { popupAddServerClientListenerField.isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException exception) {
                println("Unable to open Details")
            }
        }

        then: 'Provide values for new server'
        for(count=0; count<numberOfTrials; count++) {
            try {
                waitFor { popupAddServerNameField.value("new_server") }
                waitFor { popupAddServerHostNameField.value("new_host") }

                waitFor { popupAddServerClientListenerField.value("12") }
                waitFor { popupAddServerAdminListenerField.value("13") }
                waitFor { popupAddServerHttpListenerField.value("14") }
                waitFor { popupAddServerInternalListenerField.value("15") }
                waitFor { popupAddServerZookeeperListenerField.value("16") }
                waitFor { popupAddServerReplicationListenerField.value("17") }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to provide value to the text fields - Retrying")
            }
        }

        when: 'Click Ok to save the Server'
        for(count=0; count<numberOfTrials; count++) {
            try {
                popupAddServerButtonOk.click()
                waitFor { !popupAddServerButtonOk.isDisplayed() }
                break
            } catch (geb.waiting.WaitTimeoutException e) {
                println("Unable to Close Popup - Retrying")
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                try {
                    waitFor { !popupAddServerButtonOk.isDisplayed() }
                } catch (geb.waiting.WaitTimeoutException f) {
                    println("Popup Closed")
                    break
                }
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        then:
        println()
        report "hello"
        try {
            waitFor { $(serverIdNext).text() == "new_server" }
            println($(serverIdNext).text() + " get Id of server")
            println("The new server with duplicate name was created")
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Test Fail: The new server was not created")
            assert false
        }
    }

    def cleanup() {
        to ClusterSettingsPage

        println("Cleaning up")
        int counter= 0
        openDatabase()

        while(counter<numberOfTrials) {
            counter++
            try {
                waitFor { $(id:page.getIdOfDeleteButton(counter)).isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                break
            }
        }

        println("The counter is " + counter)

        if(counter>=1) {
            int count=0
            for(count=0; count<numberOfTrials; count++) {
                try {
                    buttonDatabase.click()
                    waitFor { buttonAddDatabase.isDisplayed() }

                    break
                } catch (geb.waiting.WaitTimeoutException e) {
                    println("Waiting - Retrying")
                }
            }

            chooseDatabase(1, "Database")
            println("Database 1 chosen")
            println(buttonDatabase.text())

            for(count=0; count<numberOfTrials; count++) {
                try {
                    buttonDatabase.click()
                    waitFor { buttonAddDatabase.isDisplayed() }

                    break
                } catch (geb.waiting.WaitTimeoutException e) {
                    println("Waiting - Retrying")
                }
            }
            count=0
            while(count<numberOfTrials) {
                count++
                try {
                    waitFor { $(id:page.getIdOfDeleteButton(count)).isDisplayed() }

                } catch(geb.waiting.WaitTimeoutException e) {
                    break
                }
            }
            // at the moment count gives the value of the last Database, the current limitation is use of id
            println("the count " + count)
            deleteNewDatabase(count, "name_src")
        }
        else {
            println("There is only one database")
        }
    }
}
