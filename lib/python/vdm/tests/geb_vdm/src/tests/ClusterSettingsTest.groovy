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
    String testingName = getTestingUrl()

    def setup() { // called before each test
        for(count=0; count<numberOfTrials; count++) {
            try {
                setup: 'Open Cluster Settings page'
                to ClusterSettingsPage
                expect: 'to be on Cluster Settings page'
                at ClusterSettingsPage

                //browser.driver.executeScript("VdmUI.isTestServer = true;",1)

                break
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                println("ElementNotVisibleException: Unable to Start the test")
                println("Retrying")
            }
        }
    }

    def createEditDeleteServer() {
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
//        chooseDatabase(indexOfNewDatabase, "name_src")

        int countNext = 0
//        for (count = 0; count < numberOfTrials; count++) {
        try {
            for(countNext=0; countNext<numberOfTrials; countNext++) {
                try {
                    waitFor { buttonAddDatabase.isDisplayed() }
//                        break
                } catch(geb.waiting.WaitTimeoutException exception) {
                    currentDatabase.click()
                }
            }
            $(id:getIdOfDatabase(String.valueOf(indexOfNewDatabase))).click()
            waitFor { currentDatabase.text().equals("name_src") }

        } catch (geb.waiting.WaitTimeoutException exception) {
            println("Waiting - Retrying")
        } catch (org.openqa.selenium.StaleElementReferenceException e) {
            println("Stale Element exception - Retrying")
        } catch(org.openqa.selenium.ElementNotVisibleException exception) {
            try {
                waitFor { currentDatabase.text().equals("name_src") }
            } catch (geb.waiting.WaitTimeoutException exc) {
                println("Waiting - Retrying")
            }
        }
//        }

        when: 'Get the count for next server'
        count = 1
        and: 'Set the id for new server'
        String editId   = getCssPathOfEditServer(count)
        String serverId = getCssPathOfServer(count)
        println("this " + " " + serverId + " " + editId)
        then: 'Click Add Server button to open popup'
        for(count=0; count<numberOfTrials; count++) {
            try {
                page.btnAddServerOption.click()
                waitFor { page.popupAddServer.isDisplayed() }
                page.popupAddServerDetails.click()
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
                waitFor { popupAddServerHostNameField.value(testingName) }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to provide value to the text fields - Retrying")
            }
        }
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

        when: 'Click on the server name'
        $("#dropdownMenu1 > a > span.clsServerList").click()
        and: 'Click on edit button'
        $("#btnUpdateServer > a").click()
        then: 'Wait for the popup to be displayed'
        waitFor { popupAddServer.isDisplayed() }

        when: 'provide value for edit'
        for(count=0; count<numberOfTrials; count++) {
            try {
                waitFor { popupAddServerNameField.value("new_edited_server") }
                // waitFor { popupAddServerHostNameField.value("new_edited_host") }
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
                waitFor { $(serverId).text().equals("new_edited_server") }
                println("The server was edited")
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find edit button - Retrying")
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to find edited server - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException e) {
                try {
                    waitFor { $(serverId).isDisplayed() }
                    waitFor { $(serverId).text().equals("new_edited_server") }
                    println("The server was edited")
                    break
                } catch(geb.waiting.WaitTimeoutException exc) {
                    println("Unable to find edited server - Retrying")
                }
            }
        }
        when: 'Click on the server name'
        $("#dropdownMenu1 > a > span.clsServerList").click()
        then: 'Click on edit button'
        $("#btnDeleteServer > a").click()
        println($("#btnDeleteServer > a").text())

        when: 'Wait for the popup to be displayed'
        for(count=0; count<numberOfTrials; count++) {
            try {
                waitFor { popupDeleteServerButtonOk.isDisplayed() }
                popupDeleteServerButtonOk.click()
                break
            } catch (geb.waiting.WaitTimeoutException exception) {

            }
        }
        report "1"
        then:
        for(count=0; count<numberOfTrials; count++) {
            try {
                waitFor { !popupDeleteServerButtonOk.isDisplayed() }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {

            }
        }

        report "2"
    }

    def ensureServerNameAndHostNameIsEmpty() {
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
//        chooseDatabase(indexOfNewDatabase, "name_src")
        report "test1"
        int countNext = 0
//        for (count = 0; count < numberOfTrials; count++) {
        try {
            for(countNext=0; countNext<numberOfTrials; countNext++) {
                try {
                    waitFor { buttonAddDatabase.isDisplayed() }
//                        break
                } catch(geb.waiting.WaitTimeoutException exception) {
                    currentDatabase.click()
                }
            }
            $(id:getIdOfDatabase(String.valueOf(indexOfNewDatabase))).click()
            waitFor { currentDatabase.text().equals("name_src") }
        } catch (geb.waiting.WaitTimeoutException exception) {
            println("Waiting - Retrying")
        } catch (org.openqa.selenium.StaleElementReferenceException e) {
            println("Stale Element exception - Retrying")
        } catch(org.openqa.selenium.ElementNotVisibleException exception) {
            try {
                waitFor { currentDatabase.text().equals("name_src") }
            } catch (geb.waiting.WaitTimeoutException exc) {
                println("Waiting - Retrying")
            }
        }
//        }

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
                //waitFor { page.buttonAddServer.click() }
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
//        chooseDatabase(indexOfNewDatabase, "name_src")

        int countNext = 0
        for (count = 0; count < numberOfTrials; count++) {
            try {
                for(countNext=0; countNext<numberOfTrials; countNext++) {
                    try {
                        waitFor { buttonAddDatabase.isDisplayed() }
                            break
                    } catch(geb.waiting.WaitTimeoutException exception) {
                        currentDatabase.click()
                    }
                }
                $(id:getIdOfDatabase(String.valueOf(indexOfNewDatabase))).click()
                waitFor { currentDatabase.text().equals("name_src") }
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals("name_src") }
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }

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
                //waitFor { page.buttonAddServer.click() }
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
                report "jelly"
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
        println("Print name" + testingName)
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

        int countNext = 0
//        for (count = 0; count < numberOfTrials; count++) {
        try {
            for(countNext=0; countNext<numberOfTrials; countNext++) {
                try {
                    waitFor { buttonAddDatabase.isDisplayed() }
//                        break
                } catch(geb.waiting.WaitTimeoutException exception) {
                    currentDatabase.click()
                }
            }
            $(id:getIdOfDatabase(String.valueOf(indexOfNewDatabase))).click()
            waitFor { currentDatabase.text().equals("name_src") }
        } catch (geb.waiting.WaitTimeoutException exception) {
            println("Waiting - Retrying")
        } catch (org.openqa.selenium.StaleElementReferenceException e) {
            println("Stale Element exception - Retrying")
        } catch(org.openqa.selenium.ElementNotVisibleException exception) {
            try {
                waitFor { currentDatabase.text().equals("name_src") }

            } catch (geb.waiting.WaitTimeoutException exc) {
                println("Waiting - Retrying")
            }
        }
//        }

        when: 'Set the id for new server'
        String serverId = getCssPathOfServer(1)
        then: 'Click Add Server button to open popup'
        for(count=0; count<numberOfTrials; count++) {
            try {
                page.btnAddServerOption.click()
                // page.buttonAddServer.click()
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
                waitFor { popupAddServerHostNameField.value(testingName) }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to provide value to the text fields - Retrying")
            }
        }
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
                //page.buttonAddServer.click()
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
                waitFor { popupAddServerHostNameField.value(testingName) }
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
        then: 'Check for the error message'
        try {
            waitFor { page.errorClientPort.text().equals("Default port already exists") }
            report "hello"
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
//        chooseDatabase(indexOfNewDatabase, "name_src")

        int countNext = 0
//        for (count = 0; count < numberOfTrials; count++) {
        try {
            for(countNext=0; countNext<numberOfTrials; countNext++) {
                try {
                    waitFor { buttonAddDatabase.isDisplayed() }
//                        break
                } catch(geb.waiting.WaitTimeoutException exception) {
                    currentDatabase.click()
                }
            }
            $(id:getIdOfDatabase(String.valueOf(indexOfNewDatabase))).click()
            waitFor { currentDatabase.text().equals("name_src") }
        } catch (geb.waiting.WaitTimeoutException exception) {
            println("Waiting - Retrying")
        } catch (org.openqa.selenium.StaleElementReferenceException e) {
            println("Stale Element exception - Retrying")
        } catch(org.openqa.selenium.ElementNotVisibleException exception) {
            try {
                waitFor { currentDatabase.text().equals("name_src") }

            } catch (geb.waiting.WaitTimeoutException exc) {
                println("Waiting - Retrying")
            }
        }
//        }

        when: 'Set the id for new server'
        String serverId = getCssPathOfServer(1)
        String serverIdNext = getCssPathOfServer(2)
        then: 'Click Add Server button to open popup'
        for(count=0; count<numberOfTrials; count++) {
            try {
                page.btnAddServerOption.click()
                // page.buttonAddServer.click()
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
                waitFor { popupAddServerHostNameField.value(testingName) }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to provide value to the text fields - Retrying")
            }
        }
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
                // page.buttonAddServer.click()
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
                waitFor { popupAddServerHostNameField.value(testingName) }

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

    def logOfServer() {
        int index = 1
        when: 'Open the popup and check the title and Ok button - Retrying'
        for(count=0; count<numberOfTrials; count++) {
            try {
                $(page.getCssPathOfLog(index)).click()
                waitFor { page.logPopupTitle.isDisplayed() }
                waitFor { page.logPopupOk.isDisplayed() }
                report "hello"
                break
            } catch(geb.waiting.WaitTimeoutException exception) {
                println("WaitTimeoutException - Retrying")
            }
        }
        then: 'Close the popup'
        page.logPopupOk.click()
    }

    /*def cleanup() {
        to ClusterSettingsPage
        int indexToDelete = 2
        indexOfNewDatabase = 1
        chooseDatabase(indexOfNewDatabase, "Database")
        deleteNewDatabase(indexToDelete, "name_src")
    }*/
}