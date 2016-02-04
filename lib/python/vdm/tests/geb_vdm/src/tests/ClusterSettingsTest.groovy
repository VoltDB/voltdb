/*
This file is part of VoltDB.

Copyright (C) 2008-2016 VoltDB Inc.

This file contains original code and/or modifications of original code.
Any modifications made by VoltDB Inc. are licensed under the following
terms and conditions:

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.
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
                break
            } catch(Exception e) {
                deleteDatabase(create_DatabaseTest_File)
            } catch(org.codehaus.groovy.runtime.powerassert.PowerAssertionError e) {
                deleteDatabase(create_DatabaseTest_File)
            }
        }
        then: 'Choose new database'
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
                waitFor { popupAddServerDescriptionField.value("") }
                waitFor { popupAddServerClientListenerField.value("192.168.0.1:8000") }
                waitFor { popupAddServerAdminListenerField.value("192.168.0.2:8000") }
                waitFor { popupAddServerHttpListenerField.value("192.168.0.3:8000") }
                waitFor { popupAddServerInternalListenerField.value("192.168.0.4:8000") }
                waitFor { popupAddServerZookeeperListenerField.value("192.168.0.5:8000") }
                waitFor { popupAddServerReplicationListenerField.value("192.168.0.6:8000") }
                waitFor { popupAddServerInternalInterfaceField.value("192.168.0.7") }
                waitFor { popupAddServerExternalInterfaceField.value("192.168.0.8") }
                waitFor { popupAddServerPublicInterfaceField.value("192.168.0.9") }
                waitFor { popupAddServerPlacementGroupField.value("placement_value")}
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
            waitFor { $(serverId).text() == "new_host" }
            println($(serverId).text() + " get Id of server")
            println("The new server was created")
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Test Fail: The new server was not created")
            assert false
        }
//

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
                report "123"
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        and: 'provide value for edit'
        for(count=0; count<numberOfTrials; count++) {
            try {
                waitFor { popupAddServerNameField.value("new_edited_server") }
                waitFor { popupAddServerHostNameField.value("new_edited_host") }
                waitFor { popupAddServerDescriptionField.value("") }
                waitFor { popupAddServerClientListenerField.value("192.168.0.10:8000") }
                waitFor { popupAddServerAdminListenerField.value("192.168.0.20:8000") }
                waitFor { popupAddServerHttpListenerField.value("192.168.0.30:8000") }
                waitFor { popupAddServerInternalListenerField.value("192.168.0.40:8000") }
                waitFor { popupAddServerZookeeperListenerField.value("192.168.0.50:8000") }
                waitFor { popupAddServerReplicationListenerField.value("192.168.0.60:8000") }
                waitFor { popupAddServerInternalInterfaceField.value("192.168.0.70") }
                waitFor { popupAddServerExternalInterfaceField.value("192.168.0.80") }
                waitFor { popupAddServerPublicInterfaceField.value("192.168.0.90") }
                waitFor { popupAddServerPlacementGroupField.value("new_value_for_placement)") }
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
/*

        when: 'Click edit button to open the edit popup'
        for(count=1; count<numberOfTrials; count++) {
            try {
                $(id:editId).click()
                waitFor { popupAddServerButtonOk.isDisplayed() }
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find edit button - Retrying")
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to find the edit popup - Retrying")
            }
        }
        and: 'provide value for edit'
        for(count=0; count<numberOfTrials; count++) {
            try {
                waitFor { popupAddServerNameField.value("new_edited_server") }
                waitFor { popupAddServerHostNameField.value("new_edited_host") }
                waitFor { popupAddServerDescriptionField.value("") }
                waitFor { popupAddServerClientListenerField.value("192.168.0.10:8000") }
                waitFor { popupAddServerAdminListenerField.value("192.168.0.20:8000") }
                waitFor { popupAddServerHttpListenerField.value("192.168.0.30:8000") }
                waitFor { popupAddServerInternalListenerField.value("192.168.0.40:8000") }
                waitFor { popupAddServerZookeeperListenerField.value("192.168.0.50:8000") }
                waitFor { popupAddServerReplicationListenerField.value("192.168.0.60:8000") }
                waitFor { popupAddServerInternalInterfaceField.value("192.168.0.70") }
                waitFor { popupAddServerExternalInterfaceField.value("192.168.0.80") }
                waitFor { popupAddServerPublicInterfaceField.value("192.168.0.90") }
                waitFor { popupAddServerPlacementGroupField.value("new_value_for_placement)") }
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
                waitFor { $(id:serverId).isDisplayed() }
                waitFor { $(id:serverId).text().equals("new_edited_host") }
                println("The server was edited")
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find edit button - Retrying")
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to find edited server - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException e) {
                try {
                    waitFor { $(id:serverId).isDisplayed() }
                    waitFor { $(id:serverId).text().equals("new_edited_host") }
                    println("The server was edited")
                    break
                } catch(geb.waiting.WaitTimeoutException exc) {
                    println("Unable to find edited server - Retrying")
                }
            }
        }
*/

        try {
            waitFor(10) { 1==0 }
        } catch(geb.waiting.WaitTimeoutException exception) {
            println("waited")
        }

        when:
        for(count=0; count<numberOfTrials; count++) {
            try {
                //$(deleteId).click()
                $("#deleteServer_2").click()
                waitFor { popupDeleteServerButtonOk.isDisplayed() }
                report '1'
                try {
                    waitFor { 1==0 }
                } catch(geb.waiting.WaitTimeoutException exception) {
                    println("waited")
                }
                report '123'
                popupDeleteServerButtonOk.click()
                report '12345'
                waitFor { !$(popupDeleteServerButtonOk).isDisplayed() }
                report '1234567'

                try {
                    waitFor { 1==0 }
                } catch(geb.waiting.WaitTimeoutException exception) {
                    println("waited")
                }
                report '123456789'
                waitFor { trialerror.text().equals("No servers available.") }
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find delete button - Retrying")
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to find the delete popup - Retrying")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element - Retrying")
            }
        }
        then:
        for(count=0; count<numberOfTrials; count++) {
            try {
                popupDeleteServerButtonOk.click()
                waitFor { !$(serverId).click() }
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find delete button - Retrying")
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to find the delete popup - Retrying")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException e) {
                try {
                    waitFor { !$(serverId).click() }
                    break
                } catch(geb.waiting.WaitTimeoutException exc) {
                    println("Unable to find the delete popup - Retrying")
                }
            }
        }

        when: 'Choose the database with index 1'
        openDatabase()
        chooseDatabase(indexOfLocal, "local")
        and: 'Click delete for the required database'
        openDatabase()
        then: 'Delete the database'
        deleteNewDatabase(indexOfNewDatabase, "name_src")
        println()
    }

    def "Ensure Server name and Host name is not empty"(){
        when:"Click Add Server button"
        try {
            page.btnAddServerOption.click()
            page.buttonAddServer.click()
//            waitFor { page.popupAddServer.isDisplayed() }
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Unable to find Add Server button or popup - Retrying")
        }

        then: "Check popup is displayed"
        waitFor{page.popupAddServer.isDisplayed()}
        when: "click the Ok button"
        page.popupAddServerButtonOk.click()

        then: "Check validation for server name and host name exists"
        errorServerName.isDisplayed()
        errorHostName.isDisplayed()
        page.popupAddServerButtonCancel.click()
    }

    def "Ensure Duplicate Server name validation"(){
        boolean status = false
        int newValue = 1
        when:
        count = 1
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
        try {
            waitFor {page.btnAddServerOption.click() }
            waitFor { page.buttonAddServer.click() }
            waitFor { page.popupAddServer.isDisplayed() }
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Unable to find Add Server button or popup - Retrying")
        }
        and:
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
        then:
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
            }
        }

        when:"Click Add Server button"
        waitFor(30) { page.btnAddServerOption.click() }
        waitFor { page.buttonAddServer.click() }
        then:
        waitFor { page.popupAddServer.isDisplayed() }

        when: "click the Ok button"
        waitFor { page.popupAddServerButtonOk.click() }
        then: "Check validation for server name and host name exists"
        waitFor { page.errorServerName.isDisplayed() }
        waitFor { page.errorHostName.isDisplayed() }
        println(errorHostName.text())
        println(errorServerName.text())
//        errorServerName.text().equals("This server name already exists.")
//        errorHostName.text().equals("This host name already exists.")
    }

    def "Ensure client listener is valid"(){
        boolean status = false
        int newValue = 1
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
        try {
            waitFor { page.btnAddServerOption.click() }
            waitFor { page.buttonAddServer.click() }
            waitFor { page.popupAddServer.isDisplayed() }
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Unable to find Add Server button or popup - Retrying")
        }
        and:
        for(count=0; count<numberOfTrials; count++) {
            try {
                waitFor { popupAddServerNameField.value("new_server") }
                waitFor { popupAddServerHostNameField.value("new_host") }
                waitFor { popupAddServerDescriptionField.value("") }
                waitFor { popupAddServerClientPortField.value("192.168.0.1") }
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
                waitFor { !popupAddServerButtonOk.isDisplayed() }
                errorClientPort.isDisplayed()
                errorClientPort.text().equals("Please enter a valid value.(e.g, 127.0.0.1:8000 or 8000(1-65535))")
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
    }

    def "Ensure internal interface is valid"(){
        boolean status = false
        int newValue = 1
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
        try {
            waitFor { page.btnAddServerOption.click() }
            waitFor { page.buttonAddServer.click() }
            waitFor { page.popupAddServer.isDisplayed() }
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Unable to find Add Server button or popup - Retrying")
        }
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
                waitFor { !popupAddServerButtonOk.isDisplayed() }
                errorInternalInterface.isDisplayed()
                errorInternalInterface.text().equals("Please enter a valid IP address.")
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
    }

    def cleanupSpec() {
        to ClusterSettingsPage

        println("CleanupSpec: Cleaning up any residue")
        int count = 1
        int newValue = 1
        while(count<numberOfTrials) {
            count++
            try {
                waitFor { $(id:page.getIdOfDeleteButton(count)).isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        newValue = count
        for(count=1; count<numberOfTrials; count++) {
            try {
                $(id:page.getIdOfDeleteButton(newValue)).click()
                waitFor { popupDeleteServerButtonOk.isDisplayed() }
                println("Deleted")
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to find the delete server button - Retrying")
            }
        }
        for(count=1; count<numberOfTrials; count++) {
            try {
                popupDeleteServerButtonOk.click()
                waitFor { !$(id:page.getIdOfDeleteButton(count)).isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Deleted")
                break
            }
        }
    }
}