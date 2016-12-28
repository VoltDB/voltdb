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

import java.io.BufferedReader
import java.io.FileNotFoundException
import java.io.FileReader
import java.io.IOException
import org.openqa.selenium.WebElement
import org.openqa.selenium.Keys

class ClusterConfigurationTest extends TestBase {
    String testingName = getTestingUrl()
    static String sitePerHost = "Site per host"
    static String ksafety = "K-safety"
    static String partitionDetection = "Partition detection"
    static String security = "Security"
    static String httpAccess = "HTTP Access"
    static String autoSnapshots = "Auto Snapshots"
    static String commandLogging = "Command Logging"
    static String _export = "Export"
    static String _import = "Import"
    static String advanced = "Advanced"

    static String maxJavaHeap = "Max Java Heap"
    static String heartbeatTimeout = "Heartbeat time out"
    static String queryTimeout = "Query time out"
    static String maxTempTableMemory = "Max temp table memory"
    static String snapshotPriority = "Snapshot priority"
    static String memoryLimit = "Memory Limit"

    static String username = "Username"
    static String role = "Role"

    static String jsonApi = "JSON API"

    static String filePrefix = "File Prefix"
    static String frequency = "Frequency"
    static String retained = "Retained"

    static String logFrequencyTime = "Log Frequency Time"
    static String logFrequencyTransactions = "Log Frequency Transactions"
    static String logSegmentSize = "Log Segment Size"

    static String rootDestination = "Root (destination)"
    static String snapshot = "Snapshot"
    static String exportOverflow = "Export Overflow"
    static String commandLog = "Command Log"
    static String commandLogSnapshots = "Command Log Snapshots"

    static String drEnabled = "DR Enabled"
    static String type = "Type"
    static String connectionSource = "Connection Source"
    static String masterCluster = "Master Cluster"
    static String servers = "Servers"

    String saveMessage = "Changes have been saved."
    boolean statusOfTest

    String create_DatabaseTest_File = "src/resources/create_DatabaseTest.csv"
    String edit_DatabaseTest_File   = "src/resources/edit_DatabaseTest.csv"
    String cvsSplitBy = ","

    String newDatabaseName = ""

    BufferedReader br = null
    String line = ""
    String[] extractedValue = ["random_input", "random_input"]
    String newValueDatabase = 0
    boolean foundStatus = false

    int indexOfNewDatabase = 0
    int countNext = 0
    int indexOfLocal = 1
    int indexOfFailure = 0

    String new_string = ""

    def setup() { // called before each test
        count = 0

        while (count < numberOfTrials) {
            count++
            try {
                setup: 'Open Cluster Settings page'
                to ClusterSettingsPage
                expect: 'to be on Cluster Settings page'
                at ClusterSettingsPage

                break
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                println("ElementNotVisibleException: Unable to Start the test")
                println("Retrying")
            }
        }
    }

    def verifySitePerHost() {
        println("Test: verifySitePerHost")
        // String oldVariable = overview.sitePerHostField.value()

        when: 'Verify if text and field are displayed'
        for(count=0; count<numberOfTrials; count++) {
            try {
                if (overview.sitePerHostText.text() == sitePerHost && overview.sitePerHostField.isDisplayed()) {
                    println("Test Pass: The text and field are displayed")
                    break
                } else if (overview.sitePerHostText.text() != sitePerHost) {
                    println("Test Fail: The text is not displayed")
                    assert false
                } else if (!overview.sitePerHostField.isDisplayed()) {
                    println("Test Fail: The field is not displayed")
                    assert false
                } else {
                    println("Test Fail: Unknown error")
                    assert false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        newDatabaseName = "name_src"
        and: 'Create new database'
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
        then: 'Choose the new database'
        //chooseDatabase(indexOfNewDatabase, "name_src")

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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }

        when: 'Provide Value for Site Per Host'
        overview.sitePerHostField.value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        overview.sitePerHostField.value("1")
        overview.sitePerHostText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Site Per Host has changed'
        if(overview.sitePerHostField.value() == "1") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Choose the database with index 1'
        openDatabase()
        //chooseDatabase(indexOfLocal, "local")
        newDatabaseName = "Database"
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }
        and: 'Click delete for the required database'
        openDatabase()
        then: 'Delete the database'
        deleteNewDatabase(indexOfNewDatabase, "name_src")
        println()
    }

    def verifyKSafety() {
        println("Test: verifyKSafety")
        String oldVariable = overview.ksafetyField.value()

        when: 'Verify if text and field are displayed'
        for(count=0; count<numberOfTrials; count++) {
            try {
                if(overview.ksafetyText.text() == ksafety && overview.ksafetyField.isDisplayed()) {
                    println("Test Pass: The text and field are displayed")
                    break
                }
                else if(overview.ksafetyText.text() != ksafety) {
                    println("Test Fail: The text is not displayed")
                    assert false
                }
                else if(!overview.ksafetyField.isDisplayed()) {
                    println("Test Fail: The field is not displayed")
                    assert false
                }
                else {
                    println("Test Fail: Unknown error")
                    assert false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        newDatabaseName = "name_src"
        and: 'Create new database'
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
        then: 'Choose the new database'
        //chooseDatabase(indexOfNewDatabase, "name_src")
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }

        when: 'Provide Value for K-Safety'
        overview.ksafetyField.value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        overview.ksafetyField.value("1")
        overview.ksafetyText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in K-Safety has changed'
        if(overview.ksafetyField.value() == "1") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Choose the database with index 1'
        openDatabase()
        //chooseDatabase(indexOfLocal, "local")
        newDatabaseName = "Database"
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }
        and: 'Click delete for the required database'
        openDatabase()
        then: 'Delete the database'
        deleteNewDatabase(indexOfNewDatabase, "name_src")
        println()
    }

    def verifyPartitionDetection() {
        println("Test: verifyPartitionDetection")
        String initialStatus = overview.partitionDetectionStatus.text()
        println("Initially " + initialStatus)
        statusOfTest = false

        when: 'Verify if text and field are displayed'
        overview.partitionDetectionText.isDisplayed()
        then: 'Provide message'
        for(count=0; count<numberOfTrials; count++) {
            try {
                if(overview.partitionDetectionText.text() == partitionDetection ) {
                    println("Test Pass: The text and field are displayed")
                    break
                }
                else if(overview.partitionDetectionText.text() != partitionDetection) {
                    println("Test Fail: The text is not displayed")
                    assert false
                }
                else {
                    println("Test Fail: Unknown error")
                    assert false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }

        when: 'Create a new database'
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
        newDatabaseName = "name_src"
        then: 'Choose the new database'
        //chooseDatabase(indexOfNewDatabase, "name_src")
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }

        when: 'Get the status of Partition Detection'
        String statusOfPartitionDetection = overview.partitionDetectionStatus.text()
        and: 'Provide Value for Partition Detection'
        overview.partitionDetectionCheckbox.click()
        overview.partitionDetectionText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Partition Detection has changed'
        if(statusOfPartitionDetection == "Off" && overview.partitionDetectionStatus.text() == "On") {
            println("Test Pass: The change is displayed")
        }
        else if(statusOfPartitionDetection == "On" && overview.partitionDetectionStatus.text() == "Off") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Choose the database with index 1'
        openDatabase()
        //chooseDatabase(indexOfLocal, "local")
        newDatabaseName = "Database"
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }
        and: 'Click delete for the required database'
        openDatabase()
        then: 'Delete the database'
        deleteNewDatabase(indexOfNewDatabase, "name_src")
        println()
    }

    def verifySecurity() {
        boolean result = overview.CheckIfSecurityChkExist()
        println("Test: verifySecurity")
        when:
        if(result) {
            String initialStatus = overview.securityStatus.text()
            println("Initially " + initialStatus)

            for (count = 0; count < numberOfTrials; count++) {
                try {
                    if (overview.securityText.text() == security) {
                        println("Test Pass: The text and field are displayed")
                        break
                    } else if (overview.securityText.text() != security) {
                        println("Test Fail: The text is not displayed")
                        assert false
                    } else {
                        println("Test Fail: Unknown error")
                        assert false
                    }
                } catch (org.openqa.selenium.StaleElementReferenceException e) {
                    println("Stale Element Exception - Retrying")
                }
            }
            overview.securityCheckbox.isDisplayed()
            overview.securityText.click()
            for (count = 0; count < numberOfTrials; count++) {
                try {
                    if (overview.usernameTitleText.text() == username && overview.roleTitleText.text() == role) {
                        println("Test Pass: The contents are present")
                        break
                    } else if (overview.usernameTitleText.text() != username) {
                        println("Test Fail: The Username has error")
                        assert false
                    } else if (overview.roleTitleText.text() != role) {
                        println("Test Fail: The Role has error")
                        assert false
                    } else {
                        println("Test Fail: Unknown Error")
                        assert false
                    }
                } catch (org.openqa.selenium.StaleElementReferenceException e) {
                    println("Stale Element Exception - Retrying")
                }
            }

            statusOfTest = false
            for (count = 0; count < numberOfTrials; count++) {
                try {
                    overview.securityCheckbox.click()
                    overview.securityText.click()

                    if (waitFor(60) { saveStatus.text().equals(saveMessage) }) {

                    } else {
                        println("Test Fail: The required text is not displayed")
                        assert false
                    }

                    if (overview.securityStatus.text().equals(initialStatus)) {
                        println("The status hasn't changed")
                    } else if (!overview.securityStatus.text().equals(initialStatus)) {
                        statusOfTest = true
                        break
                    } else {
                        println("Unknown error")
                    }
                } catch (geb.waiting.WaitTimeoutException exception) {
                    println("Waiting error - Retrying")
                }
            }
            if (statusOfTest == true) {
                println("Test Pass: The change is displayed")
            } else {
                println("Test Fail: The change isn't displayed")
                assert false
            }

            statusOfTest = false
            for (count = 0; count < numberOfTrials; count++) {
                try {
                    overview.securityCheckbox.click()
                    overview.securityText.click()
                    if (waitFor(60) { saveStatus.text().equals(saveMessage) }) {

                    } else {
                        println("Test Fail: The required text is not displayed")
                        assert false
                    }

                    if (overview.securityStatus.text().equals(initialStatus)) {
                        statusOfTest = true
                        break
                    } else if (!overview.securityStatus.text().equals(initialStatus)) {
                        println("The status hasn't restored")
                    } else {
                        println("Unknown error")
                    }
                } catch (geb.waiting.WaitTimeoutException exception) {
                    println("Waiting error - Retrying")
                }
            }
            if (statusOfTest == true) {
                println("Test Pass: The restore is displayed")
            } else {
                println("Test Fail: The restore isn't displayed")
                assert false
            }
        }
        else {
            println("Security is not enabled.")
        }
        then:
        println()
    }

    def verifyHttpAccess() {
        println("Test: verifyHttpAccess")
        String initialStatus = overview.httpAccessStatus.text()
        println("Initially " + initialStatus)

        when: 'Verify if text and field are displayed'
        for(count=0; count<numberOfTrials; count++) {
            try {
                if(overview.httpAccessText.text() == httpAccess) {
                    println("Test Pass: The text and field are displayed")
                    break
                }
                else if(overview.httpAccessText.text() != httpAccess) {
                    println("Test Fail: The text is not displayed")
                    assert false
                }
                else {
                    println("Test Fail: Unknown error")
                    assert false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        then: 'Provide message'
        overview.httpAccessText.click()
        for(count=0; count<numberOfTrials; count++) {
            try {
                if(overview.jsonApiText.text() == jsonApi) {
                    println("Test Pass: The contents are present")
                    break
                }
                else {
                    println("Unknown Error")
                    assert false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }

        when: 'Create a new database'
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
        newDatabaseName = "name_src"
        then: 'Choose the new database'
        //chooseDatabase(indexOfNewDatabase, "name_src")
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }

        when: 'Get the status of Http Access'
        String statusOfHttpAccess = overview.httpAccessStatus.text()
        and: 'Provide Value for Http Access'
        overview.httpAccessCheckbox.click()
        overview.httpAccessText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Http Access has changed'
        if(statusOfHttpAccess == "Off" && overview.httpAccessStatus.text() == "On") {
            println("Test Pass: The change is displayed")
        }
        else if(statusOfHttpAccess == "On" && overview.httpAccessStatus.text() == "Off") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Choose the database with index 1'
        openDatabase()
        //chooseDatabase(indexOfLocal, "local")
        newDatabaseName = "Database"
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }
        and: 'Click delete for the required database'
        openDatabase()
        then: 'Delete the database'
        deleteNewDatabase(indexOfNewDatabase, "name_src")
        println()
    }

    def verifyAutoSnapshots() {
        println("Test: verifyAutoSnapshots")

        when: 'Verify if text and field are displayed'
        for(count=0; count<numberOfTrials; count++) {
            try {
                if(overview.autoSnapshotsText.text() == autoSnapshots ) {
                    println("Test Pass: The text and field are displayed")
                    statusOfTest = true
                    break
                }
                else if(overview.autoSnapshotsText.text() != autoSnapshots) {
                    statusOfTest = false
                    indexOfFailure = 1
                }
                else {
                    statusOfTest = false
                    indexOfFailure = 0
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        then: 'Display the message regarding the text display failure if it exists'
        if(statusOfTest==true) {

        }
        else if(statusOfTest==false && indexOfFailure==0) {
            println("Test Fail: Unknown error")
            assert false
        }
        else if(statusOfTest==false && indexOfFailure==1) {
            println("Test Fail: The text is not displayed")
            assert false
        }
        else {
            println("Test Fail: Unknown error")
            assert false
        }

        when: 'Reset values'
        statusOfTest = false
        indexOfFailure = 0
        then: 'Display message regarding reset values'
        println("The values statusOfTest and indexOfFailure have been reset")

        when: 'Open and check the texts of expanded Auto Snapshots'
        for(count=0; count<numberOfTrials; count++) {
            overview.autoSnapshotsText.click()
            try {
                if(overview.filePrefixText.text() == filePrefix &&
                        overview.frequencyText.text() == frequency &&
                        overview.retainedText.text() == retained &&
                        overview.filePrefixField.isDisplayed() &&
                        overview.frequencyField.isDisplayed() &&
                        overview.retainedField.isDisplayed()
                ) {
                    println("Test Pass: The contents are present")
                    statusOfTest = true
                    break
                }
                else if(overview.filePrefixText.text() != filePrefix) {
                    statusOfTest = false
                    indexOfFailure = 1
                }
                else if(overview.frequencyText.text() != frequency) {
                    statusOfTest = false
                    indexOfFailure = 2
                }
                else if(overview.retainedText.text() != retained) {
                    statusOfTest = false
                    indexOfFailure = 3
                }
                else if(!overview.filePrefixField.isDisplayed()) {
                    statusOfTest = false
                    indexOfFailure = 4
                }
                else if(!overview.frequencyField.isDisplayed()) {
                    statusOfTest = false
                    indexOfFailure = 5
                }
                else if(!overview.retainedField.isDisplayed()) {
                    statusOfTest = false
                    indexOfFailure = 6
                }
                else {
                    statusOfTest = false
                    indexOfFailure = 0
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        then: 'Display the message regarding the text display failure if it exists'
        if(statusOfTest==true) {

        }
        else if(statusOfTest==false && indexOfFailure==0) {
            println("Test Fail: Unknown error")
            assert false
        }
        else if(statusOfTest==false && indexOfFailure==1) {
            println("Test Fail: The file prefix text has error")
            assert false
        }
        else if(statusOfTest==false && indexOfFailure==2) {
            println("Test Fail: The frequency text has error")
            assert false
        }
        else if(statusOfTest==false && indexOfFailure==3) {
            println("Test Fail: The retained text has error")
            assert false
        }
        else if(statusOfTest==false && indexOfFailure==4) {
            println("Test Fail: The file prefix field is not displayed")
            assert false
        }
        else if(statusOfTest==false && indexOfFailure==5) {
            println("Test Fail: The frequency field is not displayed")
            assert false
        }
        else if(statusOfTest==false && indexOfFailure==6) {
            println("Test Fail: The retained field is not displayed")
            assert false
        }
        else {
            println("Test Fail: Unknown error")
            assert false
        }

        when: 'Reset values'
        statusOfTest = false
        indexOfFailure = 0
        then: 'Display message regarding reset values'
        println("The values statusOfTest and indexOfFailure have been reset")

        when: 'Create new database'
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
        newDatabaseName = "name_src"
        then: 'Choose the new database'
        //chooseDatabase(indexOfNewDatabase, "name_src")
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }

        when: 'Get the status of Http Access'
        String statusOfAutoSnapshots = overview.autoSnapshotsStatus.text()
        and: 'Provide Value for Http Access'
        overview.autoSnapshotsCheckbox.click()
        overview.autoSnapshotsText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Auto Snapshots has changed'
        if(statusOfAutoSnapshots == "Off" && overview.autoSnapshotsStatus.text() == "On") {
            println("Test Pass: The change is displayed")
        }
        else if(statusOfAutoSnapshots == "On" && overview.autoSnapshotsStatus.text() == "Off") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        for(count=0; count<numberOfTrials; count++) {
            try {
                overview.autoSnapshotsText.click()
                waitFor {  overview.filePrefixField.isDisplayed }
                break
            } catch(geb.waiting.WaitTimeoutException exception) {

            }
        }

        when: 'Provide Value for File Prefix Field in Auto Snapshot'
        overview.filePrefixField.value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        overview.filePrefixField.value("At_Least")
        overview.filePrefixText.click()
        and: 'Check Save Message'
        waitFor(60) { saveStatus.text().equals(saveMessage) }
        then: 'Check if Value in File Prefix Field in Auto Snapshot has changed'
        if(overview.filePrefixField.value() == "At_Least") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Provide Value for Frequency in Auto Snapshot'
        overview.frequencyField.value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        overview.frequencyField.value("10")
        overview.frequencyText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Frequency in Auto Snapshot has changed'
        if(overview.frequencyField.value() == "10") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Provide Value for Retained in Auto Snapshot'
        overview.retainedField.value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        overview.retainedField.value("10")
        overview.retainedText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Retained in Auto Snapshot has changed'
        if(overview.retainedField.value() == "10") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Choose the database with index 1'
        openDatabase()
        //chooseDatabase(indexOfLocal, "local")
        newDatabaseName = "Database"
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }
        and: 'Click delete for the required database'
        openDatabase()
        then: 'Delete the database'
        deleteNewDatabase(indexOfNewDatabase, "10")
        println()
    }

    def verifyCommandLogging() {
        println("Test: verifyCommandLogging")
        String oldVariableLogFrequencyTime          = overview.logFrequencyTimeField.value()
        String oldVariableLogFrequencyTransactions  = overview.logFrequencyTransactionsField.value()
        String oldVariableLogSegmentSize            = overview.logSegmentSizeField.value()
        String initialStatus                        = overview.commandLoggingStatus.text()
        println("Initially " + initialStatus)

        when: 'Verify if text and field are displayed'
        for(count=0; count<numberOfTrials; count++) {
            try {
                if(overview.commandLoggingText.text() == commandLogging ) {
                    println("Test Pass: The text and field are displayed")
                    break
                }
                else if(overview.commandLoggingText.text() != commandLogging) {
                    println("Test Fail: The text is not displayed")
                    assert false
                }
                else {
                    println("Test Fail: Unknown error")
                    assert false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        then: 'Provide message'
        overview.commandLoggingText.click()
        for(count=0; count<numberOfTrials; count++) {
            try {
                if(overview.logFrequencyTimeText.text() == logFrequencyTime &&
                        overview.logFrequencyTransactionsText.text() == logFrequencyTransactions &&
                        overview.logSegmentSizeText.text() == logSegmentSize &&
                        overview.logFrequencyTimeField.isDisplayed() &&
                        overview.logFrequencyTransactionsField.isDisplayed() &&
                        overview.logSegmentSizeField.isDisplayed()
                ) {
                    println("Test Pass: The contents are present")
                    break
                }
                else if (overview.logFrequencyTimeText.text() != logFrequencyTime) {
                    println("Test Fail: The Log Frequency Time text has error")
                    assert false
                }
                else if (overview.logFrequencyTransactionsText.text() != logFrequencyTransactions) {
                    println("Test Fail: The Log Frequency Transactions text has error")
                    assert false
                }
                else if (overview.logSegmentSizeText.text() != logSegmentSize) {
                    println("Test Fail: The Log Segment Size text has error")
                    assert false
                }
                else if (!overview.logFrequencyTimeField.isDisplayed()) {
                    println("Test Fail: The Log Frequency Time field is not displayed")
                    assert false
                }
                else if (!overview.logFrequencyTransactionsField.isDisplayed()) {
                    println("Test Fail: The Log Frequency Transaction field is not displayed")
                    assert false
                }
                else if (!overview.logSegmentSizeField.isDisplayed()) {
                    println("Test Fail: The Log Segment Size field is not displayed")
                    assert false
                }
                else {
                    println("Test Fail: Unknown Error")
                    assert false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }

        when: 'Create new database that returns'
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
        newDatabaseName = "name_src"
        then: 'Choose the new database'
        //chooseDatabase(indexOfNewDatabase, "name_src")
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }

        when: 'Get the status of Command Logging'
        String statusOfCommandLogging = overview.commandLoggingStatus.text()
        and: 'Provide Value for Command Logging'
        overview.commandLoggingCheckbox.click()
        overview.commandLoggingText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Command Logging has changed'
        if(statusOfCommandLogging == "Off" && overview.commandLoggingStatus.text() == "On") {
            println("Test Pass: The change is displayed")
        }
        else if(statusOfCommandLogging == "On" && overview.commandLoggingStatus.text() == "Off") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Provide Value for Log Frequency Time in Command Logging'
        overview.commandLoggingText.click()
        overview.logFrequencyTimeField.value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        overview.logFrequencyTimeField.value("10")
        overview.logFrequencyTimeText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Log Frequency Time in Command Logging has changed'
        println(overview.logFrequencyTimeField.value())
        if(overview.logFrequencyTimeField.value() == "10") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Provide Value for Log Frequency Transactions in Command Logging'
        overview.logFrequencyTransactionsField.value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        overview.logFrequencyTransactionsField.value("10")
        overview.logFrequencyTransactionsText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Log Frequency Transactions in Command Logging has changed'
        if(overview.logFrequencyTransactionsField.value() == "10") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Provide Value for Log Segment Size in Command Logging'
        overview.logSegmentSizeField.value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        overview.logSegmentSizeField.value("10")
        overview.logSegmentSizeText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Log Segment Size in Command Logging has changed'
        if(overview.logSegmentSizeField.value() == "10") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Choose the database with index 1'
        openDatabase()
        //chooseDatabase(indexOfLocal, "Database")
        newDatabaseName = "Database"
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }
        and: 'Click delete for the required database'
        openDatabase()
        then: 'Delete the database'
        deleteNewDatabase(indexOfNewDatabase, "10")
        println()
    }

    def verifyExport() {
        println("Test: verifyExport")
        when: 'Verify if text and field are displayed'
        overview.exportText.isDisplayed()

        then: 'Provide message'
        for(count=0; count<numberOfTrials; count++) {
            try {
                if(overview.exportText.text() == _export ) {
                    println("Test Pass: The text and field are displayed")
                    break
                }
                else if(overview.exportText.text() != _export) {
                    println("Test Fail: The text is not displayed")
                    assert false
                }
                else {
                    println("Test Fail: Unknown error")
                    assert false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        println()
    }

    def verifyImport() {
        println("Test: verifyImport")
        when: 'Verify if text and field are displayed'
        overview.importText.isDisplayed()

        then: 'Provide message'
        for(count=0; count<numberOfTrials; count++) {
            try {
                if(overview.importText.text() == _import ) {
                    println("Test Pass: The text and field are displayed")
                    break
                }
                else if(overview.importText.text() != _import) {
                    println("Test Fail: The text is not displayed")
                    assert false
                }
                else {
                    println("Test Fail: Unknown error")
                    assert false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        println()
    }

    def verifyAdvanced() {
        println("Test: verifyAdvanced")
        String oldVariableHeartbeatTimeout      = overview.heartbeatTimeoutField.value()
        String oldVariableQueryTimeout          = overview.queryTimeoutField.value()
        String oldVariableMaxTempTableMemory    = overview.maxTempTableMemoryField.value()
        String oldVariableSnapshotPriority      = overview.snapshotPriorityField.value()
        String oldVariableMemoryLimit           = overview.memoryLimitField.value()

        when: 'Verify if text and field are displayed'
        for(count=0; count<numberOfTrials; count++) {
            try {
                if(overview.advancedText.text() == advanced ) {
                    println("Test Pass: The text and field are displayed")
                    break
                }

                else if(overview.advancedText.text() != advanced) {
                    println("Test Fail: The text is not displayed")
                    assert false
                }
                else {
                    println("Test Fail: Unknown error")
                    assert false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        and: 'Open advanced'
        overview.advancedText.click()
        for(count=0; count<numberOfTrials; count++) {
            try {
                if(overview.heartbeatTimeoutText.text() == heartbeatTimeout &&
                        overview.queryTimeoutText.text() == queryTimeout &&
                        overview.maxTempTableMemoryText.text() == maxTempTableMemory &&
                        overview.snapshotPriorityText.text() == snapshotPriority &&
                        overview.memoryLimitText.text() == memoryLimit) {
                    println("Test Pass: The text contents are present")
                    break
                }
                else if(overview.heartbeatTimeoutText.text() != heartbeatTimeout) {
                    println("Test Fail: The text of Heartbeat Timeout has error")
                    assert false
                }
                else if(overview.queryTimeoutText.text() != queryTimeout) {
                    println("Test Fail: The text of Max Query Timeout has error")
                    assert false
                }
                else if(overview.maxTempTableMemoryText.text() != maxTempTableMemory) {
                    println("Test Fail: The text of Max Temp Table has error")
                    assert false
                }
                else if(overview.snapshotPriorityText.text() != snapshotPriority) {
                    println("Test Fail: The text of Snapshot Priority has error")
                    assert false
                }
                else if(overview.memoryLimitText.text() != memoryLimit) {
                    println("Test Fail: The text of memory Limit has error")
                    assert false
                }
                else {
                    println("Test Fail: Unknown Error")
                    assert false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        then: 'Check the contents'
        for(count=0; count<numberOfTrials; count++) {
            try {
                if( overview.heartbeatTimeoutField.isDisplayed() &&
                        overview.queryTimeoutField.isDisplayed() &&
                        overview.maxTempTableMemoryField.isDisplayed() &&
                        overview.snapshotPriorityField.isDisplayed() &&
                        overview.memoryLimitField.isDisplayed()
                ) {
                    println("Test Pass: The text contents are present")
                    break
                }
                else if(overview.heartbeatTimeoutField.isDisplayed()) {
                    println("Test Fail: The box of Heartbeat Timeout has error")
                    assert false
                }
                else if(overview.queryTimeoutField.isDisplayed()) {
                    println("Test Fail: The box of Max Query Timeout has error")
                    assert false
                }
                else if(overview.maxTempTableMemoryField.isDisplayed()) {
                    println("Test Fail: The box of Max Temp Table has error")
                    assert false
                }
                else if(overview.snapshotPriorityField.isDisplayed()) {
                    println("Test Fail: The box of Snapshot Priority has error")
                    assert false
                }
                else if(overview.memoryLimitField.isDisplayed()) {
                    println("Test Fail: The box of memory Limit has error")
                    assert false
                }
                else {
                    println("Test Fail: Unknown Error")
                    assert false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }

        when: 'Create new database that returns'
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
        newDatabaseName = "name_src"
        then: 'Choose the new database'
        //chooseDatabase(indexOfNewDatabase, "name_src")
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }

        when: 'Provide Value for Heartbeat Timeout in Advanced'
        overview.heartbeatTimeoutField.value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        overview.heartbeatTimeoutField.value("10")
        overview.heartbeatTimeoutText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Heartbeat Timeout in Advanced has changed'
        if(overview.heartbeatTimeoutField.value() == "10") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Provide Value for Query Timeout in Advanced'
        overview.queryTimeoutField.value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        overview.queryTimeoutField.value("10")
        overview.queryTimeoutText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Query Timeout in Advanced has changed'
        if(overview.queryTimeoutField.value() == "10") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Provide Value for Max Temp Table Memory in Advanced'
        overview.maxTempTableMemoryField.value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        overview.maxTempTableMemoryField.value("10")
        overview.maxTempTableMemoryText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Max Temp Table Memory in Advanced has changed'
        if(overview.maxTempTableMemoryField.value() == "10") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Provide Value for Snapshot Priority in Advanced'
        overview.snapshotPriorityField.value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        overview.snapshotPriorityField.value("10")
        overview.maxTempTableMemoryText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Snapshot Priority in Advanced has changed'
        if(overview.snapshotPriorityField.value() == "10") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Provide Value for Memory Limit in Advanced'
        overview.memoryLimitField.value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        overview.memoryLimitField.value("10")
        overview.maxTempTableMemoryText.click()

        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Memory Limit in Advanced has changed'
        if(overview.memoryLimitField.value() == "10") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Choose the database with index 1'
        openDatabase()
        //chooseDatabase(indexOfLocal, "Database")
        newDatabaseName = "Database"
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }
        and: 'Click delete for the required database'
        openDatabase()
        then: 'Delete the database'
        deleteNewDatabase(indexOfNewDatabase, "10")
        println()
    }

    def verifyRootDestination() {
        println("Test: verifyRootDestination")
        String oldVariablesRootDestination = directories.rootDestinationField.value()
//        directories.selectServer.value("Default Setting")
        when: 'Verify if text is displayed'

            println("test" +  directories.rootDestinationText.text())
            if(directories.rootDestinationText.text() == rootDestination) {
                println("Test Pass: The text is displayed")
            }
            else if(directories.rootDestinationText.text() != rootDestination) {
                println("Test Fail: The text is not displayed")
                assert false
            }
            else {
                println("Test Fail: Unknown error")
                assert false
            }
        then: 'Verify if field is displayed'
        for(count=0; count<numberOfTrials; count++) {
            try {
                if(directories.rootDestinationField.isDisplayed()) {
                    println("Test Pass: The field is displayed")
                    break
                }
                else if(!directories.rootDestinationField.isDisplayed()) {
                    println("Test Fail: The field is not displayed")
                    assert false
                }
                else {
                    println("Test Fail: Unknown error")
                    assert false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }

        when: 'Create new database'
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
        newDatabaseName = "name_src"
        then: 'Choose the new database'
        //chooseDatabase(indexOfNewDatabase, "name_src")
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }

        when: 'Provide Value for Root in Directories'
        directories.rootDestinationField.value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        directories.rootDestinationField.value("new_value")
        directories.rootDestinationText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Root in Directories has changed'
        if(directories.rootDestinationField.value() == "new_value") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Choose the database with index 1'
        openDatabase()
        //chooseDatabase(indexOfLocal, "Database")
        newDatabaseName = "Database"
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }
        and: 'Click delete for the required database'
        openDatabase()
        then: 'Delete the database'
        deleteNewDatabase(indexOfNewDatabase, "name_src")
        println()
    }

    def verifySnapshot() {
        println("Test: verifySnapshot")
        String oldVariablesSnapshot = directories.snapshotField.value()

        when: 'Verify if text is displayed'
            if(directories.snapshotText.text() == snapshot) {
                println("Test Pass: The text is displayed")
            }
            else if(directories.snapshotText.text() != snapshot) {
                println("Test Fail: The text is not displayed")
                assert false
            }
            else {
                println("Test Fail: Unknown error")
                assert false
            }

        then: 'Verify if field is displayed'

            if(directories.snapshotField.isDisplayed()) {
                println("Test Pass: The field is displayed")
            }
            else if(!directories.snapshotField.isDisplayed()) {
                println("Test Fail: The field is not displayed")
                assert false
            }
            else {
                println("Test Fail: Unknown error")
                assert false
            }


        when: 'Create new database'
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
        newDatabaseName = "name_src"
        then: 'Choose the new database'
        //chooseDatabase(indexOfNewDatabase, "name_src")
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }

        when: 'Provide Value for Snapshots in Directories'
        directories.snapshotField.value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        directories.snapshotField.value("new_value")
        directories.snapshotText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Snapshots has changed'
        if(directories.snapshotField.value() == "new_value") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Choose the database with index 1'
        openDatabase()
        //chooseDatabase(indexOfLocal, "Database")
        newDatabaseName = "Database"
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }
        and: 'Click delete for the required database'
        openDatabase()
        then: 'Delete the database'
        deleteNewDatabase(indexOfNewDatabase, "name_src")
        println()
    }

    def verifyExportOverflow() {
        println("Test: verifyExportOverflow")
        String oldVariablesExportOverflow = directories.exportOverflowField.value()

        when: 'Verify if text is displayed'
            if(directories.exportOverflowText.text() == exportOverflow) {
                println("Test Pass: The text is displayed")
            }
            else if(directories.exportOverflowText.text() != exportOverflow) {
                println("Test Fail: The text is not displayed")
                assert false
            }
            else {
                println("Test Fail: Unknown error")
                assert false
            }
        then: 'Verify if field is displayed'
            if(directories.exportOverflowField.isDisplayed()) {
                println("Test Pass: The field is displayed")
            }
            else if(!directories.exportOverflowField.isDisplayed()) {
                println("Test Fail: The field is not displayed")
                assert false
            }
            else {
                println("Test Fail: Unknown error")
                assert false
            }

        when: 'Create new database'
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
        newDatabaseName = "name_src"
        then: 'Choose the new database'
        //chooseDatabase(indexOfNewDatabase, "name_src")
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }

        when: 'Provide Value for Export Overflow in Directories'
        directories.exportOverflowField.value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        directories.exportOverflowField.value("new_value")
        directories.exportOverflowText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Export Overflow has changed'
        if(directories.exportOverflowField.value() == "new_value") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Choose the database with index 1'
        openDatabase()
        //chooseDatabase(indexOfLocal, "Database")
        newDatabaseName = "Database"
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }
        and: 'Click delete for the required database'
        openDatabase()
        then: 'Delete the database'
        deleteNewDatabase(indexOfNewDatabase, "name_src")
        println()
    }

    def verifyCommandLog() {
        println("Test: verifyCommandLog")
        String oldVariablesCommandLog = directories.commandLogField.value()

        when: 'Verify if text is displayed'
        for(count=0; count<numberOfTrials; count++) {
            try {
                if(directories.commandLogText.text() == commandLog) {
                    println("Test Pass: The text is displayed")
                    break
                }
                else if(directories.commandLogText.text() != commandLog) {
                    println("Test Fail: The text is not displayed")
                    assert false
                }
                else {
                    println("Test Fail: Unknown error")
                    assert false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        then: 'Verify if field is displayed'
        for(count=0; count<numberOfTrials; count++) {
            try {
                if(directories.commandLogField.isDisplayed()) {
                    println("Test Pass: The field is displayed")
                    break
                }
                else if(!directories.commandLogField.isDisplayed()) {
                    println("Test Fail: The field is not displayed")
                    assert false
                }
                else {
                    println("Test Fail: Unknown error")
                    assert false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }

        when: 'Create new database'
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
        newDatabaseName = "name_src"
        then: 'Choose the new database'
        //chooseDatabase(indexOfNewDatabase, "name_src")
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }

        when: 'Provide Value for Command Log Field in Directories'
        directories.commandLogField.value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        directories.commandLogField.value("new_value")
        directories.commandLogText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Command Log Field has changed'
        if(directories.commandLogField.value() == "new_value") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Choose the database with index 1'
        openDatabase()
        //chooseDatabase(indexOfLocal, "Database")
        newDatabaseName = "Database"
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }
        and: 'Click delete for the required database'
        openDatabase()
        then: 'Delete the database'
        deleteNewDatabase(indexOfNewDatabase, "name_src")
        println()
    }

    def verifyCommandLogSnapshots() {
        println("Test: verifyCommandLogSnapshots")
        String oldVariablesCommandLogSnapshots = directories.commandLogSnapshotsField.value()

        when: 'Verify if text is displayed'
        for(count=0; count<numberOfTrials; count++) {
            try {
                if(directories.commandLogSnapshotsText.text() == commandLogSnapshots) {
                    println("Test Pass: The text is displayed")
                    break
                }
                else if(directories.commandLogSnapshotsText.text() != commandLogSnapshots) {
                    println("Test Fail: The text is not displayed")
                    assert false
                }
                else {
                    println("Test Fail: Unknown error")
                    assert false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        then: 'Verify if field is displayed'
        for(count=0; count<numberOfTrials; count++) {
            try {
                if(directories.commandLogSnapshotsField.isDisplayed()) {
                    println("Test Pass: The field is displayed")
                    break
                }
                else if(!directories.commandLogSnapshotsField.isDisplayed()) {
                    println("Test Fail: The field is not displayed")
                    assert false
                }
                else {
                    println("Test Fail: Unknown error")
                    assert false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }

        when: 'Create new database'
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
        newDatabaseName = "name_src"
        then: 'Choose the new database'
        //chooseDatabase(indexOfNewDatabase, "name_src")
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }

        when: 'Provide Value for Command Log Snapshots in Directories'
        directories.commandLogSnapshotsField.value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        directories.commandLogSnapshotsField.value("new_value")
        directories.commandLogSnapshotsText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Command Log Snapshots has changed'
        if(directories.commandLogSnapshotsField.value() == "new_value") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Choose the database with index 1'
        openDatabase()
        //chooseDatabase(indexOfLocal, "Database")
        newDatabaseName = "Database"
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
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }
        and: 'Click delete for the required database'
        openDatabase()
        then: 'Delete the database'
        deleteNewDatabase(indexOfNewDatabase, "name_src")
        println()
    }

    def verifyCreateEditAndDeleteSecurityUsers() {
        String username     = "usename"
        String password     = "password"
        String role         = "role"

        when: 'Expand Security if not already expanded'
        overview.expandSecurity()
        then: 'Open the Add User Popup'
        for(count=0; count<numberOfTrials; count++) {
            try {
                overview.addSecurityButton.click()
                waitFor { overview.saveUserOkButton.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException exception) {
                println("Unable to find the add security button - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { overview.saveUserOkButton.isDisplayed() }
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("Unable to find the add security button - Retrying")
                }
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }

        when: 'Provide Value For User'
        overview.provideValueForUser(username, password, role)
        and: 'Save the User'
        for(count=0; count<numberOfTrials; count++) {
            try {
                overview.saveUserOkButton.click()
                waitFor { !overview.saveUserOkButton.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException exception) {
                println("Unable to find the add security button - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { !overview.saveUserOkButton.isDisplayed() }
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("Unable to find the add security button - Retrying")
                }
            }
        }
        then: 'Check Username text'
        waitFor { overview.usernameOne.text().equals(username) }
        waitFor { overview.roleOne.text().equals(role) }

        when: 'Click Edit to open Edit Popup'
        username    = "username_edited"
        password    = "password_edited"
        role        = "user_edited"

        for(count=0; count<numberOfTrials; count++) {
            try {
                overview.editSecurityOne.click()
                waitFor { overview.editSecurityOne.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException exception) {
                println("Unable to find the add security button - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { overview.saveUserOkButton.isDisplayed() }
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("Unable to find the add security button - Retrying")
                }
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        and: 'Provide value for Edit Popup'
        overview.provideValueForUser(username, password, role)
        then:
        for(count=0; count<numberOfTrials; count++) {
            try {
                overview.saveUserOkButton.click()
                waitFor { !overview.saveUserOkButton.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException exception) {
                println("Unable to find the add security button - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { !overview.saveUserOkButton.isDisplayed() }
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("Unable to find the add security button - Retrying")
                }
            }
        }
        then: 'Check Username and Role text'
        waitFor { overview.usernameOne.text().equals(username) }
        waitFor { overview.roleOne.text().equals(role) }

        when: 'Click Edit to open Edit Popup'
        for(count=0; count<numberOfTrials; count++) {
            try {
                overview.editSecurityOne.click()
                waitFor { overview.editSecurityOne.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException exception) {
                println("Unable to find the add security button - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { overview.saveUserOkButton.isDisplayed() }
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("Unable to find the add security button - Retrying")
                }
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        then: "Delete the User"
        for(count=0; count<numberOfTrials; count++) {
            try {
                overview.deleteUserButton.click()
                waitFor { !overview.saveUserOkButton.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException exception) {
                println("Unable to find the add security button - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { !overview.saveUserOkButton.isDisplayed() }
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("Unable to find the add security button - Retrying")
                }
            }
        }

        when: "Check Save Message"
        checkSaveMessage()
        try {
            waitFor { 1==0 }
        } catch(geb.waiting.WaitTimeoutException exception) {

        }
        then: "Check No Security Available Text"
        waitFor { overview.noSecurityAvailable.isDisplayed() }
        waitFor { overview.noSecurityAvailable.text().equals("No security available.") }
    }

    def verifyServerDropDown(){
        when: 'Create database'
        for(count=0; count<numberOfTrials; count++) {
            try {
                indexOfNewDatabase = createNewDatabase("name_src")
                println("The index of database " + indexOfNewDatabase)
                break
            } catch(Exception e) {
                //deleteDatabase(create_DatabaseTest_File)
            } catch(org.codehaus.groovy.runtime.powerassert.PowerAssertionError e) {
                //deleteDatabase(create_DatabaseTest_File)
            }
        }
        then: 'Choose new database'
        int countNext = 0
        try {
            for(countNext=0; countNext<numberOfTrials; countNext++) {
                try {
                    waitFor { buttonAddDatabase.isDisplayed() }
                } catch(geb.waiting.WaitTimeoutException exception) {
                    currentDatabase.click()
                }
            }
            $(id:getIdOfDatabase(String.valueOf(indexOfNewDatabase))).click()
            waitFor { currentDatabase.text().equals("name_src1") }

        } catch (geb.waiting.WaitTimeoutException exception) {
            println("Waiting - Retrying")
        } catch (org.openqa.selenium.StaleElementReferenceException e) {
            println("Stale Element exception - Retrying")
        } catch(org.openqa.selenium.ElementNotVisibleException exception) {
            try {
                waitFor { currentDatabase.text().equals("name_src1") }
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
            report "hello1"
            waitFor { $(serverId).text() == "new_server" }
            println($(serverId).text() + " get Id of server")
            println("The new server was created")
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Test Fail: The new server was not created")
            assert false
        }
        when:"select server from dropdown"

        report "hello2"
//        page.chooseSelectOption('Default Setting')
//          page.selectserver.click()
//            selectOption.click()

        then:"check directory values"
        directories.rootDestinationField.isDisplayed()
        if(directories.rootDestinationField.value()=="voltdbroot"){
            println("Default value is set")
        }
        else{
            println("Value has not changed")
            assert false
        }
    }

    def cleanup() { // called after each test
        /*count = 0

        while (count < numberOfTrials) {
            count++
            try {
                setup: 'Open Cluster Settings page'
                to ClusterSettingsPage
                expect: 'to be on Cluster Settings page'
                at ClusterSettingsPage

                break
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                println("ElementNotVisibleException: Unable to Start the test")
                println("Retrying")
            }
        }

        String databaseName = nameOfDatabaseInCSV(create_DatabaseTest_File)
        int numberOfDatabases =  $('.btnDbList').size()
        buttonDatabase.click()
        int indexOfDatabaseToDelete = returnTheDatabaseIndexToDelete(numberOfDatabases, databaseName)
        if(indexOfDatabaseToDelete==0) {
            println("Cleanup: Database wasn't found")
        }
        else {
            try {
                waitFor { buttonDatabase.isDisplayed() }
            }
            catch(geb.waiting.WaitTimeoutException exception) {
                openDatabase()
            }

            //chooseDatabase(indexOfLocal, "Database")
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
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exception) {
                    println("Waiting - Retrying")
                } catch (org.openqa.selenium.StaleElementReferenceException e) {
                    println("Stale Element exception - Retrying")
                } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                    try {
                        waitFor { currentDatabase.text().equals(newDatabaseName) }
                        break
                    } catch (geb.waiting.WaitTimeoutException exc) {
                        println("Waiting - Retrying")
                    }
                }
            }
            openDatabase()

            for(count=0; count<numberOfTrials; count++) {
                try {
                    $(returnCssPathOfDatabaseDelete(indexOfDatabaseToDelete)).click()
                    waitFor { popupDeleteDatabaseButtonOk.isDisplayed() }
                    break
                } catch(geb.waiting.WaitTimeoutException exception) {

                }
            }
            for(count=0; count<numberOfTrials; count++) {
                try {
                    popupDeleteDatabaseButtonOk.click()
                    if(checkIfDatabaseExists(numberOfDatabases, databaseName, false)==false) {
                        println("Cleanup: Database was deleted")
                    }
                } catch(Exception e) {

                }
            }
            println()
        }*/
        to ClusterSettingsPage
        int indexToDelete = 2
        indexOfNewDatabase = 1
        chooseDatabase(indexOfNewDatabase, "Database")
        deleteNewDatabase(indexToDelete, "name_src")
    }
}