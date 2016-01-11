/*
This file is part of VoltDB.

Copyright (C) 2008-2015 VoltDB Inc.

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

import java.io.BufferedReader
import java.io.FileNotFoundException
import java.io.FileReader
import java.io.IOException

class ClusterConfigurationTest extends TestBase {
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

    BufferedReader br = null
    String line = ""
    String[] extractedValue = ["random_input", "random_input"]
    String newValueDatabase = 0
    boolean foundStatus = false

    int indexOfNewDatabase = 0
    int indexOfLocal = 1
    int nextCount =0
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
        String oldVariable = overview.sitePerHostField.value()

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
        and: 'Create new database'
        indexOfNewDatabase = createNewDatabase(create_DatabaseTest_File)
        then: 'Choose the new database'
        chooseDatabase(indexOfNewDatabase, "name_src")

        when: 'Provide Value for Site Per Host'
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
        chooseDatabase(indexOfLocal, "local")
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
        and: 'Create new database'
        indexOfNewDatabase = createNewDatabase(create_DatabaseTest_File)
        then: 'Choose the new database'
        chooseDatabase(indexOfNewDatabase, "name_src")

        when: 'Provide Value for K-Safety'
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
        chooseDatabase(indexOfLocal, "local")
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

        // Change the checkbox status
        when: 'Click the Checkbox'
        for(count=0; count<numberOfTrials; count++) {
            try {
                overview.partitionDetectionCheckbox.click()
                overview.partitionDetectionText.click()

                if(waitFor(60) { saveStatus.text().equals(saveMessage) }) {

                }
                else {
                    println("Test Fail: The required text is not displayed")
                    assert false
                }

                if(overview.partitionDetectionStatus.text().equals(initialStatus)) {
                    println("The status hasn't changed")
                }
                else if(!overview.partitionDetectionStatus.text().equals(initialStatus)) {
                    statusOfTest = true
                    break
                }
                else {
                    println("Unknown error")
                }
            } catch(geb.waiting.WaitTimeoutException exception) {
                println("Waiting error - Retrying")
            }
        }
        then: 'Check the status of test'
        if(statusOfTest == true) {
            println("Test Pass: The change was displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        // Restore the checkbox status
        when: 'Click the Checkbox'
        for(count=0; count<numberOfTrials; count++) {
            try {
                overview.partitionDetectionCheckbox.click()
                overview.partitionDetectionText.click()

                if(waitFor(60) { saveStatus.text().equals(saveMessage) }) {

                }
                else {
                    println("Test Fail: The required text is not displayed")
                    assert false
                }

                if(!overview.partitionDetectionStatus.text().equals(initialStatus)) {
                    println("The status hasn't changed")
                }
                else if(overview.partitionDetectionStatus.text().equals(initialStatus)) {
                    statusOfTest = true
                    break
                }
                else {
                    println("Unknown error")
                }
            } catch(geb.waiting.WaitTimeoutException exception) {
                println("Waiting error - Retrying")
            }
        }
        then: 'Check the status of test'
        if(statusOfTest == true) {
            println("Test Pass: The restore was displayed")
        }
        else {
            println("Test Fail: The restore wasn't displayed")
            assert false
        }
        println()
    }
    //  EDIT
    def verifySecurity() {
        boolean result = overview.CheckIfSecurityChkExist()
        println("Test: verifySecurity")
        when:
        if(result) {
            String initialStatus = overview.securityStatus.text()
            println("Initially " + initialStatus)

            //when: 'Verify if text and field are displayed'
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
            //then: 'Verify Elements of Security are displayed'
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

            // Change the checkbox status
            //when: 'Click the Checkbox'
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
            //then: 'Check the status of test'
            if (statusOfTest == true) {
                println("Test Pass: The change is displayed")
            } else {
                println("Test Fail: The change isn't displayed")
                assert false
            }

            // Restore the checkbox status
            //when: 'Click the Checkbox'
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
           // then: 'Check the status of test'
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

        // Change the checkbox status
        when: 'Click the Checkbox'
        statusOfTest = false
        for(count=0; count<numberOfTrials; count++) {
            try {
                overview.httpAccessCheckbox.click()
                overview.httpAccessText.click()

                if(waitFor(60) { saveStatus.text().equals(saveMessage) }) {

                }
                else {
                    println("Test Fail: The required text is not displayed")
                    assert false
                }

                if(overview.httpAccessStatus.text().equals(initialStatus)) {
                    println("The status hasn't changed")
                }
                else if(!overview.httpAccessStatus.text().equals(initialStatus)) {
                    statusOfTest = true
                    break
                }
                else {
                    println("Unknown error")
                }
            } catch(geb.waiting.WaitTimeoutException exception) {
                println("Waiting error - Retrying")
            }
        }
        then: 'Check the status of test'
        if(statusOfTest == true) {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        // Restore the checkbox status
        when: 'Click the Checkbox'
        statusOfTest = false
        for(count=0; count<numberOfTrials; count++) {
            try {
                overview.httpAccessCheckbox.click()
                overview.httpAccessText.click()
                if(waitFor(60) { saveStatus.text().equals(saveMessage) }) {

                }
                else {
                    println("Test Fail: The required text is not displayed")
                    assert false
                }

                if(overview.httpAccessStatus.text().equals(initialStatus)) {
                    statusOfTest = true
                    break
                }
                else if(!overview.httpAccessStatus.text().equals(initialStatus)) {
                    println("The status hasn't restored")
                }
                else {
                    println("Unknown error")
                }
            } catch(geb.waiting.WaitTimeoutException exception) {
                println("Waiting error - Retrying")
            }
        }
        then: 'Check the status of test'
        if(statusOfTest == true) {
            println("Test Pass: The restore is displayed")
        }
        else {
            println("Test Fail: The restore isn't displayed")
            assert false
        }
        println()
    }

    def verifyAutoSnapshots() {
        println("Test: verifyAutoSnapshots")
        String oldVariableFilePrefix    = overview.filePrefixField.value()
        String oldVariableFrequency     = overview.frequencyField.value()
        String oldVariableRetained      = overview.retainedField.value()
        String initialStatus            = overview.autoSnapshotsStatus.text()
        println("Initially " + initialStatus)

        when: 'Verify if text and field are displayed'
        for(count=0; count<numberOfTrials; count++) {
            try {
                if(overview.autoSnapshotsText.text() == autoSnapshots ) {
                    println("Test Pass: The text and field are displayed")
                    break
                }
                else if(overview.autoSnapshotsText.text() != autoSnapshots) {
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
        overview.autoSnapshotsText.click()
        for(count=0; count<numberOfTrials; count++) {
            try {
                if(overview.filePrefixText.text() == filePrefix &&
                        overview.frequencyText.text() == frequency &&
                        overview.retainedText.text() == retained &&
                        overview.filePrefixField.isDisplayed() &&
                        overview.frequencyField.isDisplayed() &&
                        overview.retainedField.isDisplayed()
                ) {
                    println("Test Pass: The contents are present")
                    break
                }
                else if(overview.filePrefixText.text() != filePrefix) {
                    println("Test Fail: The file prefix text has error")
                    assert false
                }
                else if(overview.frequencyText.text() != frequency) {
                    println("Test Fail: The frequency text has error")
                    assert false
                }
                else if(overview.retainedText.text() != retained) {
                    println("Test Fail: The retained text has error")
                    assert false
                }
                else if(!overview.filePrefixField.isDisplayed()) {
                    println("Test Fail: The file prefix field is not displayed")
                    assert false
                }
                else if(!overview.frequencyField.isDisplayed()) {
                    println("Test Fail: The frequency field is not displayed")
                    assert false
                }
                else if(!overview.retainedField.isDisplayed()) {
                    println("Test Fail: The retained field is not displayed")
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

        when: 'Create new database'
        indexOfNewDatabase = createNewDatabase(create_DatabaseTest_File)
        then: 'Choose the new database'
        chooseDatabase(indexOfNewDatabase, "name_src")

        when: 'Provide Value for File Prefix Field in Auto Snapshot'
        overview.filePrefixField.value("At_Least")
        overview.filePrefixText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in File Prefix Field in Auto Snapshot has changed'
        if(overview.filePrefixField.value() == "At_Least") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Provide Value for Frequency in Auto Snapshot'
        overview.frequencyField.value("10")
        overview.frequencyText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Site Per Host has changed'
        if(overview.frequencyField.value() == "10") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Provide Value for Retained in Auto Snapshot'
        overview.retainedField.value("10")
        overview.retainedText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Site Per Host has changed'
        if(overview.retainedField.value() == "10") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Choose the database with index 1'
        openDatabase()
        chooseDatabase(indexOfLocal, "local")
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
        indexOfNewDatabase = createNewDatabase(create_DatabaseTest_File)
        then: 'Choose the new database'
        chooseDatabase(indexOfNewDatabase, "name_src")

        when: 'Provide Value for Log Frequency Time in Command Logging'
        overview.logFrequencyTimeField.value("10")
        overview.logFrequencyTimeText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Log Frequency Time in Command Logging has changed'
        if(overview.logFrequencyTimeField.value() == "10") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Provide Value for Frequency in Auto Snapshot'
        overview.logFrequencyTransactionsField.value("10")
        overview.logFrequencyTransactionsText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Site Per Host has changed'
        if(overview.logFrequencyTransactionsField.value() == "10") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Provide Value for Log Segment Size in Auto Snapshot'
        overview.logSegmentSizeField.value("10")
        overview.logSegmentSizeText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Log Segment Size has changed'
        if(overview.logSegmentSizeField.value() == "10") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Choose the database with index 1'
        openDatabase()
        chooseDatabase(indexOfLocal, "local")
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
        String oldVariableMaxJavaHeap           = overview.maxJavaHeapField.value()
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
                if(overview.maxJavaHeapText.text() == maxJavaHeap &&
                        overview.heartbeatTimeoutText.text() == heartbeatTimeout &&
                        overview.queryTimeoutText.text() == queryTimeout &&
                        overview.maxTempTableMemoryText.text() == maxTempTableMemory &&
                        overview.snapshotPriorityText.text() == snapshotPriority &&
                        overview.memoryLimitText.text() == memoryLimit) {
                    println("Test Pass: The text contents are present")
                    break
                }
                else if(overview.maxJavaHeapText.text() != maxJavaHeap) {
                    println("Test Fail: The text of Max Java Heap has error")
                    assert false
                }
                else if(overview.maxJavaHeapText.text() != heartbeatTimeout) {
                    println("Test Fail: The text of Heartbeat Timeout has error")
                    assert false
                }
                else if(overview.maxJavaHeapText.text() != queryTimeout) {
                    println("Test Fail: The text of Max Query Timeout has error")
                    assert false
                }
                else if(overview.maxJavaHeapText.text() != maxTempTableMemory) {
                    println("Test Fail: The text of Max Temp Table has error")
                    assert false
                }
                else if(overview.maxJavaHeapText.text() != snapshotPriority) {
                    println("Test Fail: The text of Snapshot Priority has error")
                    assert false
                }
                else if(overview.maxJavaHeapText.text() != memoryLimit) {
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
                if( overview.maxJavaHeapField.isDisplayed() &&
                        overview.heartbeatTimeoutField.isDisplayed() &&
                        overview.queryTimeoutField.isDisplayed() &&
                        overview.maxTempTableMemoryField.isDisplayed() &&
                        overview.snapshotPriorityField.isDisplayed() &&
                        overview.memoryLimitField.isDisplayed()
                ) {
                    println("Test Pass: The text contents are present")
                    break
                }
                else if(overview.maxJavaHeapField.isDisplayed()) {
                    println("Test Fail: The box of Max Java Heap has error")
                    assert false
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
        indexOfNewDatabase = createNewDatabase(create_DatabaseTest_File)
        then: 'Choose the new database'
        chooseDatabase(indexOfNewDatabase, "name_src")

        when: 'Provide Value for Max Java Heap in Advanced'
        overview.maxJavaHeapField.value("10")
        overview.maxJavaHeapText.click()
        and: 'Check Save Message'
        checkSaveMessage()
        then: 'Check if Value in Max Java Heap in Advanced has changed'
        if(overview.maxJavaHeapField.value() == "10") {
            println("Test Pass: The change is displayed")
        }
        else {
            println("Test Fail: The change isn't displayed")
            assert false
        }

        when: 'Provide Value for Heartbeat Timeout in Advanced'
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
        overview.snapshotPriorityField.value("10")
        overview.snapshotPriorityText.click()
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
        overview.memoryLimitField.value("10")
        overview.memoryLimitText.click()
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
        chooseDatabase(indexOfLocal, "local")
        and: 'Click delete for the required database'
        openDatabase()
        then: 'Delete the database'
        deleteNewDatabase(indexOfNewDatabase, "10")
        println()
    }

    def verifyRootDestination() {
        println("Test: verifyRootDestination")
        String oldVariablesRootDestination = directories.rootDestinationField.value()

        when: 'Verify if text is displayed'
        for(count=0; count<numberOfTrials; count++) {
            try {
                if(directories.rootDestinationText.text() == rootDestination) {
                    println("Test Pass: The text is displayed")
                    break
                }
                else if(directories.rootDestinationText.text() != rootDestination) {
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
        indexOfNewDatabase = createNewDatabase(create_DatabaseTest_File)
        then: 'Choose the new database'
        chooseDatabase(indexOfNewDatabase, "name_src")

        when: 'Provide Value for Root in Directories'
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
        chooseDatabase(indexOfLocal, "local")
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
        for(count=0; count<numberOfTrials; count++) {
            try {
                if(directories.snapshotText.text() == snapshot) {
                    println("Test Pass: The text is displayed")
                    break
                }
                else if(directories.snapshotText.text() != snapshot) {
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
                if(directories.snapshotField.isDisplayed()) {
                    println("Test Pass: The field is displayed")
                    break
                }
                else if(!directories.snapshotField.isDisplayed()) {
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
        indexOfNewDatabase = createNewDatabase(create_DatabaseTest_File)
        then: 'Choose the new database'
        chooseDatabase(indexOfNewDatabase, "name_src")

        when: 'Provide Value for Snapshots in Directories'
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
        chooseDatabase(indexOfLocal, "local")
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
        for(count=0; count<numberOfTrials; count++) {
            try {
                if(directories.exportOverflowText.text() == exportOverflow) {
                    println("Test Pass: The text is displayed")
                    break
                }
                else if(directories.exportOverflowText.text() != exportOverflow) {
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
                if(directories.exportOverflowField.isDisplayed()) {
                    println("Test Pass: The field is displayed")
                    break
                }
                else if(!directories.exportOverflowField.isDisplayed()) {
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
        indexOfNewDatabase = createNewDatabase(create_DatabaseTest_File)
        then: 'Choose the new database'
        chooseDatabase(indexOfNewDatabase, "name_src")

        when: 'Provide Value for Export Overflow in Directories'
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
        chooseDatabase(indexOfLocal, "local")
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
        indexOfNewDatabase = createNewDatabase(create_DatabaseTest_File)
        then: 'Choose the new database'
        chooseDatabase(indexOfNewDatabase, "name_src")

        when: 'Provide Value for Command Log Field in Directories'
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
        chooseDatabase(indexOfLocal, "local")
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
        indexOfNewDatabase = createNewDatabase(create_DatabaseTest_File)
        then: 'Choose the new database'
        chooseDatabase(indexOfNewDatabase, "name_src")

        when: 'Provide Value for Command Log Snapshots in Directories'
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
        chooseDatabase(indexOfLocal, "local")
        and: 'Click delete for the required database'
        openDatabase()
        then: 'Delete the database'
        deleteNewDatabase(indexOfNewDatabase, "name_src")
        println()
    }

    def cleanup() { // called after each test
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

        String databaseName = nameOfDatabaseInCSV(create_DatabaseTest_File)
        int numberOfDatabases =  $('.btnDbList').size()
        buttonDatabase.click()
        int indexOfDatabaseToDelete = returnTheDatabaseIndexToDelete(numberOfDatabases, databaseName)

        if(indexOfDatabaseToDelete==0) {
            println("Cleanup: Database wasn't found")
        }
        else {
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
        }
    }
}
