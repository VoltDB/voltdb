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

/**
 * Created by anrai on 2/12/15.
 */


package vmcTest.tests

import org.junit.Test
import vmcTest.pages.*
import geb.Page.*

/**
 * This class contains tests of the 'Admin' tab of the VoltDB Management
 * Center (VMC) page, which is the VoltDB (new) web UI.
 */

class AdminTest extends TestBase {
    static String initialPrefix
    static String initialFreq
    static String initialFreqUnit
    static String initialRetained

    static String initialHeartTimeout
    static String initialQueryTimeout
    static String initialMemoryLimit = "-1"

    static boolean revertAutosnapshots = false
    static boolean revertHeartTimeout = false
    static boolean revertQueryTimeout = false
    static boolean revertMemorySize =false

    int count = 0
    def setup() { // called before each test
        count = 0

        while(count<numberOfTrials) {
            count ++
            try {
                setup: 'Open VMC page'
                to VoltDBManagementCenterPage
                page.loginIfNeeded()
                expect: 'to be on VMC page'
                at VoltDBManagementCenterPage

                when: 'click the Admin link (if needed)'
                page.openAdminPage()
                then: 'should be on Admin page'
                at AdminPage

                break
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                println("ElementNotVisibleException: Unable to Start the test")
                println("Retrying")
            }
        }
    }

    // DIRECTORIES

    def directoriesTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    directories.title.isDisplayed()
                    directories.title.text().toLowerCase().equals("Directories".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def rootTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    directories.rootTitle.isDisplayed()
                    directories.rootTitle.text().toLowerCase().equals("Root (Destination)".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def snapshotTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    directories.snapshotTitle.isDisplayed()
                    directories.snapshotTitle.text().toLowerCase().equals("Snapshot".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def exportOverflowTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    directories.exportOverflowTitle.isDisplayed()
                    directories.exportOverflowTitle.text().toLowerCase().equals("Export Overflow".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def commandLogsTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    directories.commandLogsTitle.isDisplayed()
                    directories.commandLogsTitle.text().toLowerCase().equals("Command Log".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def commandLogSnapshotsTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    directories.commandLogSnapshotTitle.isDisplayed()
                    directories.commandLogSnapshotTitle.text().toLowerCase().equals("Command Log Snapshots".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def drOverflowTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    directories.drOverflowTitle.isDisplayed()
                    directories.drOverflowTitle.text().toLowerCase().equals("DR Overflow".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def rootValueNotEmpty() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    directories.rootValue.isDisplayed()
                    !directories.rootValue.text().equals("")
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def snapshotValueNotEmpty() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    directories.snapshotValue.isDisplayed()
                    !directories.snapshotValue.text().equals("")
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def exportOverflowValueNotEmpty() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    directories.exportOverflowValue.isDisplayed()
                    !directories.exportOverflowValue.text().equals("")
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def commandLogsValueNotEmpty() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    directories.commandLogsValue.isDisplayed()
                    !directories.commandLogsValue.text().equals("")
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def logSnapshotsValueNotEmpty() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    directories.commandLogSnapshotValue.isDisplayed()
                    !directories.commandLogSnapshotValue.text().equals("")
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def drOverflowValueNotEmpty() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    directories.drOverflowValue.isDisplayed()
                    !directories.drOverflowValue.text().equals("")
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    // OVERVIEW

    def overviewTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    overview.title.isDisplayed()
                    overview.title.text().toLowerCase().equals("Overview".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def sitePerHostTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(35) {
                    overview.sitePerHost.isDisplayed()
                    overview.sitePerHost.text().toLowerCase().equals("Sites Per Host".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def kSafetyTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    overview.ksafety.isDisplayed()
                    overview.ksafety.text().toLowerCase().equals("K-safety".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def partitionDetectionTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    overview.partitionDetection.isDisplayed()
                    overview.partitionDetection.text().toLowerCase().equals("Partition detection".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def securityTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    overview.security.isDisplayed()
                    overview.security.text().toLowerCase().equals("Security".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def httpAccessTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    overview.httpAccess.isDisplayed()
                    overview.httpAccess.text().toLowerCase().equals("HTTP Access".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def autoSnapshotsTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    overview.autoSnapshots.isDisplayed()
                    overview.autoSnapshots.text().toLowerCase().equals("Auto Snapshots".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def commandLoggingTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    overview.commandLogging.isDisplayed()
                    overview.commandLogging.text().toLowerCase().equals("Command Logging".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def exportTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    overview.export.isDisplayed()
                    overview.export.text().toLowerCase().equals("Export".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    //values

    def sitePerHostValue() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            overview.sitePerHostValue.isDisplayed()
            !overview.sitePerHostValue.text().equals("")
        }
    }

    def kSafetyValue() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            overview.ksafetyValue.isDisplayed()
            !overview.ksafetyValue.text().equals("")
        }
    }

    def partitionDetectionValue() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            overview.partitionDetectionValue.isDisplayed()
            !overview.partitionDetectionValue.text().equals("")
        }
    }

    def securityValue() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            overview.securityValue.isDisplayed()
            !overview.securityValue.text().equals("")
        }
    }

    def httpAccessValue() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            overview.httpAccessValue.isDisplayed()
            !overview.httpAccessValue.text().equals("")
        }
    }

    def autoSnapshotsValue() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            overview.autoSnapshotsValue.isDisplayed()
            !overview.autoSnapshotsValue.text().equals("")
        }
    }

    def commandLoggingValue() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            overview.commandLoggingValue.isDisplayed()
            !overview.commandLoggingValue.text().equals("")
        }
    }

    // SECURITY

    def securityButton(){
        when:
        at AdminPage
        then:
        if(page.overview.getListOfUsers()!="") {
            try {
                page.securityEdit.click()
                page.securityEditOk.isDisplayed()
                page.securityEditCancel.isDisplayed()
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Security Edit cannot be displayed")
            } catch(org.openqa.selenium.ElementNotVisibleException e) {
                println("Security Edit cannot be displayed")
            }
        }
        else {
            println("Atleast one security credential should be added first")
        }
        println("Security Edit is Disabled")
    }

    def securityEditButtonAndCancel(){
        when:
        at AdminPage
        then:
        try {
            page.securityEdit.click()
            page.securityEditOk.isDisplayed()
            page.securityEditCancel.isDisplayed()
            page.securityEditCancel.click()
            println("security edit canceled!")
            page.securityEdit.isDisplayed()
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Security Edit cannot be displayed")
        } catch(org.openqa.selenium.ElementNotVisibleException e) {
            println("Security Edit cannot be displayed")
        }
    }

    def securityEditButtonAndCancelPopup(){
        when:
        at AdminPage
        then:
        try {
            page.securityEdit.click()
            page.securityEditOk.isDisplayed()
            page.securityEditCancel.isDisplayed()
            page.securityEditOk.click()
            println("security edit ok clicked!")
            waitFor(waitTime) {
                page.securityPopupOk.isDisplayed()
                page.securityPopupCancel.isDisplayed()
                page.securityPopupCancel.click()
                println("cancel clicked")
                page.securityEdit.isDisplayed()
            }
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Security Edit cannot be displayed")
        } catch(org.openqa.selenium.ElementNotVisibleException e) {
            println("Security Edit cannot be displayed")
        }
    }

    def securityEditButtonAndOkAndOk(){
        when:
        at AdminPage
        then:
        try {
            page.securityEdit.click()
            page.securityEditOk.isDisplayed()
            page.securityEditCancel.isDisplayed()
            page.securityEditOk.click()
            println("security edit ok clicked!")

            waitFor(waitTime) {
                page.securityPopupOk.isDisplayed()
                page.securityPopupCancel.isDisplayed()
                page.securityPopupOk.click()
            }
        } catch(geb.waiting.WaitTimeoutException e){
            println("Security Edit cannot be displayed")
        } catch(org.openqa.selenium.ElementNotVisibleException e) {
            println("Security Edit cannot be displayed")
        }
    }

    // autosnapshot
    def autoSnapshotsEdit() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){ page.autoSnapshotsEdit.isDisplayed() }
        String string = page.autoSnapshotsEdit.text()
        !(string.equals(""))
    }

    def verifyAutoSnapshotsEdit() {
        when:
        at AdminPage
        then:
        waitFor(waitTime) {
            page.autoSnapshotsEdit.isDisplayed()
        }
        page.autoSnapshotsEdit.click()

        waitFor(waitTime) {
            page.autoSnapshotsEditCheckbox.isDisplayed()
            page.autoSnapshotsEditOk.isDisplayed()
            page.autoSnapshotsEditCancel.isDisplayed()
        }
        waitFor(waitTime){
            page.frequencyEdit.isDisplayed()
            //println("first wait")
            page.retainedEdit.isDisplayed()
            page.fileprefixEdit.isDisplayed()
        }
    }

    def autoSnapshotAndClickCancel() {
        when:
        at AdminPage
        then:
        waitFor(waitTime) {
            page.autoSnapshotsEdit.isDisplayed()
        }

        when:
        page.autoSnapshotsEdit.click()
        then:
        waitFor(waitTime) {
            page.autoSnapshotsEditOk.isDisplayed()
            page.autoSnapshotsEditCancel.isDisplayed()
        }

        when:
        page.autoSnapshotsEditCancel.click()
        then:
        waitFor(waitTime) {
            !(page.autoSnapshotsEditCancel.isDisplayed())
            !(page.autoSnapshotsEditOk.isDisplayed())
        }
    }

    def verifyAutoSnapshotsAndCheckbox() {
        when:
        at AdminPage
        then:
        waitFor(waitTime) {
            page.autoSnapshotsEdit.isDisplayed()
        }

        when:
        page.autoSnapshotsEdit.click()
        String enabledDisabled = page.autoSnapshotsValue.text()
        println(enabledDisabled)
        then:
        waitFor(waitTime){
            page.autoSnapshotsEditCheckbox.isDisplayed()
            page.autoSnapshotsEditOk.isDisplayed()
            page.autoSnapshotsEditCancel.isDisplayed()
        }

        when:
        page.autoSnapshotsEditCheckbox1.click()
        then:
        String enabledDisabledEdited = page.autoSnapshotsValue.text()
        println(enabledDisabledEdited)

        if ( enabledDisabled.toLowerCase() == "on" ) {
            assert enabledDisabledEdited.toLowerCase().equals("off")
        }
        else if ( enabledDisabled.toLowerCase() == "off" ) {
            assert enabledDisabledEdited.toLowerCase().equals("on")
        }
    }

    def verifyClickEditAndCancel() {
        String title            = "Auto Snapshots"
        String display          = "Do you want to save the value?"
        String ok               = "Ok"
        String cancel           = "Cancel"

        when:
        at AdminPage
        then:
        waitFor(waitTime) {
            page.autoSnapshotsEdit.isDisplayed()
            page.autoSnapshotsValue.isDisplayed()
        }

        when:
        page.autoSnapshotsEdit.click()
        String string = page.autoSnapshotsValue.text()
        then:
        waitFor(waitTime) {
            page.autoSnapshotsEditCheckbox.isDisplayed()
            page.autoSnapshotsEditOk.isDisplayed()
            page.autoSnapshotsEditCancel.isDisplayed()
        }
        page.autoSnapshotsEditOk.click()

        when:
        page.autoSnapshotsEditCancel.click()
        println("cancel clicked successfully")
        then:
        waitFor(waitTime) {
            page.autoSnapshotsEdit.isDisplayed()
        }
    }

    def verifyClickEditAndOk() {
        String prefix           = "SNAPSHOTNONCE"
        String frequency        = "10"
        String frequencyUnit        = "Hrs"
        String retained         = "1"

        String title            = "Auto Snapshots"
        String display          = "Do you want to save the value?"
        String ok               = "Ok"
        String cancel           = "Cancel"

        when:
        at AdminPage
        page.autoSnapshots.click()
        waitFor(waitTime) {
            page.filePrefix.isDisplayed()
            page.frequency.isDisplayed()
            page.frequencyUnit.isDisplayed()
            page.retained.isDisplayed()
        }
        initialPrefix   = page.filePrefix.text()
        initialFreq     = page.frequency.text()
        initialFreqUnit = page.frequencyUnit.text()
        initialRetained = page.retained.text()
        then:
        waitFor(waitTime) {
            page.autoSnapshotsEdit.isDisplayed()
            page.autoSnapshotsValue.isDisplayed()
            initialFreq
        }

        when:
        page.autoSnapshotsEdit.click()
        then:
        waitFor(waitTime) {
            page.autoSnapshotsEditCheckbox.isDisplayed()
            page.autoSnapshotsEditOk.isDisplayed()
            page.autoSnapshotsEditCancel.isDisplayed()
        }
        page.filePrefixField.value(prefix)
        page.frequencyField.value(frequency)
        page.frequencyUnitField.click()
        page.frequencyUnitField.value(frequencyUnit)
        page.retainedField.value(retained)

        if(page.fileprefixEdit.text() != " "){
            println("fileprefix passed, found non-empty")

            if(
            page.frequencyEdit.text() != " "){
                println("frequency passed, found non-empty")}
            // page.frequencyUnitField.click()
            if( page.frequencyUnitField.text() != " "){
                println("frequency unit passed, found non-empty")}
            if ( page.retainedEdit.text() != " "){
                println("retained passed, found non-empty")}
        }
        page.autoSnapshotsEditOk.click()
        println("pop up visible")

        when:
        while(true) {
            page.autosnapshotsconfirmok.click()
            println("inside ok clicked successfully")
            if(page.filePrefixField.isDisplayed()== false)
                break
        }
        then:
        int count = 0
        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.filePrefix.text().equals(prefix)
                    page.frequency.text().equals(frequency)
                    page.frequencyUnit.text().equals(frequencyUnit)
                    page.retained.text().equals(retained)
                    page.filePrefix.isDisplayed()
                }
                break
            } catch (geb.waiting.WaitTimeoutException e) {
                println("Try")
            }
        }
    }

    // NETWORK INTERFACES

    def networkInterfacesTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'
        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.networkInterfaces.title.isDisplayed()
                    page.networkInterfaces.title.text().toLowerCase().equals("Network Interfaces".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def portNameTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'
        at AdminPage
        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.networkInterfaces.portNameTitle.isDisplayed()
                    page.networkInterfaces.portNameTitle.text().equals("Port Name")
                }
                then:

                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
    }

    def clusterSettingsTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'
        at AdminPage
        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.networkInterfaces.clusterSettingTitle.isDisplayed()
                    page.networkInterfaces.clusterSettingTitle.text().toLowerCase().equals("Cluster Settings".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def serverSettingsTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'
        at AdminPage
        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.networkInterfaces.serverSettingTitle.isDisplayed()
                    page.networkInterfaces.serverSettingTitle.text().toLowerCase().equals("Server Settings".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def clientPortTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'
        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.networkInterfaces.clientPortTitle.isDisplayed()
                    page.networkInterfaces.clientPortTitle.text().toLowerCase().equals("Client Port".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def adminPortTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'
        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.networkInterfaces.adminPortTitle.isDisplayed()
                    page.networkInterfaces.adminPortTitle.text().toLowerCase().equals("Admin Port".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def httpPortTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'
        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.networkInterfaces.httpPortTitle.isDisplayed()
                    page.networkInterfaces.httpPortTitle.text().toLowerCase().equals("HTTP Port".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def internalPortTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'
        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.networkInterfaces.internalPortTitle.isDisplayed()
                    page.networkInterfaces.internalPortTitle.text().toLowerCase().equals("Internal Port".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def zookeeperPortTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'
        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.networkInterfaces.zookeeperPortTitle.isDisplayed()
                    page.networkInterfaces.zookeeperPortTitle.text().toLowerCase().equals("Zookeeper Port".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def replicationPortTitle() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'
        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.networkInterfaces.replicationPortTitle.isDisplayed()
                    page.networkInterfaces.replicationPortTitle.text().toLowerCase().equals("Replication Port".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    // value

    def clientPortValueNotEmpty() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            page.networkInterfaces.clusterClientPortValue.isDisplayed()
            !page.networkInterfaces.clusterClientPortValue.text().equals("")
        }
    }

    def adminPortValueNotEmpty() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            page.networkInterfaces.clusterAdminPortValue.isDisplayed()
            !page.networkInterfaces.clusterAdminPortValue.text().equals("")
        }
    }

    def httpPortValueNotEmpty() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            page.networkInterfaces.clusterHttpPortValue.isDisplayed()
            !page.networkInterfaces.clusterHttpPortValue.text().equals("")
        }
    }

    def internalPortValueNotEmpty() {
        when:
        at AdminPage
        then:
        waitFor(waitTime) {
            page.networkInterfaces.clusterInternalPortValue.isDisplayed()
            !page.networkInterfaces.clusterInternalPortValue.text().equals("")
        }
    }


    def zookeeperPortValueNotEmpty() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            page.networkInterfaces.clusterZookeeperPortValue.isDisplayed()
            !page.networkInterfaces.clusterZookeeperPortValue.text().equals("")
        }
    }


    def replicationPortValueNotEmpty() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            page.networkInterfaces.clusterReplicationPortValue.isDisplayed()
            !page.networkInterfaces.clusterReplicationPortValue.text().equals("")
        }
    }

    // HEADER TESTS

    def headerBanner() {
        when:
        at AdminPage
        then:
        waitFor(waitTime) { header.banner.isDisplayed() }
    }

    def headerImage() {
        when:
        at AdminPage
        then:
        waitFor(waitTime) { header.image.isDisplayed() }
    }

    def headerLogout() {
        when:'Check Security Enabled'
        at AdminPage
        waitFor(waitTime) { page.overview.securityValue.isDisplayed() }
        then:
        if(page.overview.securityValue.text().equals("Off"))
        {
            println("PASS")
        }
        else if (page.overview.securityValue.text().equals("On"))
        {
            println("fail")
            page.overview.securityValue.text().equals("On")
            println("test" + page.overview.securityValue.text())


            waitFor(waitTime) { header.logout.isDisplayed() }
        }
    }

    def headerHelp() {
        when:
        at AdminPage
        then:
        waitFor(30) { page.header.help.isDisplayed() }
        int count = 0
        while(count<5) {
            count++
            try {
                interact {
                    moveToElement(page.header.help)
                }
                waitFor(30) { page.header.showHelp.isDisplayed() }
                break
            } catch (geb.waiting.WaitTimeoutException e) {
                println("Already tried")
            }
        }
    }

    // HEADER TAB TESTS

    def tabDbmonitor() {
        when:
        at AdminPage
        then:
        waitFor(waitTime) {
            header.tabDBMonitor.isDisplayed()
            header.tabDBMonitor.text().toLowerCase().equals("DB Monitor".toLowerCase())
        }
    }

    def tabAdmin() {
        when:
        at AdminPage
        then:
        waitFor(waitTime) {
            header.tabAdmin.isDisplayed()
            header.tabAdmin.text().toLowerCase().equals("Admin".toLowerCase())
        }
    }

    def tabSchema() {
        when:
        at AdminPage
        then:
        waitFor(waitTime) {
            header.tabSchema.isDisplayed()
            header.tabSchema.text().toLowerCase().equals("Schema".toLowerCase())

        }
    }

    def tabSqlQuery() {
        when:
        at AdminPage
        then:
        waitFor(waitTime) { header.tabSQLQuery.isDisplayed()
            header.tabSQLQuery.text().toLowerCase().equals("SQL Query".toLowerCase())
        }
    }

    def usernameIfEnabled() {
        when:'Check Security Enabled'
        at AdminPage
        waitFor(waitTime) { page.overview.securityValue.isDisplayed() }
        then:
        if(page.overview.securityValue.text().equals("Off"))
        {
            println("PASS")
        }
        else if (page.overview.securityValue.text().equals("On"))
        {
            waitFor(waitTime) {
                header.usernameInHeader.isDisplayed()
                header.usernameInHeader.text().equals(username)
            }
        }
    }

    def usernameClickAndClose() {
        when:
        at AdminPage
        then:
        waitFor(waitTime) { header.usernameInHeader.isDisplayed() }
        header.usernameInHeader.click()
        waitFor(waitTime) {
            header.logoutPopupOkButton.isDisplayed()
            header.logoutPopupCancelButton.isDisplayed()
            header.popupClose.isDisplayed()
        }
        header.popupClose.click()
    }

    def headerUsernameClickAndCancel() {
        when:
        at AdminPage
        then:
        waitFor(waitTime) { header.usernameInHeader.isDisplayed() }
        header.usernameInHeader.click()
        waitFor(waitTime) {
            header.logoutPopupOkButton.isDisplayed()
            header.logoutPopupCancelButton.isDisplayed()
            header.popupClose.isDisplayed()
        }
        header.logoutPopupCancelButton.click()
    }

    // LOGOUT TEST

    def logoutButtonClose()  {
        when:'Check Security Enabled'
        at AdminPage
        waitFor(waitTime) { page.overview.securityValue.isDisplayed() }
        then:
        if(page.overview.securityValue.text().equals("Off"))
        {
            println("PASS")
        }
        else if (page.overview.securityValue.text().equals("On"))
        {
            waitFor(waitTime) { header.logout.isDisplayed() }
            header.logout.click()
            waitFor(waitTime) {
                header.logoutPopupOkButton.isDisplayed()
                header.logoutPopupCancelButton.isDisplayed()
                header.popupClose.isDisplayed()
            }
            header.popupClose.click()
        }
    }

    def logoutButtonCancel()  {
        when:'Check Security Enabled'
        at AdminPage
        waitFor(waitTime) { page.overview.securityValue.isDisplayed() }
        then:
        if(page.overview.securityValue.text().equals("Off"))
        {
            println("PASS")
        }
        else if (page.overview.securityValue.text().equals("On"))
        {
            waitFor(waitTime) { header.logout.isDisplayed() }
            header.logout.click()
            waitFor(waitTime) {
                header.logoutPopupOkButton.isDisplayed()
                header.logoutPopupCancelButton.isDisplayed()
                header.popupClose.isDisplayed()
            }
            header.logoutPopupCancelButton.click()
        }
    }

    // HELP POPUP TEST

    def helpPopup() {
        when:
        at AdminPage
        then:
        waitFor(waitTime) { page.header.help.isDisplayed() }
        int count = 0
        while(count<5) {
            count++
            try {
                interact {
                    moveToElement(page.header.help)
                }
                waitFor(30) { page.header.showHelp.isDisplayed() }
                break
            } catch (geb.waiting.WaitTimeoutException e) {
                println("Already tried")
            }
        }

        when:
        page.header.showHelp.click()
        then:
        waitFor(waitTime) { page.header.popupClose.isDisplayed() }
        waitFor(waitTime) { page.header.popupTitle.text().toLowerCase().contains("help".toLowerCase()) }
    }

    // FOOTER TESTS

    def footer() {
        when:
        at AdminPage
        then:
        waitFor(waitTime) { footer.banner.isDisplayed() }
    }

    def footerText() {
        when:
        at AdminPage
        then:
        waitFor(waitTime) {
            footer.banner.isDisplayed()
            footer.text.isDisplayed()
            footer.text.text().toLowerCase().contains("All rights reserved.".toLowerCase())
        }
    }

    //download automation test

    def downloadConfiguration() {
        when:
        at AdminPage
        waitFor(waitTime) { page.downloadconfigurationbutton.isDisplayed() }
        println("downloadbutton seen")
        then:
        page.downloadconfigurationbutton.text().toLowerCase().equals("Download Configuration".toLowerCase())
        println("download configuration button text has verified,\n click cannot be performed in firefox")
    }

    //CLUSTER

    def clusterTitle() {
        when:
        at AdminPage
        waitFor(waitTime) { cluster.clusterTitle.isDisplayed() }
        then:
        cluster.clusterTitle.text().equals("Cluster")
    }

    def checkPromoteButton() {
        when:
        at AdminPage
        then:
        waitFor(waitTime) { cluster.promotebutton.isDisplayed() }
    }

    def checkPauseCancel() {
        boolean result = false
        int count = 0
        when:
        at AdminPage
        try {
            waitFor(waitTime) { page.cluster.resumebutton.isDisplayed() }
            println("Resume button is displayed")
            result = false
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Resume button is not displayed")
            result = true
        }

        if (result == false) {
            println("Resume VMC")

            try {
                page.cluster.resumebutton.click()
                waitFor(waitTime) { page.cluster.resumeok.isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Error: Resume confirmation was not found")
                assert false
            }

            try {
                page.cluster.resumeok.click()
                waitFor(waitTime) { page.cluster.pausebutton.isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Error: Pause button was not found")
                assert false
            }
        }
        then:
        println()

        when:
        count = 0
        while(count<numberOfTrials) {
            count ++
            try {
                page.cluster.pausebutton.click()
                waitFor(waitTime) { page.cluster.pauseok.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        count = 0
        while(count<numberOfTrials) {
            count ++
            try {
                page.cluster.pausecancel.click()
                waitFor(waitTime) { page.cluster.pausebutton.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        then:
        println()

        when:
        if (result == false) {
            println("Pause VMC")
            try {
                page.cluster.pausebutton.click()
                waitFor(waitTime) { page.cluster.pauseok.isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Error: Pause confirmation was not found")
                assert false
            }

            try {
                page.cluster.pauseok.click()
                waitFor(waitTime) { page.cluster.resumebutton.isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Error: Resume button was not found")
                assert false
            }
        }
        then:
        println()
    }

    def pauseAndResume(){
        boolean result = false
        int count = 0
        when:
        at AdminPage
        try {
            waitFor(waitTime) { page.cluster.resumebutton.isDisplayed() }
            println("Resume button is displayed")
            result = false
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Resume button is not displayed")
            result = true
        }
        if (result == false) {
            println("Resume VMC")

            count = 0
            while(count<numberOfTrials) {
                try {
                    count++
                    page.cluster.resumebutton.click()
                    waitFor(waitTime) { page.cluster.resumeok.isDisplayed() }
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("Error: Resume confirmation was not found")
                    assert false
                }
            }

            count = 0
            while(count<numberOfTrials) {
                try {
                    count++
                    page.cluster.resumeok.click()
                    waitFor(waitTime) { page.cluster.pausebutton.isDisplayed() }
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("Error: Pause button was not found")
                    assert false
                }
            }
        }
        then:
        println()

        when:
        count = 0
        while(count<numberOfTrials) {
            count ++
            try {
                page.cluster.pausebutton.click()
                waitFor(waitTime) { page.cluster.pauseok.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        count = 0
        while(count<numberOfTrials) {
            count ++
            try {
                page.cluster.pauseok.click()
                waitFor(waitTime) { page.cluster.resumebutton.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        count = 0
        while(count<numberOfTrials) {
            count ++
            try {
                page.cluster.resumebutton.click()
                waitFor(waitTime) { page.cluster.resumeok.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        count = 0
        while(count<numberOfTrials) {
            count ++
            try {
                page.cluster.resumeok.click()
                waitFor(waitTime) { page.cluster.pausebutton.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        then:
        println()

        when:
        if (result == false) {
            println("Pause VMC")

            count = 0
            while(count<numberOfTrials) {
                try {
                    count++
                    page.cluster.pausebutton.click()
                    waitFor(waitTime) { page.cluster.pauseok.isDisplayed() }
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("Error: Pause confirmation was not found")
                    assert false
                }
            }

            count = 0
            while(count<numberOfTrials) {
                try {
                    count++
                    page.cluster.pauseok.click()
                    waitFor(waitTime) { page.cluster.resumebutton.isDisplayed() }
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("Error: Resume button was not found")
                    assert false
                }
            }
        }
        then:
        println()
    }

    def saveAndCancelPopup(){
        when:
        at AdminPage
        waitFor(waitTime) { cluster.savebutton.isDisplayed() }
        cluster.savebutton.click()
        then:
        waitFor(waitTime) { cluster.saveconfirmation.isDisplayed() }
        cluster.saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
        cluster.savecancel.click()
    }

    def saveInEmptyPath(){
        String emptyPath = page.getEmptyPath()
        when:

        at AdminPage
        waitFor(waitTime) { cluster.savebutton.isDisplayed() }
        cluster.savebutton.click()
        then:
        waitFor(waitTime) { cluster.saveconfirmation.isDisplayed() }
        cluster.saveconfirmation.text().toLowerCase().equals("Save".toLowerCase())
        cluster.savedirectory.value(emptyPath)
        cluster.saveok.click()
        cluster.saveerrormsg.isDisplayed()
        cluster.saveerrormsg.text().toLowerCase().equals("Please enter a valid directory path.".toLowerCase())
        println("error message verified")
    }

    def saveForInvalidPath(){
        String invalidPath = page.getInvalidPath()

        when:
        at AdminPage
        waitFor(waitTime) { cluster.savebutton.isDisplayed() }
        cluster.savebutton.click()
        then:
        waitFor(waitTime) { cluster.saveconfirmation.isDisplayed() }
        cluster.saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
        cluster.savedirectory.value(invalidPath)
        cluster.saveok.click()
        waitFor(waitTime){cluster.failedsaveok.isDisplayed()}
        cluster.failedsaveok.click()
        println("error location for saving verified")
    }

    def saveSuccess(){
        String validPath = page.getValidPath()
        when:
        at AdminPage
        waitFor(waitTime) { cluster.savebutton.isDisplayed() }
        cluster.savebutton.click()
        then:
        waitFor(waitTime) { cluster.saveconfirmation.isDisplayed() }
        cluster.saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
        cluster.savedirectory.value(validPath)
        cluster.saveok.click()
        waitFor(waitTime){cluster.savesuccessok.isDisplayed()}
        cluster.savesuccessok.click()
        println("save succeeded and clicked!!")
    }

    def restoreButtonClickAndCancelPopup(){
        String validPath = page.getValidPath()
        when:
        at AdminPage
        waitFor(waitTime) {   cluster.restorebutton.isDisplayed()
            cluster.restorestatus.isDisplayed()
            cluster.restorebutton.click()
        }
        println("restore button clicked")
        then:
        waitFor(waitTime) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase());
        cluster.restoredirectory.value(validPath)
        cluster.restoresearch.click()
        waitFor(waitTime){cluster.restorecancelbutton.isDisplayed()}
        cluster.restorecancelbutton.click()
    }

    def restoreButtonClickAndClosePopup(){
        when:
        at AdminPage
        waitFor(waitTime) { cluster.restorebutton.isDisplayed()
            cluster.restorestatus.isDisplayed()
            cluster.restorebutton.click()
        }
        println("restore clicked")
        then:
        waitFor(waitTime) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase())
        cluster.restoreclosebutton.click()
    }

    def restoreClickAndSearchFail(){
        String invalidPath = page.getInvalidPath()
        when:
        at AdminPage
        waitFor(waitTime) { cluster.restorebutton.isDisplayed()
            cluster.restorestatus.isDisplayed()
            cluster.restorebutton.click()
        }
        println("restore clicked")
        then:
        waitFor(waitTime) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase())
    }

    def searchButtonClickInEmptyPathOfRestore(){
        String emptyPath = page.getEmptyPath()
        when:
        waitFor(waitTime) { cluster.restorebutton.isDisplayed()
            cluster.restorestatus.isDisplayed()
            cluster.restorebutton.click()
        }
        then:
        waitFor(waitTime) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase())
    }

    def restoreClickAndVerifyRestorePopupForNo(){
        when:
        at AdminPage
        waitFor(waitTime) { cluster.restorebutton.isDisplayed()
            cluster.restorestatus.isDisplayed()
            cluster.restorebutton.click()
        }
        println("restore clicked")
        then:
        waitFor(waitTime) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase())

        // FOR UAT TESTING ENABLE BELOW CODE
        //   waitFor(waitTime){cluster.buttonrestore.isDisplayed()}
        //  cluster.buttonrestore.click()
        //  waitFor(waitTime){cluster.restorepopupno.isDisplayed()
        //               cluster.restorepopupyes.isDisplayed()}
        //  cluster.restorepopupno.click()
        //  println("No clicked for restore popup")
        // waitFor(waitTime){cluster.restorecancelbutton.isDisplayed()}
        // cluster.restorecancelbutton.click()
    }

    def restoreClickAndRestoreForYes(){
        when:
        at AdminPage
        waitFor(waitTime) { cluster.restorebutton.isDisplayed()
            cluster.restorestatus.isDisplayed()
            cluster.restorebutton.click()
        }
        println("restore clicked")
        then:
        waitFor(waitTime) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase())

        // FOR UAT TESTING ENABLE BELOW CODE
        //  waitFor(waitTime){cluster.buttonrestore.isDisplayed()}
        //  cluster.buttonrestore.click()
        //  waitFor(waitTime){cluster.restorepopupno.isDisplayed()
        //             cluster.restorepopupyes.isDisplayed()}
        //  cluster.restorepopupyes.click()
        //  println("Yes clicked for restore popup")
        //  waitFor(waitTime){cluster.savesuccessok.isDisplayed()}
        //  cluster.savesuccessok.click()
        // println("ok clicked and message displayed after restoring")
    }

    def shutdownCancel(){
        when:
        at AdminPage
        waitFor(waitTime) { cluster.shutdownbutton.isDisplayed() }
        cluster.shutdownbutton.click()
        then:
        waitFor(waitTime) { cluster.shutdownconfirmation.isDisplayed() }
        cluster.shutdownconfirmation.text().toLowerCase().equals("Shutdown: Confirmation".toLowerCase())
        cluster.shutdowncancelbutton.click()
    }

    def shutdownClose(){
        when:
        at AdminPage
        waitFor(waitTime) { cluster.shutdownbutton.isDisplayed() }
        cluster.shutdownbutton.click()
        then:
        waitFor(waitTime) { cluster.shutdownconfirmation.isDisplayed() }
        cluster.shutdownconfirmation.text().toLowerCase().equals("Shutdown: Confirmation".toLowerCase())
        cluster.shutdownclosebutton.click()
    }

    //server name list test

    def checkClusterStatus() {
        when: 'clicked server button'
        at AdminPage
        page.serverbutton.isDisplayed()
        page.serverbutton.click()
        then:
        if (waitFor(waitTime) { page.mainservername.isDisplayed() && page.servername.isDisplayed() }) {

            println("server name is displayed as: " + page.mainservername.text().replaceAll("Stop", "").replaceAll("Paused", ""))
            println("currently running server is : "+ page.servername.text())
        }
        try {
            if (!page.cluster.resumebutton.displayed) {
                if (page.shutdownServerStop.displayed) {
                    println("Servers are stopped")
                }
            }
        } catch(geb.error.RequiredPageContentNotPresent e) {
            println("Resume button is not displayed")
        }

        try {
            if ( page.cluster.resumebutton.displayed ) {
                if (page.shutdownServerPause.displayed) {
                    println("Servers are paused!!!")
                }

            }
        } catch(geb.error.RequiredPageContentNotPresent e) {
            println("Resume button is displayed")
        }
    }

    // Overview Expansion

    def httpAccessExpandCheckText() {
        when:
        page.overview.httpAccess.click()
        then:
        waitFor(waitTime) {
            page.overview.jsonApi.text().equals("JSON API")
            !page.overview.jsonApiStatus.text().equals("")
        }
    }

    def commandLoggingExpandCheckText() {
        int count = 0

        when:
        page.overview.commandLogging.click()
        then:
        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.overview.logFrequencyTime.text().equals("Log Frequency Time")
                    !page.overview.logFrequencyTimeValue.text().equals("")

                    page.overview.logFrequencyTransactions.text().equals("Log Frequency Transactions")
                    !page.overview.logFrequencyTransactionsValue.text().equals("")

                    page.overview.logSize.text().equals("Log Size")
                    !page.overview.logSizeValue.text().equals("")
                }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
    }

    //server setting

    def checkServerSettingAndDisplayItsRespectiveValue() {
        when:
        while(true){
            if(waitFor(waitTime){page.networkInterfaces.serverSettingTitle.isDisplayed()} && page.networkInterfaces.serverSettingTitle.text() !=""){
                println("Title displayed as:"+page.networkInterfaces.serverSettingTitle.text())
            }else println("Server setting title not displayed so not processing further")
            break;
        }
        then:
        if(page.networkInterfaces.serversettingclientvalue.text()==""){
            println("Client port value in server setting is empty")}
        else{println("Client port value in server setting is not empty, value:" +page.networkInterfaces.serversettingclientvalue.text())}

        if(page.networkInterfaces.serversettingadminvalue.text()==""){
            println("Admin port value in server setting is empty")}
        else{println("Admin port value in server setting is not empty, value:" +page.networkInterfaces.serversettingadminvalue.text())}

        if(page.networkInterfaces.serversettinghttpvalue.text()==""){
            println("HTTP port value in server setting is empty")}
        else{println("HTTP port value in server setting is not empty, value:" +page.networkInterfaces.serversettinghttpvalue.text())}

        if(page.networkInterfaces.serversettinginternalvalue.text()==""){
            println("Internal port value in server setting is empty")}
        else{println("Internal port value in server setting is not empty, value:" +page.networkInterfaces.serversettinginternalvalue.text())}

        if(page.networkInterfaces.serversettingzookeepervalue.text()==""){
            println("Zookeeper port value in server setting is empty")}
        else{println("Zookeeper port value in server setting is not empty, value:" +page.networkInterfaces.serversettingzookeepervalue.text())}

        if(page.networkInterfaces.serversettingreplicationvalue.text()==""){
            println("Replication port value in server setting is empty")}
        else{println("Replication port value in server setting is not empty, value:" +page.networkInterfaces.serversettingreplicationvalue.text())}
    }

    def cleanupSpec() {
        if (!(page instanceof VoltDBManagementCenterPage)) {
            when: 'Open VMC page'
            ensureOnVoltDBManagementCenterPage()
            then: 'to be on VMC page'
            at VoltDBManagementCenterPage
        }

        page.loginIfNeeded()

        when: 'click the Admin link (if needed)'
        page.openAdminPage()
        then: 'should be on Admin page'
        at AdminPage

        String initialPrefix    = "DEFAULT"
        String initialFreq      = "10"
        String initialFreqUnit  = "Hrs"
        String initialRetained  = "10"

        String initialHeartTimeout = "10"
        String initialQueryTimeout = "10"

        // autosnapshot revert

        if (revertAutosnapshots == true) {
            when:
            page.autoSnapshotsEdit.click()
            then:
            waitFor(waitTime) {
                page.autoSnapshotsEditCheckbox.isDisplayed()
                page.autoSnapshotsEditOk.isDisplayed()
                page.autoSnapshotsEditCancel.isDisplayed()
            }

            page.filePrefixField.value(initialPrefix)
            page.frequencyField.value(initialFreq)
            page.frequencyUnitField.click()
            page.frequencyUnitField.value(initialFreqUnit)
            page.retainedField.value(initialRetained)

            if(page.fileprefixEdit.text() != " "){
                println("fileprefix passed, found non-empty")

                if(
                page.frequencyEdit.text() != " "){
                    println("frequency passed, found non-empty")}
                // page.frequencyUnitField.click()
                if( page.frequencyUnitField.text() != " "){
                    println("frequency unit passed, found non-empty")}
                if ( page.retainedEdit.text() != " "){
                    println("retained passed, found non-empty")}
            }
            page.autoSnapshotsEditOk.click()
            println("pop up visible")

            when:

            while(true) {
                page.autosnapshotsconfirmok.click()
                println("inside ok clicked successfully")
                if(page.filePrefixField.isDisplayed()== false)
                    break
            }

            then:

            waitFor(waitTime){
                page.filePrefix.text().equals(initialPrefix)
                page.frequency.text().equals(initialFreq)
                page.frequencyUnit.text().equals(initialFreqUnit)
                page.retained.text().equals(initialRetained)
            }
        }



        // query timeout revert

        if (revertQueryTimeout==false) {
            when:
            page.advanced.click()
            then:
            waitFor(waitTime) { page.overview.queryTimeoutEdit.isDisplayed() }

            when:
            waitFor(waitTime) { page.overview.queryTimeoutEdit.click() }
            then:
            waitFor(waitTime) {
                page.overview.queryTimeoutField.isDisplayed()
                page.overview.queryTimeoutOk.isDisplayed()
                page.overview.queryTimeoutCancel.isDisplayed()
            }

            when:
            page.overview.queryTimeoutField.value(initialQueryTimeout)
            waitFor(waitTime) {
                page.overview.queryTimeoutOk.click()
            }
            then:
            waitFor(waitTime) {
                page.overview.queryTimeoutPopupOk.isDisplayed()
                page.overview.queryTimeoutPopupCancel.isDisplayed()
            }


            waitFor(waitTime) {
                try {
                    page.overview.queryTimeoutPopupOk.click()
                } catch (org.openqa.selenium.ElementNotVisibleException e) {
                    println("retrying")
                }

                page.overview.queryTimeoutEdit.isDisplayed()
                page.overview.queryTimeoutValue.text().equals(initialQueryTimeout)
                !page.overview.queryTimeoutPopupOk.isDisplayed()
                !page.overview.queryTimeoutPopupCancel.isDisplayed()
            }
        }
    }

}
