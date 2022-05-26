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

class AdminAdvancedTest extends TestBase {
    static String initialPrefix
    static String initialFreq
    static String initialFreqUnit
    static String initialRetained

    static String initialHeartTimeout
    static String initialQueryTimeout
    static String initialMemoryLimit = "-1"

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

    def checkAdvanced() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'
        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    overview.advanced.isDisplayed()
                    overview.advanced.text().toLowerCase().equals("Advanced".toLowerCase())
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

    // overview: advanced expansion-Edits

    def checkClickHeartTimeoutEditAndCancel() {
        when:
        page.advanced.click()
        then:
        waitFor(waitTime) { page.overview.heartTimeoutEdit.isDisplayed() }

        when:
        waitFor(waitTime) { page.overview.heartTimeoutEdit.click() }
        then:
        waitFor(waitTime) {
            page.overview.heartTimeoutField.isDisplayed()
            page.overview.heartTimeoutOk.isDisplayed()
            page.overview.heartTimeoutCancel.isDisplayed()
        }

        when:
        waitFor(waitTime) { page.overview.heartTimeoutCancel.click() }
        then:
        waitFor(waitTime) {
            !page.overview.heartTimeoutField.isDisplayed()
            !page.overview.heartTimeoutOk.isDisplayed()
            !page.overview.heartTimeoutCancel.isDisplayed()
        }
    }

    def checkClickHeartTimeoutEditAndClickOkAndThenCancel() {
        when:
        page.advanced.click()
        then:
        waitFor(waitTime) { page.overview.heartTimeoutEdit.isDisplayed() }

        when:
        waitFor(waitTime) { page.overview.heartTimeoutEdit.click() }
        then:
        waitFor(waitTime) {
            page.overview.heartTimeoutField.isDisplayed()
            page.overview.heartTimeoutOk.isDisplayed()
            page.overview.heartTimeoutCancel.isDisplayed()
        }

        when:
        page.overview.heartTimeoutField.value("10")
        waitFor(waitTime) {
            page.overview.heartTimeoutOk.click()
        }
        then:
        waitFor(waitTime) {
            page.overview.heartTimeoutPopupOk.isDisplayed()
            page.overview.heartTimeoutPopupCancel.isDisplayed()
        }

        int count = 0
        while(count<numberOfTrials) {
            count++
            println("Try")
            try {
                try {
                    page.overview.heartTimeoutPopupCancel.click()
                } catch (org.openqa.selenium.ElementNotVisibleException e) {
                    println("PASS")
                    break
                }
                page.overview.heartTimeoutEdit.isDisplayed()
                !page.overview.heartTimeoutPopupOk.isDisplayed()
                !page.overview.heartTimeoutPopupCancel.isDisplayed()
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                println("Try 2")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Try sele")
            }
        }
    }

    def checkClickHeartTimeoutEditAndClickOkAndThenOk() {
        when:
        String heartTimeout = 20
        page.advanced.click()
        waitFor(waitTime) {
            page.overview.heartTimeoutValue.isDisplayed()
        }
        initialHeartTimeout = page.overview.heartTimeoutValue.text()
        println("Initial Heartbeat time "+ initialHeartTimeout)
        revertHeartTimeout = true

        then:
        waitFor(waitTime) { page.overview.heartTimeoutEdit.isDisplayed() }

        when:
        waitFor(waitTime) { page.overview.heartTimeoutEdit.click() }
        then:
        waitFor(waitTime) {
            page.overview.heartTimeoutField.isDisplayed()
            page.overview.heartTimeoutOk.isDisplayed()
            page.overview.heartTimeoutCancel.isDisplayed()
        }

        when:
        page.overview.heartTimeoutField.value(heartTimeout)
        waitFor(waitTime) {
            page.overview.heartTimeoutOk.click()
        }
        then:
        waitFor(waitTime) {
            page.overview.heartTimeoutPopupOk.isDisplayed()
            page.overview.heartTimeoutPopupCancel.isDisplayed()
        }


        waitFor(waitTime) {
            try {
                page.overview.heartTimeoutPopupOk.click()
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                println("retrying")
            }

            page.overview.heartTimeoutEdit.isDisplayed()
            page.overview.heartTimeoutValue.text().equals(heartTimeout)
            !page.overview.heartTimeoutPopupOk.isDisplayed()
            !page.overview.heartTimeoutPopupCancel.isDisplayed()
        }
    }

    def heartbeatTimeoutCheckErrorMessageIfEmptyData() {
        when:
        String heartTimeout = ""
        page.advanced.click()
        then:
        waitFor(waitTime) { page.overview.heartTimeoutEdit.isDisplayed() }

        when:
        waitFor(waitTime) { page.overview.heartTimeoutEdit.click() }
        then:
        waitFor(waitTime) {
            page.overview.heartTimeoutField.isDisplayed()
            page.overview.heartTimeoutOk.isDisplayed()
            page.overview.heartTimeoutCancel.isDisplayed()
        }

        when:
        page.overview.heartTimeoutField.value(heartTimeout)
        waitFor(waitTime) {
            page.overview.heartTimeoutOk.click()
        }
        then:
        waitFor(waitTime) {
            page.overview.errorMsgHeartbeat.isDisplayed()
            page.overview.errorMsgHeartbeat.text().equals("Please enter a valid positive number.")
        }

    }

    def heartbeatTimeoutCheckErrorMessageIsValueLessThenOne() {
        when:
        String heartTimeout = "0"
        page.advanced.click()
        then:
        waitFor(waitTime) { page.overview.heartTimeoutEdit.isDisplayed() }

        when:
        waitFor(waitTime) { page.overview.heartTimeoutEdit.click() }
        then:
        waitFor(waitTime) {
            page.overview.heartTimeoutField.isDisplayed()
            page.overview.heartTimeoutOk.isDisplayed()
            page.overview.heartTimeoutCancel.isDisplayed()
        }

        when:
        page.overview.heartTimeoutField.value(heartTimeout)
        waitFor(waitTime) {
            page.overview.heartTimeoutOk.click()
        }
        then:
        waitFor(waitTime) {
            page.overview.errorMsgHeartbeat.isDisplayed()
            page.overview.errorMsgHeartbeat.text().equals("Please enter a positive number. Its minimum value should be 1.")
        }

    }

    // query timeout

    def checkClickQueryTimeoutEditAndCancel() {
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
        waitFor(waitTime) { page.overview.queryTimeoutCancel.click() }
        then:
        waitFor(waitTime) {
            !page.overview.queryTimeoutField.isDisplayed()
            !page.overview.queryTimeoutOk.isDisplayed()
            !page.overview.queryTimeoutCancel.isDisplayed()
        }
    }

    def checkClickQueryTimeoutEditAndClickOkAndThenCancel() {
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
        page.overview.queryTimeoutField.value("10")
        then:
        waitFor(waitTime) {
            page.overview.queryTimeoutOk.click()
            page.overview.queryTimeoutPopupOk.isDisplayed()
            page.overview.queryTimeoutPopupCancel.isDisplayed()
        }

        int count = 0
        while(count<5) {
            count++
            try {
                try {
                    page.overview.queryTimeoutPopupCancel.click()
                } catch(org.openqa.selenium.ElementNotVisibleException e) {
                    if(count > 0) {
                        println("")
                        break
                    }
                }

                page.overview.queryTimeoutEdit.isDisplayed()
                !page.overview.queryTimeoutPopupOk.isDisplayed()
                !page.overview.queryTimeoutPopupCancel.isDisplayed()
                println("")
            }catch(org.openqa.selenium.ElementNotVisibleException f){
                println("")
            }catch(org.openqa.selenium.StaleElementReferenceException f){
                println("")
            }
        }
    }

    def checkClickQueryTimeoutEditAndClickOkAndThenOk() {
        when:
        String queryTimeout = 20
        page.advanced.click()
        waitFor(waitTime) {
            page.overview.queryTimeoutValue.isDisplayed()
        }
        initialQueryTimeout = page.overview.queryTimeoutValue.text()
        println("Initial Query Timeout " + initialQueryTimeout)
        revertQueryTimeout = true

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
        page.overview.queryTimeoutField.value(queryTimeout)
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
            page.overview.queryTimeoutValue.text().equals(queryTimeout)
            !page.overview.queryTimeoutPopupOk.isDisplayed()
            !page.overview.queryTimeoutPopupCancel.isDisplayed()
        }
    }

    def queryTimeoutCheckErrorMessageIfEmptyData() {
        when:
        String queryTimeout = ""
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
        page.overview.queryTimeoutField.value(queryTimeout)
        waitFor(waitTime) {
            page.overview.queryTimeoutOk.click()
        }
        then:
        waitFor(waitTime) {
            page.overview.errorQuery.isDisplayed()
            page.overview.errorQuery.text().equals("Please enter a valid positive number.")
        }
    }

    //Memory Limit
    def checkTheMemoryLimitEditButtonAndThenCancelButton() {
        when:
        page.advanced.click()
        then:
        waitFor(waitTime) { page.overview.memoryLimitEdit.isDisplayed() }

        when:
        waitFor(waitTime) { page.overview.memoryLimitEdit.click() }
        then:
        waitFor(waitTime) {
            page.overview.memoryLimitField.isDisplayed()
            page.overview.memoryLimitDdlUnit.isDisplayed()
            page.overview.memoryLimitOk.isDisplayed()
            page.overview.memoryLimitCancel.isDisplayed()
        }

        when:
        waitFor(waitTime) { page.overview.memoryLimitCancel.click() }
        then:
        waitFor(waitTime) {
            !page.overview.memoryLimitField.isDisplayed()
            !page.overview.memoryLimitOk.isDisplayed()
            !page.overview.memoryLimitCancel.isDisplayed()
            page.overview.memoryLimitEdit.isDisplayed()
        }
    }

    def checkMemoryLimitEditAndThenClickOkAndCancel() {
        when:
        page.advanced.click()
        then:
        waitFor(waitTime) { page.overview.memoryLimitEdit.isDisplayed() }

        when:
        waitFor(waitTime) { page.overview.memoryLimitEdit.click() }
        then:
        waitFor(waitTime) {
            page.overview.memoryLimitField.isDisplayed()
            page.overview.memoryLimitDdlUnit.isDisplayed()
            page.overview.memoryLimitOk.isDisplayed()
            page.overview.memoryLimitCancel.isDisplayed()
        }

        when:
        page.overview.memoryLimitField.value("10")
        waitFor(waitTime) {
            page.overview.memoryLimitOk.click()
        }
        then:
        waitFor(waitTime) {
            page.overview.memoryLimitPopupOk.isDisplayed()
            page.overview.memoryLimitPopupCancel.isDisplayed()
        }
        int count = 0
        while(count<numberOfTrials) {
            count++
            println("Try")
            try {
                try {
                    page.overview.memoryLimitPopupCancel.click()
                } catch (org.openqa.selenium.ElementNotVisibleException e) {
                    println("MemoryLimitPopupCancel button clicked.")
                    break
                }
                page.overview.memoryLimitEdit.isDisplayed()
                !page.overview.memoryLimitPopupOk.isDisplayed()
                !page.overview.memoryLimitPopupCancel.isDisplayed()
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                println("ElementNotVisibleException, trying again.")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("StaleElementReferenceException, trying again.")
            }
        }
    }

    def checkMemoryLimitEditAndThenClickOkAndConfirmOkUsingGB() {
        when:
        String memoryLimit = 20
        String memoryLimitUnit = "GB"
        page.advanced.click()
        waitFor(waitTime) {
            page.overview.memoryLimitValue.isDisplayed()
        }
        initialMemoryLimit = page.overview.memoryLimitValue.text()
        println("Initial value of memory limit "+ initialMemoryLimit)
        revertMemorySize = true
        then:
        waitFor(waitTime) { page.overview.memoryLimitEdit.isDisplayed() }

        when:
        waitFor(waitTime) { page.overview.memoryLimitEdit.click() }
        then:
        waitFor(waitTime) {
            page.overview.memoryLimitField.isDisplayed()
            page.overview.memoryLimitDdlUnit.isDisplayed()
            page.overview.memoryLimitOk.isDisplayed()
            page.overview.memoryLimitCancel.isDisplayed()
        }

        when:
        page.overview.memoryLimitField.value(memoryLimit)
        page.overview.memoryLimitDdlUnit.value(memoryLimitUnit)
        waitFor(waitTime) {
            page.overview.memoryLimitOk.click()
        }
        then:
        waitFor(waitTime) {
            page.overview.memoryLimitPopupOk.isDisplayed()
            page.overview.memoryLimitPopupCancel.isDisplayed()
        }
        waitFor(waitTime) {
            try {
                page.overview.memoryLimitPopupOk.click()
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                println("Retrying")
            }

            page.overview.memoryLimitEdit.isDisplayed()
            page.overview.memoryLimitValue.text().equals(memoryLimit)
            !page.overview.memoryLimitPopupOk.isDisplayed()
            !page.overview.memoryLimitPopupCancel.isDisplayed()
        }
    }

    def checkMemoryLimitEditAndThenClickOkAndConfirmOkUsingPercentage() {
        when:
        String memoryLimit = 50
        String memoryLimitUnit = "%"
        page.advanced.click()
        waitFor(waitTime) {
            page.overview.memoryLimitValue.isDisplayed()
        }
        if(initialMemoryLimit == "-1"){
            initialMemoryLimit = page.overview.memoryLimitValue.text()
            println("Initial value of memory limit "+ initialMemoryLimit)
            revertMemorySize = true
        }
        then:
        waitFor(waitTime) { page.overview.memoryLimitEdit.isDisplayed() }

        when:
        waitFor(waitTime) { page.overview.memoryLimitEdit.click() }
        then:
        waitFor(waitTime) {
            page.overview.memoryLimitField.isDisplayed()
            page.overview.memoryLimitDdlUnit.isDisplayed()
            page.overview.memoryLimitOk.isDisplayed()
            page.overview.memoryLimitCancel.isDisplayed()
        }

        when:
        page.overview.memoryLimitField.value(memoryLimit)
        page.overview.memoryLimitDdlUnit.value(memoryLimitUnit)
        waitFor(waitTime) {
            page.overview.memoryLimitOk.click()
        }
        then:
        waitFor(waitTime) {
            page.overview.memoryLimitPopupOk.isDisplayed()
            page.overview.memoryLimitPopupCancel.isDisplayed()
        }

        waitFor(waitTime) {
            try {
                page.overview.memoryLimitPopupOk.click()
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                println("Retrying")
            }

            page.overview.memoryLimitEdit.isDisplayed()
            page.overview.memoryLimitValue.text().equals(memoryLimit)
            !page.overview.memoryLimitPopupOk.isDisplayed()
            !page.overview.memoryLimitPopupCancel.isDisplayed()
        }
    }

    def verifyEmptyEntryIsNotAcceptedInMemoryLimit() {
        int count = 0
        println("Test Start: Verify empty entry is not accepted in Memory Limit")

        when:
        String memoryLimit = ""
        page.advanced.click()
        waitFor(waitTime) {
            page.overview.memoryLimitValue.isDisplayed()
        }
        if(initialMemoryLimit == "-1"){
            initialMemoryLimit = page.overview.memoryLimitValue.text()
            revertMemorySize = true
        }
        then:
        waitFor(waitTime) { page.overview.memoryLimitEdit.isDisplayed() }

        when:
        waitFor(waitTime) { page.overview.memoryLimitEdit.click() }
        then:
        waitFor(waitTime) {
            page.overview.memoryLimitField.isDisplayed()
            page.overview.memoryLimitOk.isDisplayed()
            page.overview.memoryLimitCancel.isDisplayed()
        }

        when:
        page.overview.memoryLimitField.value(memoryLimit)
        then:
        while(count < numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.overview.memoryLimitOk.click()
                }

                waitFor(waitTime) {
                    page.overview.memoryLimitError.isDisplayed()
                }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        println("Test End: Verify empty entry is not accepted in Memory Limit")
    }

//    def "Check the error message for memory Limit when more than 4 digits are placed after decimal"() {
//        when:
//        String invalidMemoryLimit = "20.12345"
//        String validMemoryLimit ="20.1234"
//        page.advanced.click()
//        then:
//        waitFor(waitTime) { page.overview.memoryLimitEdit.isDisplayed() }
//
//        when:
//        waitFor(waitTime) { page.overview.memoryLimitEdit.click() }
//        then:
//        waitFor(waitTime) {
//            page.overview.memoryLimitField.isDisplayed()
//            page.overview.memoryLimitOk.isDisplayed()
//            page.overview.memoryLimitCancel.isDisplayed()
//        }
//
//        when:
//        page.overview.memoryLimitField.value(invalidMemoryLimit)
//        waitFor(waitTime) {
//            page.overview.memoryLimitOk.click()
//        }
//        then:
//        waitFor(waitTime) {
//            page.overview.memoryLimitError.isDisplayed()
//            page.overview.memoryLimitError.text().equals("Only four digits are allowed after decimal.")
//        }
//
//        when:
//        page.overview.memoryLimitField.value(validMemoryLimit)
//        then:
//        !page.overview.memoryLimitError.isDisplayed()
//    }

    def checkTheErrorMessageForMemoryLimitWhenValueIsLessThanZero() {
        when:
        String invalidMemoryLimit = "-1"
        String validMemoryLimit = "1"
        page.advanced.click()
        then:
        waitFor(waitTime) { page.overview.memoryLimitEdit.isDisplayed() }

        when:
        waitFor(waitTime) { page.overview.memoryLimitEdit.click() }
        then:
        waitFor(waitTime) {
            page.overview.memoryLimitField.isDisplayed()
            page.overview.memoryLimitOk.isDisplayed()
            page.overview.memoryLimitCancel.isDisplayed()
        }

        when:
        page.overview.memoryLimitField.value(invalidMemoryLimit)
        waitFor(waitTime) {
            page.overview.memoryLimitOk.click()
        }
        then:
        waitFor(waitTime) {
            page.overview.memoryLimitError.isDisplayed()
            page.overview.memoryLimitError.text().equals("Please enter a positive number.")
        }
        when:
        page.overview.memoryLimitField.value(validMemoryLimit)
        then:
        !page.overview.memoryLimitError.isDisplayed()
    }

    def checkMemoryLimitMaximumLimitValidationWhenUnitIsPercentage(){
        when:
        String invalidMemoryLimitInPer = "100"
        String validMemoryLimitInPer = "99"
        page.advanced.click()
        then:
        waitFor(waitTime) { page.overview.memoryLimitEdit.isDisplayed() }

        when:
        waitFor(waitTime) { page.overview.memoryLimitEdit.click() }
        then:
        waitFor(waitTime) {
            page.overview.memoryLimitField.isDisplayed()
            page.overview.memoryLimitDdlUnit.isDisplayed()
            page.overview.memoryLimitOk.isDisplayed()
            page.overview.memoryLimitCancel.isDisplayed()
        }

        when:
        page.overview.memoryLimitField.value(invalidMemoryLimitInPer)
        page.overview.memoryLimitDdlUnit.value("%")
        waitFor(waitTime) {
            page.overview.memoryLimitOk.click()
        }
        then:
        waitFor(waitTime) {
            page.overview.memoryLimitError.isDisplayed()
            page.overview.memoryLimitError.text().equals("Maximum value of percentage cannot be greater than 99.")
        }
        when:
        page.overview.memoryLimitField.value(validMemoryLimitInPer)
        then:
        !page.overview.memoryLimitError.isDisplayed()
    }

   // Disk Limit

    def checkDiskLimitClickAndCheckItsValue() {
        when:"Open Advanced"
        page.advanced.click()
        then:
        waitFor(waitTime) { page.overview.diskLimit.isDisplayed() }
        when:
        page.overview.diskLimit.click()
        then:
        waitFor(waitTime) { page.noFeaturestxt.isDisplayed() }
        if(page.noFeaturestxt.text()=="No features available.") {
            println("Currently, No Features are available in Disk Limit")
        } else {
            println("Early presence of Features settings detected!")
        }
        page.overview.diskLimit.click()
    }

    def verifyAddDiskLimitForSNAPSHOTFeature(){
        when:"Open Advanced"
        page.advanced.click()
        then:"Check if DiskLimit is displayed"
         waitFor(10){page.overview.diskLimit.isDisplayed()}

        when:"Open Edit Disk Limit"
        println("Opening Disk Limit Popup")
        waitFor(30) { page.overview.diskLimitEdit.isDisplayed() }
        page.overview.diskLimitEdit.click()
        waitFor(30) { page.overview.addDiskLimitHeader.isDisplayed() }
        then:"check elements"
        waitFor(40){page.overview.lnkAddNewFeature.isDisplayed()}
        when:"Add SNAPSHOT feature"
        page.overview.lnkAddNewFeature.click()
        then:"check elements"
        page.overview.featureName1.isDisplayed()
        page.overview.featureValue1.isDisplayed()
        page.overview.featureUnit1.isDisplayed()

        when:"Provide values for the elements"
        page.overview.featureName1.value("SNAPSHOTS")
        page.overview.featureValue1.value("13")
        page.overview.featureUnit1.value("GB")
        then:"Save New Feature"
        page.overview.btnAddDiskLimitSave.isDisplayed()
        page.overview.btnAddDiskLimitSave.click()
        waitFor(30){page.overview.btnSaveDiskLimitOk.isDisplayed()}
        page.overview.btnSaveDiskLimitOk.click()

        when: "expand Disk Limit"
        waitFor(30){diskLimitExpanded.click()}
        //waitFor(waitTime){ page.overview.expandDiskLimit()}
        then:
        println("Add succeeded")
        //snapShotName.text().equals("SNAPSHOTS")

        when:"Open Edit Disk Limit"
        page.overview.openEditDiskLimitPopup()
        then:"check elements"
        waitFor(30){page.overview.deleteFirstFeature.isDisplayed()}

        when:"Delete Feature"
        page.overview.deleteFirstFeature.click()
        then:"Save New Feature"
        page.overview.btnAddDiskLimitSave.isDisplayed()
        page.overview.btnAddDiskLimitSave.click()

        page.overview.btnSaveDiskLimitOk.isDisplayed()
        page.overview.btnSaveDiskLimitOk.click()

//        when: "expand Disk Limit"
//        diskLimitExpanded.click()
//        //waitFor(waitTime){ page.overview.expandDiskLimit()}
//        then:
//        snapShotName.isDisplayed()
    }

    def verifyAddDiskLimitforCOMMANDLOGfeature(){
        when:"Open Advanced"
        page.advanced.click()
        then:"Check if DiskLimit is displayed"
        waitFor(10){page.overview.diskLimit.isDisplayed()}

        when:"Open Edit Disk Limit"
        waitFor(30) { page.overview.diskLimitEdit.isDisplayed() }
        page.overview.diskLimitEdit.click()
        waitFor(30) { page.overview.addDiskLimitHeader.isDisplayed() }
        then:"check elements"
        page.overview.lnkAddNewFeature.isDisplayed()

        when:"Add SNAPSHOT feature"
        page.overview.lnkAddNewFeature.click()
        then:"check elements"
        page.overview.featureName1.isDisplayed()
        page.overview.featureValue1.isDisplayed()
        page.overview.featureUnit1.isDisplayed()

        when:"Provide values for the elements"
        page.overview.featureName1.value("COMMANDLOG")
        page.overview.featureValue1.value("13")
        page.overview.featureUnit1.value("GB")
        then:"Save New Feature"
        page.overview.btnAddDiskLimitSave.isDisplayed()
        page.overview.btnAddDiskLimitSave.click()

        waitFor(30){page.overview.btnSaveDiskLimitOk.isDisplayed()}
        page.overview.btnSaveDiskLimitOk.click()

        when: "expand Disk Limit"
        waitFor(30){diskLimitExpanded.click()}
        //waitFor(waitTime){ page.overview.expandDiskLimit()}
        then:
        println("Add succeeded")
        //snapShotName.text().equals("SNAPSHOTS")

        when:"Open Edit Disk Limit"
        page.overview.openEditDiskLimitPopup()
        then:"check elements"
        waitFor(30){page.overview.deleteFirstFeature.isDisplayed()}

        when:"Delete Feature"
        page.overview.deleteFirstFeature.click()
        then:"Save New Feature"
        page.overview.btnAddDiskLimitSave.isDisplayed()
        page.overview.btnAddDiskLimitSave.click()
        page.overview.btnSaveDiskLimitOk.isDisplayed()
        page.overview.btnSaveDiskLimitOk.click()

//        when: "expand Disk Limit"
//        diskLimitExpanded.click()
//        //waitFor(waitTime){ page.overview.expandDiskLimit()}
//        then:
//        snapShotName.isDisplayed()
    }

    def verifyAddDiskLimitForExportOverflowFeature(){
        when:"Open Advanced"
        page.advanced.click()
        then:"Check if DiskLimit is displayed"
        waitFor(10) { page.overview.diskLimit.isDisplayed() }

        when:"Open Edit Disk Limit"
        waitFor(30) { page.overview.diskLimitEdit.isDisplayed() }
        page.overview.diskLimitEdit.click()
        waitFor(30) { page.overview.addDiskLimitHeader.isDisplayed() }
        then:"check elements"
        page.overview.lnkAddNewFeature.isDisplayed()

        when:"Add SNAPSHOT feature"
        page.overview.lnkAddNewFeature.click()
        then:"check elements"
        page.overview.featureName1.isDisplayed()
        page.overview.featureValue1.isDisplayed()
        page.overview.featureUnit1.isDisplayed()

        when:"Provide values for the elements"
        page.overview.featureName1.value("EXPORTOVERFLOW")
        page.overview.featureValue1.value("13")
        page.overview.featureUnit1.value("GB")
        then:"Save New Feature"
        page.overview.btnAddDiskLimitSave.isDisplayed()
        page.overview.btnAddDiskLimitSave.click()

        waitFor(30) { page.overview.btnSaveDiskLimitOk.isDisplayed() }
        page.overview.btnSaveDiskLimitOk.click()

        when: "expand Disk Limit"
        waitFor(30) { diskLimitExpanded.click() }
        //waitFor(waitTime){ page.overview.expandDiskLimit()}
        then:
        println("Add succeeded")
        //snapShotName.text().equals("SNAPSHOTS")

        when:"Open Edit Disk Limit"
        page.overview.openEditDiskLimitPopup()
        then:"check elements"
        waitFor(30){page.overview.deleteFirstFeature.isDisplayed()}

        when:"Delete Feature"
        page.overview.deleteFirstFeature.click()
        then:"Save New Feature"
        page.overview.btnAddDiskLimitSave.isDisplayed()
        page.overview.btnAddDiskLimitSave.click()
        page.overview.btnSaveDiskLimitOk.isDisplayed()
        page.overview.btnSaveDiskLimitOk.click()
//        when: "expand Disk Limit"
//        diskLimitExpanded.click()
//        //waitFor(waitTime){ page.overview.expandDiskLimit()}
//        then:
//        snapShotName.isDisplayed()
    }

    def verifyAddDiskLimitForDroverflowFeature() {
        when:"Open Advanced"
        page.advanced.click()
        then:"Check if DiskLimit is displayed"
        waitFor(10) { page.overview.diskLimit.isDisplayed() }

        when:"Open Edit Disk Limit"
        waitFor(30) { page.overview.diskLimitEdit.isDisplayed() }
        page.overview.diskLimitEdit.click()
        waitFor(30) { page.overview.addDiskLimitHeader.isDisplayed() }
        then:"check elements"
        page.overview.lnkAddNewFeature.isDisplayed()

        when:"Add SNAPSHOT feature"
        page.overview.lnkAddNewFeature.click()
        then:"check elements"
        page.overview.featureName1.isDisplayed()
        page.overview.featureValue1.isDisplayed()
        page.overview.featureUnit1.isDisplayed()

        when:"Provide values for the elements"
        page.overview.featureName1.value("DROVERFLOW")
        page.overview.featureValue1.value("13")
        page.overview.featureUnit1.value("GB")
        then:"Save New Feature"
        page.overview.btnAddDiskLimitSave.isDisplayed()
        page.overview.btnAddDiskLimitSave.click()
        waitFor(30) { page.overview.btnSaveDiskLimitOk.isDisplayed() }
        page.overview.btnSaveDiskLimitOk.click()

        when: "expand Disk Limit"
        waitFor(30){diskLimitExpanded.click()}
        //waitFor(waitTime){ page.overview.expandDiskLimit()}
        then:
        println("Add succeeded")
        //snapShotName.text().equals("SNAPSHOTS")

        when:"Open Edit Disk Limit"
        page.overview.openEditDiskLimitPopup()
        then:"check elements"
        waitFor(30){page.overview.deleteFirstFeature.isDisplayed()}

        when:"Delete Feature"
        page.overview.deleteFirstFeature.click()
        then:"Save New Feature"
        page.overview.btnAddDiskLimitSave.isDisplayed()
        page.overview.btnAddDiskLimitSave.click()
        page.overview.btnSaveDiskLimitOk.isDisplayed()
        page.overview.btnSaveDiskLimitOk.click()
//        when: "expand Disk Limit"
//        diskLimitExpanded.click()
//        //waitFor(waitTime){ page.overview.expandDiskLimit()}
//        then:
//        snapShotName.isDisplayed()
    }

    def verifyAddDiskLimitForCommandLogSnapshopFeature() {
        when:"Open Advanced"
        page.advanced.click()
        then:"Check if DiskLimit is displayed"
        waitFor(10){page.overview.diskLimit.isDisplayed()}

        when:"Open Edit Disk Limit"
        waitFor(30) { page.overview.diskLimitEdit.isDisplayed() }
        page.overview.diskLimitEdit.click()
        waitFor(30) { page.overview.addDiskLimitHeader.isDisplayed() }
        then:"check elements"
        page.overview.lnkAddNewFeature.isDisplayed()

        when:"Add SNAPSHOT feature"
        page.overview.lnkAddNewFeature.click()
        then:"check elements"
        page.overview.featureName1.isDisplayed()
        page.overview.featureValue1.isDisplayed()
        page.overview.featureUnit1.isDisplayed()

        when:"Provide values for the elements"
        page.overview.featureName1.value("COMMANDLOGSNAPSHOT")
        page.overview.featureValue1.value("13")
        page.overview.featureUnit1.value("GB")
        then:"Save New Feature"
        page.overview.btnAddDiskLimitSave.isDisplayed()
        page.overview.btnAddDiskLimitSave.click()
        waitFor(30){page.overview.btnSaveDiskLimitOk.isDisplayed()}
        page.overview.btnSaveDiskLimitOk.click()

        when: "expand Disk Limit"
        waitFor(30){diskLimitExpanded.click()}
        //waitFor(waitTime){ page.overview.expandDiskLimit()}
        then:
        println("Add succeeded")
        //snapShotName.text().equals("SNAPSHOTS")

        when:"Open Edit Disk Limit"
        page.overview.openEditDiskLimitPopup()
        then:"check elements"
        waitFor(30){page.overview.deleteFirstFeature.isDisplayed()}

        when:"Delete Feature"
        page.overview.deleteFirstFeature.click()
        then:"Save New Feature"
        page.overview.btnAddDiskLimitSave.isDisplayed()
        page.overview.btnAddDiskLimitSave.click()
        page.overview.btnSaveDiskLimitOk.isDisplayed()
        page.overview.btnSaveDiskLimitOk.click()
//        when: "expand Disk Limit"
//        diskLimitExpanded.click()
//        //waitFor(waitTime){ page.overview.expandDiskLimit()}
//        then:
//        snapShotName.isDisplayed()
    }

    def verifyErrorMessageForDuplicateFeatures() {
        when:"Open Advanced"
        page.advanced.click()
        then:"Check if DiskLimit is displayed"
        waitFor(10) { page.overview.diskLimit.isDisplayed() }

        when:"Open Edit Disk Limit"
        waitFor(30) { page.overview.diskLimitEdit.isDisplayed() }
        page.overview.diskLimitEdit.click()
        waitFor(30) { page.overview.addDiskLimitHeader.isDisplayed() }
        then:"check elements"
        page.overview.lnkAddNewFeature.isDisplayed()

        when:"Add SNAPSHOT feature"
        page.overview.lnkAddNewFeature.click()
        then:"check elements and add values"
        page.overview.featureName1.isDisplayed()
        page.overview.featureValue1.isDisplayed()
        page.overview.featureUnit1.isDisplayed()
        page.overview.featureName1.value("SNAPSHOTS")
        page.overview.featureValue1.value("13")
        page.overview.featureUnit1.value("GB")

        when:"Add SNAPSHOT feature"
        page.overview.lnkAddNewFeature.click()
        then:"Add another SNAPSHOT feature"
        page.overview.featureName2.isDisplayed()
        page.overview.featureValue2.isDisplayed()
        page.overview.featureUnit2.isDisplayed()
        page.overview.featureName2.value("SNAPSHOTS")
        page.overview.featureValue2.value("13")
        page.overview.featureUnit2.value("GB")

        when:"Save New Feature"
        page.overview.btnAddDiskLimitSave.isDisplayed()
        page.overview.btnAddDiskLimitSave.click()
        then: "Check for validation"
        page.overview.errortxtName1.isDisplayed()
        page.overview.errortxtName2.isDisplayed()
    }

    def verifyErrorMessageForFeaturesSize() {
        when:"Open Advanced"
        page.advanced.click()
        then:"Check if DiskLimit is displayed"
        waitFor(10) { page.overview.diskLimit.isDisplayed() }

        when:"Open Edit Disk Limit"
        waitFor(30) { page.overview.diskLimitEdit.isDisplayed() }
        page.overview.diskLimitEdit.click()
        waitFor(30) { page.overview.addDiskLimitHeader.isDisplayed() }
        then:"check elements"
        page.overview.lnkAddNewFeature.isDisplayed()

        when:"Add SNAPSHOT feature"
        page.overview.lnkAddNewFeature.click()
        then:"check elements"
        page.overview.featureName1.isDisplayed()
        page.overview.featureValue1.isDisplayed()
        page.overview.featureUnit1.isDisplayed()

        when:"Save New Feature"
        page.overview.btnAddDiskLimitSave.isDisplayed()
        page.overview.btnAddDiskLimitSave.click()
        then: "Check for validation"
        page.overview.errorValue1.isDisplayed()
        page.overview.errorValue1.text().equals("This field is required")
    }

    def verifyErrorMessageForFeaturesUnit() {
        when:"Open Advanced"
        page.advanced.click()
        then:"Check if DiskLimit is displayed"
        waitFor(10) { page.overview.diskLimit.isDisplayed() }

        when:"Open Edit Disk Limit"
        waitFor(30) { page.overview.diskLimitEdit.isDisplayed() }
        page.overview.diskLimitEdit.click()
        waitFor(30) { page.overview.addDiskLimitHeader.isDisplayed() }
        then:"check elements"
        page.overview.lnkAddNewFeature.isDisplayed()

        when:"Add SNAPSHOT feature"
        page.overview.lnkAddNewFeature.click()
        then:"check elements and add values"
        page.overview.featureName1.isDisplayed()
        page.overview.featureValue1.isDisplayed()
        page.overview.featureUnit1.isDisplayed()
        page.overview.featureName1.value("SNAPSHOTS")
        page.overview.featureValue1.value("130")
        page.overview.featureUnit1.value("%")

        when:"Save New Feature"
        page.overview.btnAddDiskLimitSave.isDisplayed()
        page.overview.btnAddDiskLimitSave.click()
        then: "Check for validation"
        page.overview.errorValue1.isDisplayed()
        page.overview.errorValue1.text().equals("Maximum value of percentage cannot be greater than 99.")
    }

    def verifyErrorMessageForFeatureInvalidDecimalValue() {
        when:"Open Advanced"
        page.advanced.click()
        then:"Check if DiskLimit is displayed"
        waitFor(10){page.overview.diskLimit.isDisplayed()}

        when:"Open Edit Disk Limit"
        waitFor(30) { page.overview.diskLimitEdit.isDisplayed() }
        page.overview.diskLimitEdit.click()
        waitFor(30) { page.overview.addDiskLimitHeader.isDisplayed() }
        then:"check elements"
        page.overview.lnkAddNewFeature.isDisplayed()

        when:"Add SNAPSHOT feature"
        page.overview.lnkAddNewFeature.click()
        then:"check elements and add values"
        page.overview.featureName1.isDisplayed()
        page.overview.featureValue1.isDisplayed()
        page.overview.featureUnit1.isDisplayed()
        page.overview.featureName1.value("SNAPSHOTS")
        page.overview.featureValue1.value("13.33")
        page.overview.featureUnit1.value("%")

        when:"Save New Feature"
        page.overview.btnAddDiskLimitSave.isDisplayed()
        page.overview.btnAddDiskLimitSave.click()
        then: "Check for validation"
        page.overview.errorValue1.isDisplayed()
        page.overview.errorValue1.text().equals("Please enter a positive number without any decimal.")
    }

    def verifyAddDiskLimitCancel() {
        when:"Open Advanced"
        page.advanced.click()
        then:"Check if DiskLimit is displayed"
        waitFor(10) { page.overview.diskLimit.isDisplayed() }

        when:"Open Edit Disk Limit"
        waitFor(30) { page.overview.diskLimitEdit.isDisplayed() }
        page.overview.diskLimitEdit.click()
        waitFor(30) { page.overview.addDiskLimitHeader.isDisplayed() }
        then:"check elements"
        page.overview.lnkAddNewFeature.isDisplayed()

        when:"Add SNAPSHOT feature"
        page.overview.lnkAddNewFeature.click()
        then:"check elements and add values"
        page.overview.featureName1.isDisplayed()
        page.overview.featureValue1.isDisplayed()
        page.overview.featureUnit1.isDisplayed()
        page.overview.featureName1.value("SNAPSHOTS")
        page.overview.featureValue1.value("13")
        page.overview.featureUnit1.value("%")

        when:"Save New Feature"
        page.overview.btnAddDiskLimitSave.isDisplayed()
        page.overview.btnAddDiskLimitSave.click()
        page.overview.btnSaveDiskLimitCancel.isDisplayed()
        page.overview.btnSaveDiskLimitCancel.click()
        then: "Cancelled"
        println("Confirmation popup has been cancelled")
    }

    def advancedExpandCheckText() {
        when:
        page.overview.advanced.click()
        then:
        waitFor(waitTime) {
            page.overview.maxJavaHeap.text().equals("Max Java Heap")
            !page.overview.maxJavaHeapValue.text().equals("")
        }
        waitFor(waitTime){
            page.overview.heartbeatTimeout.text().equals("Heartbeat Timeout")
            !page.overview.heartbeatTimeoutValue.text().equals("")
        }
        waitFor(waitTime){
            page.overview.queryTimeout.text().equals("Query Timeout")
            !page.overview.queryTimeoutValue.text().equals("")
        }
        waitFor(waitTime){
            page.overview.maxTempTableMemory.text().equals("Max Temp Table Memory")
            !page.overview.maxTempTableMemoryValue.text().equals("")
        }
        waitFor(waitTime){
            page.overview.snapshotPriority.text().equals("Snapshot Priority")
            !page.overview.snapshotPriorityValue.text().equals("")

        }
        waitFor(waitTime){
            page.overview.memoryLimitSize.text().equals("Memory Limit")
            !page.overview.memoryLimitSizeValue.text().equals("")

            if(page.overview.memoryLimitSizeValue.text() == "Not Enforced"){
                page.overview.memoryLimitSizeUnit.text().equals("")
            } else {
                !page.overview.memoryLimitSizeUnit.text().equals("")
            }
        }
        waitFor(waitTime){
            page.overview.diskLimit.text().equals("Disk Limit")
        }
    }

//    def cleanupSpec() {
//        int count = 0
//        if (!(page instanceof VoltDBManagementCenterPage)) {
//            when: 'Open VMC page'
//            ensureOnVoltDBManagementCenterPage()
//            then: 'to be on VMC page'
//            at VoltDBManagementCenterPage
//        }
//
//        page.loginIfNeeded()
//
//        when: 'click the Admin link (if needed)'
//        page.openAdminPage()
//        then: 'should be on Admin page'
//        at AdminPage
//
//        String initialPrefix    = "DEFAULT"
//        String initialFreq      = "10"
//        String initialFreqUnit  = "Hrs"
//        String initialRetained  = "10"
//
//        String initialHeartTimeout = "10"
//        String initialQueryTimeout = "10"
//
//        // heartbeat timeout revert
//
//        if (revertHeartTimeout==false) {
//            when:
//            page.advanced.click()
//            then:
//            waitFor(waitTime) { page.overview.heartTimeoutEdit.isDisplayed() }
//
//            when:
//            waitFor(waitTime) { page.overview.heartTimeoutEdit.click() }
//            then:
//            waitFor(waitTime) {
//                page.overview.heartTimeoutField.isDisplayed()
//                page.overview.heartTimeoutOk.isDisplayed()
//                page.overview.heartTimeoutCancel.isDisplayed()
//            }
//
//            when:
//            page.overview.heartTimeoutField.value(initialHeartTimeout)
//            waitFor(waitTime) {
//                page.overview.heartTimeoutOk.click()
//            }
//            then:
//            waitFor(waitTime) {
//                page.overview.heartTimeoutPopupOk.isDisplayed()
//                page.overview.heartTimeoutPopupCancel.isDisplayed()
//            }
//
//
//            waitFor(waitTime) {
//                try {
//                    page.overview.heartTimeoutPopupOk.click()
//                } catch (org.openqa.selenium.ElementNotVisibleException e) {
//                    println("retrying")
//                }
//
//                page.overview.heartTimeoutEdit.isDisplayed()
//                page.overview.heartTimeoutValue.text().equals(initialHeartTimeout)
//                !page.overview.heartTimeoutPopupOk.isDisplayed()
//                !page.overview.heartTimeoutPopupCancel.isDisplayed()
//            }
//        }
//
//        // query timeout revert
//
//        if (revertQueryTimeout==false) {
//            when:
//            page.advanced.click()
//            then:
//            waitFor(waitTime) { page.overview.queryTimeoutEdit.isDisplayed() }
//
//            when:
//            waitFor(waitTime) { page.overview.queryTimeoutEdit.click() }
//            then:
//            waitFor(waitTime) {
//                page.overview.queryTimeoutField.isDisplayed()
//                page.overview.queryTimeoutOk.isDisplayed()
//                page.overview.queryTimeoutCancel.isDisplayed()
//            }
//
//            when:
//            page.overview.queryTimeoutField.value(initialQueryTimeout)
//            waitFor(waitTime) {
//                page.overview.queryTimeoutOk.click()
//            }
//            then:
//            waitFor(waitTime) {
//                page.overview.queryTimeoutPopupOk.isDisplayed()
//                page.overview.queryTimeoutPopupCancel.isDisplayed()
//            }
//
//
//            waitFor(waitTime) {
//                try {
//                    page.overview.queryTimeoutPopupOk.click()
//                } catch (org.openqa.selenium.ElementNotVisibleException e) {
//                    println("retrying")
//                }
//
//                page.overview.queryTimeoutEdit.isDisplayed()
//                page.overview.queryTimeoutValue.text().equals(initialQueryTimeout)
//                !page.overview.queryTimeoutPopupOk.isDisplayed()
//                !page.overview.queryTimeoutPopupCancel.isDisplayed()
//            }
//        }
//
//        //memory limit revert
//        if(revertMemorySize == true){
//            when:
//            page.advanced.click()
//            then:
//            waitFor(waitTime) { page.overview.memoryLimitEdit.isDisplayed() }
//
//            when:
//            waitFor(waitTime) { page.overview.memoryLimitEdit.click() }
//            then:
//            waitFor(waitTime) {
//                page.overview.memoryLimitField.isDisplayed()
//                page.overview.memoryLimitOk.isDisplayed()
//                page.overview.memoryLimitCancel.isDisplayed()
//            }
//
//            when:
//            if(initialMemoryLimit == "Not Enforced") {
//                if(page.overview.memoryLimitDelete.isDisplayed()) {
//                    count = 0
//                    while(count<numberOfTrials) {
//                        count++
//                        try {
//                            page.overview.memoryLimitDelete.click()
//                            waitFor(waitTime) { page.overview.btnDelPopupMemoryLimitOk.isDisplayed() }
//                            break
//                        } catch(geb.waiting.WaitTimeoutException e) {
//                        }
//                    }
//
//                    count = 0
//                    while(count<numberOfTrials) {
//                        count++
//                        try {
//                            page.overview.btnDelPopupMemoryLimitOk.click()
//                            waitFor(waitTime) { page.overview.memoryLimitSizeValue.isDisplayed() }
//                            break
//                        } catch(geb.waiting.WaitTimeoutException e) {
//                        }
//                    }
//                }
//            }
//        }
//    }
}
