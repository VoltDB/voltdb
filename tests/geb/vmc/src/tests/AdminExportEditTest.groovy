/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */package vmcTest.tests

import org.junit.Test
import vmcTest.pages.*
import geb.Page.*


class AdminExportEditTest extends TestBase {

    def setup() { // called before each test
        int count = 0

        while(count<numberOfTrials) {
            count ++
            try {
                setup: 'Open VMC page'
                to VoltDBManagementCenterPage
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

    /*
     *  This test creates a new configuration, adds a property to it and deletes it
     */

    def "Verify 'Add configuration' in Export and delete it"() {
        println("Test Start: Verify 'Add configuration' in Export and delete it")
        int count = 0
        testStatus = false

        // Create Add Configuration
        when: 'Open Add ConfigurationPopup'
        page.overview.openAddConfigurationPopup()
        page.overview.textType.value("KAFKA")
        then: 'Check elements'
        page.overview.addProperty.isDisplayed()
        page.overview.save.isDisplayed()
        page.overview.cancel.isDisplayed()
        page.overview.metadatabroker.value().equals("metadata.broker.list")
        page.overview.metadatabroker.isDisplayed()

        when: 'Provide values for add configuration'
        page.overview.stream.value("kafkaTest")
        page.overview.metadatabrokerValue.value("metadataValue")
        and: 'Save the values'
        count = 0
        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) { page.overview.save.click() }
                waitFor(waitTime) { page.overview.confirmyesbtn.isDisplayed() }
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        count = 0
        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) { page.overview.confirmyesbtn.click() }
                waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        then: 'Print saved'
        println("Saved")

        // Add property in the existing Configuration
        when: 'Expand export'
        count = 0
        while(count<numberOfTrials) {
            count++
            try {
                export.click()
                waitFor(waitTime) { page.overview.exportExpanded.isDisplayed() }
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        then: 'Display the created KAFKA'
        waitFor(waitTime) { page.overview.kafkaName.isDisplayed() }
        println("Configuration created")

        when: 'Edit button is displayed'
        waitFor(waitTime){ page.overview.editExportConfiguration.isDisplayed() }
        then: 'Click edit button'
        page.overview.editExportConfiguration.click()

        when: 'Add property is clicked'
        waitFor(waitTime) { page.overview.addProperty.click() }
        then: 'New text and value fields are displayed'
        page.overview.newTextField.isDisplayed()
        page.overview.newValueField.isDisplayed()
        page.overview.deleteFirstProperty.isDisplayed()

        when: 'Provide values for text and value fields'
        page.overview.newTextField.value("value1")
        page.overview.newValueField.value("value2")
        and: 'Save the values'
        count = 0
        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) { page.overview.save.click() }
                waitFor(waitTime) { page.overview.confirmyesbtn.isDisplayed() }
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        count = 0
        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) { page.overview.confirmyesbtn.click() }
                waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        then: 'Print Added Value'
        println("Added Value")

        // Delete the configuration
        when: 'Expand export'
        page.overview.export.isDisplayed()
        page.overview.expandExport()
        then: 'Display the KAFKA'
        waitFor(waitTime) { page.overview.kafkaName.isDisplayed() }

        when: 'Edit button is displayed'
        waitFor(waitTime) { page.overview.editExportConfiguration.isDisplayed() }
        and: 'Click edit button'
        page.overview.editExportConfiguration.click()
        then: 'Delete is displayed'
        waitFor(waitTime) { page.overview.deleteConfiguration.isDisplayed() }

        when: 'Delete is clicked'
        count = 0
        while(count<numberOfTrials) {
            count++
            try {
                page.overview.deleteConfiguration.click()
                waitFor(waitTime) { page.overview.confirmyesbtn.isDisplayed() }
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        and: 'Confirm delete'
        count = 0
        while(count<numberOfTrials) {
            count++
            try {
                page.overview.confirmyesbtn.click()
                waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        then:
        println("Configuration Deleted")

        println("Test End: Verify 'Add configuration' in Export and delete it")
    }

    /*
     *  This test creates a new configuration, edits it and deletes it
     */
    def "Verify edit in Export and delete it"() {
        println("Test Start: Verify edit in Export and delete it")
        int count = 0
        testStatus = false

        // Create Add Configuration
        when: 'Open Add ConfigurationPopup'
        page.overview.openAddConfigurationPopup()
        page.overview.textType.value("KAFKA")
        then: 'Check elements'
        page.overview.addProperty.isDisplayed()
        page.overview.save.isDisplayed()
        page.overview.cancel.isDisplayed()
        page.overview.metadatabroker.value().equals("metadata.broker.list")
        page.overview.metadatabroker.isDisplayed()

        when: 'Provide values for add configuration'
        page.overview.stream.value("kafkaTest")
        page.overview.metadatabrokerValue.value("metadataValue")
        and: 'Save the values'
        count = 0
        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) { page.overview.save.click() }
                waitFor(waitTime) { page.overview.confirmyesbtn.isDisplayed() }
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        count = 0
        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) { page.overview.confirmyesbtn.click() }
                waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        then: 'Print saved'
        println("Saved")

        // Add property in the existing Configuration
        when: 'Expand export'
        page.overview.export.isDisplayed()
        page.overview.expandExport()
        then: 'Display the created KAFKA'
        waitFor(waitTime) { page.overview.kafkaName.isDisplayed() }
        println("Configuration created")

        when: 'Edit button is displayed'
        waitFor(waitTime){ page.overview.editExportConfiguration.isDisplayed() }
        then: 'Click edit button'
        page.overview.editExportConfiguration.click()

        when: 'Change the type'
        page.overview.textType.click()
        page.overview.textType.value("ELASTICSEARCH")
        then: 'Check the new fields'
        waitFor(waitTime) {
            page.overview.endpointES.isDisplayed()
            page.overview.endpointESValue.isDisplayed()
        }

        when: 'Provide values for text and value fields'
        page.overview.endpointESValue.value("endpointESValue")
        and: 'Save the values'
        count = 0
        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) { page.overview.save.click() }
                waitFor(waitTime) { page.overview.confirmyesbtn.isDisplayed() }
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        count = 0
        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) { page.overview.confirmyesbtn.click() }
                waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        then: 'Print Added Value'
        println("Configuration Edited")

        // Delete the configuration
        when: 'Expand export'
        count = 0
        while(count<numberOfTrials) {
            count++
            try {
                export.click()
                waitFor(waitTime) { page.overview.exportExpanded.isDisplayed() }
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        then: 'Display the KAFKA'
        waitFor(waitTime) { page.overview.kafkaName.isDisplayed() }

        when: 'Edit button is displayed'
        waitFor(waitTime) { page.overview.editExportConfiguration.isDisplayed() }
        and: 'Click edit button'
        page.overview.editExportConfiguration.click()
        then: 'Delete is displayed'
        waitFor(waitTime) { page.overview.deleteConfiguration.isDisplayed() }

        when: 'Delete is clicked'
        count = 0
        while(count<numberOfTrials) {
            count++
            try {
                page.overview.deleteConfiguration.click()
                waitFor(waitTime) { page.overview.confirmyesbtn.isDisplayed() }
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        and: 'Confirm delete'
        count = 0
        while(count<numberOfTrials) {
            count++
            try {
                page.overview.confirmyesbtn.click()
                waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        then:
        println("Configuration Deleted")

        println("Test End: Verify edit in Export and delete it")
    }
}
