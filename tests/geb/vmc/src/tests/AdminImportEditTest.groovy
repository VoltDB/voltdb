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

import vmcTest.pages.*
import geb.Page.*

class AdminImportEditTest extends TestBase {

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
     *  This test creates a new import configuration, adds a property to it and deletes it
     */

    def verifyAddEditAndDeleteConfigurationProperty() {
        when: 'Open Add Import Configuration Popup'
        page.overview.openAddImportConfigurationPopup()
        page.overview.textImportType.value("KAFKA")
        then: 'Check elements'
        page.overview.addImportProperty.isDisplayed()
        page.overview.saveImport.isDisplayed()
        page.overview.cancelImport.isDisplayed()
        page.overview.txtTopics.value().equals("topics")
        page.overview.txtProcedure.value().equals("procedure")
        page.overview.txtBrokers.value().equals("brokers")
        page.overview.txtTopicsValue.isDisplayed()
        page.overview.txtProcedureValue.isDisplayed()
        page.overview.txtBrokersValue.isDisplayed()

        when: 'Provide values for import configuration'
        page.overview.txtImportFormat.value("csv")
        page.overview.txtTopicsValue.value("topicValue")
        page.overview.txtProcedureValue.value("procedureValue")
        page.overview.txtBrokersValue.value("brokersValue")
        int count = 0
        while (count < numberOfTrials) {
            count++
            try {
                page.overview.saveImport.click()
                waitFor(waitTime) { !page.overview.saveImport.isDisplayed() }
                break
            } catch (geb.error.RequiredPageContentNotPresent e) {

            } catch (org.openqa.selenium.StaleElementReferenceException e) {
            } catch (geb.waiting.WaitTimeoutException e) {
            }
        }
        count = 0
        while (count < numberOfTrials) {
            count++
            try {
                page.overview.btnSaveImportConfigOk.click()
                waitFor(waitTime) { !page.overview.btnSaveImportConfigOk.isDisplayed() }
                break
            } catch (geb.error.RequiredPageContentNotPresent e) {
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
            } catch (geb.waiting.WaitTimeoutException e) {
            }
        }
        and: 'Expand import'
        waitFor(waitTime) { page.overview.importConfig.isDisplayed() }
        page.overview.importConfig.click()
        waitFor(waitTime) { page.overview.importExpanded.isDisplayed() }
        then: 'Display the created KAFKA'
        waitFor(waitTime) { page.overview.KafkaImportName.isDisplayed() }
        println("Import configuration created")

        when: 'Edit button is displayed'
        waitFor(waitTime) { page.overview.editImportConfiguration.isDisplayed() }
        then: 'Click edit button'
        page.overview.editImportConfiguration.click()

        when: 'Add property is clicked'
        waitFor(waitTime) { page.overview.addImportProperty.click() }
        then: 'New property and value fields are displayed'
        page.overview.newImportTextField.isDisplayed()
        page.overview.newImportValueField.isDisplayed()
        page.overview.deleteFirstImportProperty.isDisplayed()

        when: 'Provide values for property and value fields'
        page.overview.newImportTextField.value("value1")
        page.overview.newImportValueField.value("value2")
        and: 'Save the values'
        count = 0
        while (count < numberOfTrials) {
            count++
            try {
                waitFor(waitTime) { page.overview.saveImport.click() }
                waitFor(waitTime) { page.overview.btnSaveImportConfigOk.isDisplayed() }
                break
            } catch (geb.error.RequiredPageContentNotPresent e) {
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
            } catch (geb.waiting.WaitTimeoutException e) {
            }
        }
        count = 0
        while (count < numberOfTrials) {
            count++
            try {
                waitFor(waitTime) { page.overview.btnSaveImportConfigOk.click() }
                waitFor(waitTime) { !page.overview.btnSaveImportConfigOk.isDisplayed() }
                break
            } catch (geb.error.RequiredPageContentNotPresent e) {
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
            } catch (geb.waiting.WaitTimeoutException e) {
            }
        }
        then: 'Print Added Value'
        println("Added new property to import configuration.")

        // Delete the configuration
        when: 'Expand import'
        waitFor(waitTime) { page.overview.importConfig.isDisplayed() }
        page.overview.expandImport()
        then: 'Display the KAFKA'
        waitFor(waitTime) { page.overview.KafkaImportName.isDisplayed() }

        when: 'Edit button is displayed'
        waitFor(waitTime) { page.overview.editImportConfiguration.isDisplayed() }
        then: 'Click edit button'
        page.overview.editImportConfiguration.click()

        when: 'Delete Configuration is displayed'
        page.overview.deleteConfiguration.isDisplayed()
        count = 0
        while (count < numberOfTrials) {
            count++
            try {
                page.overview.deleteImportConfiguration.click()
                waitFor(waitTime) { !page.overview.deleteImportConfiguration.isDisplayed() }
                break
            } catch (geb.error.RequiredPageContentNotPresent e) {
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
            } catch (geb.waiting.WaitTimeoutException e) {
            }
        }
        count = 0
        while (count < numberOfTrials) {
            count++
            try {
                page.overview.btnSaveImportConfigOk.click()
                waitFor(waitTime) { !page.overview.btnSaveImportConfigOk.isDisplayed() }
                break
            } catch (geb.error.RequiredPageContentNotPresent e) {
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
            } catch (geb.waiting.WaitTimeoutException e) {
            }
        }
        then: 'Print Deleted'
        println("Deleted Import Configuration")
    }

    /*
     *  This test creates a new import configuration, edit its type,
     *  provide values of new properties(related to new type) and deletes it
     */
    def verifyAddEditAndDeleteConfigurations() {
        when: 'Open Add Import Configuration Popup'
        page.overview.openAddImportConfigurationPopup()
        page.overview.textImportType.value("KAFKA")
        then: 'Check elements'
        page.overview.addImportProperty.isDisplayed()
        page.overview.saveImport.isDisplayed()
        page.overview.cancelImport.isDisplayed()
        page.overview.txtTopics.value().equals("topics")
        page.overview.txtProcedure.value().equals("procedure")
        page.overview.txtBrokers.value().equals("brokers")
        page.overview.txtTopicsValue.isDisplayed()
        page.overview.txtProcedureValue.isDisplayed()
        page.overview.txtBrokersValue.isDisplayed()

        when: 'Provide values for import configuration'
        page.overview.txtImportFormat.value("csv")
        page.overview.txtTopicsValue.value("topicValue")
        page.overview.txtProcedureValue.value("procedureValue")
        page.overview.txtBrokersValue.value("brokersValue")
        int count = 0
        while (count < numberOfTrials) {
            count++
            try {
                page.overview.saveImport.click()
                waitFor(waitTime) { !page.overview.saveImport.isDisplayed() }
                break
            } catch (geb.error.RequiredPageContentNotPresent e) {

            } catch (org.openqa.selenium.StaleElementReferenceException e) {
            } catch (geb.waiting.WaitTimeoutException e) {
            }
        }
        count = 0
        while (count < numberOfTrials) {
            count++
            try {
                page.overview.btnSaveImportConfigOk.click()
                waitFor(waitTime) { !page.overview.btnSaveImportConfigOk.isDisplayed() }
                break
            } catch (geb.error.RequiredPageContentNotPresent e) {
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
            } catch (geb.waiting.WaitTimeoutException e) {
            }
        }
        and: 'Expand import'
        waitFor(waitTime) { page.overview.importConfig.isDisplayed() }
        page.overview.importConfig.click()
        waitFor(waitTime) { page.overview.importExpanded.isDisplayed() }

        then: 'Display the created KAFKA'
        waitFor(waitTime) { page.overview.KafkaImportName.isDisplayed() }
        println("Configuration created")

        when: 'Edit button is displayed'
        waitFor(waitTime) { page.overview.editImportConfiguration.isDisplayed() }
        then: 'Click edit button'
        page.overview.editImportConfiguration.click()

        when: 'Change the type'
        waitFor(waitTime) {
            page.overview.textImportType.isDisplayed()
            page.overview.textImportType.click()
            page.overview.textImportType.value("KINESIS")
            }
        then: 'Check the new fields'
        waitFor(waitTime) {
            page.overview.addImportProperty.isDisplayed()
            page.overview.saveImport.isDisplayed()
            page.overview.cancelImport.isDisplayed()
            page.overview.txtRegion.isDisplayed()
            page.overview.txtSecretKey.isDisplayed()
            page.overview.txtAccessKey.isDisplayed()
            page.overview.txtStreamName.isDisplayed()
            page.overview.txtProcedureKi.isDisplayed()
            page.overview.txtAppName.isDisplayed()
            page.overview.txtRegionValue.isDisplayed()
            page.overview.txtSecretKeyValue.isDisplayed()
            page.overview.txtAccessKeyValue.isDisplayed()
            page.overview.txtStreamNameValue.isDisplayed()
            page.overview.txtProcedureKiValue.isDisplayed()
            page.overview.txtAppNameValue.isDisplayed()
        }

        when: 'Provide values for text and value fields'
        page.overview.txtRegionValue.value("regionValue")
        page.overview.txtSecretKeyValue.value("secretKeyValue")
        page.overview.txtAccessKeyValue.value("accessKeyValue")
        page.overview.txtStreamNameValue.value("streamNameValue")
        page.overview.txtProcedureKiValue.value("procedureValue")
        page.overview.txtAppNameValue.value("appNameValue")
        and: 'Save the values'
        count = 0
        while (count < numberOfTrials) {
            count++
            try {
                page.overview.saveImport.click()
                waitFor(waitTime) { !page.overview.saveImport.isDisplayed() }
                break
            } catch (geb.error.RequiredPageContentNotPresent e) {

            } catch (org.openqa.selenium.StaleElementReferenceException e) {
            } catch (geb.waiting.WaitTimeoutException e) {
            }
        }
        count = 0
        while (count < numberOfTrials) {
            count++
            try {
                page.overview.btnSaveImportConfigOk.click()
                waitFor(waitTime) { !page.overview.btnSaveImportConfigOk.isDisplayed() }
                break
            } catch (geb.error.RequiredPageContentNotPresent e) {
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
            } catch (geb.waiting.WaitTimeoutException e) {
            }
        }
        then: 'Print Added Value'
        println("Configuration Edited")

        // Delete the configuration
        when: 'Expand import'
        waitFor(waitTime) { page.overview.importConfig.isDisplayed() }
        page.overview.expandImport()
        then: 'Display the Kinesis'
        waitFor(waitTime) { page.overview.KinesisImportName.isDisplayed() }

        when: 'Edit button is displayed'
        waitFor(waitTime) { page.overview.editImportConfiguration.isDisplayed() }
        then: 'Click edit button'
        page.overview.editImportConfiguration.click()

        when: 'Delete Configuration is displayed'
        page.overview.deleteConfiguration.isDisplayed()
        count = 0
        while (count < numberOfTrials) {
            count++
            try {
                page.overview.deleteImportConfiguration.click()
                waitFor(waitTime) { !page.overview.deleteImportConfiguration.isDisplayed() }
                break
            } catch (geb.error.RequiredPageContentNotPresent e) {
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
            } catch (geb.waiting.WaitTimeoutException e) {
            }
        }
        count = 0
        while (count < numberOfTrials) {
            count++
            try {
                page.overview.btnSaveImportConfigOk.click()
                waitFor(waitTime) { !page.overview.btnSaveImportConfigOk.isDisplayed() }
                break
            } catch (geb.error.RequiredPageContentNotPresent e) {
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
            } catch (geb.waiting.WaitTimeoutException e) {
            }
        }
        then: 'Print Deleted'
        println("Deleted Configuration")
    }
}
