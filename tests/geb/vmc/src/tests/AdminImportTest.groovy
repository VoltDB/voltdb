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

import org.junit.Test
import vmcTest.pages.*
import geb.Page.*


class AdminImportTest extends TestBase {

    def setup() { // called before each test
        int count = 0

        while (count < numberOfTrials) {
            count++
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

    def checkImportClickAndCheckItsValue() {
        when:
        waitFor(waitTime) { page.importbtn.isDisplayed() }
        page.importbtn.click()
        then:
        waitFor(waitTime) { page.noimportconfigtxt.isDisplayed() }
        if (page.noconfigtxt.text() == "No configuration available.") {
            println("Currently, No configurations are available in import")
        } else {
            println("Early presence of Configuration settings detected!")
        }
        page.importbtn.click()
    }

    def verifyErrorMessagesOfAddImportConfigurationForKafka() {
        when:
        if (waitFor(10){page.overview.addImportConfig.isDisplayed()}) {
            when: 'Open Add Import Configuration Popup'
            page.overview.openAddImportConfigurationPopup()
            page.overview.textImportType.value("KAFKA")
            then: 'Check elements'
            waitFor(waitTime) {
                page.overview.addImportProperty.isDisplayed()
                page.overview.saveImport.isDisplayed()
                page.overview.cancelImport.isDisplayed()
                page.overview.txtTopics.value().equals("topics")
                page.overview.txtProcedure.value().equals("procedure")
                page.overview.txtBrokers.value().equals("brokers")
                page.overview.txtTopicsValue.isDisplayed()
                page.overview.txtProcedureValue.isDisplayed()
                page.overview.txtBrokersValue.isDisplayed()
            }

            when: 'Save button is clicked'
            page.overview.saveImport.click()
            then: 'Error messages are displayed'
            waitFor(waitTime) {
                page.overview.errorFormat.isDisplayed()
                page.overview.errorTopicValue.isDisplayed()
                page.overview.errorProcedureValue.isDisplayed()
                page.overview.errorBrokersValue.isDisplayed()
            }
        }
        then:
        println("passed")
    }

    def verifyErrorMessagesOfAddImportConfigurationForKinesis() {
        when:
        if (waitFor(10){page.overview.addImportConfig.isDisplayed()}) {
            when: 'Open Add Import Configuration Popup'
            page.overview.openAddImportConfigurationPopup()
            page.overview.textImportType.value("KINESIS")
            then: 'Check elements'
            waitFor(waitTime) {
                page.overview.addImportProperty.isDisplayed()
                page.overview.saveImport.isDisplayed()
                page.overview.cancelImport.isDisplayed()
                page.overview.txtRegion.value().equals("region")
                page.overview.txtSecretKey.value().equals("secret.key")
                page.overview.txtAccessKey.value().equals("access.key")
                page.overview.txtStreamName.value().equals("stream.name")
                page.overview.txtProcedureKi.value().equals("procedure")
                page.overview.txtAppName.value().equals("app.name")
                page.overview.txtRegionValue.isDisplayed()
                page.overview.txtSecretKeyValue.isDisplayed()
                page.overview.txtAccessKeyValue.isDisplayed()
                page.overview.txtStreamNameValue.isDisplayed()
                page.overview.txtProcedureKiValue.isDisplayed()
                page.overview.txtAppNameValue.isDisplayed()
            }

            when: 'Save button is clicked'
            page.overview.saveImport.click()
            then: 'Error messages are displayed'
            waitFor(waitTime) {
                page.overview.errorFormat.isDisplayed()
                page.overview.errorRegionValue.isDisplayed()
                page.overview.errorSecretKeyValue.isDisplayed()
                page.overview.errorAccessKeyValue.isDisplayed()
                page.overview.errorStreamNameValue.isDisplayed()
                page.overview.errorProcedureKiValue.isDisplayed()
                page.overview.errorAppNameValue.isDisplayed()
            }
        }
        then:
        println("passed")
    }

    def verifyAddConfigurationForImportKafkaCreated() {
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

        when: 'Provide values for add import configuration'
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

        when: 'Delete Configuration is displayed'
        page.overview.deleteImportConfiguration.isDisplayed()
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

    def verifyAddConfigurationForImportKinesisCreated() {
        when: 'Open Add Import Configuration Popup'
        page.overview.openAddImportConfigurationPopup()
        page.overview.textImportType.value("KINESIS")
        then: 'Check elements'
        page.overview.addImportProperty.isDisplayed()
        page.overview.saveImport.isDisplayed()
        page.overview.cancelImport.isDisplayed()
        page.overview.txtRegion.value().equals("region")
        page.overview.txtSecretKey.value().equals("secret.key")
        page.overview.txtAccessKey.value().equals("access.key")
        page.overview.txtStreamName.value().equals("stream.name")
        page.overview.txtProcedureKi.value().equals("procedure")
        page.overview.txtAppName.value().equals("app.name")
        page.overview.txtRegionValue.isDisplayed()
        page.overview.txtSecretKeyValue.isDisplayed()
        page.overview.txtAccessKeyValue.isDisplayed()
        page.overview.txtStreamNameValue.isDisplayed()
        page.overview.txtProcedureKiValue.isDisplayed()
        page.overview.txtAppNameValue.isDisplayed()

        when: 'Provide values for add import configuration'
        page.overview.txtImportFormat.value("csv")
        page.overview.txtRegionValue.value("regionValue")
        page.overview.txtSecretKeyValue.value("secretKeyValue")
        page.overview.txtAccessKeyValue.value("accessKeyValue")
        page.overview.txtStreamNameValue.value("streamNameValue")
        page.overview.txtProcedureKiValue.value("procedureValue")
        page.overview.txtAppNameValue.value("appNameValue")
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
        then: 'Display the created Kinesis'
        waitFor(waitTime) { page.overview.KinesisImportName.isDisplayed() }
        println("Configuration created")

        when: 'Edit button is displayed'
        waitFor(waitTime) { page.overview.editImportConfiguration.isDisplayed() }
        then: 'Click edit button'
        page.overview.editImportConfiguration.click()

        when: 'Delete Configuration is displayed'
        page.overview.deleteImportConfiguration.isDisplayed()
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

    def verifyAddPropertyInImportConfiguration(){
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

        when: 'Add property is clicked'
        page.overview.addImportProperty.click()
        then: 'New text and value fields are displayed'
        page.overview.newImportTextField.isDisplayed()
        page.overview.newImportValueField.isDisplayed()
        page.overview.deleteFirstImportProperty.isDisplayed()
        when: 'Provide values for add configuration'
        page.overview.txtImportFormat.value("csv")
        page.overview.txtTopicsValue.value("topicValue")
        page.overview.txtProcedureValue.value("procedureValue")
        page.overview.txtBrokersValue.value("brokersValue")
        page.overview.newImportTextField.value("value1")
        page.overview.newImportValueField.value("value2")
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
        report 'OpenAddConfig'
        waitFor(waitTime){page.overview.importConfig.click()}
        waitFor(waitTime) { page.overview.importExpanded.isDisplayed() }
        then: 'Display the created KAFKA'
        waitFor(waitTime) { page.overview.KafkaImportName.isDisplayed() }
        println("Configuration created")

        when: 'Edit button is displayed'
        waitFor(waitTime) { page.overview.editImportConfiguration.isDisplayed() }
        then: 'Click edit button'
        page.overview.editImportConfiguration.click()

        when: 'Delete Configuration is displayed'
        page.overview.deleteImportConfiguration.isDisplayed()
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

    def verifyErrorMessagesForAddProperty() {
        when: 'Open Add Import Configuration Popup'
        page.overview.openAddImportConfigurationPopup()
        waitFor(waitTime) {
            page.overview.textImportType.isDisplayed()
            page.overview.textImportType.value("KAFKA")
        }
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

        when: 'Add property is clicked'
        page.overview.addImportProperty.click()
        then: 'New text and value fields are displayed'
        page.overview.newImportTextField.isDisplayed()
        page.overview.newImportValueField.isDisplayed()
        page.overview.deleteFirstImportProperty.isDisplayed()

        when: 'Save button is clicked'
        page.overview.saveImport.click()
        then: 'Error messages are displayed'
        page.overview.errorImportName1.isDisplayed()
        page.overview.errorImportValue1.isDisplayed()
    }
}
