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


class AdminExportTest extends TestBase {
    int insideCount = 0
    boolean loopStatus = false

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

    def checkExportClickAndCheckItsValue() {
        when:
        waitFor(waitTime) { page.exportbtn.isDisplayed() }
        page.exportbtn.click()
        then:
        waitFor(waitTime) { page.noconfigtxt.isDisplayed() }
        if(page.noconfigtxt.text()=="No configuration available.") {
            println("Currently, No configurations are available in export")
        } else {
            println("Early presence of Configuration settings detected!")
        }
        page.exportbtn.click()
    }

    def verifyErrormessagesofAddConfigurationforFILE() {
        when:
        if (page.overview.addconfig.isDisplayed()) {
            when: 'Open Add ConfigurationPopup'
            page.overview.openAddConfigurationPopup()
            page.overview.textType.value("FILE")
            then: 'Check elements'
            waitFor(waitTime) {
                page.overview.addProperty.isDisplayed()
                page.overview.save.isDisplayed()
                page.overview.cancel.isDisplayed()
                page.overview.type.value().equals("type")
                page.overview.nonce.value().equals("nonce")
                page.overview.outdir.value().equals("outdir")
                page.overview.typeValue.isDisplayed()
                page.overview.nonceValue.isDisplayed()
                page.overview.outdirValue.isDisplayed()
            }

            when: 'Save button is clicked'
            page.overview.save.click()
            then: 'Error messages are displayed'
            waitFor(waitTime) {
                page.overview.errorStream.isDisplayed()
                page.overview.errorFileTypeValue.isDisplayed()
                page.overview.errornonceValue.isDisplayed()
                page.overview.errorOutdirValue.isDisplayed()
            }
        }
        else{
            println("Add Export is not supported in Community Edition")
        }
        then:
            println("passed")
    }

    def verifyErrorMessagesofAddConfigurationforJDBC() {
        when:
        if (page.overview.addconfig.isDisplayed()) {
            when: 'Open Add ConfigurationPopup'
            page.overview.openAddConfigurationPopup()
            page.overview.textType.value("JDBC")
            then: 'Check elements'
            waitFor(waitTime) {
                page.overview.addProperty.isDisplayed()
                page.overview.save.isDisplayed()
                page.overview.cancel.isDisplayed()
                page.overview.jdbcdriver.value().equals("jdbcdriver")
                page.overview.jdbcurl.value().equals("jdbcurl")
                page.overview.jdbcdriverValue.isDisplayed()
                page.overview.jdbcurlValue.isDisplayed()
            }

            when: 'Save button is clicked'
            page.overview.save.click()
            then: 'Error messages are displayed'
            waitFor(waitTime) {
                page.overview.errorStream.isDisplayed()
                page.overview.errorJdbcDriverValue.isDisplayed()
                page.overview.errorJdbcUrlValue.isDisplayed()
            }
        }
        else{
            println("Add Export is not supported in Community Edition")
        }
        then:
        println("passed")
    }

    def verifyErrormessagesofAddConfigurationforKAFKA() {
        when:
        if (page.overview.addconfig.isDisplayed()) {
            when: 'Open Add ConfigurationPopup'
            page.overview.openAddConfigurationPopup()
            page.overview.textType.value("KAFKA")
            then: 'Check elements'
            waitFor(waitTime) {
                page.overview.addProperty.isDisplayed()
                page.overview.save.isDisplayed()
                page.overview.cancel.isDisplayed()
                page.overview.metadatabroker.value().equals("metadata.broker.list")
                page.overview.metadatabrokerValue.isDisplayed()
            }

            when: 'Save button is clicked'
            page.overview.save.click()
            then: 'Error messages are displayed'
            waitFor(waitTime) {
                page.overview.errorStream.isDisplayed()
                page.overview.errorMetadataBrokerListValue.isDisplayed()
            }
        }
        else{
            println("Add Export is not supported in Community Edition")
        }
        then:
        println("passed")
    }


    def verifyErrormessagesofAddConfigurationforHTTP() {
        when:
        if (page.overview.addconfig.isDisplayed()) {
            when: 'Open Add ConfigurationPopup'
            page.overview.openAddConfigurationPopup()
            page.overview.textType.value("HTTP")
            then: 'Check elements'
            waitFor(waitTime) {
                page.overview.addProperty.isDisplayed()
                page.overview.save.isDisplayed()
                page.overview.cancel.isDisplayed()
                page.overview.endpoint.value().equals("endpoint")
                page.overview.endpointValue.isDisplayed()
            }

            when: 'Save button is clicked'
            page.overview.save.click()
            then: 'Error messages are displayed'
            waitFor(waitTime) {
                page.overview.errorStream.isDisplayed()
                page.overview.errorEndpointValue.isDisplayed()
            }
        }
        else{
            println("Add Export is not supported in Community Edition")
        }
        then:
        println("passed")
    }

    def verifyErrormessagesofAddConfigurationforRABBITMQ() {
        when:
        if (page.overview.addconfig.isDisplayed()) {
            when: 'Open Add ConfigurationPopup'
            page.overview.openAddConfigurationPopup()
            page.overview.textType.value("RABBITMQ")
            then: 'Check elements'
            waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
            waitFor(waitTime) { page.overview.save.isDisplayed() }
            waitFor(waitTime) { page.overview.cancel.isDisplayed() }
            waitFor(waitTime) { page.overview.rabbitMq.value().equals("broker.host") }
            waitFor(waitTime) { page.overview.rabbitMqValue.isDisplayed() }

            when: 'Save button is clicked'
            page.overview.save.click()
            then: 'Error messages are displayed'
            waitFor(waitTime) { page.overview.errorStream.isDisplayed() }
            waitFor(waitTime) { page.overview.errorRabbitMqValue.isDisplayed() }
        }
        else{
            println("Add Export is not supported in Community Edition")
        }
        then:
        println("passed")
    }

    def verifyErrormessagesofAddConfigurationforRABBITMQamqpuri() {
        when:
        if (page.overview.addconfig.isDisplayed()) {
            when: 'Open Add ConfigurationPopup'
            page.overview.openAddConfigurationPopup()
            page.overview.textType.value("RABBITMQ")
            waitFor(waitTime) { page.overview.rabbitMq.isDisplayed() }
            page.overview.rabbitMq.click()
            page.overview.rabbitMq.value("amqp.uri")
            then: 'Check elements'
            waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
            waitFor(waitTime) { page.overview.save.isDisplayed() }
            waitFor(waitTime) { page.overview.cancel.isDisplayed() }
            waitFor(waitTime) { page.overview.rabbitMq.value().equals("amqp.uri") }
            waitFor(waitTime) { page.overview.rabbitMqValue.isDisplayed() }

            when: 'Save button is clicked'
            page.overview.save.click()
            then: 'Error messages are displayed'
            waitFor(waitTime) { page.overview.errorStream.isDisplayed() }
            waitFor(waitTime) { page.overview.errorRabbitMqValue.isDisplayed() }
        }
        else{
            println("Add Export is not supported in Community Edition")
        }
        then:
        println("passed")
    }

    def verifyErrormessagesofAddConfigurationforCUSTOM() {
        when:
        if (page.overview.addconfig.isDisplayed()) {
            when: 'Open Add ConfigurationPopup'
            page.overview.openAddConfigurationPopup()
            page.overview.textType.value("CUSTOM")
            then: 'Check elements'
            waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
            waitFor(waitTime) { page.overview.save.isDisplayed() }
            waitFor(waitTime) { page.overview.cancel.isDisplayed() }
            waitFor(waitTime) { page.overview.exportConnectorClass.isDisplayed() }

            when: 'Save button is clicked'
            page.overview.save.click()
            then: 'Error messages are displayed'
            waitFor(waitTime) { page.overview.errorStream.isDisplayed() }
            waitFor(waitTime) { page.overview.errorExportConnectorClass.isDisplayed() }
        }
        else{
            println("Add Export is not supported in Community Edition")
        }
        then:
        println("passed")
    }

    def verifyErrormessagesofAddConfigurationforELASTICSEARCH() {
        when:
        if (page.overview.addconfig.isDisplayed()) {
            when: 'Open Add ConfigurationPopup'
            page.overview.openAddConfigurationPopup()
            page.overview.textType.value("ELASTICSEARCH")
            then: 'Check elements'
            waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
            waitFor(waitTime) { page.overview.save.isDisplayed() }
            waitFor(waitTime) { page.overview.cancel.isDisplayed() }
            waitFor(waitTime) { page.overview.endpointES.value().equals("endpoint") }
            waitFor(waitTime) { page.overview.endpointESValue.isDisplayed() }

            when: 'Save button is clicked'
            page.overview.save.click()
            then: 'Error messages are displayed'
            waitFor(waitTime) { page.overview.errorStream.isDisplayed() }
            waitFor(waitTime) { page.overview.errorEndpointESValue.isDisplayed() }
        }
        else{
            println("Add Export is not supported in Community Edition")
        }
        then:
        println("passed")
    }


    def verifyAddConfigurationforFilecreated() {
        // String fileTestName     = page.overview.getFileTestName()
        //String fileValueOne     = page.overview.getFileValueOne()
        //String fileValueTwo     = page.overview.getFileValueTwo()
        //String fileValueThree   = page.overview.getFileValueThree()
        when:
        if (page.overview.addconfig.isDisplayed()) {
            when: 'Open Add ConfigurationPopup'
            page.overview.openAddConfigurationPopup()
            page.overview.textType.value("FILE")
            then: 'Check elements'
            page.overview.addProperty.isDisplayed()
            page.overview.save.isDisplayed()
            page.overview.cancel.isDisplayed()
            page.overview.type.value().equals("type")
            page.overview.nonce.value().equals("nonce")
            page.overview.outdir.value().equals("outdir")
            page.overview.typeValue.isDisplayed()
            page.overview.nonceValue.isDisplayed()
            page.overview.outdirValue.isDisplayed()

            when: 'Provide values for add configuration'
            page.overview.stream.value("fileTest")
            page.overview.typeValue.value("test")
            page.overview.nonceValue.value("test")
            page.overview.outdirValue.value("test")
            int count = 0
            boolean isPro = false
            while (count < numberOfTrials) {
                count++
                try {
                    page.overview.save.click()
                    waitFor(waitTime) { !page.overview.save.isDisplayed() }
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
                    page.overview.confirmyesbtn.click()
                    waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
                    break
                } catch (geb.error.RequiredPageContentNotPresent e) {
                } catch (org.openqa.selenium.StaleElementReferenceException e) {
                } catch (geb.waiting.WaitTimeoutException e) {
                }

            }

            and: 'Expand export'
            if (!waitFor(10) { updateInnerErrorPopup.isDisplayed() }) {
                isPro = True
                while (count < numberOfTrials) {
                    count++
                    try {
                        export.click()
                        waitFor(waitTime) { page.overview.exportExpanded.isDisplayed() }
                        break
                    } catch (geb.error.RequiredPageContentNotPresent e) {
                    } catch (org.openqa.selenium.StaleElementReferenceException e) {
                    } catch (geb.waiting.WaitTimeoutException e) {
                    }
                }
            }
//        else
//        {
//            println("Could not update the value of \"Export Configuration\". Unable to update deployment configuration: Error validating deployment configuration: Export is a PRO version only feature")
//        }

            then: 'Display the created FILE'
            if (isPro) {
                waitFor(waitTime) { page.overview.fileName.isDisplayed() }
                println("Configuration created")
            }


            if (isPro) {
                when: 'Edit button is displayed'
                page.overview.editExportConfiguration.isDisplayed()
                then: 'Click edit button'

                page.overview.editExportConfiguration.click()

                when: 'Delete Configuration is displayed'
                page.overview.deleteConfiguration.isDisplayed()
                count = 0
                while (count < numberOfTrials) {
                    count++
                    try {
                        page.overview.deleteConfiguration.click()
                        waitFor(waitTime) { !page.overview.deleteConfiguration.isDisplayed() }
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
                        page.overview.confirmyesbtn.click()
                        waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
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
        then:
        println("passed")
    }

    def verifyAddConfigurationforJdbcCreated() {
        boolean isPro = false
        String jdbcTestName = page.overview.getJdbcTestName()
        String jdbcValueOne = page.overview.getJdbcValueOne()
        String jdbcValueTwo = page.overview.getJdbcValueTwo()

        when:
        if (page.overview.addconfig.isDisplayed()) {
            when: 'Open Add ConfigurationPopup'
            page.overview.openAddConfigurationPopup()
            page.overview.textType.value("JDBC")
            then: 'Check elements'
            page.overview.addProperty.isDisplayed()
            page.overview.save.isDisplayed()
            page.overview.cancel.isDisplayed()
            page.overview.jdbcdriver.value().equals("jdbcdriver")
            page.overview.jdbcurl.value().equals("jdbcurl")
            page.overview.jdbcdriverValue.isDisplayed()
            page.overview.jdbcurlValue.isDisplayed()

            when: 'Provide values for add configuration'
            page.overview.stream.value("jdbcTest")
            page.overview.jdbcdriverValue.value("value1")
            page.overview.jdbcurlValue.value("value2")
            int count = 0
            while (count < numberOfTrials) {
                count++
                try {
                    page.overview.save.click()
                    waitFor(waitTime) { !page.overview.save.isDisplayed() }
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
                    page.overview.confirmyesbtn.click()
                    waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
                    break
                } catch (geb.error.RequiredPageContentNotPresent e) {
                } catch (org.openqa.selenium.StaleElementReferenceException e) {
                } catch (geb.waiting.WaitTimeoutException e) {
                }
            }
            and: 'Expand export'
            if (!waitFor(10) { updateInnerErrorPopup.isDisplayed() }) {
                isPro = true
                count = 0
                while (count < numberOfTrials) {
                    count++
                    try {
                        export.click()
                        waitFor(waitTime) { page.overview.exportExpanded.isDisplayed() }
                        break
                    } catch (geb.error.RequiredPageContentNotPresent e) {
                    } catch (org.openqa.selenium.StaleElementReferenceException e) {
                    } catch (geb.waiting.WaitTimeoutException e) {
                    }
                }
            }
            then: 'Display the created JDBC'
            if (isPro) {
                waitFor(waitTime) { page.overview.jdbcName.isDisplayed() }
                println("Configuration created")
            }

            if (isPro) {
                when: 'Edit button is displayed'
                page.overview.editExportConfiguration.isDisplayed()
                then: 'Click edit button'
                page.overview.editExportConfiguration.click()

                when: 'Delete Configuration is displayed'
                page.overview.deleteConfiguration.isDisplayed()
                count = 0
                while (count < numberOfTrials) {
                    count++
                    try {
                        page.overview.deleteConfiguration.click()
                        waitFor(waitTime) { !page.overview.deleteConfiguration.isDisplayed() }
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
                        page.overview.confirmyesbtn.click()
                        waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
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
        then:
        println("passed")
    }

    def verifyAddConfigurationforKafkaCreated() {
//        when:
//        if (waitFor(10){page.overview.addconfig.isDisplayed()}) {

            boolean isPro = false
            String kafkaTestName = page.overview.getKafkaTestName()
            String metadataValue = page.overview.getMetadataValue()
            when:
            if (page.overview.addconfig.isDisplayed()) {
                when: 'Open Add ConfigurationPopup'
                page.overview.openAddConfigurationPopup()
                page.overview.textType.value("KAFKA")
                then: 'Check elements'
                page.overview.addProperty.isDisplayed()
                page.overview.save.isDisplayed()
                page.overview.cancel.isDisplayed()
                page.overview.metadatabroker.value().equals("metadata.broker.list")
                page.overview.metadatabrokerValue.isDisplayed()

                when: 'Provide values for add configuration'
                page.overview.stream.value("kafkaTest")
                page.overview.metadatabrokerValue.value("metadataValue")
                int count = 0
                while (count < numberOfTrials) {
                    count++
                    try {
                        page.overview.save.click()
                        waitFor(waitTime) { !page.overview.save.isDisplayed() }
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
                        page.overview.confirmyesbtn.click()
                        waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
                        break
                    } catch (geb.error.RequiredPageContentNotPresent e) {
                    } catch (org.openqa.selenium.StaleElementReferenceException e) {
                    } catch (geb.waiting.WaitTimeoutException e) {
                    }
                }
                and: 'Expand export'
                if (!waitFor(10) { page.overview.addconfig.isDisplayed() }) {
                    isPro = true
                    count = 0
                    while (count < numberOfTrials) {
                        count++
                        try {
                            export.click()
                            waitFor(waitTime) { page.overview.exportExpanded.isDisplayed() }
                            break
                        } catch (geb.error.RequiredPageContentNotPresent e) {
                        } catch (org.openqa.selenium.StaleElementReferenceException e) {
                        } catch (geb.waiting.WaitTimeoutException e) {
                        }
                    }
                }
                then: 'Display the created KAFKA'
                if (isPro) {
                    waitFor(waitTime) { page.overview.kafkaName.isDisplayed() }
                    println("Configuration created")
                }

                if (isPro) {
                    when: 'Edit button is displayed'
                    waitFor(waitTime) { page.overview.editExportConfiguration.isDisplayed() }
                    then: 'Click edit button'
                    page.overview.editExportConfiguration.click()

                    when: 'Delete Configuration is displayed'
                    page.overview.deleteConfiguration.isDisplayed()
                    count = 0
                    while (count < numberOfTrials) {
                        count++
                        try {
                            page.overview.deleteConfiguration.click()
                            waitFor(waitTime) { !page.overview.deleteConfiguration.isDisplayed() }
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
                            page.overview.confirmyesbtn.click()
                            waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
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
                    then:
        println("passed")
    }

    def verifyAddConfigurationforHttpCreated() {
        boolean isPro = false
        String httpTestName = page.overview.getHttpTestName()
        String endValue = page.overview.getEndValue()
        when:
        if (page.overview.addconfig.isDisplayed()) {
            when: 'Open Add ConfigurationPopup'
            page.overview.openAddConfigurationPopup()
            page.overview.textType.value("HTTP")
            then: 'Check elements'
            page.overview.addProperty.isDisplayed()
            page.overview.save.isDisplayed()
            page.overview.cancel.isDisplayed()
            page.overview.endpoint.value().equals("endpoint")
            page.overview.endpointValue.isDisplayed()

            when: 'Provide values for add configuration'
            page.overview.stream.value(httpTestName)
            page.overview.endpointValue.value(endValue)
            int count = 0
            while (count < numberOfTrials) {
                count++
                try {
                    page.overview.save.click()
                    waitFor(waitTime) { !page.overview.save.isDisplayed() }
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
                    page.overview.confirmyesbtn.click()
                    waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
                    break
                } catch (geb.error.RequiredPageContentNotPresent e) {
                } catch (org.openqa.selenium.StaleElementReferenceException e) {
                } catch (geb.waiting.WaitTimeoutException e) {
                }
            }
            and: 'Expand export'
            if (!waitFor(10) { updateInnerErrorPopup.isDisplayed() }) {
                isPro = true
                count = 0
                while (count < numberOfTrials) {
                    count++
                    try {
                        export.click()
                        waitFor(waitTime) { page.overview.exportExpanded.isDisplayed() }
                        break
                    } catch (geb.error.RequiredPageContentNotPresent e) {
                    } catch (org.openqa.selenium.StaleElementReferenceException e) {
                    } catch (geb.waiting.WaitTimeoutException e) {
                    }
                }
            }
            then: 'Display the created HTTP'
            if (isPro) {
                waitFor(waitTime) { page.overview.httpName.isDisplayed() }
                println("Configuration created")
            }

            if (isPro) {
                when: 'Edit button is displayed'
                page.overview.editExportConfiguration.isDisplayed()
                then: 'Click edit button'
                page.overview.editExportConfiguration.click()

                when: 'Delete Configuration is displayed'
                page.overview.deleteConfiguration.isDisplayed()
                count = 0
                while (count < numberOfTrials) {
                    count++
                    try {
                        page.overview.deleteConfiguration.click()
                        waitFor(waitTime) { !page.overview.deleteConfiguration.isDisplayed() }
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
                        page.overview.confirmyesbtn.click()
                        waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
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
        then:
        println("passed")
    }

    def verifyAddConfigurationforRABBITMQbrokerhostcreated() {
        boolean isPro = false
        String rabbitmqBrokerTestName = page.overview.getRabbitmqBrokerTestName()
        String brokerValue = page.overview.getBrokerValue()
        when:
        if (page.overview.addconfig.isDisplayed()) {
            when: 'Open Add ConfigurationPopup'
            page.overview.openAddConfigurationPopup()
            page.overview.textType.value("RABBITMQ")
            then: 'Check elements'
            page.overview.addProperty.isDisplayed()
            page.overview.save.isDisplayed()
            page.overview.cancel.isDisplayed()
            page.overview.rabbitMqValue.isDisplayed()

            when: 'Provide values for add configuration'
            page.overview.stream.value(rabbitmqBrokerTestName)
            page.overview.rabbitMqValue.value(brokerValue)
            int count = 0
            while (count < numberOfTrials) {
                count++
                try {
                    page.overview.save.click()
                    waitFor(waitTime) { !page.overview.save.isDisplayed() }
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
                    page.overview.confirmyesbtn.click()
                    waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
                    break
                } catch (geb.error.RequiredPageContentNotPresent e) {
                } catch (org.openqa.selenium.StaleElementReferenceException e) {
                } catch (geb.waiting.WaitTimeoutException e) {
                }
            }
            and: 'Expand export'
            if (!waitFor(10) { updateInnerErrorPopup.isDisplayed() }) {
                isPro = true
                count = 0
                while (count < numberOfTrials) {
                    count++
                    try {
                        export.click()
                        waitFor(waitTime) { page.overview.exportExpanded.isDisplayed() }
                        break
                    } catch (geb.error.RequiredPageContentNotPresent e) {
                    } catch (org.openqa.selenium.StaleElementReferenceException e) {
                    } catch (geb.waiting.WaitTimeoutException e) {
                    }
                }
            }
            then: 'Display the created RABBITMQ'
            if (isPro) {
                waitFor(waitTime) { page.overview.rabbitMqBrokerName.isDisplayed() }
                println("Configuration created")
            }

            if (isPro) {
                when: 'Edit button is displayed'
                page.overview.editExportConfiguration.isDisplayed()
                then: 'Click edit button'
                page.overview.editExportConfiguration.click()

                when: 'Delete Configuration is displayed'
                page.overview.deleteConfiguration.isDisplayed()
                count = 0
                while (count < numberOfTrials) {
                    count++
                    try {
                        page.overview.deleteConfiguration.click()
                        waitFor(waitTime) { !page.overview.deleteConfiguration.isDisplayed() }
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
                        page.overview.confirmyesbtn.click()
                        waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
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
        then:
        println("passed")

    }

    def verifyAddConfigurationforRABBITMQamqpuricreated() {
        boolean isPro = false
        String rabbitmqAmqpTestName = page.overview.getRabbitmqAmqpTestName()
        String amqpValue = page.overview.getAmqpValue()

        when:
        if (page.overview.addconfig.isDisplayed()) {
            when: 'Open Add ConfigurationPopup'
            page.overview.openAddConfigurationPopup()
            page.overview.textType.value("RABBITMQ")
            waitFor(waitTime) { page.overview.rabbitMq.isDisplayed() }
            page.overview.rabbitMq.click()
            page.overview.rabbitMq.value("amqp.uri")
            then: 'Check elements'
            page.overview.addProperty.isDisplayed()
            page.overview.save.isDisplayed()
            page.overview.cancel.isDisplayed()
            page.overview.rabbitMqValue.isDisplayed()

            when: 'Provide values for add configuration'
            page.overview.stream.value(rabbitmqAmqpTestName)
            page.overview.rabbitMqValue.value(amqpValue)
            int count = 0
            while (count < numberOfTrials) {
                count++
                try {
                    page.overview.save.click()
                    waitFor(waitTime) { !page.overview.save.isDisplayed() }
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
                    page.overview.confirmyesbtn.click()
                    waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
                    break
                } catch (geb.error.RequiredPageContentNotPresent e) {
                } catch (org.openqa.selenium.StaleElementReferenceException e) {
                } catch (geb.waiting.WaitTimeoutException e) {
                }
            }

            and: 'Expand export'
            if (!waitFor(10) { updateInnerErrorPopup.isDisplayed() }) {
                isPro = true
                count = 0
                while (count < numberOfTrials) {
                    count++
                    try {
                        export.click()
                        waitFor(waitTime) { page.overview.exportExpanded.isDisplayed() }
                        break
                    } catch (geb.error.RequiredPageContentNotPresent e) {
                    } catch (org.openqa.selenium.StaleElementReferenceException e) {
                    } catch (geb.waiting.WaitTimeoutException e) {
                    }
                }
            }
            then: 'Display the created RABBITMQ'
            if (isPro) {
                waitFor(waitTime) { page.overview.rabbitMqAmqpName.isDisplayed() }
                println("Configuration created")
            }

            if (isPro) {
                when: 'Edit button is displayed'
                page.overview.editExportConfiguration.isDisplayed()
                then: 'Click edit button'
                page.overview.editExportConfiguration.click()

                when: 'Delete Configuration is displayed'
                page.overview.deleteConfiguration.isDisplayed()
                count = 0
                while (count < numberOfTrials) {
                    count++
                    try {
                        page.overview.deleteConfiguration.click()
                        waitFor(waitTime) { !page.overview.deleteConfiguration.isDisplayed() }
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
                        page.overview.confirmyesbtn.click()
                        waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
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
        then:
        println("passed")
    }

    def verifyAddConfigurationforCUSTOMcreated() {
        boolean isPro = false
        String customTestName = page.overview.getCustomTestName()
        String customConnectorClass = page.overview.getCustomConnectorClass()

        when:
        if (page.overview.addconfig.isDisplayed()) {
            when: 'Open Add ConfigurationPopup'
            page.overview.openAddConfigurationPopup()
            page.overview.textType.value("CUSTOM")
            then: 'Check elements'
            page.overview.addProperty.isDisplayed()
            page.overview.save.isDisplayed()
            page.overview.cancel.isDisplayed()
            page.overview.exportConnectorClass.isDisplayed()

            when: 'Provide values for add configuration'
            page.overview.stream.value(customTestName)
            page.overview.exportConnectorClass.value(customConnectorClass)
            int count = 0
            while (count < numberOfTrials) {
                count++
                try {
                    page.overview.save.click()
                    waitFor(waitTime) { !page.overview.save.isDisplayed() }
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
                    page.overview.confirmyesbtn.click()
                    waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
                    break
                } catch (geb.error.RequiredPageContentNotPresent e) {
                } catch (org.openqa.selenium.StaleElementReferenceException e) {
                } catch (geb.waiting.WaitTimeoutException e) {
                }
            }
            and: 'Expand export'
            if (!waitFor(10) { updateInnerErrorPopup.isDisplayed() }) {
                isPro = true
                count = 0
                while (count < numberOfTrials) {
                    count++
                    try {
                        export.click()
                        waitFor(waitTime) { page.overview.exportExpanded.isDisplayed() }
                        break
                    } catch (geb.error.RequiredPageContentNotPresent e) {
                    } catch (org.openqa.selenium.StaleElementReferenceException e) {
                    } catch (geb.waiting.WaitTimeoutException e) {
                    }
                }
            }
            then: 'Display the created CUSTOM'
            if (isPro) {
                waitFor(waitTime) { page.overview.customName.isDisplayed() }
                println("Configuration created")
            }

            if (isPro) {
                when: 'Edit button is displayed'
                page.overview.editExportConfiguration.isDisplayed()
                then: 'Click edit button'
                page.overview.editExportConfiguration.click()

                when: 'Delete Configuration is displayed'
                page.overview.deleteConfiguration.isDisplayed()
                count = 0
                while (count < numberOfTrials) {
                    count++
                    try {
                        page.overview.deleteConfiguration.click()
                        waitFor(waitTime) { !page.overview.deleteConfiguration.isDisplayed() }
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
                        page.overview.confirmyesbtn.click()
                        waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
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
        then:
        println("passed")
    }

    def verifyAddConfigurationforELASTICSEARCHcreated() {
        boolean isPro = false
        String elasticSearchTestName     = page.overview.getElasticSearchTestName()
        String elasticSearchValueOne     = page.overview.getElasticSearchValueOne()
        when:
        if (page.overview.addconfig.isDisplayed()) {
            when: 'Open Add ConfigurationPopup'
            page.overview.openAddConfigurationPopup()
            page.overview.textType.value("ELASTICSEARCH")
            then: 'Check elements'
            page.overview.addProperty.isDisplayed()
            page.overview.save.isDisplayed()
            page.overview.cancel.isDisplayed()
            page.overview.endpointES.value().equals("endpoint")
            page.overview.endpointESValue.isDisplayed()

            when: 'Provide values for add configuration'
            page.overview.stream.value("elasticSearchTest")
            page.overview.endpointESValue.value("endpointValue")
            int count = 0
            while (count < numberOfTrials) {
                count++
                try {
                    page.overview.save.click()
                    waitFor(waitTime) { !page.overview.save.isDisplayed() }
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
                    page.overview.confirmyesbtn.click()
                    waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
                    break
                } catch (geb.error.RequiredPageContentNotPresent e) {
                } catch (org.openqa.selenium.StaleElementReferenceException e) {
                } catch (geb.waiting.WaitTimeoutException e) {
                }
            }
            and: 'Expand export'
            if (!waitFor(10) { updateInnerErrorPopup.isDisplayed() }) {
                isPro = true
                count = 0
                while (count < numberOfTrials) {
                    count++
                    try {
                        export.click()
                        waitFor(waitTime) { page.overview.exportExpanded.isDisplayed() }
                        break
                    } catch (geb.error.RequiredPageContentNotPresent e) {
                    } catch (org.openqa.selenium.StaleElementReferenceException e) {
                    } catch (geb.waiting.WaitTimeoutException e) {
                    }
                }
            }
            then: 'Display the created ELASTICSEARCH'
            if (isPro) {
                waitFor(waitTime) { page.overview.elasticSearchName.isDisplayed() }
            }

            if (isPro) {
                when: 'Edit button is displayed'
                page.overview.editExportConfiguration.isDisplayed()
                then: 'Click edit button'
                page.overview.editExportConfiguration.click()

                when: 'Delete Configuration is displayed'
                page.overview.deleteConfiguration.isDisplayed()
                count = 0
                while (count < numberOfTrials) {
                    count++
                    try {
                        page.overview.deleteConfiguration.click()
                        waitFor(waitTime) { !page.overview.deleteConfiguration.isDisplayed() }
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
                        page.overview.confirmyesbtn.click()
                        waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
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
        then:
        println("passed")
    }

    def verifyAddPropertyInExportConfiguration(){
        when:
        if (page.overview.addconfig.isDisplayed()) {
            when: 'Open Add ConfigurationPopup'
            page.overview.openAddConfigurationPopup()
            page.overview.textType.value("KAFKA")
            then: 'Check elements'
            page.overview.addProperty.isDisplayed()
            page.overview.save.isDisplayed()
            page.overview.cancel.isDisplayed()
            page.overview.metadatabroker.value().equals("metadata.broker.list")
            page.overview.metadatabroker.isDisplayed()

            when: 'Add property is clicked'
            page.overview.addProperty.click()
            then: 'New text and value fields are displayed'
            page.overview.newTextField.isDisplayed()
            page.overview.newValueField.isDisplayed()
            page.overview.deleteFirstProperty.isDisplayed()
            when: 'Provide values for add configuration'
            page.overview.stream.value("kafkaTest")
            page.overview.metadatabrokerValue.value("metadataValue")
            page.overview.newTextField.value("value1")
            page.overview.newValueField.value("value2")
            int count = 0
            while (count < numberOfTrials) {
                count++
                try {
                    page.overview.deleteConfiguration.click()
                    waitFor(waitTime) { !page.overview.deleteConfiguration.isDisplayed() }
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
                    page.overview.confirmyesbtn.click()
                    waitFor(waitTime) { !page.overview.confirmyesbtn.isDisplayed() }
                    break
                } catch (geb.error.RequiredPageContentNotPresent e) {
                } catch (org.openqa.selenium.StaleElementReferenceException e) {
                } catch (org.openqa.selenium.ElementNotVisibleException e) {
                } catch (geb.waiting.WaitTimeoutException e) {
                }
            }
            and: 'Expand export'
            page.overview.expandExport()
            then: 'Display the created KAFKA'
            waitFor(waitTime) { page.overview.kafkaName.isDisplayed() }
            println("Configuration created")
        }
        then:
        println("passed")
    }

    def verifyErrorMessagesOfAddPropertyInExportConfiguration() {
        when:
        if (page.overview.addconfig.isDisplayed()) {
            when: 'Open Add ConfigurationPopup'
            page.overview.openAddConfigurationPopup()
            page.overview.textType.value("KAFKA")
            then: 'Check elements'
            page.overview.addProperty.isDisplayed()
            page.overview.save.isDisplayed()
            page.overview.cancel.isDisplayed()
            page.overview.metadatabroker.value().equals("metadata.broker.list")
            page.overview.metadatabroker.isDisplayed()

            when: 'Add property is clicked'
            page.overview.addProperty.click()
            then: 'New text and value fields are displayed'
            page.overview.newTextField.isDisplayed()
            page.overview.newValueField.isDisplayed()
            page.overview.deleteFirstProperty.isDisplayed()
            when: 'Save button is clicked'
            page.overview.save.click()
            then: 'Error messages are displayed'
            page.overview.errorPropertyName1.isDisplayed()
            page.overview.errorPropertyValue1.isDisplayed()
        }
        then:
        println("passed")
    }
}
