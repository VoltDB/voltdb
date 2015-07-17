package vmcTest.tests

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

    def "Verify Edit in a Configuration"() {
        when: 'Open Add ConfigurationPopup'
        page.overview.openAddConfigurationPopup()
        page.overview.textType.value("KAFKA")
        then: 'Check elements'
        waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
        waitFor(waitTime) { page.overview.metadatabroker.value().equals("metadata.broker.list") }
	    waitFor(waitTime) { page.overview.metadatabroker.isDisplayed() }

	    
	    when: 'Provide values for add configuration'
	    page.overview.stream.value("kafkaTest")
	    page.overview.metadatabrokerValue.value("metadataValue")
	    then: 'Click Save'
	    page.overview.clickSave()
	    when: 'Expand export'
	    page.overview.expandExport()
	    then: 'Display the created KAFKA'
	    waitFor(waitTime) { page.overview.kafkaName.isDisplayed() }
	    println("Configuration created")
	    
	    // Edit: Add Configuration
	    
	    when: 'Edit button is displayed'
	    waitFor(waitTime) { page.overview.editExportConfiguration.isDisplayed() }
        then: 'Click edit button'
        page.overview.editExportConfiguration.click()
        
        when: 'Add property is clicked'
        page.overview.addProperty.click()
        then: 'New text and value fields are displayed'
        waitFor(waitTime) { page.overview.newTextField.isDisplayed() }
        waitFor(waitTime) { page.overview.newValueField.isDisplayed() }
        waitFor(waitTime) { page.overview.deleteFirstProperty.isDisplayed() }
        
        when: 'Provide values for text and value fields'
        page.overview.newTextField.value("value1")
        page.overview.newValueField.value("value2")
        then: 'Click Save'
        page.overview.clickSave()
        
        // Edit: Change the file type
        
        when: 'Edit button is displayed'
	    waitFor(waitTime) { page.overview.editExportConfiguration.isDisplayed() }
        then: 'Click edit button'
        page.overview.editExportConfiguration.click()
        
        when: 'Check all the properties'
        waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
        waitFor(waitTime) { page.overview.metadatabroker.value().equals("metadata.broker.list") }
	    waitFor(waitTime) { page.overview.metadatabrokerValue.isDisplayed() }
	    waitFor(waitTime) { page.overview.newTextField.isDisplayed() }
	    waitFor(waitTime) { page.overview.newValueField.isDisplayed() }
        then: 'Check for previous changes'
        waitFor(waitTime) { page.overview.newTextField.value().equals("value1") }
	    waitFor(waitTime) { page.overview.newValueField.value().equals("value2") }
        
        when: 'Change to jdbc'
        page.overview.textType.value("JDBC")
        then: 'jdbc text fields are displayed'
        page.overview.jdbcdriverValue.isDisplayed()
        page.overview.jdbcurlValue.isDisplayed()
        
        when: 'Provide values for jdbc'
        page.overview.jdbcdriverValue.value("value")
        page.overview.jdbcurlValue.value("value")
        then: 'Click Save'
        page.overview.clickSave()
        
        // Edit: Change the file type to kafka

        when: 'Edit button is displayed'
        waitFor(waitTime) { page.overview.editExportConfiguration.isDisplayed() }
        then: 'Click edit button'
        page.overview.editExportConfiguration.click()

        when: 'Check all the properties'
        waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
        waitFor(waitTime) { page.overview.save.isDisplayed() }
        waitFor(waitTime) { page.overview.cancel.isDisplayed() }
        then: 'Check for previous changes'
        waitFor(waitTime) { page.overview.jdbcdriverValue.value().equals("value") }
        waitFor(waitTime) { page.overview.jdbcurlValue.value().equals("value") }

        when: 'Change to kafka'
        page.overview.textType.value("KAFKA")
        then: 'jdbc text fields are displayed'
        page.overview.metadatabrokerValue.isDisplayed()

        when: 'Provide values for kafka'
        page.overview.metadatabrokerValue.value("metadataValue")
        then: 'Click Save'
        page.overview.clickSave()



       Edit: Change the file type to ELASTICSEARCH

        when: 'Edit button is displayed'
        waitFor(waitTime) { page.overview.editExportConfiguration.isDisplayed() }
        then: 'Click edit button'
        page.overview.editExportConfiguration.click()

        when: 'Check all the properties'
        waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
        waitFor(waitTime) { page.overview.save.isDisplayed() }
        waitFor(waitTime) { page.overview.cancel.isDisplayed() }

        then: 'Check for previous changes'
        waitFor(waitTime) { page.overview.metadatabrokerValue.value().equals("metadataValue") }


        when: 'Change to ELASTICSEARCH'
        page.overview.textType.value("ELASTICSEARCH")
        then: 'ELASTICSEARCH text fields are displayed'
        waitFor(waitTime) { page.overview.endpointESValue.isDisplayed() }

        when: 'Provide values for ELASTICSEARCH'
        page.overview.endpointESValue.value("endpointValue")
        then: 'Click Save'
        page.overview.clickSave()

        //Edit: Change the file type to HTTP

        when: 'Edit button is displayed'
        waitFor(waitTime) { page.overview.editExportConfiguration.isDisplayed() }
        then: 'Click edit button'
        page.overview.editExportConfiguration.click()

        when: 'Check all the properties'
        waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
        waitFor(waitTime) { page.overview.save.isDisplayed() }
        waitFor(waitTime) { page.overview.cancel.isDisplayed() }

        then: 'Check for previous changes'
        waitFor(waitTime) { page.overview.endpointESValue.value().equals("endpointValue") }


        when: 'Change to HTTP'
        page.overview.textType.value("HTTP")
        then: 'HTTP text fields are displayed'
        waitFor(waitTime) { page.overview.endpointValue.isDisplayed() }

        when: 'Provide values for HTTP'
        page.overview.endpointValue.value("endValue")
        then: 'Click Save'
        page.overview.clickSave()


       // Edit: Change the file type to RABBITMQ

//        when: 'Edit button is displayed'
//        waitFor(waitTime) { page.overview.editExportConfiguration.isDisplayed() }
//        then: 'Click edit button'
//        page.overview.editExportConfiguration.click()
//
//        when: 'Check all the properties'
//        waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
//        waitFor(waitTime) { page.overview.save.isDisplayed() }
//        waitFor(waitTime) { page.overview.cancel.isDisplayed() }
//
//        then: 'Check for previous changes'
//        waitFor(waitTime) { page.overview.endpointValue.value().equals("endValue") }
//
//
//        when: 'Change to RABBITMQ'
//        page.overview.textType.value("RABBITMQ")
//        then: 'RABBITMQ text fields are displayed'
//        waitFor(waitTime) { page.overview.rabbitMqValue.isDisplayed() }
//
//        when: 'Provide values for RABBITMQ'
//        page.overview.rabbitMqValue.value("brokerValue")
//        then: 'Click Save'
//        page.overview.clickSave()


        // Edit: Change the file type to CUSTOM

//        when: 'Edit button is displayed'
//        waitFor(waitTime) { page.overview.editExportConfiguration.isDisplayed() }
//        then: 'Click edit button'
//        page.overview.editExportConfiguration.click()
//
//        when: 'Check all the properties'
//        waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
//        waitFor(waitTime) { page.overview.save.isDisplayed() }
//        waitFor(waitTime) { page.overview.cancel.isDisplayed() }
//
//        then: 'Check for previous changes'
//        waitFor(waitTime) { page.overview.metadatabrokerValue.value().equals("brokerValue") }
//
//
//
//        when: 'Change to CUSTOM'
//        page.overview.textType.value("CUSTOM")
//        then: 'CUSTOM text fields are displayed'
//        waitFor(waitTime) { page.overview.metadatabrokerValue.isDisplayed() }
//
//        when: 'Provide values for CUSTOM'
//        page.overview.endpointESValue.value("endpointValue")
//        then: 'Click Save'
//        page.overview.clickSave()

        // Delete Configuration
        
        when: 'Edit button is displayed'
	    waitFor(waitTime) { page.overview.editExportConfiguration.isDisplayed() }
        then: 'Click edit button'
        page.overview.editExportConfiguration.click()
        
        when: 'Delete Configuration is displayed'
        page.overview.deleteConfiguration.isDisplayed()
        then: 'Click delete configuration'
        page.overview.deleteExportConfiguration()
        
        when: 'Expand export'
	    page.overview.expandExport()
	    then: 'Check if FILE is deleted'
	    try {
	        waitFor(waitTime) { page.overview.fileName.isDisplayed() }
	    } catch(geb.waiting.WaitTimeoutException e) {
	        println("Configuration deleted")
	    }
    }
}
