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
        then: 'Check elements'
        waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	    waitFor(waitTime) { page.overview.type.value().equals("type") }
	    waitFor(waitTime) { page.overview.nonce.value().equals("nonce") }
	    waitFor(waitTime) { page.overview.outdir.value().equals("outdir") }
	    waitFor(waitTime) { page.overview.typeValue.isDisplayed() }
	    waitFor(waitTime) { page.overview.nonceValue.isDisplayed() }
	    waitFor(waitTime) { page.overview.outdirValue.isDisplayed() } 
	    
	    when: 'Provide values for add configuration'
	    page.overview.stream.value("fileTest")
	    page.overview.typeValue.value("typeValue")
	    page.overview.nonceValue.value("nonceValue")
	    page.overview.outdirValue.value("outdirValue")
	    then: 'Click Save'
	    page.overview.clickSave()
	    when: 'Expand export'
	    page.overview.expandExport()
	    then: 'Display the created FILE'
	    waitFor(waitTime) { page.overview.fileName.isDisplayed() }
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
	    waitFor(waitTime) { page.overview.type.value().equals("type") }
	    waitFor(waitTime) { page.overview.nonce.value().equals("nonce") }
	    waitFor(waitTime) { page.overview.outdir.value().equals("outdir") }
	    waitFor(waitTime) { page.overview.typeValue.isDisplayed() }
	    waitFor(waitTime) { page.overview.nonceValue.isDisplayed() }
	    waitFor(waitTime) { page.overview.outdirValue.isDisplayed() }
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
        
        
        // Edit: Change the file type to kafka
        /*
        when: 'Edit button is displayed'
	    waitFor(waitTime) { page.overview.editExportConfiguration.isDisplayed() }
        then: 'Click edit button'
        page.overview.editExportConfiguration.click()
        
        when: 'Check all the properties'
        waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	    waitFor(waitTime) { page.overview.type.value().equals("type") }
	    waitFor(waitTime) { page.overview.nonce.value().equals("nonce") }
	    waitFor(waitTime) { page.overview.outdir.value().equals("outdir") }
	    waitFor(waitTime) { page.overview.typeValue.isDisplayed() }
	    waitFor(waitTime) { page.overview.nonceValue.isDisplayed() }
	    waitFor(waitTime) { page.overview.outdirValue.isDisplayed() }
	    waitFor(waitTime) { page.overview.newTextField.isDisplayed() }
	    waitFor(waitTime) { page.overview.newValueField.isDisplayed() }
	    waitFor(waitTime) { page.overview.newTextField.value().equals("value1") }
	    waitFor(waitTime) { page.overview.newValueField.value().equals("value2") }
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
        */
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
