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
    
	def "Check export Click and check its value"() {	
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
	
	def "Verify Export Tables button and texts" () {
	    when: 'Export and AddConfig is displayed'
	    waitFor(waitTime) { page.export.isDisplayed() }
	    waitFor(waitTime) { page.addconfig.isDisplayed() }
	    then: 'Expand Export'
	    page.export.click()
	    waitFor(waitTime) { page.overview.exportTablesText.isDisplayed() }
	    waitFor(waitTime) { page.overview.listOfExport.isDisplayed() }
	}
	
	def "Verify Add Configuration opens/Verify Add Configuration for FILE"() {
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
	}

    def "Verify Add Configuration for JDBC"() {
        when: 'Open Add ConfigurationPopup'
	    page.overview.openAddConfigurationPopup()
	    page.overview.textType.value("JDBC")
	    then: 'Check elements'
	    waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	    waitFor(waitTime) { page.overview.jdbcdriver.value().equals("jdbcdriver") }
	    waitFor(waitTime) { page.overview.jdbcurl.value().equals("jdbcurl") }
	    waitFor(waitTime) { page.overview.jdbcdriverValue.isDisplayed() }
	    waitFor(waitTime) { page.overview.jdbcurlValue.isDisplayed() }
    }
    
    def "Verify Add Configuration for KAFKA"() {
        when: 'Open Add ConfigurationPopup'
	    page.overview.openAddConfigurationPopup()
	    page.overview.textType.value("KAFKA")
	    then: 'Check elements'
	    waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	    waitFor(waitTime) { page.overview.metadatabroker.value().equals("metadata.broker.list") }
	    waitFor(waitTime) { page.overview.metadatabrokerValue.isDisplayed() }
    }
    
    def "Verify Add Configuration for HTTP"() {
        when: 'Open Add ConfigurationPopup'
	    page.overview.openAddConfigurationPopup()
	    page.overview.textType.value("HTTP")
	    then: 'Check elements'
	    waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	    waitFor(waitTime) { page.overview.endpoint.value().equals("endpoint") }
	    waitFor(waitTime) { page.overview.endpointValue.isDisplayed() }
    }
    
    def "Verify Add Configuration for RABBITMQ/Verify Add Configuration for RABBITMQ broker.host"() {
        when: 'Open Add ConfigurationPopup'
	    page.overview.openAddConfigurationPopup()
	    page.overview.textType.value("RABBITMQ")
	    then: 'Check elements'
	    waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	    waitFor(waitTime) { page.overview.rabbitMq.value().equals("broker.host") }
	    waitFor(waitTime) { page.overview.rabbitMqValue.isDisplayed() }
    }
    
    def "Verify Add Configuration for RABBITMQ amqp.uri"() {
        when: 'Open Add ConfigurationPopup'
	    page.overview.openAddConfigurationPopup()
	    page.overview.textType.value("RABBITMQ")
	    page.overview.rabbitMq.value("amqp.uri")
	    then: 'Check elements'
	    waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	    waitFor(waitTime) { page.overview.rabbitMq.value().equals("amqp.uri") }
	    waitFor(waitTime) { page.overview.rabbitMqValue.isDisplayed() }
    }
    
    def "Verify Add Configuration for CUSTOM"() {
        when: 'Open Add ConfigurationPopup'
	    page.overview.openAddConfigurationPopup()
	    page.overview.textType.value("CUSTOM")
	    then: 'Check elements'
	    waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	    waitFor(waitTime) { page.overview.exportConnectorClass.isDisplayed() }
    }
    
    def "Verify Error messages of Add Configuration for FILE"() {
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
	    
	    when: 'Save button is clicked'
	    page.overview.save.click()
	    then: 'Error messages are displayed'
	    waitFor(waitTime) { page.overview.errorStream.isDisplayed() }
	    waitFor(waitTime) { page.overview.errorFileTypeValue.isDisplayed() }
	    waitFor(waitTime) { page.overview.errornonceValue.isDisplayed() }
	    waitFor(waitTime) { page.overview.errorOutdirValue.isDisplayed() }
    }
    
    def "Verify Error messages of Add Configuration for JDBC"() {
        when: 'Open Add ConfigurationPopup'
	    page.overview.openAddConfigurationPopup()
	    page.overview.textType.value("JDBC")
	    then: 'Check elements'
	    waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	    waitFor(waitTime) { page.overview.jdbcdriver.value().equals("jdbcdriver") }
	    waitFor(waitTime) { page.overview.jdbcurl.value().equals("jdbcurl") }
	    waitFor(waitTime) { page.overview.jdbcdriverValue.isDisplayed() }
	    waitFor(waitTime) { page.overview.jdbcurlValue.isDisplayed() }
	    
	    when: 'Save button is clicked'
	    page.overview.save.click()
	    then: 'Error messages are displayed'
	    waitFor(waitTime) { page.overview.errorStream.isDisplayed() }
	    waitFor(waitTime) { page.overview.errorJdbcDriverValue.isDisplayed() }
	    waitFor(waitTime) { page.overview.errorJdbcUrlValue.isDisplayed() }
    }
    
    def "Verify Error messages of Add Configuration for KAFKA"() {
        when: 'Open Add ConfigurationPopup'
	    page.overview.openAddConfigurationPopup()
	    page.overview.textType.value("KAFKA")
	    then: 'Check elements'
	    waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	    waitFor(waitTime) { page.overview.metadatabroker.value().equals("metadata.broker.list") }
	    waitFor(waitTime) { page.overview.metadatabrokerValue.isDisplayed() }
	    
	    when: 'Save button is clicked'
	    page.overview.save.click()
	    then: 'Error messages are displayed'
	    waitFor(waitTime) { page.overview.errorStream.isDisplayed() }
	    waitFor(waitTime) { page.overview.errorMetadataBrokerListValue.isDisplayed() }
    }
    
    def "Verify Error messages of Add Configuration for HTTP"() {
        when: 'Open Add ConfigurationPopup'
	    page.overview.openAddConfigurationPopup()
	    page.overview.textType.value("HTTP")
	    then: 'Check elements'
	    waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }  
	    waitFor(waitTime) { page.overview.endpoint.value().equals("endpoint") }
	    waitFor(waitTime) { page.overview.endpointValue.isDisplayed() }
	    
	    when: 'Save button is clicked'
	    page.overview.save.click()
	    then: 'Error messages are displayed'
	    waitFor(waitTime) { page.overview.errorStream.isDisplayed() }
	    waitFor(waitTime) { page.overview.errorEndpointValue.isDisplayed() }
    }
    
    def "Verify Error messages of Add Configuration for RABBITMQ/Verify Error messages of Add Configuration for RABBITMQ broker.host"() {
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
    
    def "Verify Error messages of Add Configuration for RABBITMQ ampq.uri"() {
        when: 'Open Add ConfigurationPopup'
	    page.overview.openAddConfigurationPopup()
	    page.overview.textType.value("RABBITMQ")
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
    
    def "Verify Error messages of Add Configuration for CUSTOM"() {
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
    
    def "Verify Add Configuration for FILE created"() {
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
	    then: 'Display the created FILE'
	    try {
	        waitFor(waitTime) { page.overview.fileName.isDisplayed() }
	    } catch(geb.waiting.WaitTimeoutException e) {
	        println("Configuration deleted")
	    }
    }
    
    def "Verify Add Configuration for JDBC created"() {
        when: 'Open Add ConfigurationPopup'
	    page.overview.openAddConfigurationPopup()
	    page.overview.textType.value("JDBC")
	    then: 'Check elements'
	    waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	    waitFor(waitTime) { page.overview.jdbcdriver.value().equals("jdbcdriver") }
	    waitFor(waitTime) { page.overview.jdbcurl.value().equals("jdbcurl") }
	    waitFor(waitTime) { page.overview.jdbcdriverValue.isDisplayed() }
	    waitFor(waitTime) { page.overview.jdbcurlValue.isDisplayed() }
	    
	    when: 'Provide values for add configuration'
	    page.overview.stream.value("jdbcTest")
	    page.overview.jdbcdriverValue.value("jdbcdriverValue")
	    page.overview.jdbcurlValue.value("jdbcurlValue")
	    then: 'Click Save'
	    page.overview.clickSave()
	    
	    when: 'Expand export'
	    page.overview.expandExport()
	    then: 'Display the created JDBC'
	    waitFor(waitTime) { page.overview.jdbcName.isDisplayed() }
	    println("Configuration created")
	    
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
	    then: 'Display the created JDBC'
	    try {
	        waitFor(waitTime) { page.overview.jdbcName.isDisplayed() }
	    } catch(geb.waiting.WaitTimeoutException e) {
	        println("Configuration deleted")
	    }
    }
    
    def "Verify Add Configuration for KAFKA created"() {
        when: 'Open Add ConfigurationPopup'
	    page.overview.openAddConfigurationPopup()
	    page.overview.textType.value("KAFKA")
	    then: 'Check elements'
	    waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	    waitFor(waitTime) { page.overview.metadatabroker.value().equals("metadata.broker.list") }
	    waitFor(waitTime) { page.overview.metadatabrokerValue.isDisplayed() }
	    
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
	    then: 'Display the created KAFKA'
	    try {
	        waitFor(waitTime) { page.overview.kafkaName.isDisplayed() }
	    } catch(geb.waiting.WaitTimeoutException e) {
	        println("Configuration deleted")
	    }
    }
    
    def "Verify Add Configuration for HTTP created"() {
        when: 'Open Add ConfigurationPopup'
	    page.overview.openAddConfigurationPopup()
	    page.overview.textType.value("HTTP")
	    then: 'Check elements'
	    waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	    waitFor(waitTime) { page.overview.endpoint.value().equals("endpoint") }
	    waitFor(waitTime) { page.overview.endpointValue.isDisplayed() }
	    
	    when: 'Provide values for add configuration'
	    page.overview.stream.value("httpTest")
	    page.overview.endpointValue.value("endpointValue")
	    then: 'Click Save'
	    page.overview.clickSave()
	    
	    when: 'Expand export'
	    page.overview.expandExport()
	    then: 'Display the created HTTP'
	    waitFor(waitTime) { page.overview.httpName.isDisplayed() }
	    println("Configuration created")
	    
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
	    then: 'Display the created HTTP'
	    try {
	        waitFor(waitTime) { page.overview.httpName.isDisplayed() }
	    } catch(geb.waiting.WaitTimeoutException e) {
	        println("Configuration deleted")
	    }
    }

    def "Verify Add Configuration for RABBITMQ/Verify Add Configuration for RABBITMQ broker.host created"() {
        when: 'Open Add ConfigurationPopup'
        page.overview.openAddConfigurationPopup()
	    page.overview.textType.value("RABBITMQ")
	    then: 'Check elements'
	    waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	    waitFor(waitTime) { page.overview.rabbitMqValue.isDisplayed() }
	    
	    when: 'Provide values for add configuration'
	    page.overview.stream.value("rabbitmqBrokerTest")
	    page.overview.rabbitMqValue.value("rabbitmqAmpqValue")
	    then: 'Click Save'
	    page.overview.clickSave()
	    
	    when: 'Expand export'
	    page.overview.expandExport()
	    then: 'Display the created RABBITMQ'
	    waitFor(waitTime) { page.overview.rabbitMqBrokerName.isDisplayed() }
	    println("Configuration created")
	    
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
	    then: 'Display the created RABBITMQ'
	    try {
	        waitFor(waitTime) { page.overview.rabbitMqBrokerName.isDisplayed() }
	    } catch(geb.waiting.WaitTimeoutException e) {
	        println("Configuration deleted")
	    }
    }
    
    def "Verify Add Configuration for RABBITMQ amqp.uri created"() {
        when: 'Open Add ConfigurationPopup'
	    page.overview.openAddConfigurationPopup()
	    page.overview.textType.value("RABBITMQ")
	    page.overview.rabbitMq.value("amqp.uri")
	    then: 'Check elements'
	    waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	    waitFor(waitTime) { page.overview.rabbitMqValue.isDisplayed() }
	    
	    when: 'Provide values for add configuration'
	    page.overview.stream.value("rabbitmqAmpqTest")
	    page.overview.rabbitMqValue.value("rabbitmqAmpqValue")
	    then: 'Click Save'
	    page.overview.clickSave()
	    
	    when: 'Expand export'
	    page.overview.expandExport()
	    then: 'Display the created RABBITMQ'
	    waitFor(waitTime) { page.overview.rabbitMqAmpqName.isDisplayed() }
	    println("Configuration created")
	    
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
	    then: 'Display the created RABBITMQ'
	    try {
	        waitFor(waitTime) { page.overview.rabbitMqAmpqName.isDisplayed() }
	    } catch(geb.waiting.WaitTimeoutException e) {
	        println("Configuration deleted")
	    }
    }
    
    def "Verify Add Configuration for CUSTOM created"() {
        when: 'Open Add ConfigurationPopup'
	    page.overview.openAddConfigurationPopup()
	    page.overview.textType.value("CUSTOM")
	    then: 'Check elements'
	    waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	    waitFor(waitTime) { page.overview.exportConnectorClass.isDisplayed() }
	    
	    when: 'Provide values for add configuration'
	    page.overview.stream.value("customTest")
	    page.overview.exportConnectorClass.value("exportConnectorClassValue")
	    then: 'Click Save'
	    page.overview.clickSave()
	    
	    when: 'Expand export'
	    page.overview.expandExport()
	    then: 'Display the created CUSTOM'
	    waitFor(waitTime) { page.overview.customName.isDisplayed() }
	    println("Configuration created")
	    
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
	    then: 'Display the created CUSTOM'
	    try {
	        waitFor(waitTime) { page.overview.customName.isDisplayed() }
	    } catch(geb.waiting.WaitTimeoutException e) {
	        println("Configuration deleted")
	    }
    }
}
