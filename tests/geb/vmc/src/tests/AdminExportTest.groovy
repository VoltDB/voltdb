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
	    when: 'Add Configuration is displayed'
	    waitFor(waitTime) { page.addconfig.isDisplayed() }
	    then: 'Add Configuration Popup open'
	    int count = 0
	    while(count<numberOfTrials) {
	        count++
	        try {
	            page.addconfig.click()
	            waitFor(waitTime) { 
	                page.overview.exportAddConfigPopupTitle.isDisplayed() 
	            }
	            break
	        } catch(geb.waiting.WaitTimeoutException e) {
	        }
	    }
	    
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
        when: 'Add Configuration is displayed'
	    waitFor(waitTime) { page.addconfig.isDisplayed() }
	    then: 'Add Configuration Popup open'
	    int count = 0
	    while(count<numberOfTrials) {
	        count++
	        try {
	            page.addconfig.click()
	            waitFor(waitTime) { page.overview.exportAddConfigPopupTitle.isDisplayed() }
	            break
	        } catch(geb.waiting.WaitTimeoutException e) {
	        }
	    }
	    
	    page.overview.textType.value("JDBC")
	    
	    waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	            
	    waitFor(waitTime) { page.overview.jdbcdriver.value().equals("jdbcdriver") }
	    waitFor(waitTime) { page.overview.jdbcurl.value().equals("jdbcurl") }
	            
	    waitFor(waitTime) { page.overview.jdbcdriverValue.isDisplayed() }
	    waitFor(waitTime) { page.overview.jdbcurlValue.isDisplayed() }
    }
    
    def "Verify Add Configuration for KAFKA"() {
        when: 'Add Configuration is displayed'
	    waitFor(waitTime) { page.addconfig.isDisplayed() }
	    then: 'Add Configuration Popup open'
	    int count = 0
	    while(count<numberOfTrials) {
	        count++
	        try {
	            page.addconfig.click()
	            waitFor(waitTime) { page.overview.exportAddConfigPopupTitle.isDisplayed() }
	            break
	        } catch(geb.waiting.WaitTimeoutException e) {
	        }
	    }
	    
	    page.overview.textType.value("KAFKA")
	    
	    waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	            
	    waitFor(waitTime) { page.overview.metadatabroker.value().equals("metadata.broker.list") }
	            
	    waitFor(waitTime) { page.overview.metadatabrokerValue.isDisplayed() }
    }
    
    def "Verify Add Configuration for HTTP"() {
        when: 'Add Configuration is displayed'
	    waitFor(waitTime) { page.addconfig.isDisplayed() }
	    then: 'Add Configuration Popup open'
	    int count = 0
	    while(count<numberOfTrials) {
	        count++
	        try {
	            page.addconfig.click()
	            waitFor(waitTime) { page.overview.exportAddConfigPopupTitle.isDisplayed() }
	            break
	        } catch(geb.waiting.WaitTimeoutException e) {
	        }
	    }
	    
	    page.overview.textType.value("HTTP")
	    
	    waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	            
	    waitFor(waitTime) { page.overview.endpoint.value().equals("endpoint") }
	            
	    waitFor(waitTime) { page.overview.endpointValue.isDisplayed() }
    }
    
    def "Verify Add Configuration for RABBITMQ/Verify Add Configuration for RABBITMQ broker.host"() {
        when: 'Add Configuration is displayed'
	    waitFor(waitTime) { page.addconfig.isDisplayed() }
	    then: 'Add Configuration Popup open'
	    int count = 0
	    while(count<numberOfTrials) {
	        count++
	        try {
	            page.addconfig.click()
	            waitFor(waitTime) { page.overview.exportAddConfigPopupTitle.isDisplayed() }
	            break
	        } catch(geb.waiting.WaitTimeoutException e) {
	        }
	    }
	    
	    page.overview.textType.value("RABBITMQ")
	    
	    waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	            
	    waitFor(waitTime) { page.overview.rabbitMq.value().equals("broker.host") }
	            
	    waitFor(waitTime) { page.overview.rabbitMqValue.isDisplayed() }
    }
    
    def "Verify Add Configuration for RABBITMQ amqp.uri"() {
        when: 'Add Configuration is displayed'
	    waitFor(waitTime) { page.addconfig.isDisplayed() }
	    then: 'Add Configuration Popup open'
	    int count = 0
	    while(count<numberOfTrials) {
	        count++
	        try {
	            page.addconfig.click()
	            waitFor(waitTime) { page.overview.exportAddConfigPopupTitle.isDisplayed() }
	            break
	        } catch(geb.waiting.WaitTimeoutException e) {
	        }
	    }
	    
	    page.overview.textType.value("RABBITMQ")
	    
	    page.overview.rabbitMq.value("amqp.uri")
	    
	    waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	            
	    waitFor(waitTime) { page.overview.rabbitMq.value().equals("amqp.uri") }
	            
	    waitFor(waitTime) { page.overview.rabbitMqValue.isDisplayed() }
    }
    
    def "Verify Add Configuration for CUSTOM"() {
        when: 'Add Configuration is displayed'
	    waitFor(waitTime) { page.addconfig.isDisplayed() }
	    then: 'Add Configuration Popup open'
	    int count = 0
	    while(count<numberOfTrials) {
	        count++
	        try {
	            page.addconfig.click()
	            waitFor(waitTime) { page.overview.exportAddConfigPopupTitle.isDisplayed() }
	            break
	        } catch(geb.waiting.WaitTimeoutException e) {
	        }
	    }
	    
	    page.overview.textType.value("CUSTOM")
	    
	    waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
	    waitFor(waitTime) { page.overview.save.isDisplayed() }
	    waitFor(waitTime) { page.overview.cancel.isDisplayed() }
	            
	    waitFor(waitTime) { page.overview.exportConnectorClass.isDisplayed() }
    }
    
    def "Verify Error messages of Add Configuration for FILE"() {
        when: 'Add Configuration is displayed'
        waitFor(waitTime) { page.addconfig.isDisplayed() }
        then: 'Add Configuration Popup open'
        int count = 0
	    while(count<numberOfTrials) {
	        count++
	        try {
	            page.addconfig.click()
	            waitFor(waitTime) { page.overview.exportAddConfigPopupTitle.isDisplayed() }
	            break
	        } catch(geb.waiting.WaitTimeoutException e) {

	        }
	    }
	    
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
        when: 'Add Configuration is displayed'
        waitFor(waitTime) { page.addconfig.isDisplayed() }
        then: 'Add Configuration Popup open'
        int count = 0
	    while(count<numberOfTrials) {
	        count++
	        try {
	            page.addconfig.click()
	            waitFor(waitTime) { page.overview.exportAddConfigPopupTitle.isDisplayed() }
	            break
	        } catch(geb.waiting.WaitTimeoutException e) {
	        }
	    }
	    
	    page.overview.textType.value("JDBC")
	    
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
        when: 'Add Configuration is displayed'
	    waitFor(waitTime) { page.addconfig.isDisplayed() }
	    then: 'Add Configuration Popup open'
	    int count = 0
	    while(count<numberOfTrials) {
	        count++
	        try {
	            page.addconfig.click()
	            waitFor(waitTime) { page.overview.exportAddConfigPopupTitle.isDisplayed() }
	            break
	        } catch(geb.waiting.WaitTimeoutException e) {
	        }
	    }
	    
	    page.overview.textType.value("KAFKA")
	    
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
        when: 'Add Configuration is displayed'
	    waitFor(waitTime) { page.addconfig.isDisplayed() }
	    then: 'Add Configuration Popup open'
	    int count = 0
	    while(count<numberOfTrials) {
	        count++
	        try {
	            page.addconfig.click()
	            waitFor(waitTime) { page.overview.exportAddConfigPopupTitle.isDisplayed() }
	            break
	        } catch(geb.waiting.WaitTimeoutException e) {
	        }
	    }
	    
	    page.overview.textType.value("HTTP")
	    
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
        when: 'Add Configuration is displayed'
	    waitFor(waitTime) { page.addconfig.isDisplayed() }
	    then: 'Add Configuration Popup open'
	    int count = 0
	    while(count<numberOfTrials) {
	        count++
	        try {
	            page.addconfig.click()
	            waitFor(waitTime) { page.overview.exportAddConfigPopupTitle.isDisplayed() }
	            break
	        } catch(geb.waiting.WaitTimeoutException e) {
	        }
	    }
	    
	    page.overview.textType.value("RABBITMQ")
	    
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
        when: 'Add Configuration is displayed'
	    waitFor(waitTime) { page.addconfig.isDisplayed() }
	    then: 'Add Configuration Popup open'
	    int count = 0
	    while(count<numberOfTrials) {
	        count++
	        try {
	            page.addconfig.click()
	            waitFor(waitTime) { page.overview.exportAddConfigPopupTitle.isDisplayed() }
	            break
	        } catch(geb.waiting.WaitTimeoutException e) {
	        }
	    }
	    
	    page.overview.textType.value("RABBITMQ")
	    
	    page.overview.rabbitMq.value("amqp.uri")
	    
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
        when: 'Add Configuration is displayed'
	    waitFor(waitTime) { page.addconfig.isDisplayed() }
	    then: 'Add Configuration Popup open'
	    int count = 0
	    while(count<numberOfTrials) {
	        count++
	        try {
	            page.addconfig.click()
	            waitFor(waitTime) { page.overview.exportAddConfigPopupTitle.isDisplayed() }
	            break
	        } catch(geb.waiting.WaitTimeoutException e) {
	        }
	    }
	    
	    page.overview.textType.value("CUSTOM")
	    
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
}
