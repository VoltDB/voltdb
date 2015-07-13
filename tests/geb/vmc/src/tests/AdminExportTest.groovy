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
    
    //String customConnectorClass = page.overview.getCustomConnectorClass()
    


    def "Verify Add Configuration for ELASTICSEARCH created"() {
        String elasticSearchTestName     = page.overview.getElasticSearchTestName()
        String elasticSearchValueOne     = page.overview.getElasticSearchValueOne()


        when: 'Open Add ConfigurationPopup'
        page.overview.openAddConfigurationPopup()
        page.overview.textType.value("ELASTICSEARCH")
        then: 'Check elements'
        waitFor(waitTime) { page.overview.addProperty.isDisplayed() }
        waitFor(waitTime) { page.overview.save.isDisplayed() }
        waitFor(waitTime) { page.overview.cancel.isDisplayed() }
        waitFor(waitTime) { page.overview.endpointES.value().equals("endpoint") }
        waitFor(waitTime) { page.overview.endpointESValue.isDisplayed() }

        when: 'Provide values for add configuration'
        page.overview.stream.value(elasticSearchTestName)
        page.overview.endpointESValue.value(elasticSearchValueOne)

        then: 'Click Save'
        page.overview.clickSave()

        when: 'Expand export'
        page.overview.expandExport()
        int count = 0
        then: 'Display the created ELASTICSEARCH'
        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) { page.overview.elasticSearchName.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {}
        }
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
        then: 'Check for deleted'
        try {
            waitFor(waitTime) { page.overview.elasticSearchName.isDisplayed() }
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Configuration deleted")
        }
    }
}
