package vmcTest.tests

import org.junit.Test
import vmcTest.pages.*
import geb.Page.*


class AdminImportTest extends TestBase {
    int insideCount = 0
    boolean loopStatus = false

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

    //String customConnectorClass = page.overview.getCustomConnectorClass()

    def "Check import Click and check its value"() {
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


}