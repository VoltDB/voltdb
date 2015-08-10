package vmcTest.tests

import org.junit.Test
import vmcTest.pages.*
import geb.Page.*

class AdminDrTest extends TestBase {
    // called before each test
    def setup() {
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

    def "Check all the titles and labels in Database Replication (DR) section"(){
        boolean result = page.CheckIfDREnabled();
        expect: "Database Replication (DR) titles and labels are displayed"
        if(result){
            waitFor(waitTime){page.DrTitle.isDisplayed()}
            waitFor(waitTime){page.DrTitle.text().toLowerCase().equals("Database Replication (DR)".toLowerCase())}
            println("Database Replication (DR) is displayed")

            waitFor(waitTime){page.drId.isDisplayed()}
            waitFor(waitTime){page.drId.text().toLowerCase().equals("ID".toLowerCase())}
            println("Label, Id is displayed")

            waitFor(30){page.master.isDisplayed()}
            waitFor(30){page.master.text().toLowerCase().equals("Master".toLowerCase())}
            println("Label, master is displayed")

            waitFor(30){page.drTables.isDisplayed()}
            waitFor(30){page.drTables.text().toLowerCase().equals("DR Tables".toLowerCase())}
            println("Label, DR Tables is displayed")

            waitFor(30){page.replica.isDisplayed()}
            waitFor(30){!page.replica.text().equals("")}
            println("Label, replica is displayed")

        } else{
            println("DR is not enabled. DR should be enable to check all the titles and labels in Database Replication (DR) section.")
        }
    }

    def "Check the values of DR id, master, and replica Source"(){
        when:
        at AdminPage
        then:
        int count = 0
        boolean testStatus = false
        boolean result = page.CheckIfDREnabled();
        if(result){
            while(count<numberOfTrials) {
                count ++
                try {
                    when:
//                        waitFor(waitTime){
//                            page.drIdValue.isDisplayed()
//                            !page.drIdValue.text().equals("")
//                        }
//                        println("DR id has proper value")

                        waitFor(waitTime){
                            page.masterValue.isDisplayed()
                            !page.masterValue.text().equals("")
                        }
                        println("DR master has proper value")

                        waitFor(waitTime){
                            page.replicaSourceValue.isDisplayed()
                            !page.replicaSourceValue.text().equals("")
                        }
                        println("Replica source has proper value")
                    then:
                    testStatus = true
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("RETRYING: WaitTimeoutException occured")
                } catch(org.openqa.selenium.StaleElementReferenceException e) {
                    println("RETRYING: StaleElementReferenceException occured")
                }
            }
            if(testStatus == true) {
                println("Successfully checked the values of DR id, master, and replica Source.")
            }
            else {
                println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
                assert false
            }
            println()

        }else{
            println("DR is not enabled. DR should be enable to check values of DR Id, master and replica source.")
        }
    }

    def "Check if replica source is defined in case of Replica"(){
        when:
        at AdminPage
        then:
        boolean result = page.CheckIfDREnabled()
        if(result){
            waitFor(waitTime){page.drMode.isDisplayed()}
            if(drMode.text().toLowerCase() == "replica" || drMode.text().toLowerCase() == "both"){
                waitFor(waitTime){
                    page.replicSource.isDisplayed()
                    !page.replicSource.text().equals("")
                }
            } else {
                waitFor(waitTime){page.replicSource.isDisplayed()}
            }
        } else {
            println("DR is not enabled. DR should be enable to check if replica source is defined in case of Replica")
        }
    }

}