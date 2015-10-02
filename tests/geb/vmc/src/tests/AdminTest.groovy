/**
 * Created by anrai on 2/12/15.
 */


package vmcTest.tests

import org.junit.Test
import vmcTest.pages.*
import geb.Page.*

/**
 * This class contains tests of the 'Admin' tab of the VoltDB Management
 * Center (VMC) page, which is the VoltDB (new) web UI.
 */

class AdminTest extends TestBase {
    static String initialPrefix
    static String initialFreq
    static String initialFreqUnit
    static String initialRetained

    static String initialHeartTimeout
    static String initialQueryTimeout
    static String initialMemoryLimit = "-1"

    static boolean revertAutosnapshots = false
    static boolean revertHeartTimeout = false
    static boolean revertQueryTimeout = false
    static boolean revertMemorySize =false

	int count = 0
    def setup() { // called before each test
        count = 0

		while(count<numberOfTrials) {
			count ++
			try {
				setup: 'Open VMC page'
				to VoltDBManagementCenterPage
				page.loginIfNeeded()
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


    // DIRECTORIES

    def "check Directories title"() {
        int count = 0
        testStatus = false

       	expect: 'at Admin Page'

        while(count<numberOfTrials) {
        	count ++
        	try {
		    	when:
				waitFor(waitTime) {
				    directories.title.isDisplayed()
				    directories.title.text().toLowerCase().equals("Directories".toLowerCase())
				}
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
        	println("PASS")
        }
        else {
        	println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
        	assert false
        }
        println()
    }

    def "check Root title"() {
        int count = 0
        testStatus = false

       	expect: 'at Admin Page'

        while(count<numberOfTrials) {
        	count ++
        	try {
		    	when:
				waitFor(waitTime) {
				    directories.rootTitle.isDisplayed()
            		directories.rootTitle.text().toLowerCase().equals("Root (Destination)".toLowerCase())
				}
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
        	println("PASS")
        }
        else {
        	println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
        	assert false
        }
        println()
    }

    def "check Snapshot title"() {
        int count = 0
        testStatus = false

       	expect: 'at Admin Page'

        while(count<numberOfTrials) {
        	count ++
        	try {
		    	when:
				waitFor(waitTime) {
				   	directories.snapshotTitle.isDisplayed()
           			directories.snapshotTitle.text().toLowerCase().equals("Snapshot".toLowerCase())
				}
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
        	println("PASS")
        }
        else {
        	println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
        	assert false
        }
        println()
    }

    def "check Export Overflow title"() {
        int count = 0
        testStatus = false

       	expect: 'at Admin Page'

        while(count<numberOfTrials) {
        	count ++
        	try {
		    	when:
				waitFor(waitTime) {
				   	directories.exportOverflowTitle.isDisplayed()
            		directories.exportOverflowTitle.text().toLowerCase().equals("Export Overflow".toLowerCase())
				}
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
        	println("PASS")
        }
        else {
        	println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
        	assert false
        }
        println()
    }

    def "check Command Logs title"() {
        int count = 0
        testStatus = false

       	expect: 'at Admin Page'

        while(count<numberOfTrials) {
        	count ++
        	try {
		    	when:
				waitFor(waitTime) {
				   	directories.commandLogsTitle.isDisplayed()
            		directories.commandLogsTitle.text().toLowerCase().equals("Command Log".toLowerCase())
				}
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
        	println("PASS")
        }
        else {
        	println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
        	assert false
        }
        println()
    }

    def "check Command Log Snapshots title"() {
        int count = 0
        testStatus = false

       	expect: 'at Admin Page'

        while(count<numberOfTrials) {
        	count ++
        	try {
		    	when:
				waitFor(waitTime) {
				   	directories.commandLogSnapshotTitle.isDisplayed()
           			directories.commandLogSnapshotTitle.text().toLowerCase().equals("Dr Overflow".toLowerCase())
				}
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
        	println("PASS")
        }
        else {
        	println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
        	assert false
        }
        println()
    }

    def "check DR Overflow title"() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    directories.drOverflowTitle.isDisplayed()
                    directories.drOverflowTitle.text().toLowerCase().equals("DR Overflow".toLowerCase())
                }
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
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def "check Root Value not empty"() {
        int count = 0
        testStatus = false

       	expect: 'at Admin Page'

        while(count<numberOfTrials) {
        	count ++
        	try {
		    	when:
				waitFor(waitTime) {
				   	directories.rootValue.isDisplayed()
            		!directories.rootValue.text().equals("")
				}
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
        	println("PASS")
        }
        else {
        	println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
        	assert false
        }
        println()
    }

    def "check SnapShot Value not empty"() {
        int count = 0
        testStatus = false

       	expect: 'at Admin Page'

        while(count<numberOfTrials) {
        	count ++
        	try {
		    	when:
				waitFor(waitTime) {
				   	directories.snapshotValue.isDisplayed()
            		!directories.snapshotValue.text().equals("")
				}
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
        	println("PASS")
        }
        else {
        	println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
        	assert false
        }
        println()
    }

    def "check Export Overflow Value not empty"() {
        int count = 0
        testStatus = false

       	expect: 'at Admin Page'

        while(count<numberOfTrials) {
        	count ++
        	try {
		    	when:
				waitFor(waitTime) {
				   	directories.exportOverflowValue.isDisplayed()
            		!directories.exportOverflowValue.text().equals("")
				}
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
        	println("PASS")
        }
        else {
        	println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
        	assert false
        }
        println()
    }

    def "check Command Logs Value not empty"() {
        int count = 0
        testStatus = false

       	expect: 'at Admin Page'

        while(count<numberOfTrials) {
        	count ++
        	try {
		    	when:
				waitFor(waitTime) {
				   	directories.commandLogsValue.isDisplayed()
           	 		!directories.commandLogsValue.text().equals("")
				}
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
        	println("PASS")
        }
        else {
        	println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
        	assert false
        }
        println()
    }

    def "check Log Snapshot Value not empty"() {
        int count = 0
        testStatus = false

       	expect: 'at Admin Page'

        while(count<numberOfTrials) {
        	count ++
        	try {
		    	when:
				waitFor(waitTime) {
				   	directories.commandLogSnapshotValue.isDisplayed()
            		!directories.commandLogSnapshotValue.text().equals("")
				}
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
        	println("PASS")
        }
        else {
        	println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
        	assert false
        }
        println()
    }

    def "check DR Overflow Value not empty"() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    directories.drOverflowValue.isDisplayed()
                    !directories.drOverflowValue.text().equals("")
                }
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
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    // OVERVIEW

    def "check title"() {
        int count = 0
        testStatus = false

       	expect: 'at Admin Page'

        while(count<numberOfTrials) {
        	count ++
        	try {
		    	when:
				waitFor(waitTime) {
				   	overview.title.isDisplayed()
            		overview.title.text().toLowerCase().equals("Overview".toLowerCase())
				}
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
        	println("PASS")
        }
        else {
        	println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
        	assert false
        }
        println()
    }

    def "check Site Per Host"() {
        int count = 0
        testStatus = false

       	expect: 'at Admin Page'

        while(count<numberOfTrials) {
        	count ++
        	try {
		    	when:
				waitFor(waitTime) {
				   	overview.sitePerHost.isDisplayed()
           	 		overview.sitePerHost.text().toLowerCase().equals("Sites Per Host".toLowerCase())
				}
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
        	println("PASS")
        }
        else {
        	println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
        	assert false
        }
        println()
    }

    def "check K-safety"() {
        int count = 0
        testStatus = false

       	expect: 'at Admin Page'

        while(count<numberOfTrials) {
        	count ++
        	try {
		    	when:
				waitFor(waitTime) {
				   	overview.ksafety.isDisplayed()
            		overview.ksafety.text().toLowerCase().equals("K-safety".toLowerCase())
				}
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
        	println("PASS")
        }
        else {
        	println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
        	assert false
        }
        println()
    }

    def "check Partition Detection"() {
        int count = 0
        testStatus = false

       	expect: 'at Admin Page'

        while(count<numberOfTrials) {
        	count ++
        	try {
		    	when:
				waitFor(waitTime) {
				   	overview.partitionDetection.isDisplayed()
            		overview.partitionDetection.text().toLowerCase().equals("Partition detection".toLowerCase())
				}
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
        	println("PASS")
        }
        else {
        	println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
        	assert false
        }
        println()
    }

    def "check Security"() {
        int count = 0
        testStatus = false

       	expect: 'at Admin Page'

        while(count<numberOfTrials) {
        	count ++
        	try {
		    	when:
				waitFor(waitTime) {
				   	overview.security.isDisplayed()
            		overview.security.text().toLowerCase().equals("Security".toLowerCase())
				}
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
        	println("PASS")
        }
        else {
        	println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
        	assert false
        }
        println()
    }

    def "check HTTP Access"() {
        int count = 0
        testStatus = false

       	expect: 'at Admin Page'

        while(count<numberOfTrials) {
        	count ++
        	try {
		    	when:
				waitFor(waitTime) {
				   	overview.httpAccess.isDisplayed()
            		overview.httpAccess.text().toLowerCase().equals("HTTP Access".toLowerCase())
				}
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
        	println("PASS")
        }
        else {
        	println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
        	assert false
        }
        println()
    }

    def "check Auto Snapshots"() {
        int count = 0
        testStatus = false

       	expect: 'at Admin Page'

        while(count<numberOfTrials) {
        	count ++
        	try {
		    	when:
				waitFor(waitTime) {
				   	overview.autoSnapshots.isDisplayed()
            		overview.autoSnapshots.text().toLowerCase().equals("Auto Snapshots".toLowerCase())
				}
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
        	println("PASS")
        }
        else {
        	println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
        	assert false
        }
        println()
    }

    def "check Command Logging"() {
        int count = 0
        testStatus = false

       	expect: 'at Admin Page'

        while(count<numberOfTrials) {
        	count ++
        	try {
		    	when:
				waitFor(waitTime) {
				   	overview.commandLogging.isDisplayed()
            		overview.commandLogging.text().toLowerCase().equals("Command Logging".toLowerCase())
				}
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
        	println("PASS")
        }
        else {
        	println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
        	assert false
        }
        println()
    }

    def "check Export"() {
        int count = 0
        testStatus = false

       	expect: 'at Admin Page'

        while(count<numberOfTrials) {
        	count ++
        	try {
		    	when:
				waitFor(waitTime) {
				   	overview.export.isDisplayed()
            		overview.export.text().toLowerCase().equals("Export".toLowerCase())
				}
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
        	println("PASS")
        }
        else {
        	println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
        	assert false
        }
        println()
    }

    //values

    def "check Site Per Host value"() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            overview.sitePerHostValue.isDisplayed()
            !overview.sitePerHostValue.text().equals("")
        }
    }

    def "check K-safety value"() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            overview.ksafetyValue.isDisplayed()
            !overview.ksafetyValue.text().equals("")
        }
    }

    def "check Partition Detection value"() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            overview.partitionDetectionValue.isDisplayed()
            !overview.partitionDetectionValue.text().equals("")
        }
    }

    def "check Security value"() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            overview.securityValue.isDisplayed()
            !overview.securityValue.text().equals("")
        }
    }

    def "check HTTP Access value"() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            overview.httpAccessValue.isDisplayed()
            !overview.httpAccessValue.text().equals("")
        }
    }

    def "check Auto Snapshots value"() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            overview.autoSnapshotsValue.isDisplayed()
            !overview.autoSnapshotsValue.text().equals("")
        }
    }

    def "check Command Logging value"() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            overview.commandLoggingValue.isDisplayed()
            !overview.commandLoggingValue.text().equals("")
        }
    }

//    // SECURITY
//
    def "click security button"(){


        when:
        at AdminPage

        then:
        if(page.overview.getListOfUsers()!="")
        {
            try {
                page.securityEdit.click()
                page.securityEditOk.isDisplayed()
                page.securityEditCancel.isDisplayed()
            }
            catch(geb.waiting.WaitTimeoutException e){
                println("Security Edit cannot be displayed")
            }
            catch(org.openqa.selenium.ElementNotVisibleException e)
            {
                println("Security Edit cannot be displayed")
            }
        }
        else
        {
            println("Atleast one security credential should be added first")
        }
        println("Security Edit is Disabled")

    }

    def "click security edit button and cancel"(){
        when:
        at AdminPage
        then:

        try {
            page.securityEdit.click()
            page.securityEditOk.isDisplayed()
            page.securityEditCancel.isDisplayed()
            page.securityEditCancel.click()
            println("security edit canceled!")
            page.securityEdit.isDisplayed()
        }
        catch(geb.waiting.WaitTimeoutException e){
            println("Security Edit cannot be displayed")
        }
        catch(org.openqa.selenium.ElementNotVisibleException e)
        {
            println("Security Edit cannot be displayed")
        }
    }

    def "click security edit button and cancel popup"(){
        when:
        at AdminPage
        then:
        try {
            page.securityEdit.click()
            page.securityEditOk.isDisplayed()
            page.securityEditCancel.isDisplayed()
            page.securityEditOk.click()
            println("security edit ok clicked!")
            waitFor(waitTime) {
                //  page.securityPopup.isDisplayed()
                page.securityPopupOk.isDisplayed()
                page.securityPopupCancel.isDisplayed()
                page.securityPopupCancel.click()
                println("cancel clicked")
                page.securityEdit.isDisplayed()


            }
        }
        catch(geb.waiting.WaitTimeoutException e){
            println("Security Edit cannot be displayed")
        }
        catch(org.openqa.selenium.ElementNotVisibleException e)
        {
            println("Security Edit cannot be displayed")
        }
    }


    def "click security edit button and ok and ok"(){
        when:
        at AdminPage

        then:

        try {
            page.securityEdit.click()
            page.securityEditOk.isDisplayed()
            page.securityEditCancel.isDisplayed()
            page.securityEditOk.click()
            println("security edit ok clicked!")

            waitFor(waitTime) {
                page.securityPopupOk.isDisplayed()
                page.securityPopupCancel.isDisplayed()
                page.securityPopupOk.click()
            }
        }
        catch(geb.waiting.WaitTimeoutException e){
            println("Security Edit cannot be displayed")
        }
        catch(org.openqa.selenium.ElementNotVisibleException e)
        {
            println("Security Edit cannot be displayed")
        }

    }

    // autosnapshot
    def "check Auto Snapshots edit"() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){ page.autoSnapshotsEdit.isDisplayed() }
        String string = page.autoSnapshotsEdit.text()
        !(string.equals(""))
    }


    def "click edit Auto Snapshots and check"() {
        when:
        at AdminPage
        then:
        waitFor(waitTime) {
            page.autoSnapshotsEdit.isDisplayed()
        }
        page.autoSnapshotsEdit.click()

        waitFor(waitTime) {
            page.autoSnapshotsEditCheckbox.isDisplayed()
            page.autoSnapshotsEditOk.isDisplayed()
            page.autoSnapshotsEditCancel.isDisplayed()

        }
        waitFor(waitTime){

            page.frequencyEdit.isDisplayed()
            //println("first wait")
            page.retainedEdit.isDisplayed()
            page.fileprefixEdit.isDisplayed()


        }
    }


    def "click Auto Snapshot edit and click cancel"() {
        when:
        at AdminPage
        then:
        waitFor(waitTime) {
            page.autoSnapshotsEdit.isDisplayed()
        }

        when:
        page.autoSnapshotsEdit.click()
        then:
        waitFor(waitTime) {
            page.autoSnapshotsEditOk.isDisplayed()
            page.autoSnapshotsEditCancel.isDisplayed()
        }

        when:
        page.autoSnapshotsEditCancel.click()
        then:
        waitFor(waitTime) {
            !(page.autoSnapshotsEditCancel.isDisplayed())
            !(page.autoSnapshotsEditOk.isDisplayed())
        }
    }

    def "click Auto Snapshots edit and click checkbox to change on off"() {
        when:
        at AdminPage
        then:
        waitFor(waitTime) {
            page.autoSnapshotsEdit.isDisplayed()
        }

        when:
        page.autoSnapshotsEdit.click()
        String enabledDisabled = page.autoSnapshotsValue.text()
        println(enabledDisabled)
        then:
        waitFor(waitTime){
            page.autoSnapshotsEditCheckbox.isDisplayed()
            page.autoSnapshotsEditOk.isDisplayed()
            page.autoSnapshotsEditCancel.isDisplayed()
        }

        when:
        page.autoSnapshotsEditCheckbox1.click()
        then:
        String enabledDisabledEdited = page.autoSnapshotsValue.text()
        println(enabledDisabledEdited)

        if ( enabledDisabled.toLowerCase() == "on" ) {
            assert enabledDisabledEdited.toLowerCase().equals("off")
        }
        else if ( enabledDisabled.toLowerCase() == "off" ) {
            assert enabledDisabledEdited.toLowerCase().equals("on")
        }

    }

    def "click edit and cancel to check popup"() {


        String title			= "Auto Snapshots"
        String display			= "Do you want to save the value?"
        String ok				= "Ok"
        String cancel			= "Cancel"

        when:
        at AdminPage
        then:
        waitFor(waitTime) {
            page.autoSnapshotsEdit.isDisplayed()
            page.autoSnapshotsValue.isDisplayed()
        }

        when:
        page.autoSnapshotsEdit.click()
        String string = page.autoSnapshotsValue.text()
        then:
        waitFor(waitTime) {
            page.autoSnapshotsEditCheckbox.isDisplayed()
            page.autoSnapshotsEditOk.isDisplayed()
            page.autoSnapshotsEditCancel.isDisplayed()
        }

        //assert withConfirm(true) { page.autoSnapshotsEditOk.click() } == "Do you want to save the value?"
        page.autoSnapshotsEditOk.click()
        when:

        page.autoSnapshotsEditCancel.click()
        println("cancel clicked successfully")
        then:
        waitFor(waitTime) {
            page.autoSnapshotsEdit.isDisplayed()
        }


    }

    def "click edit and ok to check popup"() {
        String prefix 			= "SNAPSHOTNONCE"
        String frequency 		= "10"
        String frequencyUnit		= "Hrs"
        String retained 		= "1"

        String title			= "Auto Snapshots"
        String display			= "Do you want to save the value?"
        String ok				= "Ok"
        String cancel			= "Cancel"

        when:
        at AdminPage
        page.autoSnapshots.click()
        waitFor(waitTime) {
            page.filePrefix.isDisplayed()
            page.frequency.isDisplayed()
            page.frequencyUnit.isDisplayed()
            page.retained.isDisplayed()
        }
        initialPrefix 	= page.filePrefix.text()
        initialFreq		= page.frequency.text()
        initialFreqUnit	= page.frequencyUnit.text()
        initialRetained	= page.retained.text()

        then:
        waitFor(waitTime) {
            page.autoSnapshotsEdit.isDisplayed()
            page.autoSnapshotsValue.isDisplayed()
            initialFreq
        }

        when:
        page.autoSnapshotsEdit.click()
        then:
        waitFor(waitTime) {
            page.autoSnapshotsEditCheckbox.isDisplayed()
            page.autoSnapshotsEditOk.isDisplayed()
            page.autoSnapshotsEditCancel.isDisplayed()
        }

        page.filePrefixField.value(prefix)
        page.frequencyField.value(frequency)
        page.frequencyUnitField.click()
        page.frequencyUnitField.value(frequencyUnit)
        page.retainedField.value(retained)

        if(page.fileprefixEdit.text() != " "){
            println("fileprefix passed, found non-empty")

            if(
            page.frequencyEdit.text() != " "){
                println("frequency passed, found non-empty")}
            // page.frequencyUnitField.click()
            if( page.frequencyUnitField.text() != " "){
                println("frequency unit passed, found non-empty")}
            if ( page.retainedEdit.text() != " "){
                println("retained passed, found non-empty")}
        }
        page.autoSnapshotsEditOk.click()
        println("pop up visible")

        when:

        while(true) {
            page.autosnapshotsconfirmok.click()
            println("inside ok clicked successfully")
            if(page.filePrefixField.isDisplayed()== false)
                break
        }

        then:
        int count = 0
        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.filePrefix.text().equals(prefix)
                    page.frequency.text().equals(frequency)
                    page.frequencyUnit.text().equals(frequencyUnit)
                    page.retained.text().equals(retained)
                    page.filePrefix.isDisplayed()
                }
                break
            } catch (geb.waiting.WaitTimeoutException e) {
                println("Try")
            }
        }

    }


    // NETWORK INTERFACES


    def "check Network Interfaces title"() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.networkInterfaces.title.isDisplayed()
                    page.networkInterfaces.title.text().toLowerCase().equals("Network Interfaces".toLowerCase())
                }
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
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def "check Port Name title"() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'
        at AdminPage

//        when:
//        waitFor(20) {
//            page.networkInterfaces.portNameTitle.isDisplayed()
//        }
//        then:
//        page.networkInterfaces.portNameTitle.text().equals("Port Name")
//        println("Test case passed")

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.networkInterfaces.portNameTitle.isDisplayed()
                    page.networkInterfaces.portNameTitle.text().equals("Port Name")
                }
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
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }

    }
//
//
    def "check Cluster Setting title"() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'
        at AdminPage

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.networkInterfaces.clusterSettingTitle.isDisplayed()
                    page.networkInterfaces.clusterSettingTitle.text().toLowerCase().equals("Cluster Settings".toLowerCase())
                }
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
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def "check Server Setting title"() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'
        at AdminPage
        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.networkInterfaces.serverSettingTitle.isDisplayed()
                    page.networkInterfaces.serverSettingTitle.text().toLowerCase().equals("Server Settings".toLowerCase())
                }
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
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def "check Client Port title"() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.networkInterfaces.clientPortTitle.isDisplayed()
                    page.networkInterfaces.clientPortTitle.text().toLowerCase().equals("Client Port".toLowerCase())
                }
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
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def "check Admin Port title"() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.networkInterfaces.adminPortTitle.isDisplayed()
                    page.networkInterfaces.adminPortTitle.text().toLowerCase().equals("Admin Port".toLowerCase())
                }
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
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def "check HTTP Port title"() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'


        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.networkInterfaces.httpPortTitle.isDisplayed()
                    page.networkInterfaces.httpPortTitle.text().toLowerCase().equals("HTTP Port".toLowerCase())
                }
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
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def "check Internal Port title"() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.networkInterfaces.internalPortTitle.isDisplayed()
                    page.networkInterfaces.internalPortTitle.text().toLowerCase().equals("Internal Port".toLowerCase())
                }
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
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def "check Zookeeper Port title"() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.networkInterfaces.zookeeperPortTitle.isDisplayed()
                    page.networkInterfaces.zookeeperPortTitle.text().toLowerCase().equals("Zookeeper Port".toLowerCase())
                }
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
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    def "check Replication Port title"() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.networkInterfaces.replicationPortTitle.isDisplayed()
                    page.networkInterfaces.replicationPortTitle.text().toLowerCase().equals("Replication Port".toLowerCase())
                }
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
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
        println()
    }

    // value

    def "check Client Port Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            page.networkInterfaces.clusterClientPortValue.isDisplayed()
            !page.networkInterfaces.clusterClientPortValue.text().equals("")
        }
    }

    def "check Admin Port Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            page.networkInterfaces.clusterAdminPortValue.isDisplayed()
            !page.networkInterfaces.clusterAdminPortValue.text().equals("")
        }
    }

    def "check HTTP Port Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            page.networkInterfaces.clusterHttpPortValue.isDisplayed()
            !page.networkInterfaces.clusterHttpPortValue.text().equals("")
        }
    }

    def "check Internal Port Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(waitTime) {
            page.networkInterfaces.clusterInternalPortValue.isDisplayed()
            !page.networkInterfaces.clusterInternalPortValue.text().equals("")
        }
    }

    def "check Zookeeper Port Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            page.networkInterfaces.clusterZookeeperPortValue.isDisplayed()
            !page.networkInterfaces.clusterZookeeperPortValue.text().equals("")
        }
    }

    def "check Replication Port Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(waitTime){
            page.networkInterfaces.clusterReplicationPortValue.isDisplayed()
            !page.networkInterfaces.clusterReplicationPortValue.text().equals("")
        }
    }

    // HEADER TESTS

    def "header banner exists" () {
        when:
        at AdminPage
        then:
        waitFor(waitTime) { header.banner.isDisplayed() }
    }


    def "header image exists" () {
        when:
        at AdminPage
        then:
        waitFor(waitTime) { header.image.isDisplayed() }
    }

    def "header username exists" () {
        when:
        at AdminPage
        then:
        waitFor(waitTime) { header.usernameInHeader.isDisplayed() }
    }

    def "header logout exists" () {
        when:'Check Security Enabled'
        at AdminPage
        waitFor(waitTime) { page.overview.securityValue.isDisplayed() }
        then:
        if(page.overview.securityValue.text().equals("Off"))
        {
            println("PASS")
        }
        else if (page.overview.securityValue.text().equals("On"))
        {
            println("fail")
            page.overview.securityValue.text().equals("On")
            println("test" + page.overview.securityValue.text())


            waitFor(waitTime) { header.logout.isDisplayed() }
        }




    }

    def "header help exists" () {
        when:
        at AdminPage
        then:
        waitFor(30) { page.header.help.isDisplayed() }
        int count = 0
        while(count<5) {
            count++
            try {
                interact {
                    moveToElement(page.header.help)
                }
                waitFor(30) { page.header.showHelp.isDisplayed() }
                break
            } catch (geb.waiting.WaitTimeoutException e) {
                println("Already tried")
            }
        }
    }

    // HEADER TAB TESTS

    def "header tab dbmonitor exists" () {
        when:
        at AdminPage
        then:
        waitFor(waitTime) {
            header.tabDBMonitor.isDisplayed()
            header.tabDBMonitor.text().toLowerCase().equals("DB Monitor".toLowerCase())
        }
    }

    def "header tab admin exists" () {
        when:
        at AdminPage
        then:
        waitFor(waitTime) {
            header.tabAdmin.isDisplayed()
            header.tabAdmin.text().toLowerCase().equals("Admin".toLowerCase())
        }
    }

    def "header tab schema exists" () {
        when:
        at AdminPage
        then:
        waitFor(waitTime) {
            header.tabSchema.isDisplayed()
            header.tabSchema.text().toLowerCase().equals("Schema".toLowerCase())

        }
    }

    def "header tab sql query exists" () {
        when:
        at AdminPage
        then:
        waitFor(waitTime) { header.tabSQLQuery.isDisplayed()
            header.tabSQLQuery.text().toLowerCase().equals("SQL Query".toLowerCase())
        }
    }

    def "header username check" () {


        when:'Check Security Enabled'
        at AdminPage
        waitFor(waitTime) { page.overview.securityValue.isDisplayed() }
        then:
        if(page.overview.securityValue.text().equals("Off"))
        {
            println("PASS")
        }
        else if (page.overview.securityValue.text().equals("On"))
        {
            waitFor(waitTime) {
                header.usernameInHeader.isDisplayed()
                header.usernameInHeader.text().equals(username)
            }
        }
    }

    def "header username click and close" () {
        when:
        at AdminPage
        then:
        waitFor(waitTime) { header.usernameInHeader.isDisplayed() }
        header.usernameInHeader.click()
        waitFor(waitTime) {
            header.logoutPopupOkButton.isDisplayed()
            header.logoutPopupCancelButton.isDisplayed()
            header.popupClose.isDisplayed()
        }
        header.popupClose.click()
    }

    def "header username click and cancel" () {
        when:
        at AdminPage
        then:
        waitFor(waitTime) { header.usernameInHeader.isDisplayed() }
        header.usernameInHeader.click()
        waitFor(waitTime) {
            header.logoutPopupOkButton.isDisplayed()
            header.logoutPopupCancelButton.isDisplayed()
            header.popupClose.isDisplayed()
        }
        header.logoutPopupCancelButton.click()
    }


    // LOGOUT TEST

    def "logout button test close" ()  {
        when:'Check Security Enabled'
        at AdminPage
        waitFor(waitTime) { page.overview.securityValue.isDisplayed() }
        then:
        if(page.overview.securityValue.text().equals("Off"))
        {
            println("PASS")
        }
        else if (page.overview.securityValue.text().equals("On"))
        {
            waitFor(waitTime) { header.logout.isDisplayed() }
            header.logout.click()
            waitFor(waitTime) {
                header.logoutPopupOkButton.isDisplayed()
                header.logoutPopupCancelButton.isDisplayed()
                header.popupClose.isDisplayed()
            }
            header.popupClose.click()
        }
    }

    def "logout button test cancel" ()  {
        when:'Check Security Enabled'
        at AdminPage
        waitFor(waitTime) { page.overview.securityValue.isDisplayed() }
        then:
        if(page.overview.securityValue.text().equals("Off"))
        {
            println("PASS")
        }
        else if (page.overview.securityValue.text().equals("On"))
        {
            waitFor(waitTime) { header.logout.isDisplayed() }
            header.logout.click()
            waitFor(waitTime) {
                header.logoutPopupOkButton.isDisplayed()
                header.logoutPopupCancelButton.isDisplayed()
                header.popupClose.isDisplayed()
            }
            header.logoutPopupCancelButton.click()
        }
    }

    // HELP POPUP TEST

    def "help popup existance" () {
        when:
        at AdminPage
        then:
        waitFor(waitTime) { page.header.help.isDisplayed() }
        int count = 0
        while(count<5) {
            count++
            try {
                interact {
                    moveToElement(page.header.help)
                }
                waitFor(30) { page.header.showHelp.isDisplayed() }
                break
            } catch (geb.waiting.WaitTimeoutException e) {
                println("Already tried")
            }
        }

        when:
        page.header.showHelp.click()
        then:
        waitFor(waitTime) { page.header.popupClose.isDisplayed() }
        waitFor(waitTime) { page.header.popupTitle.text().toLowerCase().contains("help".toLowerCase()) }
    }

    // FOOTER TESTS

    def "footer exists" () {
        when:
        at AdminPage
        then:
        waitFor(waitTime) { footer.banner.isDisplayed() }
    }

    def "footer text exists and valid"() {
        when:
        at AdminPage
        then:
        waitFor(waitTime) {
            footer.banner.isDisplayed()
            footer.text.isDisplayed()
            footer.text.text().toLowerCase().contains("VoltDB. All rights reserved.".toLowerCase())
        }
    }

    //

    //download automation test
    def "check download configuration and verify text"() {

        when:
        at AdminPage

        waitFor(waitTime) { 	page.downloadconfigurationbutton.isDisplayed() }
        println("downloadbutton seen")
        then:

        page.downloadconfigurationbutton.text().toLowerCase().equals("Download Configuration".toLowerCase())
        println("download configuration button text has verified,\n click cannot be performed in firefox")
        //page.downloadconfigurationbutton.click()



    }

    //CLUSTER
    def "cluster title"(){
        when:
        at AdminPage
        waitFor(waitTime) { cluster.clusterTitle.isDisplayed() }
        then:
        cluster.clusterTitle.text().equals("Cluster")
    }


    def "check promote button"(){
        when:
        at AdminPage
        then:
        waitFor(waitTime) { cluster.promotebutton.isDisplayed() }
    }

    def "check pause cancel"(){
        boolean result = false
        int count = 0
        when:
        at AdminPage
        try {
            waitFor(waitTime) { page.cluster.resumebutton.isDisplayed() }
            println("Resume button is displayed")
            result = false
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Resume button is not displayed")
            result = true
        }

        if (result == false) {
            println("Resume VMC")

            try {
                page.cluster.resumebutton.click()
                waitFor(waitTime) { page.cluster.resumeok.isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Error: Resume confirmation was not found")
                assert false
            }

            try {
                page.cluster.resumeok.click()
                waitFor(waitTime) { page.cluster.pausebutton.isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Error: Pause button was not found")
                assert false
            }
        }
        then:
        println()

        when:
        count = 0
        while(count<numberOfTrials) {
            count ++
            try {
                page.cluster.pausebutton.click()
                waitFor(waitTime) { page.cluster.pauseok.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }

        count = 0
        while(count<numberOfTrials) {
            count ++
            try {
                page.cluster.pausecancel.click()
                waitFor(waitTime) { page.cluster.pausebutton.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        then:
        println()

        when:
        if (result == false) {
            println("Pause VMC")

            try {
                page.cluster.pausebutton.click()
                waitFor(waitTime) { page.cluster.pauseok.isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Error: Pause confirmation was not found")
                assert false
            }

            try {
                page.cluster.pauseok.click()
                waitFor(waitTime) { page.cluster.resumebutton.isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Error: Resume button was not found")
                assert false
            }
        }
        then:
        println()

    }

    def "check pause and verify resume too"(){
        boolean result = false
        int count = 0
        when:
        at AdminPage
        try {
            waitFor(waitTime) { page.cluster.resumebutton.isDisplayed() }
            println("Resume button is displayed")
            result = false
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Resume button is not displayed")
            result = true
        }

        if (result == false) {
            println("Resume VMC")

            count = 0
            while(count<numberOfTrials) {
                try {
                    count++
                    page.cluster.resumebutton.click()
                    waitFor(waitTime) { page.cluster.resumeok.isDisplayed() }
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("Error: Resume confirmation was not found")
                    assert false
                }
            }

            count = 0
            while(count<numberOfTrials) {
                try {
                    count++
                    page.cluster.resumeok.click()
                    waitFor(waitTime) { page.cluster.pausebutton.isDisplayed() }
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("Error: Pause button was not found")
                    assert false
                }
            }
        }
        then:
        println()

        when:
        count = 0
        while(count<numberOfTrials) {
            count ++
            try {
                page.cluster.pausebutton.click()
                waitFor(waitTime) { page.cluster.pauseok.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }

        count = 0
        while(count<numberOfTrials) {
            count ++
            try {
                page.cluster.pauseok.click()
                waitFor(waitTime) { page.cluster.resumebutton.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }

        count = 0
        while(count<numberOfTrials) {
            count ++
            try {
                page.cluster.resumebutton.click()
                waitFor(waitTime) { page.cluster.resumeok.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }

        count = 0
        while(count<numberOfTrials) {
            count ++
            try {
                page.cluster.resumeok.click()
                waitFor(waitTime) { page.cluster.pausebutton.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
        then:
        println()

        when:
        if (result == false) {
            println("Pause VMC")

            count = 0
            while(count<numberOfTrials) {
                try {
                    count++
                    page.cluster.pausebutton.click()
                    waitFor(waitTime) { page.cluster.pauseok.isDisplayed() }
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("Error: Pause confirmation was not found")
                    assert false
                }
            }

            count = 0
            while(count<numberOfTrials) {
                try {
                    count++
                    page.cluster.pauseok.click()
                    waitFor(waitTime) { page.cluster.resumebutton.isDisplayed() }
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("Error: Resume button was not found")
                    assert false
                }
            }
        }
        then:
        println()
    }



    def "when save and cancel popup"(){
        when:
        at AdminPage
        waitFor(waitTime) { cluster.savebutton.isDisplayed() }
        cluster.savebutton.click()
        then:
        waitFor(waitTime) { cluster.saveconfirmation.isDisplayed() }
        cluster.saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
        cluster.savecancel.click()
    }


    def "when save in empty path"(){
        String emptyPath = page.getEmptyPath()
        when:

        at AdminPage
        waitFor(waitTime) { cluster.savebutton.isDisplayed() }
        cluster.savebutton.click()
        then:
        waitFor(waitTime) { cluster.saveconfirmation.isDisplayed() }
        cluster.saveconfirmation.text().toLowerCase().equals("Save".toLowerCase())
        cluster.savedirectory.value(emptyPath)
        cluster.saveok.click()
        cluster.saveerrormsg.isDisplayed()
        cluster.saveerrormsg.text().toLowerCase().equals("Please enter a valid directory path.".toLowerCase())
        println("error message verified")


    }

    def "when save for invalid path"(){
        String invalidPath = page.getInvalidPath()

        when:

        at AdminPage
        waitFor(waitTime) { cluster.savebutton.isDisplayed() }
        cluster.savebutton.click()
        then:
        waitFor(waitTime) { cluster.saveconfirmation.isDisplayed() }
        cluster.saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
        cluster.savedirectory.value(invalidPath)
        cluster.saveok.click()
        waitFor(waitTime){cluster.failedsaveok.isDisplayed()}
        cluster.failedsaveok.click()
        println("error location for saving verified")


    }


    def "when save succeeded"(){
        String validPath = page.getValidPath()

        when:
        at AdminPage
        waitFor(waitTime) { cluster.savebutton.isDisplayed() }
        cluster.savebutton.click()
        then:
        waitFor(waitTime) { cluster.saveconfirmation.isDisplayed() }
        cluster.saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
        cluster.savedirectory.value(validPath)
        cluster.saveok.click()
        waitFor(waitTime){cluster.savesuccessok.isDisplayed()}
        cluster.savesuccessok.click()
        println("save succeeded and clicked!!")
    }



    def "when restore button clicked and cancel popup"(){
        String validPath = page.getValidPath()
        when:
        at AdminPage
        waitFor(waitTime) {   cluster.restorebutton.isDisplayed()
            cluster.restorestatus.isDisplayed()
            cluster.restorebutton.click()
        }

        println("restore button clicked")

        then:
        waitFor(waitTime) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase());
        cluster.restoredirectory.value(validPath)
        cluster.restoresearch.click()
        waitFor(waitTime){cluster.restorecancelbutton.isDisplayed()}
        cluster.restorecancelbutton.click()
    }


    def "when restore button clicked and close popup"(){
        when:
        at AdminPage
        waitFor(waitTime) { cluster.restorebutton.isDisplayed()
            cluster.restorestatus.isDisplayed()
            cluster.restorebutton.click()
        }

        println("restore clicked")

        then:
        waitFor(waitTime) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase())
        cluster.restoreclosebutton.click()

    }

    def "when restore clicked and search failed"(){
        String invalidPath = page.getInvalidPath()
        when:
        at AdminPage
        waitFor(waitTime) { cluster.restorebutton.isDisplayed()
            cluster.restorestatus.isDisplayed()
            cluster.restorebutton.click()
        }

        println("restore clicked")

        then:
        waitFor(waitTime) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase())

        // FOR UAT TESTING ENABLE BELOW CODE
        //    waitFor(waitTime){cluster.restoresearch.isDisplayed()
        //    cluster.restoredirectory.isDisplayed()}
        //     cluster.restoredirectory.value(invalidPath)
        //      cluster.restoresearch.click()
        //   if(waitFor(waitTime){cluster.restoreerrormsg.isDisplayed()}){
        //       cluster.restoreerrormsg.text().toLowerCase().equals("Error: Failure getting snapshots.Path is not a directory".toLowerCase())
        //      println("error message for restore search verified!!")}

    }

    def "when search button clicked in empty path of Restore"(){
        String emptyPath = page.getEmptyPath()
        when:
        waitFor(waitTime) { cluster.restorebutton.isDisplayed()
            cluster.restorestatus.isDisplayed()
            cluster.restorebutton.click()
        }
        then:
        waitFor(waitTime) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase())
        // FOR UAT TESTING ENABLE BELOW CODE
        //       waitFor(waitTime){cluster.restoresearch.isDisplayed()
        //       cluster.restoredirectory.isDisplayed()}
        //       cluster.restoredirectory.value(emptyPath)
        //      cluster.restoresearch.click()
        //      if(waitFor(waitTime){cluster.emptysearchrestore.isDisplayed()}){
        //          cluster.emptysearchrestore.text().toLowerCase().equals("Please enter a valid directory path.".toLowerCase())
        //      println("error message for empty restore search verified!!")}
    }

    def "when restore clicked and verify restore popup for No"(){


        when:
        at AdminPage
        waitFor(waitTime) { cluster.restorebutton.isDisplayed()
            cluster.restorestatus.isDisplayed()
            cluster.restorebutton.click()
        }

        println("restore clicked")

        then:
        waitFor(waitTime) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase())

        // FOR UAT TESTING ENABLE BELOW CODE
        //   waitFor(waitTime){cluster.buttonrestore.isDisplayed()}
        //  cluster.buttonrestore.click()
        //  waitFor(waitTime){cluster.restorepopupno.isDisplayed()
        //               cluster.restorepopupyes.isDisplayed()}
        //  cluster.restorepopupno.click()
        //  println("No clicked for restore popup")
        // waitFor(waitTime){cluster.restorecancelbutton.isDisplayed()}
        // cluster.restorecancelbutton.click()

    }

    def "when restore clicked and verify restore popup for Yes"(){


        when:
        at AdminPage
        waitFor(waitTime) { cluster.restorebutton.isDisplayed()
            cluster.restorestatus.isDisplayed()
            cluster.restorebutton.click()
        }

        println("restore clicked")

        then:
        waitFor(waitTime) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase())

        // FOR UAT TESTING ENABLE BELOW CODE
        //  waitFor(waitTime){cluster.buttonrestore.isDisplayed()}
        //  cluster.buttonrestore.click()
        //  waitFor(waitTime){cluster.restorepopupno.isDisplayed()
        //             cluster.restorepopupyes.isDisplayed()}
        //  cluster.restorepopupyes.click()
        //  println("Yes clicked for restore popup")
        //  waitFor(waitTime){cluster.savesuccessok.isDisplayed()}
        //  cluster.savesuccessok.click()
        // println("ok clicked and message displayed after restoring")

    }


    def "when shutdown and cancel popup"(){
        when:
        at AdminPage
        waitFor(waitTime) { cluster.shutdownbutton.isDisplayed() }
        cluster.shutdownbutton.click()
        then:
        waitFor(waitTime) { cluster.shutdownconfirmation.isDisplayed() }
        cluster.shutdownconfirmation.text().toLowerCase().equals("Shutdown: Confirmation".toLowerCase())
        cluster.shutdowncancelbutton.click()
    }

    def "when shutdown and close popup"(){
        when:
        at AdminPage
        waitFor(waitTime) { cluster.shutdownbutton.isDisplayed() }
        cluster.shutdownbutton.click()
        then:
        waitFor(waitTime) { cluster.shutdownconfirmation.isDisplayed() }
        cluster.shutdownconfirmation.text().toLowerCase().equals("Shutdown: Confirmation".toLowerCase())
        cluster.shutdownclosebutton.click()
    }

//server name list test

    def "Check Cluster Status"() {

        when: 'clicked server button'
        at AdminPage
        page.serverbutton.isDisplayed()
        page.serverbutton.click()
        then:
        if (waitFor(waitTime) { page.mainservername.isDisplayed() && page.servername.isDisplayed() }) {

            println("server name is displayed as: " + page.mainservername.text().replaceAll("Stop", "").replaceAll("Paused", ""))
            println("currently running server is : "+ page.servername.text())
        }
        try {

            if (!page.cluster.resumebutton.displayed) {
                if (page.shutdownServerStop.displayed) {
                    println("Servers are stopped")
                }
            }
        }
        catch(geb.error.RequiredPageContentNotPresent e)
        {
            println("Resume button is not displayed")
        }

        try {
            if ( page.cluster.resumebutton.displayed ) {
                if (page.shutdownServerPause.displayed) {
                    println("Servers are paused!!!")
                }

            }
        }
        catch(geb.error.RequiredPageContentNotPresent e)
        {
            println("Resume button is displayed")
        }
    }



    // Overview Expansion

    def "HTTP Access Expand:Check Text"() {
        when:
        page.overview.httpAccess.click()
        then:
        waitFor(waitTime) {
            page.overview.jsonApi.text().equals("JSON API")
            !page.overview.jsonApiStatus.text().equals("")
        }
    }

    def "Command Logging Expand:Check Text"() {
        int count = 0

        when:
        page.overview.commandLogging.click()
        then:
        while(count<numberOfTrials) {
            count++
            try {
                waitFor(waitTime) {
                    page.overview.logFrequencyTime.text().equals("Log Frequency Time")
                    !page.overview.logFrequencyTimeValue.text().equals("")

                    page.overview.logFrequencyTransactions.text().equals("Log Frequency Transactions")
                    !page.overview.logFrequencyTransactionsValue.text().equals("")

                    page.overview.logSize.text().equals("Log Size")
                    !page.overview.logSizeValue.text().equals("")
                }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
    }



    def cleanupSpec() {
        if (!(page instanceof VoltDBManagementCenterPage)) {
            when: 'Open VMC page'
            ensureOnVoltDBManagementCenterPage()
            then: 'to be on VMC page'
            at VoltDBManagementCenterPage
        }

        page.loginIfNeeded()

        when: 'click the Admin link (if needed)'
        page.openAdminPage()
        then: 'should be on Admin page'
        at AdminPage

        String initialPrefix 	= "DEFAULT"
        String initialFreq		= "10"
        String initialFreqUnit 	= "Hrs"
        String initialRetained 	= "10"

        String initialHeartTimeout = "10"
        String initialQueryTimeout = "10"

        // autosnapshot revert

        if (revertAutosnapshots == true) {
            when:
            page.autoSnapshotsEdit.click()
            then:
            waitFor(waitTime) {
                page.autoSnapshotsEditCheckbox.isDisplayed()
                page.autoSnapshotsEditOk.isDisplayed()
                page.autoSnapshotsEditCancel.isDisplayed()
            }

            page.filePrefixField.value(initialPrefix)
            page.frequencyField.value(initialFreq)
            page.frequencyUnitField.click()
            page.frequencyUnitField.value(initialFreqUnit)
            page.retainedField.value(initialRetained)

            if(page.fileprefixEdit.text() != " "){
                println("fileprefix passed, found non-empty")

                if(
                page.frequencyEdit.text() != " "){
                    println("frequency passed, found non-empty")}
                // page.frequencyUnitField.click()
                if( page.frequencyUnitField.text() != " "){
                    println("frequency unit passed, found non-empty")}
                if ( page.retainedEdit.text() != " "){
                    println("retained passed, found non-empty")}
            }
            page.autoSnapshotsEditOk.click()
            println("pop up visible")

            when:

            while(true) {
                page.autosnapshotsconfirmok.click()
                println("inside ok clicked successfully")
                if(page.filePrefixField.isDisplayed()== false)
                    break
            }

            then:

            waitFor(waitTime){
                page.filePrefix.text().equals(initialPrefix)
                page.frequency.text().equals(initialFreq)
                page.frequencyUnit.text().equals(initialFreqUnit)
                page.retained.text().equals(initialRetained)
            }
        }



        // query timeout revert

        if (revertQueryTimeout==false) {
            when:
            page.advanced.click()
            then:
            waitFor(waitTime) { page.overview.queryTimeoutEdit.isDisplayed() }

            when:
            waitFor(waitTime) { page.overview.queryTimeoutEdit.click() }
            then:
            waitFor(waitTime) {
                page.overview.queryTimeoutField.isDisplayed()
                page.overview.queryTimeoutOk.isDisplayed()
                page.overview.queryTimeoutCancel.isDisplayed()
            }

            when:
            page.overview.queryTimeoutField.value(initialQueryTimeout)
            waitFor(waitTime) {
                page.overview.queryTimeoutOk.click()
            }
            then:
            waitFor(waitTime) {
                page.overview.queryTimeoutPopupOk.isDisplayed()
                page.overview.queryTimeoutPopupCancel.isDisplayed()
            }


            waitFor(waitTime) {
                try {
                    page.overview.queryTimeoutPopupOk.click()
                } catch (org.openqa.selenium.ElementNotVisibleException e) {
                    println("retrying")
                }

                page.overview.queryTimeoutEdit.isDisplayed()
                page.overview.queryTimeoutValue.text().equals(initialQueryTimeout)
                !page.overview.queryTimeoutPopupOk.isDisplayed()
                !page.overview.queryTimeoutPopupCancel.isDisplayed()
            }
        }
    }

    //server setting

    def "Check server setting and display its respective value"(){

        when:
        while(true){
            if(waitFor(waitTime){page.networkInterfaces.serverSettingTitle.isDisplayed()} && page.networkInterfaces.serverSettingTitle.text() !=""){
                println("Title displayed as:"+page.networkInterfaces.serverSettingTitle.text())
            }else println("Server setting title not displayed so not processing further")
            break;
        }

        then:
        if(page.networkInterfaces.serversettingclientvalue.text()==""){
            println("Client port value in server setting is empty")}
        else{println("Client port value in server setting is not empty, value:" +page.networkInterfaces.serversettingclientvalue.text())}

        if(page.networkInterfaces.serversettingadminvalue.text()==""){
            println("Admin port value in server setting is empty")}
        else{println("Admin port value in server setting is not empty, value:" +page.networkInterfaces.serversettingadminvalue.text())}

        if(page.networkInterfaces.serversettinghttpvalue.text()==""){
            println("HTTP port value in server setting is empty")}
        else{println("HTTP port value in server setting is not empty, value:" +page.networkInterfaces.serversettinghttpvalue.text())}

        if(page.networkInterfaces.serversettinginternalvalue.text()==""){
            println("Internal port value in server setting is empty")}
        else{println("Internal port value in server setting is not empty, value:" +page.networkInterfaces.serversettinginternalvalue.text())}

        if(page.networkInterfaces.serversettingzookeepervalue.text()==""){
            println("Zookeeper port value in server setting is empty")}
        else{println("Zookeeper port value in server setting is not empty, value:" +page.networkInterfaces.serversettingzookeepervalue.text())}

        if(page.networkInterfaces.serversettingreplicationvalue.text()==""){
            println("Replication port value in server setting is empty")}
        else{println("Replication port value in server setting is not empty, value:" +page.networkInterfaces.serversettingreplicationvalue.text())}

    }

}
