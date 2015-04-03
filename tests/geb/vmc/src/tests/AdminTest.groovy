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

    static boolean revertAutosnapshots = false
    static boolean revertHeartTimeout = false
    static boolean revertQueryTimeout = false

    def setup() { // called before each test
        setup: 'Open VMC page'
        to VoltDBManagementCenterPage
        expect: 'to be on VMC page'
        at VoltDBManagementCenterPage

        when: 'click the Admin link (if needed)'
        page.openAdminPage()
        then: 'should be on Admin page'
        at AdminPage
    }


    // DIRECTORIES

    def "check Directories title"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            directories.title.isDisplayed()
            directories.title.text().toLowerCase().equals("Directories".toLowerCase())
        }
    }

    def "check Root title"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            directories.rootTitle.isDisplayed()
            directories.rootTitle.text().toLowerCase().equals("Root (Destination)".toLowerCase())
        }
    }

    def "check Snapshot title"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            directories.snapshotTitle.isDisplayed()
            directories.snapshotTitle.text().toLowerCase().equals("Snapshot".toLowerCase())
        }
    }

    def "check Export Overflow title"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            directories.exportOverflowTitle.isDisplayed()
            directories.exportOverflowTitle.text().toLowerCase().equals("Export Overflow".toLowerCase())
        }
    }

    def "check Command Logs title"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            directories.commandLogsTitle.isDisplayed()
            directories.commandLogsTitle.text().toLowerCase().equals("Command Log".toLowerCase())
        }
    }

    def "check Command Log Snapshots title"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            directories.commandLogSnapshotTitle.isDisplayed()
            directories.commandLogSnapshotTitle.text().toLowerCase().equals("Command Log Snapshots".toLowerCase())
        }
    }

    def "check Root Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(30){
            directories.rootValue.isDisplayed()
            !directories.rootValue.text().equals("")
        }
    }

    def "check SnapShot Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(30){
            directories.snapshotValue.isDisplayed()
            !directories.snapshotValue.text().equals("")
        }
    }

    def "check Export Overflow Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(30){
            directories.exportOverflowValue.isDisplayed()
            !directories.exportOverflowValue.text().equals("")
        }
    }

    def "check Command Logs Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(30){
            directories.commandLogsValue.isDisplayed()
            directories.commandLogsValue.text().equals("")
        }
    }

    def "check Log Snapshot Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(30){
            directories.commandLogSnapshotValue.isDisplayed()
            directories.commandLogSnapshotValue.text().equals("")
        }
    }

    // OVERVIEW

    def "check title"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            overview.title.isDisplayed()
            overview.title.text().toLowerCase().equals("Overview".toLowerCase())
        }
    }

    def "check Site Per Host"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            overview.sitePerHost.isDisplayed()
            overview.sitePerHost.text().toLowerCase().equals("Sites Per Host".toLowerCase())
        }
    }

    def "check K-safety"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            overview.ksafety.isDisplayed()
            overview.ksafety.text().toLowerCase().equals("K-safety".toLowerCase())
        }
    }

    def "check Partition Detection"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            overview.partitionDetection.isDisplayed()
            overview.partitionDetection.text().toLowerCase().equals("Partition detection".toLowerCase())
        }
    }

    def "check Security"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            overview.security.isDisplayed()
            overview.security.text().toLowerCase().equals("Security".toLowerCase())
        }
    }

    def "check HTTP Access"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            overview.httpAccess.isDisplayed()
            overview.httpAccess.text().toLowerCase().equals("HTTP Access".toLowerCase())
        }
    }

    def "check Auto Snapshots"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            overview.autoSnapshots.isDisplayed()
            overview.autoSnapshots.text().toLowerCase().equals("Auto Snapshots".toLowerCase())
        }
    }

    def "check Command Logging"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            overview.commandLogging.isDisplayed()
            overview.commandLogging.text().toLowerCase().equals("Command Logging".toLowerCase())
        }
    }

    def "check Export"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            overview.export.isDisplayed()
            overview.export.text().toLowerCase().equals("Export".toLowerCase())
        }
    }

    def "check Advanced"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            overview.advanced.isDisplayed()
            overview.advanced.text().toLowerCase().equals("Advanced".toLowerCase())
        }
    }

    //values

    def "check Site Per Host value"() {
        when:
        at AdminPage
        then:
        waitFor(30){
            overview.sitePerHostValue.isDisplayed()
            !overview.sitePerHostValue.text().equals("")
        }
    }

    def "check K-safety value"() {
        when:
        at AdminPage
        then:
        waitFor(30){
            overview.ksafetyValue.isDisplayed()
            !overview.ksafetyValue.text().equals("")
        }
    }

    def "check Partition Detection value"() {
        when:
        at AdminPage
        then:
        waitFor(30){
            overview.partitionDetectionValue.isDisplayed()
            !overview.partitionDetectionValue.text().equals("")
        }
    }

    def "check Security value"() {
        when:
        at AdminPage
        then:
        waitFor(30){
            overview.securityValue.isDisplayed()
            !overview.securityValue.text().equals("")
        }
    }


    def "check HTTP Access value"() {
        when:
        at AdminPage
        then:
        waitFor(30){
            overview.httpAccessValue.isDisplayed()
            !overview.httpAccessValue.text().equals("")
        }
    }

    def "check Auto Snapshots value"() {
        when:
        at AdminPage
        then:
        waitFor(30){
            overview.autoSnapshotsValue.isDisplayed()
            !overview.autoSnapshotsValue.text().equals("")
        }
    }

    def "check Command Logging value"() {
        when:
        at AdminPage
        then:
        waitFor(30){
            overview.commandLoggingValue.isDisplayed()
            !overview.commandLoggingValue.text().equals("")
        }
    }

    // export expansion

    def "Overview:Export-Expand and check configurations"() {
        when:
        page.overview.export.click()
        then:

        try {
            waitFor(30) { page.overview.exportNoConfigAvailable.isDisplayed() }
            println(page.overview.exportNoConfigAvailable.text())
        } catch(geb.error.RequiredPageContentNotPresent e ) {
            waitFor(30) { page.overview.exportConfig.isDisplayed() }
            println("The export configuration")
            println(page.overview.exportConfiguration.text().replaceAll("On","").replaceAll("Off",""))
        } catch (geb.waiting.WaitTimeoutException e ) {
            waitFor(30) { page.overview.exportConfig.isDisplayed() }
            println("The export configuration")
            println(page.overview.exportConfiguration.text().replaceAll("On","").replaceAll("Off",""))
        }
    }

    // overview: advanced expansion-Edits

    def "Check click Heart Timeout edit and Cancel"() {
        when:
        page.advanced.click()
        then:
        waitFor(30) { page.overview.heartTimeoutEdit.isDisplayed() }

        when:
        waitFor(30) { page.overview.heartTimeoutEdit.click() }
        then:
        waitFor(30) {
            page.overview.heartTimeoutField.isDisplayed()
            page.overview.heartTimeoutOk.isDisplayed()
            page.overview.heartTimeoutCancel.isDisplayed()
        }

        when:
        waitFor(30) { page.overview.heartTimeoutCancel.click() }
        then:
        waitFor(30) {
            !page.overview.heartTimeoutField.isDisplayed()
            !page.overview.heartTimeoutOk.isDisplayed()
            !page.overview.heartTimeoutCancel.isDisplayed()
        }
    }

    def "Check click Heart Timeout edit and click Ok and then Cancel"() {
        when:
        page.advanced.click()
        then:
        waitFor(30) { page.overview.heartTimeoutEdit.isDisplayed() }

        when:
        waitFor(30) { page.overview.heartTimeoutEdit.click() }
        then:
        waitFor(30) {
            page.overview.heartTimeoutField.isDisplayed()
            page.overview.heartTimeoutOk.isDisplayed()
            page.overview.heartTimeoutCancel.isDisplayed()
        }

        when:
        page.overview.heartTimeoutField.value("10")
        waitFor(30) {
            page.overview.heartTimeoutOk.click()
        }
        then:
        waitFor(30) {
            page.overview.heartTimeoutPopupOk.isDisplayed()
            page.overview.heartTimeoutPopupCancel.isDisplayed()
        }

        when:
        waitFor(30) { page.overview.heartTimeoutPopupCancel.click() }
        then:
        waitFor(30) {
            page.overview.heartTimeoutEdit.isDisplayed()
            !page.overview.heartTimeoutPopupOk.isDisplayed()
            !page.overview.heartTimeoutPopupCancel.isDisplayed()
        }
    }

    def "Check click Heart Timeout edit and click Ok and then Ok"() {
        when:
        String heartTimeout = 20
        page.advanced.click()
        waitFor(30) {
            page.overview.heartTimeoutValue.isDisplayed()
        }
        initialHeartTimeout = page.overview.heartTimeoutValue.text()
        println("Initial Heartbeat time "+ initialHeartTimeout)
        revertHeartTimeout = true

        then:
        waitFor(30) { page.overview.heartTimeoutEdit.isDisplayed() }

        when:
        waitFor(30) { page.overview.heartTimeoutEdit.click() }
        then:
        waitFor(30) {
            page.overview.heartTimeoutField.isDisplayed()
            page.overview.heartTimeoutOk.isDisplayed()
            page.overview.heartTimeoutCancel.isDisplayed()
        }

        when:
        page.overview.heartTimeoutField.value(heartTimeout)
        waitFor(30) {
            page.overview.heartTimeoutOk.click()
        }
        then:
        waitFor(30) {
            page.overview.heartTimeoutPopupOk.isDisplayed()
            page.overview.heartTimeoutPopupCancel.isDisplayed()
        }


        waitFor(30) {
            try {
                page.overview.heartTimeoutPopupOk.click()
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                println("retrying")
            }

            page.overview.heartTimeoutEdit.isDisplayed()
            page.overview.heartTimeoutValue.text().equals(heartTimeout)
            !page.overview.heartTimeoutPopupOk.isDisplayed()
            !page.overview.heartTimeoutPopupCancel.isDisplayed()
        }
    }

    def "Heartbeat timeout> check error msg if empty data"() {
        when:
        String heartTimeout = ""
        page.advanced.click()
        then:
        waitFor(30) { page.overview.heartTimeoutEdit.isDisplayed() }

        when:
        waitFor(30) { page.overview.heartTimeoutEdit.click() }
        then:
        waitFor(30) {
            page.overview.heartTimeoutField.isDisplayed()
            page.overview.heartTimeoutOk.isDisplayed()
            page.overview.heartTimeoutCancel.isDisplayed()
        }

        when:
        page.overview.heartTimeoutField.value(heartTimeout)
        waitFor(30) {
            page.overview.heartTimeoutOk.click()
        }
        then:
        waitFor(30) {
            page.overview.errorMsgHeartbeat.isDisplayed()
            page.overview.errorMsgHeartbeat.text().equals("Please enter a valid positive number.")
        }

    }

    def "Heartbeat timeout > check error msg is value less then 1"() {
        when:
        String heartTimeout = "0"
        page.advanced.click()
        then:
        waitFor(30) { page.overview.heartTimeoutEdit.isDisplayed() }

        when:
        waitFor(30) { page.overview.heartTimeoutEdit.click() }
        then:
        waitFor(30) {
            page.overview.heartTimeoutField.isDisplayed()
            page.overview.heartTimeoutOk.isDisplayed()
            page.overview.heartTimeoutCancel.isDisplayed()
        }

        when:
        page.overview.heartTimeoutField.value(heartTimeout)
        waitFor(30) {
            page.overview.heartTimeoutOk.click()
        }
        then:
        waitFor(30) {
            page.overview.errorMsgHeartbeat.isDisplayed()
            page.overview.errorMsgHeartbeat.text().equals("Please enter a positive number. Its minimum value should be 1.")
        }

    }

    // query timeout

    def "Check click Query Timeout edit and Cancel"() {
        when:
        page.advanced.click()
        then:
        waitFor(30) { page.overview.queryTimeoutEdit.isDisplayed() }

        when:
        waitFor(30) { page.overview.queryTimeoutEdit.click() }
        then:
        waitFor(30) {
            page.overview.queryTimeoutField.isDisplayed()
            page.overview.queryTimeoutOk.isDisplayed()
            page.overview.queryTimeoutCancel.isDisplayed()
        }

        when:
        waitFor(30) { page.overview.queryTimeoutCancel.click() }
        then:
        waitFor(30) {
            !page.overview.queryTimeoutField.isDisplayed()
            !page.overview.queryTimeoutOk.isDisplayed()
            !page.overview.queryTimeoutCancel.isDisplayed()
        }
    }

    def "Check click Query Timeout edit and click Ok and then Cancel"() {
        when:
        page.advanced.click()
        then:
        waitFor(30) { page.overview.queryTimeoutEdit.isDisplayed() }

        when:
        waitFor(30) { page.overview.queryTimeoutEdit.click() }
        then:
        waitFor(30) {
            page.overview.queryTimeoutField.isDisplayed()
            page.overview.queryTimeoutOk.isDisplayed()
            page.overview.queryTimeoutCancel.isDisplayed()
        }

        when:
        page.overview.queryTimeoutField.value("10")
        waitFor(30) {
            page.overview.queryTimeoutOk.click()
        }
        then:
        waitFor(30) {
            page.overview.queryTimeoutPopupOk.isDisplayed()
            page.overview.queryTimeoutPopupCancel.isDisplayed()
        }

        when:
        waitFor(30) { page.overview.queryTimeoutPopupCancel.click() }
        then:
        waitFor(30) {
            page.overview.queryTimeoutEdit.isDisplayed()
            !page.overview.queryTimeoutPopupOk.isDisplayed()
            !page.overview.queryTimeoutPopupCancel.isDisplayed()
        }
    }

    def "Check click Query Timeout edit and click Ok and then Ok"() {
        when:
        String queryTimeout = 20
        page.advanced.click()
        waitFor(30) {
            page.overview.queryTimeoutValue.isDisplayed()
        }
        initialQueryTimeout = page.overview.queryTimeoutValue.text()
        println("Initial Query Timeout " + initialQueryTimeout)
        revertQueryTimeout = true

        then:
        waitFor(30) { page.overview.queryTimeoutEdit.isDisplayed() }

        when:
        waitFor(30) { page.overview.queryTimeoutEdit.click() }
        then:
        waitFor(30) {
            page.overview.queryTimeoutField.isDisplayed()
            page.overview.queryTimeoutOk.isDisplayed()
            page.overview.queryTimeoutCancel.isDisplayed()
        }

        when:
        page.overview.queryTimeoutField.value(queryTimeout)
        waitFor(30) {
            page.overview.queryTimeoutOk.click()
        }
        then:
        waitFor(30) {
            page.overview.queryTimeoutPopupOk.isDisplayed()
            page.overview.queryTimeoutPopupCancel.isDisplayed()
        }


        waitFor(30) {
            try {
                page.overview.queryTimeoutPopupOk.click()
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                println("retrying")
            }

            page.overview.queryTimeoutEdit.isDisplayed()
            page.overview.queryTimeoutValue.text().equals(queryTimeout)
            !page.overview.queryTimeoutPopupOk.isDisplayed()
            !page.overview.queryTimeoutPopupCancel.isDisplayed()
        }
    }

    def "Query timeout> check error msg if empty data"() {
        when:
        String queryTimeout = ""
        page.advanced.click()
        then:
        waitFor(30) { page.overview.queryTimeoutEdit.isDisplayed() }

        when:
        waitFor(30) { page.overview.queryTimeoutEdit.click() }
        then:
        waitFor(30) {
            page.overview.queryTimeoutField.isDisplayed()
            page.overview.queryTimeoutOk.isDisplayed()
            page.overview.queryTimeoutCancel.isDisplayed()
        }

        when:
        page.overview.queryTimeoutField.value(queryTimeout)
        waitFor(30) {
            page.overview.queryTimeoutOk.click()
        }
        then:
        waitFor(30) {
            page.overview.errorQuery.isDisplayed()
            page.overview.errorQuery.text().equals("Please enter a valid positive number.")
        }

    }

    // SECURITY

    def "click security button"(){
        when:
        at AdminPage
        page.securityEdit.isDisplayed()
        then:
        waitFor(10){
            page.securityEdit.click()
            page.securityEditOk.isDisplayed()
            page.securityEditCancel.isDisplayed()
        }
    }

     def "click security edit button and cancel"(){
        when:
        at AdminPage
        waitFor(5) { page.securityEdit.isDisplayed()

        }

        then:

        waitFor(10) {

            page.securityEdit.click()
            page.securityEditOk.isDisplayed()
            page.securityEditCancel.isDisplayed()


        }


        page.securityEditCancel.click()
        println("security edit canceled!")

        page.securityEdit.isDisplayed()

    }

    def "click security edit button and cancel popup"(){
        when:
        at AdminPage
        waitFor(5) { page.securityEdit.isDisplayed() }
        then:

        waitFor(10) {
            page.securityEdit.click()
            page.securityEditOk.isDisplayed()
            page.securityEditCancel.isDisplayed()
        }
        page.securityEditOk.click()
        println("security edit ok clicked!")
        waitFor(10) {
          //  page.securityPopup.isDisplayed()
            page.securityPopupOk.isDisplayed()
            page.securityPopupCancel.isDisplayed()
            page.securityPopupCancel.click()
            println("cancel clicked")
            page.securityEdit.isDisplayed()


        }




    }


    def "click security edit button and ok and ok"(){
        when:
        at AdminPage
        waitFor(5) { page.securityEdit.isDisplayed() }
        then:

        waitFor(10) {
            page.securityEdit.click()
            page.securityEditOk.isDisplayed()
            page.securityEditCancel.isDisplayed()
        }
        page.securityEditOk.click()
        println("security edit ok clicked!")

        waitFor(30) {
            page.securityPopupOk.isDisplayed()
            page.securityPopupCancel.isDisplayed()
            page.securityPopupOk.click()
        }
    }

// autosnapshot
    def "check Auto Snapshots edit"() {
        when:
        at AdminPage
        then:
        waitFor(10){ page.autoSnapshotsEdit.isDisplayed() }
        String string = page.autoSnapshotsEdit.text()
        !(string.equals(""))
    }


    def "click edit Auto Snapshots and check"() {
        when:
        at AdminPage
        then:
        waitFor(10) {
            page.autoSnapshotsEdit.isDisplayed()
        }
        page.autoSnapshotsEdit.click()

        waitFor(30) {
            page.autoSnapshotsEditCheckbox.isDisplayed()
            page.autoSnapshotsEditOk.isDisplayed()
            page.autoSnapshotsEditCancel.isDisplayed()

        }
        waitFor(30){

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
        waitFor(10) {
            page.autoSnapshotsEdit.isDisplayed()
        }

        when:
        page.autoSnapshotsEdit.click()
        then:
        waitFor(30) {
            page.autoSnapshotsEditOk.isDisplayed()
            page.autoSnapshotsEditCancel.isDisplayed()
        }

        when:
        page.autoSnapshotsEditCancel.click()
        then:
        waitFor(30) {
            !(page.autoSnapshotsEditCancel.isDisplayed())
            !(page.autoSnapshotsEditOk.isDisplayed())
        }
    }








      def "click Auto Snapshots edit and click checkbox to change on off"() {
        when:
        at AdminPage
        then:
        waitFor(10) {
            page.autoSnapshotsEdit.isDisplayed()
        }

        when:
        page.autoSnapshotsEdit.click()
        String enabledDisabled = page.autoSnapshotsValue.text()
        println(enabledDisabled)
        then:
        waitFor(10){
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
        waitFor(10) {
            page.autoSnapshotsEdit.isDisplayed()
            page.autoSnapshotsValue.isDisplayed()
        }

        when:
        page.autoSnapshotsEdit.click()
        String string = page.autoSnapshotsValue.text()
        then:
        waitFor(10) {
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
        waitFor(10) {
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
        waitFor(30) {
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
        waitFor(10) {
            page.autoSnapshotsEdit.isDisplayed()
            page.autoSnapshotsValue.isDisplayed()
            initialFreq
        }

        when:
        page.autoSnapshotsEdit.click()
        then:
        waitFor(10) {
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

        waitFor(15){
            page.filePrefix.text().equals(prefix)
            page.frequency.text().equals(frequency)
            page.frequencyUnit.text().equals(frequencyUnit)
            page.retained.text().equals(retained)
            page.filePrefix.isDisplayed()
        }
        
       
    }


// NETWORK INTERFACES


    def "check Network Interfaces title"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            page.networkInterfaces.title.isDisplayed()
            page.networkInterfaces.title.text().toLowerCase().equals("Network Interfaces".toLowerCase())
        }
    }

    def "check Port Name title"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            page.networkInterfaces.portNameTitle.isDisplayed()
            page.networkInterfaces.portNameTitle.text().toLowerCase().equals("Port Name".toLowerCase())
        }
    }


    def "check Cluster Setting title"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            page.networkInterfaces.clusterSettingTitle.isDisplayed()
            page.networkInterfaces.clusterSettingTitle.text().toLowerCase().equals("Cluster Settings".toLowerCase())
        }
    }

    def "check Server Setting title"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            page.networkInterfaces.serverSettingTitle.isDisplayed()
            page.networkInterfaces.serverSettingTitle.text().toLowerCase().equals("Server Settings".toLowerCase())
        }
    }

    def "check Client Port title"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            page.networkInterfaces.clientPortTitle.isDisplayed()
            page.networkInterfaces.clientPortTitle.text().toLowerCase().equals("Client Port".toLowerCase())
        }
    }

    def "check Admin Port title"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            page.networkInterfaces.adminPortTitle.isDisplayed()
            page.networkInterfaces.adminPortTitle.text().toLowerCase().equals("Admin Port".toLowerCase())
        }
    }

    def "check HTTP Port title"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            page.networkInterfaces.httpPortTitle.isDisplayed()
            page.networkInterfaces.httpPortTitle.text().toLowerCase().equals("HTTP Port".toLowerCase())
        }
    }

    def "check Internal Port title"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            page.networkInterfaces.internalPortTitle.isDisplayed()
            page.networkInterfaces.internalPortTitle.text().toLowerCase().equals("Internal Port".toLowerCase())
        }
    }

    def "check Zookeeper Port title"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            page.networkInterfaces.zookeeperPortTitle.isDisplayed()
            page.networkInterfaces.zookeeperPortTitle.text().toLowerCase().equals("Zookeeper Port".toLowerCase())
        }
    }

    def "check Replication Port title"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            page.networkInterfaces.replicationPortTitle.isDisplayed()
            page.networkInterfaces.replicationPortTitle.text().toLowerCase().equals("Replication Port".toLowerCase())
        }
    }

    // value

    def "check Client Port Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(30){
            page.networkInterfaces.clusterClientPortValue.isDisplayed()
            page.networkInterfaces.clusterClientPortValue.text().equals("")
        }
    }

    def "check Admin Port Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(30){
            page.networkInterfaces.clusterAdminPortValue.isDisplayed()
            page.networkInterfaces.clusterAdminPortValue.text().equals("")
        }
    }

    def "check HTTP Port Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(30){
            page.networkInterfaces.clusterHttpPortValue.isDisplayed()
            page.networkInterfaces.clusterHttpPortValue.text().equals("")
        }
    }

    def "check Internal Port Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
            page.networkInterfaces.clusterInternalPortValue.isDisplayed()
            page.networkInterfaces.clusterInternalPortValue.text().equals("")
        }
    }

    def "check Zookeeper Port Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(30){
            page.networkInterfaces.clusterZookeeperPortValue.isDisplayed()
            page.networkInterfaces.clusterZookeeperPortValue.text().equals("")
        }
    }

    def "check Replication Port Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(30){
            page.networkInterfaces.clusterReplicationPortValue.isDisplayed()
            page.networkInterfaces.clusterReplicationPortValue.text().equals("")
        }
    }

    // HEADER TESTS

    def "header banner exists" () {
        when:
        at AdminPage
        then:
        waitFor(30) { header.banner.isDisplayed() }
    }


    def "header image exists" () {
        when:
        at AdminPage
        then:
        waitFor(30) { header.image.isDisplayed() }
    }

    def "header username exists" () {
        when:
        at AdminPage
        then:
        waitFor(30) { header.username.isDisplayed() }
    }

    def "header logout exists" () {
        when:
        at AdminPage
        then:
        waitFor(30) { header.logout.isDisplayed() }
    }

    def "header help exists" () {
        when:
        at AdminPage
        then:
        waitFor(30) { header.help.isDisplayed() }
    }

    // HEADER TAB TESTS

    def "header tab dbmonitor exists" () {
        when:
        at AdminPage
        then:
        waitFor(30) {
            header.tabDBMonitor.isDisplayed()
            header.tabDBMonitor.text().toLowerCase().equals("DB Monitor".toLowerCase())
        }
    }

    def "header tab admin exists" () {
        when:
        at AdminPage
        then:
        waitFor(30) {
            header.tabAdmin.isDisplayed()
            header.tabAdmin.text().toLowerCase().equals("Admin".toLowerCase())
        }
    }

    def "header tab schema exists" () {
        when:
        at AdminPage
        then:
        waitFor(30) {
            header.tabSchema.isDisplayed()
            header.tabSchema.text().toLowerCase().equals("Schema".toLowerCase())

        }
    }

    def "header tab sql query exists" () {
        when:
        at AdminPage
        then:
        waitFor(30) { header.tabSQLQuery.isDisplayed()
            header.tabSQLQuery.text().toLowerCase().equals("SQL Query".toLowerCase())
        }
    }

    def "header username check" () {
        when:
        at AdminPage
        String username = page.getUsername()
        then:
        waitFor(30) {
            header.username.isDisplayed()
            header.username.text().equals(username)
        }
    }


    def "header username click and close" () {
        when:
        at AdminPage
        then:
        waitFor(30) { header.username.isDisplayed() }
        header.username.click()
        waitFor(30) {
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
        waitFor(30) { header.username.isDisplayed() }
        header.username.click()
        waitFor(30) {
            header.logoutPopupOkButton.isDisplayed()
            header.logoutPopupCancelButton.isDisplayed()
            header.popupClose.isDisplayed()
        }
        header.logoutPopupCancelButton.click()
    }


    // LOGOUT TEST

    def "logout button test close" ()  {
        when:
        at AdminPage
        then:
        waitFor(30) { header.logout.isDisplayed() }
        header.logout.click()
        waitFor(30) {
            header.logoutPopupOkButton.isDisplayed()
            header.logoutPopupCancelButton.isDisplayed()
            header.popupClose.isDisplayed()
        }
        header.popupClose.click()

    }

    def "logout button test cancel" ()  {
        when:
        at AdminPage
        then:
        waitFor(30) { header.logout.isDisplayed() }
        header.logout.click()
        waitFor(30) {
            header.logoutPopupOkButton.isDisplayed()
            header.logoutPopupCancelButton.isDisplayed()
            header.popupClose.isDisplayed()
        }
        header.logoutPopupCancelButton.click()
    }

    // HELP POPUP TEST

    def "help popup existance" () {
        when:
        at AdminPage
        waitFor(30) { header.help.isDisplayed() }
        header.help.click()
        then:
        waitFor(30) {
            header.popup.isDisplayed()
            header.popupTitle.isDisplayed()
            header.popupClose.isDisplayed()
            header.popupTitle.text().toLowerCase().equals("help".toLowerCase());
        }

        header.popupClose.click()
    }

    // FOOTER TESTS

    def "footer exists" () {
        when:
        at AdminPage
        then:
        waitFor(30) { footer.banner.isDisplayed() }
    }

    def "footer text exists and valid"() {
        when:
        at AdminPage
        then:
        waitFor(30) {
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

        waitFor(5) { 	page.downloadconfigurationbutton.isDisplayed() }
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
        waitFor(30) { cluster.clusterTitle.isDisplayed() }
        then:
        cluster.clusterTitle.text().equals("Cluster")
    }


    def "check promote button"(){
        when:
        at AdminPage
        then:
        waitFor(30) { cluster.promotebutton.isDisplayed() }
    }

    def "check pause cancel"(){
        when:
        at AdminPage
        waitFor(5){cluster.pausebutton.isDisplayed()}

        then:
        at AdminPage
        cluster.pausebutton.click()
        waitFor(5){cluster.pausecancel.isDisplayed()}

        cluster.pausecancel.click()
        println("cancel button clicked for pause testing")

    }

    def "check pause and verify resume too"(){
        when:
        at AdminPage
        waitFor(5) { cluster.pausebutton.isDisplayed() }
        then:

        cluster.pausebutton.click()
        waitFor{cluster.pauseok.isDisplayed()}
        cluster.pauseok.click()
        waitFor(4){cluster.resumebutton.isDisplayed()

            cluster.resumebutton.click()}
        waitFor(10) {
            cluster.resumeok.isDisplayed()}
        cluster.resumeok.click()
        println("resume for ok has been clicked")
    }



    def "when save and cancel popup"(){
        when:
        at AdminPage
        waitFor(5) { cluster.savebutton.isDisplayed() }
        cluster.savebutton.click()
        then:
        waitFor(5) { cluster.saveconfirmation.isDisplayed() }
        cluster.saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
        cluster.savecancel.click()
    }


    def "when save in empty path"(){
        String emptyPath = page.getEmptyPath()
        when:

        at AdminPage
        waitFor(15) { cluster.savebutton.isDisplayed() }
        cluster.savebutton.click()
        then:
        waitFor(15) { cluster.saveconfirmation.isDisplayed() }
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
        waitFor(15) { cluster.savebutton.isDisplayed() }
        cluster.savebutton.click()
        then:
        waitFor(15) { cluster.saveconfirmation.isDisplayed() }
        cluster.saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
        cluster.savedirectory.value(invalidPath)
        cluster.saveok.click()
        waitFor(10){cluster.failedsaveok.isDisplayed()}
        cluster.failedsaveok.click()
        println("error location for saving verified")


    }


    def "when save succeeded"(){
        String validPath = page.getValidPath()

        when:
        at AdminPage
        waitFor(15) { cluster.savebutton.isDisplayed() }
        cluster.savebutton.click()
        then:
        waitFor(15) { cluster.saveconfirmation.isDisplayed() }
        cluster.saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
        cluster.savedirectory.value(validPath)
        cluster.saveok.click()
        waitFor(15){cluster.savesuccessok.isDisplayed()}
        cluster.savesuccessok.click()
        println("save succeeded and clicked!!")
    }



    def "when restore button clicked and cancel popup"(){
        String validPath = page.getValidPath()
        when:
        at AdminPage
        waitFor(20) {   cluster.restorebutton.isDisplayed()
            cluster.restorestatus.isDisplayed()
            cluster.restorebutton.click()
        }

        println("restore button clicked")

        then:
        waitFor(7) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase());
        cluster.restoredirectory.value(validPath)
        cluster.restoresearch.click()
        waitFor(5){cluster.restorecancelbutton.isDisplayed()}
        cluster.restorecancelbutton.click()
    }


    def "when restore button clicked and close popup"(){
        when:
        at AdminPage
        waitFor(7) { cluster.restorebutton.isDisplayed()
            cluster.restorestatus.isDisplayed()
            cluster.restorebutton.click()
        }

        println("restore clicked")

        then:
        waitFor(7) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase())
        cluster.restoreclosebutton.click()

    }

    def "when restore clicked and search failed"(){
        String invalidPath = page.getInvalidPath()
        when:
        at AdminPage
        waitFor(7) { cluster.restorebutton.isDisplayed()
            cluster.restorestatus.isDisplayed()
            cluster.restorebutton.click()
        }

        println("restore clicked")

        then:
        waitFor(7) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase())

        // FOR UAT TESTING ENABLE BELOW CODE
        //    waitFor(10){cluster.restoresearch.isDisplayed()
        //    cluster.restoredirectory.isDisplayed()}
        //     cluster.restoredirectory.value(invalidPath)
        //      cluster.restoresearch.click()
        //   if(waitFor(10){cluster.restoreerrormsg.isDisplayed()}){
        //       cluster.restoreerrormsg.text().toLowerCase().equals("Error: Failure getting snapshots.Path is not a directory".toLowerCase())
        //      println("error message for restore search verified!!")}

    }

    def "when search button clicked in empty path of Restore"(){
        String emptyPath = page.getEmptyPath()
        when:
        waitFor(7) { cluster.restorebutton.isDisplayed()
            cluster.restorestatus.isDisplayed()
            cluster.restorebutton.click()
        }
        then:
        waitFor(7) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase())
        // FOR UAT TESTING ENABLE BELOW CODE
        //       waitFor(10){cluster.restoresearch.isDisplayed()
        //       cluster.restoredirectory.isDisplayed()}
        //       cluster.restoredirectory.value(emptyPath)
        //      cluster.restoresearch.click()
        //      if(waitFor(10){cluster.emptysearchrestore.isDisplayed()}){
        //          cluster.emptysearchrestore.text().toLowerCase().equals("Please enter a valid directory path.".toLowerCase())
        //      println("error message for empty restore search verified!!")}
    }

    def "when restore clicked and verify restore popup for No"(){


        when:
        at AdminPage
        waitFor(7) { cluster.restorebutton.isDisplayed()
            cluster.restorestatus.isDisplayed()
            cluster.restorebutton.click()
        }

        println("restore clicked")

        then:
        waitFor(7) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase())

        // FOR UAT TESTING ENABLE BELOW CODE
        //   waitFor(10){cluster.buttonrestore.isDisplayed()}
        //  cluster.buttonrestore.click()
        //  waitFor(10){cluster.restorepopupno.isDisplayed()
        //               cluster.restorepopupyes.isDisplayed()}
        //  cluster.restorepopupno.click()
        //  println("No clicked for restore popup")
        // waitFor(10){cluster.restorecancelbutton.isDisplayed()}
        // cluster.restorecancelbutton.click()

    }

    def "when restore clicked and verify restore popup for Yes"(){


        when:
        at AdminPage
        waitFor(7) { cluster.restorebutton.isDisplayed()
            cluster.restorestatus.isDisplayed()
            cluster.restorebutton.click()
        }

        println("restore clicked")

        then:
        waitFor(7) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase())

        // FOR UAT TESTING ENABLE BELOW CODE
        //  waitFor(10){cluster.buttonrestore.isDisplayed()}
        //  cluster.buttonrestore.click()
        //  waitFor(10){cluster.restorepopupno.isDisplayed()
        //             cluster.restorepopupyes.isDisplayed()}
        //  cluster.restorepopupyes.click()
        //  println("Yes clicked for restore popup")
        //  waitFor(10){cluster.savesuccessok.isDisplayed()}
        //  cluster.savesuccessok.click()
        // println("ok clicked and message displayed after restoring")

    }


    def "when shutdown and cancel popup"(){
        when:
        at AdminPage
        waitFor(30) { cluster.shutdownbutton.isDisplayed() }
        cluster.shutdownbutton.click()
        then:
        waitFor(30) { cluster.shutdownconfirmation.isDisplayed() }
        cluster.shutdownconfirmation.text().toLowerCase().equals("Shutdown: Confirmation".toLowerCase())
        cluster.shutdowncancelbutton.click()
    }

    def "when shutdown and close popup"(){
        when:
        at AdminPage
        waitFor(30) { cluster.shutdownbutton.isDisplayed() }
        cluster.shutdownbutton.click()
        then:
        waitFor(30) { cluster.shutdownconfirmation.isDisplayed() }
        cluster.shutdownconfirmation.text().toLowerCase().equals("Shutdown: Confirmation".toLowerCase())
        cluster.shutdownclosebutton.click()
    }

//server name list test

    def "when server is clicked and check server name list and stop cancel"() {

        when: 'clicked server button'
        at AdminPage
        page.serverbutton.isDisplayed()
        page.serverbutton.click()

        if (waitFor(10) { page.mainservername.isDisplayed() && page.servername.isDisplayed() }) {

            println("server name is displayed as: " + page.mainservername.text().replaceAll("Stop", " "))
            println("currently running server is : "+ page.servername.text())
        }


        then:
        if (waitFor(5) { page.serverstopbtndisable.isDisplayed() }) {
            println("server stop button  displayed for disable mode")
        }

        //   enable below code FOR UAT TESTING
        //     if( waitFor(5) { page.serverstopbtnenable.isDisplayed() }){
        //    println("server stop button clicked for  enable mode")
        //    page.serverstopbtnenable.click()
        //   waitFor(5) { page.serverstopcancel.isDisplayed() }
        //   page.serverstopok.isDisplayed()
        //   page.serverstopcancel.click()
        //    println("server cancel button clicked")}
    }

    // Overview Expansion
	
	def "HTTP Access Expand:Check Text"() {
    	when:
    	page.overview.httpAccess.click()
    	then:
    	waitFor(30) {
    		page.overview.jsonApi.text().equals("JSON API")
    		!page.overview.jsonApiStatus.text().equals("")
    	}
    }
    
    def "Command Logging Expand:Check Text"() {
    	when:
    	page.overview.commandLogging.click()
    	then:
    	waitFor(30) {
    		page.overview.logFrequencyTime.text().equals("Log Frequency Time")
    		!page.overview.logFrequencyTimeValue.text().equals("")
    		
    		page.overview.logFrequencyTransactions.text().equals("Log Frequency Transactions")
    		!page.overview.logFrequencyTransactionsValue.text().equals("")
    		
    		page.overview.logSize.text().equals("Log Size")
    		!page.overview.logSizeValue.text().equals("")
    	}
    }
    
    def "Advanced Expand:Check Text"() {
    	when:
    	page.overview.advanced.click()
    	then:
    	waitFor(30) {
    		page.overview.maxJavaHeap.text().equals("Max Java Heap")
    		!page.overview.maxJavaHeapValue.text().equals("")
    		
    		page.overview.heartbeatTimeout.text().equals("Heartbeat Timeout")
    		!page.overview.heartbeatTimeoutValue.text().equals("")
    		
    		page.overview.queryTimeout.text().equals("Query Timeout")
    		!page.overview.queryTimeoutValue.text().equals("")
    		
    		page.overview.maxTempTableMemory.text().equals("Max Temp Table Memory")
    		!page.overview.maxTempTableMemoryValue.text().equals("")
    		
    		page.overview.snapshotPriority.text().equals("Snapshot Priority")
    		!page.overview.snapshotPriorityValue.text().equals("")
    		
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
            waitFor(10) {
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

            waitFor(15){
                page.filePrefix.text().equals(initialPrefix)
                page.frequency.text().equals(initialFreq)
                page.frequencyUnit.text().equals(initialFreqUnit)
                page.retained.text().equals(initialRetained)
            }
        }

        // heartbeat timeout revert

        if (revertHeartTimeout==false) {
            when:
            page.advanced.click()
            then:
            waitFor(30) { page.overview.heartTimeoutEdit.isDisplayed() }

            when:
            waitFor(30) { page.overview.heartTimeoutEdit.click() }
            then:
            waitFor(30) {
                page.overview.heartTimeoutField.isDisplayed()
                page.overview.heartTimeoutOk.isDisplayed()
                page.overview.heartTimeoutCancel.isDisplayed()
            }

            when:
            page.overview.heartTimeoutField.value(initialHeartTimeout)
            waitFor(30) {
                page.overview.heartTimeoutOk.click()
            }
            then:
            waitFor(30) {
                page.overview.heartTimeoutPopupOk.isDisplayed()
                page.overview.heartTimeoutPopupCancel.isDisplayed()
            }


            waitFor(30) {
                try {
                    page.overview.heartTimeoutPopupOk.click()
                } catch (org.openqa.selenium.ElementNotVisibleException e) {
                    println("retrying")
                }

                page.overview.heartTimeoutEdit.isDisplayed()
                page.overview.heartTimeoutValue.text().equals(initialHeartTimeout)
                !page.overview.heartTimeoutPopupOk.isDisplayed()
                !page.overview.heartTimeoutPopupCancel.isDisplayed()
            }
        }

        // query timeout revert

        if (revertQueryTimeout==false) {
            when:
            page.advanced.click()
            then:
            waitFor(30) { page.overview.queryTimeoutEdit.isDisplayed() }

            when:
            waitFor(30) { page.overview.queryTimeoutEdit.click() }
            then:
            waitFor(30) {
                page.overview.queryTimeoutField.isDisplayed()
                page.overview.queryTimeoutOk.isDisplayed()
                page.overview.queryTimeoutCancel.isDisplayed()
            }

            when:
            page.overview.queryTimeoutField.value(initialQueryTimeout)
            waitFor(30) {
                page.overview.queryTimeoutOk.click()
            }
            then:
            waitFor(30) {
                page.overview.queryTimeoutPopupOk.isDisplayed()
                page.overview.queryTimeoutPopupCancel.isDisplayed()
            }


            waitFor(30) {
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

}
