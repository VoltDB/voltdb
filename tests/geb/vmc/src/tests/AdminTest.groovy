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

    def "click security edit button and ok and cancel"(){
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
            page.securityPopup.isDisplayed()
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

	static String initialPrefix
	static String initialFreq
	static String initialFreqUnit
	static String initialRetained

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
        def $line
        new File("src/pages/users.txt").withReader { $line = it.readLine() }

        when:
        at AdminPage
        then:
        waitFor(30) {
            header.username.isDisplayed()
            header.username.text().equals($line)
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
    def "when download configuration is clicked"() {
        when:
        at AdminPage
        //waitFor(30) { downloadbtn.downloadconfigurationbutton.isDisplayed() }
        then:
        downloadbtn.downloadconfigurationbutton.click()
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

    def "check pause ok and resume ok"(){
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

    def "when save for cancel"(){
        when:
        at AdminPage
        waitFor(5) { cluster.savebutton.isDisplayed() }
        cluster.savebutton.click()
        then:
        waitFor(5) { cluster.saveconfirmation.isDisplayed() }
        cluster.saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
        cluster.savecancel.click()
    }

    def "when save for yes"(){
        when:
        at AdminPage
        waitFor(5) { cluster.savebutton.isDisplayed() }
        cluster.savebutton.click()
        then:
        waitFor(5) { cluster.saveconfirmation.isDisplayed() }
        cluster.saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
        cluster.savedirectory.value("/var/opt/test/manual_snapshots")
        cluster.saveok.click()
        println("success in local for yes")

        // cluster.saveyes.click()
        // failsavedok.click()
    }

    def "when save for No"(){
        when:
        at AdminPage
        waitFor(5) { cluster.savebutton.isDisplayed() }
        cluster.savebutton.click()
        then:
        waitFor(5) { cluster.saveconfirmation.isDisplayed() }
        cluster.saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
        cluster.savedirectory.value("abc/bcc/dfg")
        cluster.savecancel.click()

    }

    def "when restore and ok"(){
        when:
        at AdminPage
        waitFor(30) { cluster.restorebutton.isDisplayed() }
        cluster.restorebutton.click()
        then:
        waitFor(30) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase());
        cluster.restoredirectory.value("/var/opt/test/manual_snapshots")
        cluster.restoresearch.click()
        cluster.restorecancelbutton.click()
    }

    def "when restore and cancel"(){

        when:
        at AdminPage
        waitFor(7) { cluster.restorebutton.isDisplayed() }
        cluster.restorebutton.click()
        then:
        waitFor(7) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase())
        cluster.restorecancelbutton.click()

    }

    def "when restore and close"(){
        when:
        at AdminPage
        waitFor(7) { cluster.restorebutton.isDisplayed() }
        cluster.restorebutton.click()
        then:
        waitFor(7) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase())
        cluster.restoreclosebutton.click()

    }


    def "when shutdown cancel button"(){
        when:
        at AdminPage
        waitFor(30) { cluster.shutdownbutton.isDisplayed() }
        cluster.shutdownbutton.click()
        then:
        waitFor(30) { cluster.shutdownconfirmation.isDisplayed() }
        cluster.shutdownconfirmation.text().toLowerCase().equals("Shutdown: Confirmation".toLowerCase())
        cluster.shutdowncancelbutton.click()
    }

    def "when shutdown close button"(){
        when:
        at AdminPage
        waitFor(30) { cluster.shutdownbutton.isDisplayed() }
        cluster.shutdownbutton.click()
        then:
        waitFor(30) { cluster.shutdownconfirmation.isDisplayed() }
        cluster.shutdownconfirmation.text().toLowerCase().equals("Shutdown: Confirmation".toLowerCase())
        cluster.shutdownclosebutton.click()
    }

    def "when cluster pause in cancel"() {
        when:
        at AdminPage
        waitFor(30) { cluster.pausebutton.isDisplayed() }
        cluster.pausebutton.click()
        then:
        waitFor(30) { cluster.pauseconfirmation.isDisplayed() }
        cluster.pauseconfirmation.text().equals("Pause: Confirmation");
        cluster.pausecancel.click()
    }

    def "when cluster pause in ok"() {
        when:
        at AdminPage
        waitFor(30) { cluster.pausebutton.isDisplayed() }
        cluster.pausebutton.click()
        then:
        waitFor(30) { cluster.pauseconfirmation.isDisplayed() }
        cluster.pauseconfirmation.text().equals("Pause: Confirmation");
        cluster.pauseok.click()
    }

    //server test

    def "when server is clicked and check server name list and stop cancel"() {

        def $namelist1
        def $namelist2
        def $namelist3
        def $namelist4
        def $lineunused, $lineunused1
       // new File("src/resources/serversearch.txt").withReader {
          //  $lineunused = it.readLine()
          //  $lineunused1 = it.readLine()
          //  $namelist1 = it.readLine()
           // $namelist2 = it.readLine()
          //  $namelist3 = it.readLine()
          //  $namelist4 = it.readLine()
       // }
        when:'clicked server button'
        at AdminPage
        page.serverbutton.isDisplayed()
        page.serverbutton.click()
        //if($namelist1 ==  page.servernamelist1.text()){println(page.servernamelist1.text())
        // if($namelist2 ==  page.servernamelist2.text()){println(page.servernamelist2.text())}
        // if($namelist3 ==  page.servernamelist3.text()){println(page.servernamelist3.text())}}

        if(waitFor(10){page.servername.isDisplayed()}){

            println("server name is displayed as: "+page.servername.text())}


        then:

        waitFor(5){
            page.serverstopbuttonmain.isDisplayed()
        }
        println("server stop button displayed")
        page.serverstopbuttonmain.click()
        waitFor(5) {page.serverstopcancel.isDisplayed()
                    page.serverstopok.isDisplayed()}
        page.serverstopcancel.click()
        println("server cancel button clicked")

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
}
