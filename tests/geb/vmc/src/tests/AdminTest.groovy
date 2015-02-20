/**
 * Created by anrai on 2/12/15.
 */


package vmcTest.tests

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

    // HEADER TESTS

    def "header banner exists" () {
        when:
			at AdminPage
		then:
			header.banner.isPresent()
	}


    def "header image exists" () {
        when:
            at AdminPage
        then:
            header.image.isDisplayed();
    }

    def "header username exists" () {
        when:
            at AdminPage
        then:
            header.username.isDisplayed();
    }
   
    def "header logout exists" () {
        when:
            at AdminPage
        then:
            header.logout.isDisplayed();
    }

    def "header help exists" () {
        when:
            at AdminPage
        then:
            header.help.isDisplayed();
    }

    // HEADER TAB TESTS
   
    def "header tab dbmonitor exists" ()  {
        when:
            at AdminPage
        then:
            header.tabDBMonitor.isDisplayed();
            header.tabDBMonitor.text().toLowerCase().equals("DB Monitor".toLowerCase())
    }
   
    def "header tab admin exists" ()  {
        when:
            at AdminPage
        then:
            header.tabAdmin.isDisplayed();
            header.tabAdmin.text().toLowerCase().equals("Admin".toLowerCase())
    }
   
    def "header tab schema exists" ()  {
        when:
            at AdminPage
        then:
            header.tabSchema.isDisplayed();
            header.tabSchema.text().toLowerCase().equals("Schema".toLowerCase())
    }
   
    def "header tab sql query exists" ()  {
        when:
            at AdminPage
        then:
            header.tabSQLQuery.isDisplayed();
            header.tabSQLQuery.text().toLowerCase().equals("SQL Query".toLowerCase())
    }

    def "header username check" () {
        def $line
        new File("src/pages/users.txt").withReader { $line = it.readLine() }
        
        when:
            at AdminPage
        then:
            header.username.text().equals($line);
    }


    def "header username click and close" () {
        when:
            at AdminPage
        then:
            header.username.click()
            header.logoutPopup.isDisplayed()
            header.logoutPopupTitle.isDisplayed()
            header.logoutPopupOkButton.isDisplayed()
            header.logoutPopupCancelButton.isDisplayed()
            header.popupClose.isDisplayed()
            header.popupClose.click()
    }
   
    def "header username click and cancel" () {
        when:
            at AdminPage
        then:
            header.username.click()
            header.logoutPopup.isDisplayed()
            header.logoutPopupTitle.isDisplayed()
            header.logoutPopupOkButton.isDisplayed()
            header.logoutPopupCancelButton.isDisplayed()
            header.popupClose.isDisplayed()
            header.logoutPopupCancelButton.click()
    }

    
    // LOGOUT TEST

    def "logout button test close" ()  {
        when:
            at AdminPage
        then:
            header.logout.click()
            header.logoutPopup.isDisplayed()
            header.logoutPopupTitle.isDisplayed()
            header.logoutPopupOkButton.isDisplayed()
            header.logoutPopupCancelButton.isDisplayed()
            header.popupClose.isDisplayed()
            header.popupClose.click()
      
    }

    def "logout button test cancel" ()  {
        when:
        at AdminPage
        then:
        header.logout.click()
        header.logoutPopup.isDisplayed()
        header.logoutPopupTitle.isDisplayed()
        header.logoutPopupOkButton.isDisplayed()
        header.logoutPopupCancelButton.isDisplayed()
        header.popupClose.isDisplayed()
        header.logoutPopupCancelButton.click()

    }

    // HELP POPUP TEST

    def "help popup existance" () {
        when:
            at AdminPage
            header.help.click()
        then:
            header.popup.isDisplayed()
            header.popupTitle.text().toLowerCase().equals("help".toLowerCase());
            header.popupClose.click()
    }

	// FOOTER TESTS
    
    def "footer exists" () {
        when:
            at AdminPage
        then:
            footer.banner.isDisplayed();
    }

    def "footer text exists and valid"() {

        when:
            at AdminPage
        then:
            footer.banner.isDisplayed();
            footer.text.text().toLowerCase().contains("VoltDB. All rights reserved.".toLowerCase());
    }

    //server test

    def "when server3 is clicked"() {
        when:
        at AdminPage
        server.serverbutton.isDisplayed()
        then:
        server.serverbutton.click()
        server.serverconfirmation.text().toLowerCase().equals("Servers".toLowerCase())
        server.deerwalkserver3stopbutton.click()
        server.deeerwalkservercancelbutton.click()
        //server.serverbutton.click()
    }

    def "when server4 is clicked"() {
        when:
        at AdminPage
        serverbutton.isDisplayed()
        then:
        server.serverbutton.click()
        server.serverconfirmation.text().toLowerCase().equals("Servers".toLowerCase())
        server.deerwalkserver4stopbutton.click()
        server.deeerwalkservercancelbutton.click()
        //server.serverbutton.click()
    }


	// NETWORK INTERFACES

	def "check Network Interfaces title"() {
		when:
			at AdminPage
		then:
			networkInterfaces.title.isDisplayed()
			networkInterfaces.title.text().toLowerCase().equals("Network Interfaces".toLowerCase())
	}

	def "check Port Name title"() {
		when:
			at AdminPage
		then:
			networkInterfaces.portNameTitle.isDisplayed()
			networkInterfaces.portNameTitle.text().toLowerCase().equals("Port Name".toLowerCase())
	}


	def "check Cluster Setting title"() {
		when:
			at AdminPage
		then:
			networkInterfaces.clusterSettingTitle.isDisplayed()
			networkInterfaces.clusterSettingTitle.text().toLowerCase().equals("Cluster Setting".toLowerCase())
	}

	def "check Server Setting title"() {
		when:
			at AdminPage
		then:
			networkInterfaces.serverSettingTitle.isDisplayed()
			networkInterfaces.serverSettingTitle.text().toLowerCase().equals("Server Setting".toLowerCase())
	}

	def "check Client Port title"() {
		when:
			at AdminPage
		then:
			networkInterfaces.clientPortTitle.isDisplayed()
			networkInterfaces.clientPortTitle.text().toLowerCase().equals("Client Port".toLowerCase())
	}

	def "check Admin Port title"() {
		when:
			at AdminPage
		then:
			networkInterfaces.adminPortTitle.isDisplayed()
			networkInterfaces.adminPortTitle.text().toLowerCase().equals("Admin Port".toLowerCase())
	}

	def "check HTTP Port title"() {
		when:
			at AdminPage
		then:
			networkInterfaces.httpPortTitle.isDisplayed()
			networkInterfaces.httpPortTitle.text().toLowerCase().equals("HTTP Port".toLowerCase())
	}

	def "check Internal Port title"() {
		when:
			at AdminPage
		then:
			networkInterfaces.internalPortTitle.isDisplayed()
			networkInterfaces.internalPortTitle.text().toLowerCase().equals("Internal Port".toLowerCase())
	}

	def "check Zookeeper Port title"() {
		when:
			at AdminPage
		then:
			networkInterfaces.zookeeperPortTitle.isDisplayed()
			networkInterfaces.zookeeperPortTitle.text().toLowerCase().equals("Zookeeper Port".toLowerCase())
	}

	def "check Replication Port title"() {
		when:
			at AdminPage
		then:
			networkInterfaces.replicationPortTitle.isDisplayed()
			networkInterfaces.replicationPortTitle.text().toLowerCase().equals("Replication Port".toLowerCase())
	}

	def "check Client Port Value not empty"() {
		when:
			at AdminPage
		then:
			waitFor(10){
				networkInterfaces.clientPortValue.isDisplayed()
				String string = networkInterfaces.clientPortValue.text()
				!(string.equals(""))
			}
	}

	def "check Admin Port Value not empty"() {
		when:
			at AdminPage
		then:
			waitFor(10){
				networkInterfaces.adminPortValue.isDisplayed()
				String string = networkInterfaces.adminPortValue.text()
				!(string.equals(""))
			}
	}

	def "check HTTP Port Value not empty"() {
		when:
			at AdminPage
		then:
			waitFor(10){
				networkInterfaces.httpPortValue.isDisplayed()
				String string = networkInterfaces.httpPortValue.text()
				!(string.equals(""))
			}
	}

	def "check Internal Port Value not empty"() {
		when:
			at AdminPage
		then:
			networkInterfaces.internalPortValue.isDisplayed()
			String string = networkInterfaces.internalPortValue.text()
			!(string.equals(""))
	}

	def "check Zookeeper Port Value not empty"() {
		when:
			at AdminPage
		then:
			waitFor(10){
				networkInterfaces.zookeeperPortValue.isDisplayed()
				String string = networkInterfaces.zookeeperPortValue.text()
				!(string.equals(""))
			}
	}

	def "check Replication Port Value not empty"() {
		when:
			at AdminPage
		then:
			waitFor(10){
				networkInterfaces.replicationPortValue.isDisplayed()
				String string = networkInterfaces.replicationPortValue.text()
				!(string.equals(""))
			}
	}


    //download automation test
    def "when download configuration is clicked"(){
        when:
        at voltDBadmin
        downloadbtn.downloadconfigurationbutton.isDisplayed()
        then:
        downloadbtn.downloadconfigurationbutton.click()
    }


    //cluster test

    def "cluster title"(){
        when:
        at AdminPage
        cluster.clusterTitle.isDisplayed()
        then:
        cluster.clusterTitle.text().equals("Cluster")}


        def "check promote button"(){
            when:
            at AdminPage


            then:
            cluster.promotebutton.isDisplayed()
            /* promotebutton.click()
            promoteconfirmation.isDisplayed()
            promoteconfirmation.isDisplayed()
            promoteconfirmation.text().equals("Promote: Confirmation".toLowerCase())
            promotecancel.click()*/
        }

    def "when Resume cancel"(){
        when:
        at AdminPage
        cluster.resumebutton.click()
        then:
        cluster.resumeconfirmation.isDisplayed()
        cluster.resumeconfirmation.text().toLowerCase().equals("Do you want to resume the cluster and exit admin mode?".toLowerCase());
        cluster.resumeconfirmation.resumecancel.click()
    }
    def "when Resume ok"(){
        when:
        at AdminPage
        cluster.resumebutton.click()
        then:
        cluster.resumeconfirmation.isDisplayed()
        cluster.resumeconfirmation.text().toLowerCase().equals("Do you want to resume the cluster and exit admin mode?".toLowerCase());
        cluster.resumeconfirmation.resumeok.click()
    }

    def "when save for cancel"(){
        when:
        at AdminPage
        cluster.savebutton.click()
        then:
        cluster.saveconfirmation.isDisplayed()
        cluster.saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
        cluster.savecancel.click()
    }

    def "when save for yes"(){
        when:
        at AdminPage
        cluster.savebutton.click()
        then:
        cluster.saveconfirmation.isDisplayed()
        cluster.saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
        cluster.savedirectory.value("/var/opt/test/manual_snapshots")
        cluster.saveok.click()
        cluster.saveyes.click()
        // failsavedok.click()
    }

    def "when save for No"(){
        when:
        at AdminPage
        cluster.savebutton.click()
        then:
        cluster.saveconfirmation.isDisplayed()
        cluster.saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
        cluster.savedirectory.value("bbb")
        cluster.saveok.click()
        cluster.saveno.click()
        cluster.savecancel.click()
    }

    def "when restore and ok"(){
        when:
        at AdminPage
        cluster.restorebutton.click()
        then:
        waitFor(30) {
            cluster.restoreconfirmation.isDisplayed()
            cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase());
            cluster.restoredirectory.value("/var/opt/test/manual_snapshots")
            cluster.restoresearch.click()}
        cluster.restorecancelbutton.click()
        // restoreok.click()
        // restoreokyes.click()
    }

    def "when restore and cancel"(){
        when:
        at AdminPage
        cluster.restorebutton.click()
        then:
        cluster.restoreconfirmation.isDisplayed()
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase());
        cluster.restorecancelbutton.click()

    }

    def "when restore and close"(){
        when:
        at AdminPage
        cluster.restorebutton.click()
        then:
        cluster.restoreconfirmation.isDisplayed()
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase());
        cluster.restoreclosebutton.click()

    }

    def "when shutdown cancel button"(){
        when:
        at AdminPage
        cluster.shutdownbutton.click()
        then:
        cluster.shutdownconfirmation.isDisplayed()
        cluster.shutdownconfirmation.text().toLowerCase().equals("Shutdown: Confirmation".toLowerCase())
        cluster.shutdowncancelbutton.click()
    }

    def "when shutdown close button"(){
        when:
        at AdminPage
        cluster.shutdownbutton.click()
        then:
        cluster.shutdownconfirmation.isDisplayed()
        cluster.shutdownconfirmation.text().toLowerCase().equals("Shutdown: Confirmation".toLowerCase())
        cluster.shutdownclosebutton.click()
    }

    def "when cluster pause in cancel"() {

        when:
        at AdminPage
        cluster.pausebutton.click()
        then:
        cluster.pauseconfirmation.isDisplayed()
        cluster.pauseconfirmation.text().equals("Pause: Confirmation");
        cluster.pausecancel.click()
    }

    def "when cluster pause in ok"() {

        when:
        at AdminPage
        cluster.pausebutton.click()
        then:
        cluster.pauseconfirmation.isDisplayed()
        cluster.pauseconfirmation.text().equals("Pause: Confirmation");
        cluster.pauseok.click()
    }



    // DIRECTORIES

    def "check Directories title"() {
        when:
        at AdminPage
        then:
        directories.title.isDisplayed()
        directories.title.text().toLowerCase().equals("Directories".toLowerCase())
    }

    def "check Root title"() {
        when:
        at AdminPage
        then:
        directories.rootTitle.isDisplayed()
        directories.rootTitle.text().toLowerCase().equals("Root (Destination)".toLowerCase())
    }

    def "check Snapshot title"() {
        when:
        at AdminPage
        then:
        directories.snapshotTitle.isDisplayed()
        directories.snapshotTitle.text().toLowerCase().equals("Snapshot".toLowerCase())
    }

    def "check Export Overflow title"() {
        when:
        at AdminPage
        then:
        directories.exportOverflowTitle.isDisplayed()
        directories.exportOverflowTitle.text().toLowerCase().equals("Export Overflow".toLowerCase())
    }

    def "check Command Logs title"() {
        when:
        at AdminPage
        then:
        directories.commandLogsTitle.isDisplayed()
        directories.commandLogsTitle.text().toLowerCase().equals("Command Logs".toLowerCase())
    }

    def "check Command Log Snapshots title"() {
        when:
        at AdminPage
        then:
        directories.commandLogSnapshotTitle.isDisplayed()
        directories.commandLogSnapshotTitle.text().toLowerCase().equals("Command Log Snapshots".toLowerCase())
    }

    def "check Root Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(10){
            directories.rootValue.isDisplayed()
            String string = directories.rootValue.text()
            !(string.equals(""))
        }
    }

    def "check SnapShot Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(10){
            directories.snapshotValue.isDisplayed()
            String string = directories.snapshotValue.text()
            !(string.equals(""))
        }
    }

    def "check Export Overflow Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(10){
            directories.exportOverflowValue.isDisplayed()
            String string = directories.exportOverflowValue.text()
            !(string.equals(""))
        }
    }

    def "check Command Logs Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(10){
            directories.commandLogsValue.isDisplayed()
            String string = directories.commandLogsValue.text()
            !(string.equals(""))
        }
    }

    def "check Log Snapshot Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(10){
            directories.commandLogSnapshotValue.isDisplayed()
            String string = directories.commandLogSnapshotValue.text()
            !(string.equals(""))
        }
    }
}
