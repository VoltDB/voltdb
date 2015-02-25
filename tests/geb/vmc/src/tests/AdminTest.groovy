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
    def "when download configuration is clicked"() {
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


    //schema (SYSTEM OVERVIEW)

    @Test
    def "check refresh button"(){

        when:
        at voltDBadmin
        schema.refreshbutton.isDisplayed()
        then:
        schema.refreshbutton.click()
    }


    def "check system overview title"(){

        when:
        at voltDBadmin
        schema.systemoverviewTitle.isDisplayed()
        then:
        schema.systemoverviewTitle.text().toLowerCase().equals("System Overview".toLowerCase())
    }



    def "check system overview content i.e, mode"(){
        when:
        at voltDBadmin
        schema.modeTitle.isDisplayed()
        then:
        schema.modeTitle.text().toLowerCase().equals("Mode".toLowerCase())

    }


    def "check system overview content i.e, voltDBversion"(){
        when:
        at voltDBadmin
        schema.voltdbversion.isDisplayed()
        then:
        schema.voltdbversion.text().toLowerCase().equals("VoltDB Version".toLowerCase())

    }


    def "check system overview content i.e, BuildString"(){
        when:
        at voltDBadmin
        schema.buildstring.isDisplayed()
        then:
        schema.buildstring.text().toLowerCase().equals("Buildstring".toLowerCase())

    }


    def "check system overview content i.e, Cluster composition"(){
        when:
        at voltDBadmin
        schema.clustercomposition.isDisplayed()
        then:
        schema.clustercomposition.text().toLowerCase().equals("Cluster Composition".toLowerCase())

    }


    def "check system overview content i.e, Running Since"(){
        when:
        at voltDBadmin
        schema.runningsince.isDisplayed()
        then:
        schema.runningsince.text().toLowerCase().equals("Running Since".toLowerCase())

    }


       def "check system overview content i.e, mode-value"(){
           when:
           at voltDBadmin
           schema.modevalue.isDisplayed()
           then:
           schema.modevalue.text().toLowerCase().equals("RUNNING".toLowerCase())

       }


       def "check system overview content i.e, voltDBversion-value"(){
           when:
           at voltDBadmin
           schema.versionvalue.isDisplayed()
           then:
           schema.versionvalue.text().toLowerCase().equals("5.1".toLowerCase())

       }


       def "check system overview content i.e, buildstring-value"(){
           when:
           at voltDBadmin
           schema.buildstringvalue.isDisplayed()
           then:
           schema.buildstringvalue.text().toLowerCase().equals("voltdb-4.7-2198-g23683d1-dirty-local".toLowerCase())

       }


       def "check system overview content i.e, clusterComposition-value"(){
           when:
           at voltDBadmin
           schema.clustercompositionvalue.isDisplayed()
           then:
           schema.clustercompositionvalue.text().toLowerCase().equals("1 hosts with 8 sites (8 per host)".toLowerCase())

       }


     def "check system overview content i.e, RunningSince-value"(){
         when:
         at voltDBadmin
         schema.runningsincevalue.isDisplayed()
         then:
        // schema.runningsincevalue.text().toLowerCase().equals("Tue Feb 24 08:20:31 GMT+00:00 2015 (0d 3h 56m)".toLowerCase())
         schema.runningsincevalue.isDisplayed()
     }

    //schema CATALOG OVERVIEW STATISTICS


       def "check Catalog Overview Statistics title"(){
           when:
           at voltDBadmin
           schema.catalogoverviewstatistic.isDisplayed()
           then:
           schema.catalogoverviewstatistic.text().toLowerCase().equals("Catalog Overview Statistics".toLowerCase())

       }


       def "check Catalog Overview Statistics content i.e, Compiled by VoltDB Version"(){
           when:
           at voltDBadmin
           schema.compiledversion.isDisplayed()
           then:
           schema.compiledversion.text().toLowerCase().equals("Compiled by VoltDB Version".toLowerCase())

       }


       def "check Catalog Overview Statistics content i.e, Compiled on"(){
           when:
           at voltDBadmin
           schema.compiledonTitle.isDisplayed()
           then:
           schema.compiledonTitle.text().toLowerCase().equals("Compiled on".toLowerCase())

       }


       def "check Catalog Overview Statistics content i.e, Table Count"(){
           when:
           at voltDBadmin
           schema.tablecount.isDisplayed()
           then:
           schema.tablecount.text().toLowerCase().equals("Table Count".toLowerCase())

       }


       def "check Catalog Overview Statistics content i.e, Materialized View Count"(){
           when:
           at voltDBadmin
           schema.materializedviewcount.isDisplayed()
           then:
           schema.materializedviewcount.text().toLowerCase().equals("Materialized View Count".toLowerCase())

       }


       def "check Catalog Overview Statistics content i.e, Index Count"(){
           when:
           at voltDBadmin
           schema.indexcount.isDisplayed()
           then:
           schema.indexcount.text().toLowerCase().equals("Index Count".toLowerCase())

       }


       def "check Catalog Overview Statistics content i.e, Procedure Count"(){
           when:
           at voltDBadmin
           schema.procedurecount.isDisplayed()
           then:
           schema.procedurecount.text().toLowerCase().equals("Procedure Count".toLowerCase())

       }


       def "check Catalog Overview Statistics content i.e, SQL Statement Count"(){
           when:
           at voltDBadmin
           schema.sqlstatementcount.isDisplayed()
           then:
           schema.sqlstatementcount.text().toLowerCase().equals("SQL Statement Count".toLowerCase())

       }


     def "check Catalog Overview Statistics content i.e, compiled by voltdb version-value"(){
         when:
         at voltDBadmin
         schema.compiledversionvalue.isDisplayed()
         then:
         schema.compiledversionvalue.text().toLowerCase().equals("5.1".toLowerCase())

     }

     def "check Catalog Overview Statistics content i.e, compiled on-value"(){
         when:
         at voltDBadmin
         schema.compiledonTitlevalue.isDisplayed()
         then:
         schema.compiledonTitlevalue.text().toLowerCase().equals("Tue, 24 Feb 2015 08:20:28 GMT+00:00".toLowerCase())

     }

     def "check Catalog Overview Statistics content i.e, table count-value"(){
         when:
         at voltDBadmin
         schema.tablecountvalue.isDisplayed()
         then:
         schema.tablecountvalue.text().toLowerCase().equals("3 (1 partitioned / 2 replicated)".toLowerCase())

     }

     def "check Catalog Overview Statistics content i.e, materilized view count-value"(){
         when:
         at voltDBadmin
         schema.materializedviewcountvalue.isDisplayed()
         then:
         schema.materializedviewcountvalue.text().toLowerCase().equals("2".toLowerCase())

     }


     def "check Catalog Overview Statistics content i.e, index count-value"(){
         when:
         at voltDBadmin
         schema.indexcountvalue.isDisplayed()
         then:
         schema.indexcountvalue.text().toLowerCase().equals("4".toLowerCase())

     }


     def "check Catalog Overview Statistics content i.e, procedure count-value"(){
         when:
         at voltDBadmin
         schema.procedurecountvalue.isDisplayed()
         then:
         schema.procedurecountvalue.text().toLowerCase().equals("5 (1 partitioned / 4 replicated) (3 read-only / 2 read-write)".toLowerCase())

     }

     def "check Catalog Overview Statistics content i.e, sql statement count-value"(){
         when:
         at voltDBadmin
         schema.sqlstatementcountvalue.isDisplayed()
         then:
         schema.sqlstatementcountvalue.text().toLowerCase().equals("10".toLowerCase())

     }

    //documentation


    def "check documentation right footer"(){
        when:
        at voltDBadmin
        schema.documentationrightlabel.isDisplayed()
        then:
        schema.documentationrightlabel.text().toLowerCase().equals("Generated by VoltDB 5.1 on 24 Feb 2015 08:20:28 GMT+00:00".toLowerCase())

    }


       def "check documentation url"(){
           when:
           at voltDBadmin
           schema.documentationurl.isDisplayed()
           then:
           schema.documentationurl.click()

       }


    def "check DDL source Title"(){
        when:
        at voltDBadmin
        schema.ddlsourceTitle.isDisplayed()
        then:
        schema.ddlsourceTitle.isDisplayed()
    }


    def "check DDL source download"() {
        when:
        at voltDBadmin
        schema.ddlsourcedownload.isDisplayed()
        then:
        schema.ddlsourcedownload.click()
    }


    def "check DDL source bunch of queries"(){
        when:
        at voltDBadmin
        schema.ddlsourcequeries.isDisplayed()
        then:
        schema.ddlsourcequeries.isDisplayed()

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

    // Overview

    def "check title"() {
        when:
        at AdminPage
        then:
        overview.title.text().toLowerCase().equals("Overview".toLowerCase())
    }

    def "check Site Per Host"() {
        when:
        at AdminPage
        then:
        overview.sitePerHost.text().toLowerCase().equals("Sites Per Host".toLowerCase())
    }

    def "check K-safety"() {
        when:
        at AdminPage
        then:
        overview.ksafety.text().toLowerCase().equals("K-safety".toLowerCase())
    }

    def "check Partition Detection"() {
        when:
        at AdminPage
        then:
        overview.partitionDetection.text().toLowerCase().equals("Partition detection".toLowerCase())
    }

    def "check Security"() {
        when:
        at AdminPage
        then:
        overview.security.text().toLowerCase().equals("Security".toLowerCase())
    }

    def "check HTTP Access"() {
        when:
        at AdminPage
        then:
        overview.httpAccess.text().toLowerCase().equals("HTTP Access".toLowerCase())
    }

    def "check Auto Snapshots"() {
        when:
        at AdminPage
        then:
        overview.autoSnapshots.text().toLowerCase().equals("Auto Snapshots".toLowerCase())
    }

    def "check Command Logging"() {
        when:
        at AdminPage
        then:
        overview.commandLogging.text().toLowerCase().equals("Command Logging".toLowerCase())
    }

    def "check Export"() {
        when:
        at AdminPage
        then:
        overview.export.text().toLowerCase().equals("Export".toLowerCase())
    }

    def "check Advanced"() {
        when:
        at AdminPage
        then:
        overview.advanced.text().toLowerCase().equals("Advanced".toLowerCase())
    }

    //values

    def "check Site Per Host value"() {
        when:
        at AdminPage
        then:
        waitFor(10){
            overview.sitePerHostValue.isDisplayed()
            String string = overview.sitePerHostValue.text()
            !(string.equals(""))
        }

    }

    def "check K-safety value"() {
        when:
        at AdminPage
        then:
        waitFor(10){
            overview.ksafetyValue.isDisplayed()
            String string = overview.ksafetyValue.text()
            !(string.equals(""))
        }

    }

    def "check Partition Detection value"() {
        when:
        at AdminPage
        then:
        waitFor(10){
            overview.partitionDetectionValue.isDisplayed()
            String string = overview.partitionDetectionValue.text()
            !(string.equals(""))
        }

    }

    def "check Security value"() {
        when:
        at AdminPage
        then:
        waitFor(10){
            overview.securityValue.isDisplayed()
            String string = overview.securityValue.text()
            !(string.equals(""))
        }
    }


    def "check HTTP Access value"() {
        when:
        at AdminPage
        then:
        waitFor(10){
            overview.httpAccessValue.isDisplayed()
            String string = overview.httpAccessValue.text()
            !(string.equals(""))
        }
    }

    def "check Auto Snapshots value"() {
        when:
        at AdminPage
        then:
        waitFor(10){
            overview.autoSnapshotsValue.isDisplayed()
            String string = overview.autoSnapshotsValue.text()
            !(string.equals(""))
        }
    }

    def "check Command Logging value"() {
        when:
        at AdminPage
        then:
        waitFor(10){
            overview.commandLoggingValue.isDisplayed()
            String string = overview.commandLoggingValue.text()
            !(string.equals(""))
        }
    }

    def "check Export value"() {
        when:
        at AdminPage
        then:
        waitFor(10){
            overview.exportValue.isDisplayed()
            String string = overview.exportValue.text()
            !(string.equals(""))
        }
    }
    // edit

    //--security

    def "click security button"(){
        when:
        at AdminPage
        overview.securityEdit.isDisplayed()
        then:
        overview.securityEdit.click()
    }

    def "click security edit button and cancel"(){
        when:
        at AdminPage
        waitFor(10) {
            overview.securityEdit.isDisplayed()
        }
        then:
        overview.securityEdit.click()
        overview.securityEditOk.isDisplayed()
        overview.securityEditOk.click()
        overview.securityPopupCancel.click()
    }

    def "click security edit button and ok"(){
        when:
        at AdminPage
        waitFor(10) {
            overview.securityEdit.isDisplayed()
        }
        then:
        overview.securityEdit.click()
        overview.securityEditOk.isDisplayed()
        overview.securityEditOk.click()
        overview.securityPopupOk.click()
    }

    // --Auto snapshot

    def "check Auto Snapshots edit"() {
        when:
        at AdminPage
        then:
        waitFor(10){
            overview.autoSnapshotsEdit.isDisplayed()
            String string = overview.autoSnapshotsEdit.text()
            !(string.equals(""))
        }
    }

    def "click edit Auto Snapshots and check"() {
        when:
        at AdminPage
        then:
        waitFor(10) {
            overview.autoSnapshotsEdit.isDisplayed()
        }
        overview.autoSnapshotsEdit.click()

        overview.autoSnapshotsEditCheckbox.isDisplayed()
        overview.autoSnapshotsEditOk.isDisplayed()
        overview.autoSnapshotsEditCancel.isDisplayed()
    }

    def "click Auto Snapshot edit and click cancel"() {
        when:
        at AdminPage
        then:
        waitFor(10) {
            overview.autoSnapshotsEdit.isDisplayed()
        }

        when:
        overview.autoSnapshotsEdit.click()
        then:
        overview.autoSnapshotsEditOk.isDisplayed()
        overview.autoSnapshotsEditCancel.isDisplayed()

        when:
        overview.autoSnapshotsEditCancel.click()
        then:
        !(overview.autoSnapshotsEditCancel.isDisplayed())
        !(overview.autoSnapshotsEditOk.isDisplayed())
    }

    def "click Auto Snapshots edit and click checkbox to change on off"() {
        when:
        at AdminPage
        then:
        waitFor(10) {
            overview.autoSnapshotsEdit.isDisplayed()
        }

        when:
        overview.autoSnapshotsEdit.click()
        String string = overview.autoSnapshotsValue.text()
        then:
        waitFor(10){
            overview.autoSnapshotsEditCheckbox.isDisplayed()
            overview.autoSnapshotsEditOk.isDisplayed()
            overview.autoSnapshotsEditCancel.isDisplayed()
        }

        when:
        overview.autoSnapshotsEditCheckbox1.click()
        then:
        String stringChange = overview.autoSnapshotsValue.text()

        if ( string.toLowerCase() == "on" ) {
            assert stringChange.toLowerCase().equals("off")
        }
        else if ( string.toLowerCase() == "off" ) {
            assert stringChange.toLowerCase().equals("on")
        }
        else {
        }
    }

    def "click edit and ok to check popup"() {
        String prefix 			= "SNAPSHOTNONCE"
        String frequency 		= "10"
        String frequencyUnit	= "Hrs"
        String retained 		= "1"

        String title			= "Auto Snapshots"
        String display			= "Do you want to save the value?"
        String ok				= "Ok"
        String cancel			= "Cancel"

        when:
        at AdminPage
        then:
        waitFor(10) {
            overview.autoSnapshotsEdit.isDisplayed()
        }

        when:
        overview.autoSnapshotsEdit.click()
        String string = overview.autoSnapshotsValue.text()
        then:
        waitFor(10) {
            overview.autoSnapshotsEditCheckbox.isDisplayed()
            overview.autoSnapshotsEditOk.isDisplayed()
            overview.autoSnapshotsEditCancel.isDisplayed()
        }
        overview.filePrefixField.value(prefix)
        overview.frequencyField.value(frequency)
        overview.frequencyUnitField.click()
        overview.frequencyUnitField.value(frequencyUnit)
        overview.retainedField.value(retained)
        assert withConfirm(true) { overview.autoSnapshotsEditOk.click() } == "Do you want to save the value?"
        when:
        overview.filePrefixField.value(prefix)
        overview.frequencyField.value(frequency)
        overview.frequencyUnitField.click()
        overview.frequencyUnitField.value(frequencyUnit)
        overview.retainedField.value(retained)
        overview.autoSnapshotsEditOk.click()
        then:
        waitFor(10) {
            overview.autoSnapshotsPopup.isDisplayed()
            overview.autoSnapshotsPopupTitle.isDisplayed()
            overview.autoSnapshotsPopupDisplay.isDisplayed()
            overview.autoSnapshotsPopupClose.isDisplayed()
            overview.autoSnapshotsPopupOk.isDisplayed()
            overview.autoSnapshotsPopupCancel.isDisplayed()
        }
        overview.autoSnapshotsPopupTitle.text().equals(title)
        overview.autoSnapshotsPopupDisplay.text().equals(display)
        overview.autoSnapshotsPopupOk.text().equals(ok)
        overview.autoSnapshotsPopupCancel.text().equals(cancel)

    }


}
