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
   
    def "header tab dbmonitor exists" ()  {
        when:
            at AdminPage
        then:
            waitFor(30) { header.tabDBMonitor.isDisplayed() }
            header.tabDBMonitor.text().toLowerCase().equals("DB Monitor".toLowerCase())
    }
   
    def "header tab admin exists" ()  {
        when:
            at AdminPage
        then:
            waitFor(30) { header.tabAdmin.isDisplayed() }
            header.tabAdmin.text().toLowerCase().equals("Admin".toLowerCase())
    }
   
    def "header tab schema exists" ()  {
        when:
            at AdminPage
        then:
            waitFor(30) { header.tabpage.isDisplayed() }
            header.tabpage.text().toLowerCase().equals("Schema".toLowerCase())
    }
   
    def "header tab sql query exists" ()  {
        when:
            at AdminPage
        then:
            waitFor(30) { header.tabSQLQuery.isDisplayed() }
            header.tabSQLQuery.text().toLowerCase().equals("SQL Query".toLowerCase())
    }

    def "header username check" () {
        def $line
        new File("src/pages/users.txt").withReader { $line = it.readLine() }
        
        when:
            at AdminPage
        then:
			waitFor(30) { header.username.isDisplayed() }
            header.username.text().equals($line);
    }


    def "header username click and close" () {
        when:
            at AdminPage
        then:
			waitFor(30) { header.username.isDisplayed() }
            header.username.click()
			waitFor(30) { 
		        header.logoutPopup.isDisplayed()
		        header.logoutPopupTitle.isDisplayed()
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
				header.logoutPopup.isDisplayed()
		        header.logoutPopupTitle.isDisplayed()
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
				header.logoutPopup.isDisplayed()
		        header.logoutPopupTitle.isDisplayed()
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
		    header.logoutPopup.isDisplayed()
		    header.logoutPopupTitle.isDisplayed()
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
			}
            header.popupTitle.text().toLowerCase().equals("help".toLowerCase());
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
			}
            footer.text.text().toLowerCase().contains("VoltDB. All rights reserved.".toLowerCase());
    }

    //server test

    def "when server3 is clicked"() {
        when:
        at AdminPage
        waitFor(30) { server.serverbutton.isDisplayed() }
        then:
        server.serverbutton.click()
		waitFor(30) { server.serverconfirmation.isDisplayed() }
        server.serverconfirmation.text().toLowerCase().equals("Servers".toLowerCase())
        server.deerwalkserver3stopbutton.click()
        server.deeerwalkservercancelbutton.click()
        //server.serverbutton.click()
    }

    def "when server4 is clicked"() {
        when:
        at AdminPage
        waitFor(30) { serverbutton.isDisplayed() }
        then:
        server.serverbutton.click()
		waitFor(30) { server.serverconfirmation.isDisplayed() }	
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
			waitFor(30) { networkInterfaces.title.isDisplayed() }
			networkInterfaces.title.text().toLowerCase().equals("Network Interfaces".toLowerCase())
	}

	def "check Port Name title"() {
		when:
			at AdminPage
		then:
			waitFor(30) { networkInterfaces.portNameTitle.isDisplayed() }
			networkInterfaces.portNameTitle.text().toLowerCase().equals("Port Name".toLowerCase())
	}


	def "check Cluster Setting title"() {
		when:
			at AdminPage
		then:
			waitFor(30) { networkInterfaces.clusterSettingTitle.isDisplayed() }
			networkInterfaces.clusterSettingTitle.text().toLowerCase().equals("Cluster Settings".toLowerCase())
	}

	def "check Server Setting title"() {
		when:
			at AdminPage
		then:
			waitFor(30) { networkInterfaces.serverSettingTitle.isDisplayed() }
			networkInterfaces.serverSettingTitle.text().toLowerCase().equals("Server Settings".toLowerCase())
	}

	def "check Client Port title"() {
		when:
			at AdminPage
		then:
			waitFor(30) { networkInterfaces.clientPortTitle.isDisplayed() }
			networkInterfaces.clientPortTitle.text().toLowerCase().equals("Client Port".toLowerCase())
	}

	def "check Admin Port title"() {
		when:
			at AdminPage
		then:
			waitFor(30) { networkInterfaces.adminPortTitle.isDisplayed() }
			networkInterfaces.adminPortTitle.text().toLowerCase().equals("Admin Port".toLowerCase())
	}

	def "check HTTP Port title"() {
		when:
			at AdminPage
		then:
			waitFor(30) { networkInterfaces.httpPortTitle.isDisplayed() }
			networkInterfaces.httpPortTitle.text().toLowerCase().equals("HTTP Port".toLowerCase())
	}

	def "check Internal Port title"() {
		when:
			at AdminPage
		then:
			waitFor(30) { networkInterfaces.internalPortTitle.isDisplayed() }
			networkInterfaces.internalPortTitle.text().toLowerCase().equals("Internal Port".toLowerCase())
	}

	def "check Zookeeper Port title"() {
		when:
			at AdminPage
		then:
			waitFor(30) { networkInterfaces.zookeeperPortTitle.isDisplayed() }
			networkInterfaces.zookeeperPortTitle.text().toLowerCase().equals("Zookeeper Port".toLowerCase())
	}

	def "check Replication Port title"() {
		when:
			at AdminPage
		then:
			waitFor(30) { networkInterfaces.replicationPortTitle.isDisplayed() }
			networkInterfaces.replicationPortTitle.text().toLowerCase().equals("Replication Port".toLowerCase())
	}

	def "check Client Port Value not empty"() {
		when:
			at AdminPage
		then:
			waitFor(10){ networkInterfaces.clusterClientPortValue.isDisplayed() }
			String string = networkInterfaces.clusterClientPortValue.text()
			!(string.equals(""))
	}

	def "check Admin Port Value not empty"() {
		when:
			at AdminPage
		then:
			waitFor(10){ networkInterfaces.clusterAdminPortValue.isDisplayed() }
			String string = networkInterfaces.clusterAdminPortValue.text()
			!(string.equals(""))
	}

	def "check HTTP Port Value not empty"() {
		when:
			at AdminPage
		then:
			waitFor(10){ networkInterfaces.clusterHttpPortValue.isDisplayed() }
			String string = networkInterfaces.clusterHttpPortValue.text()
			!(string.equals(""))
	}

	def "check Internal Port Value not empty"() {
		when:
			at AdminPage
		then:
			waitFor(30) { networkInterfaces.clusterInternalPortValue.isDisplayed() }
			String string = networkInterfaces.clusterInternalPortValue.text()
			!(string.equals(""))
	}

	def "check Zookeeper Port Value not empty"() {
		when:
			at AdminPage
		then:
			waitFor(10){ networkInterfaces.clusterZookeeperPortValue.isDisplayed() }
			String string = networkInterfaces.clusterZookeeperPortValue.text()
			!(string.equals(""))
	}

	def "check Replication Port Value not empty"() {
		when:
			at AdminPage
		then:
			waitFor(10){ networkInterfaces.clusterReplicationPortValue.isDisplayed() }
			String string = networkInterfaces.clusterReplicationPortValue.text()
			!(string.equals(""))
	}


    //download automation test
    def "when download configuration is clicked"() {
        when:
        at AdminPage
        waitFor(30) { downloadbtn.downloadconfigurationbutton.isDisplayed() }
        then:
        downloadbtn.downloadconfigurationbutton.click()
    }


    //cluster test

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

    def "when Resume cancel"(){
        when:
        at AdminPage
		waitFor(30) { cluster.resumebutton.isDisplayed() }
        cluster.resumebutton.click()
        then:
        waitFor(30) { cluster.resumeconfirmation.isDisplayed() }
        cluster.resumeconfirmation.text().toLowerCase().equals("Do you want to resume the cluster and exit admin mode?".toLowerCase());
        cluster.resumeconfirmation.resumecancel.click()
    }

    def "when Resume ok"(){
        when:
        at AdminPage
		waitFor(30) { cluster.resumebutton.isDisplayed() }
        cluster.resumebutton.click()
        then:
        waitFor(30) { cluster.resumeconfirmation.isDisplayed() }
        cluster.resumeconfirmation.text().toLowerCase().equals("Do you want to resume the cluster and exit admin mode?".toLowerCase());
        cluster.resumeconfirmation.resumeok.click()
    }

    def "when save for cancel"(){
        when:
        at AdminPage
		waitFor(30) { cluster.savebutton.isDisplayed() }
        cluster.savebutton.click()
        then:
        waitFor(30) { cluster.saveconfirmation.isDisplayed() }
        cluster.saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
        cluster.savecancel.click()
    }

    def "when save for yes"(){
        when:
        at AdminPage
		waitFor(30) { cluster.savebutton.isDisplayed() }
        cluster.savebutton.click()
        then:
        waitFor(30) { cluster.saveconfirmation.isDisplayed() }
        cluster.saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
        cluster.savedirectory.value("/var/opt/test/manual_snapshots")
        cluster.saveok.click()
        cluster.saveyes.click()
        // failsavedok.click()
    }

    def "when save for No"(){
        when:
        at AdminPage
		waitFor(30) { cluster.savebutton.isDisplayed() }
        cluster.savebutton.click()
        then:
        waitFor(30) { cluster.saveconfirmation.isDisplayed() }
        cluster.saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
        cluster.savedirectory.value("bbb")
        cluster.saveok.click()
        cluster.saveno.click()
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
		waitFor(30) { cluster.restorebutton.isDisplayed() }
        cluster.restorebutton.click()
        then:
        waitFor(30) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase());
        cluster.restorecancelbutton.click()

    }

    def "when restore and close"(){
        when:
        at AdminPage
		waitFor(30) { cluster.restorebutton.isDisplayed() }
        cluster.restorebutton.click()
        then:
        waitFor(30) { cluster.restoreconfirmation.isDisplayed() }
        cluster.restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase());
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


    //schema (SYSTEM OVERVIEW)

    def "check refresh button"(){
        when:
        at AdminPage
        waitFor(30) { page.refreshbutton.isDisplayed() }
        then:
        page.refreshbutton.click()
    }


    def "check system overview title"(){
        when:
        at AdminPage
        waitFor(30) { page.systemoverviewTitle.isDisplayed() }
        then:
        page.systemoverviewTitle.text().toLowerCase().equals("System Overview".toLowerCase())
    }



    def "check system overview content i.e, mode"(){
        when:
        at AdminPage
        waitFor(30) { page.modeTitle.isDisplayed() }
        then:
        page.modeTitle.text().toLowerCase().equals("Mode".toLowerCase())

    }


    def "check system overview content i.e, voltDBversion"(){
        when:
        at AdminPage
        waitFor(30) { page.voltdbversion.isDisplayed() }
        then:
        page.voltdbversion.text().toLowerCase().equals("VoltDB Version".toLowerCase())

    }


    def "check system overview content i.e, BuildString"(){
        when:
        at AdminPage
        waitFor(30) { page.buildstring.isDisplayed() }
        then:
        page.buildstring.text().toLowerCase().equals("Buildstring".toLowerCase())

    }


    def "check system overview content i.e, Cluster composition"(){
        when:
        at AdminPage
        waitFor(30) { page.clustercomposition.isDisplayed() }
        then:
        page.clustercomposition.text().toLowerCase().equals("Cluster Composition".toLowerCase())

    }


    def "check system overview content i.e, Running Since"(){
        when:
        at AdminPage
        waitFor(30) { page.runningsince.isDisplayed() }
        then:
        page.runningsince.text().toLowerCase().equals("Running Since".toLowerCase())

    }


    def "check system overview content i.e, mode-value"(){
       when:
       at AdminPage
       waitFor(30) { page.modevalue.isDisplayed() }
       then:
       page.modevalue.text().toLowerCase().equals("RUNNING".toLowerCase())
    }


       def "check system overview content i.e, voltDBversion-value"(){
           when:
           at AdminPage
           waitFor(30) { page.versionvalue.isDisplayed() }
           then:
           page.versionvalue.text().toLowerCase().equals("5.1".toLowerCase())

       }


       def "check system overview content i.e, buildstring-value"(){
           when:
           at AdminPage
           waitFor(30) { page.buildstringvalue.isDisplayed() }
           then:
           page.buildstringvalue.text().toLowerCase().equals("voltdb-4.7-2198-g23683d1-dirty-local".toLowerCase())

       }


       def "check system overview content i.e, clusterComposition-value"(){
           when:
           at AdminPage
           waitFor(30) { page.clustercompositionvalue.isDisplayed() }
           then:
           page.clustercompositionvalue.text().toLowerCase().equals("1 hosts with 8 sites (8 per host)".toLowerCase())

       }


     def "check system overview content i.e, RunningSince-value"(){
         when:
         at AdminPage
         waitFor(30) { page.runningsincevalue.isDisplayed() }
         then:
        // page.runningsincevalue.text().toLowerCase().equals("Tue Feb 24 08:20:31 GMT+00:00 2015 (0d 3h 56m)".toLowerCase())
         page.runningsincevalue.isDisplayed()
     }

    //schema CATALOG OVERVIEW STATISTICS


       def "check Catalog Overview Statistics title"(){
           when:
           at AdminPage
           waitFor(30) { page.catalogoverviewstatistic.isDisplayed() }
           then:
           page.catalogoverviewstatistic.text().toLowerCase().equals("Catalog Overview Statistics".toLowerCase())

       }


       def "check Catalog Overview Statistics content i.e, Compiled by VoltDB Version"(){
           when:
           at AdminPage
           waitFor(30) { page.compiledversion.isDisplayed() }
           then:
           page.compiledversion.text().toLowerCase().equals("Compiled by VoltDB Version".toLowerCase())

       }


       def "check Catalog Overview Statistics content i.e, Compiled on"(){
           when:
           at AdminPage
           waitFor(30) { page.compiledonTitle.isDisplayed() }
           then:
           page.compiledonTitle.text().toLowerCase().equals("Compiled on".toLowerCase())

       }


       def "check Catalog Overview Statistics content i.e, Table Count"(){
           when:
           at AdminPage
           waitFor(30) { page.tablecount.isDisplayed() }
           then:
           page.tablecount.text().toLowerCase().equals("Table Count".toLowerCase())

       }


       def "check Catalog Overview Statistics content i.e, Materialized View Count"(){
           when:
           at AdminPage
           waitFor(30) { page.materializedviewcount.isDisplayed() }
           then:
           page.materializedviewcount.text().toLowerCase().equals("Materialized View Count".toLowerCase())

       }


       def "check Catalog Overview Statistics content i.e, Index Count"(){
           when:
           at AdminPage
           waitFor(30) { page.indexcount.isDisplayed() }
           then:
           page.indexcount.text().toLowerCase().equals("Index Count".toLowerCase())

       }


       def "check Catalog Overview Statistics content i.e, Procedure Count"(){
           when:
           at AdminPage
           waitFor(30) { page.procedurecount.isDisplayed() }
           then:
           page.procedurecount.text().toLowerCase().equals("Procedure Count".toLowerCase())

       }


       def "check Catalog Overview Statistics content i.e, SQL Statement Count"(){
           when:
           at AdminPage
           waitFor(30) { page.sqlstatementcount.isDisplayed() }
           then:
           page.sqlstatementcount.text().toLowerCase().equals("SQL Statement Count".toLowerCase())

       }


     def "check Catalog Overview Statistics content i.e, compiled by voltdb version-value"(){
         when:
         at AdminPage
         waitFor(30) { page.compiledversionvalue.isDisplayed() }
         then:
         page.compiledversionvalue.text().toLowerCase().equals("5.1".toLowerCase())

     }

     def "check Catalog Overview Statistics content i.e, compiled on-value"(){
         when:
         at AdminPage
         waitFor(30) { page.compiledonTitlevalue.isDisplayed() }
         then:
         page.compiledonTitlevalue.text().toLowerCase().equals("Tue, 24 Feb 2015 08:20:28 GMT+00:00".toLowerCase())

     }

     def "check Catalog Overview Statistics content i.e, table count-value"(){
         when:
         at AdminPage
         waitFor(30) { page.tablecountvalue.isDisplayed() }
         then:
         page.tablecountvalue.text().toLowerCase().equals("3 (1 partitioned / 2 replicated)".toLowerCase())

     }

     def "check Catalog Overview Statistics content i.e, materilized view count-value"(){
         when:
         at AdminPage
         waitFor(30) { page.materializedviewcountvalue.isDisplayed() }
         then:
         page.materializedviewcountvalue.text().toLowerCase().equals("2".toLowerCase())

     }


     def "check Catalog Overview Statistics content i.e, index count-value"(){
         when:
         at AdminPage
         waitFor(30) { page.indexcountvalue.isDisplayed() }
         then:
         page.indexcountvalue.text().toLowerCase().equals("4".toLowerCase())

     }


     def "check Catalog Overview Statistics content i.e, procedure count-value"(){
         when:
         at AdminPage
         waitFor(30) { page.procedurecountvalue.isDisplayed() }
         then:
         page.procedurecountvalue.text().toLowerCase().equals("5 (1 partitioned / 4 replicated) (3 read-only / 2 read-write)".toLowerCase())

     }

     def "check Catalog Overview Statistics content i.e, sql statement count-value"(){
         when:
         at AdminPage
         waitFor(30) { page.sqlstatementcountvalue.isDisplayed() }
         then:
         page.sqlstatementcountvalue.text().toLowerCase().equals("10".toLowerCase())

     }

    //documentation


    def "check documentation right footer"(){
        when:
        at AdminPage
        waitFor(30) { page.documentationrightlabel.isDisplayed() }
        then:
        page.documentationrightlabel.text().toLowerCase().equals("Generated by VoltDB 5.1 on 24 Feb 2015 08:20:28 GMT+00:00".toLowerCase())

    }


       def "check documentation url"(){
           when:
           at AdminPage
           waitFor(30) { page.documentationurl.isDisplayed() }
           then:
           page.documentationurl.click()

       }


    def "check DDL source Title"(){
        when:
        at AdminPage
        then:
        waitFor(30) { page.ddlsourceTitle.isDisplayed() }
    }


    def "check DDL source download"() {
        when:
        at AdminPage
        waitFor(30) { page.ddlsourcedownload.isDisplayed() }
        then:
        page.ddlsourcedownload.click()
    }


    def "check DDL source bunch of queries"(){
        when:
        at AdminPage
        then:
        waitFor(30) { page.ddlsourcequeries.isDisplayed() }

    }





    // DIRECTORIES

    def "check Directories title"() {
        when:
        at AdminPage
        then:
        waitFor(30) { directories.title.isDisplayed() }
        directories.title.text().toLowerCase().equals("Directories".toLowerCase())
    }

    def "check Root title"() {
        when:
        at AdminPage
        then:
        waitFor(30) { directories.rootTitle.isDisplayed() }
        directories.rootTitle.text().toLowerCase().equals("Root (Destination)".toLowerCase())
    }

    def "check Snapshot title"() {
        when:
        at AdminPage
        then:
        waitFor(30) { directories.snapshotTitle.isDisplayed() }
        directories.snapshotTitle.text().toLowerCase().equals("Snapshot".toLowerCase())
    }

    def "check Export Overflow title"() {
        when:
        at AdminPage
        then:
        waitFor(30) { directories.exportOverflowTitle.isDisplayed() }
        directories.exportOverflowTitle.text().toLowerCase().equals("Export Overflow".toLowerCase())
    }

    def "check Command Logs title"() {
        when:
        at AdminPage
        then:
        waitFor(30) { directories.commandLogsTitle.isDisplayed() }
        directories.commandLogsTitle.text().toLowerCase().equals("Command Logs".toLowerCase())
    }

    def "check Command Log Snapshots title"() {
        when:
        at AdminPage
        then:
        waitFor(30) { directories.commandLogSnapshotTitle.isDisplayed() }
        directories.commandLogSnapshotTitle.text().toLowerCase().equals("Command Log Snapshots".toLowerCase())
    }

    def "check Root Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(10){ directories.rootValue.isDisplayed() }
        String string = directories.rootValue.text()
        !(string.equals(""))
    }

    def "check SnapShot Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(10){ directories.snapshotValue.isDisplayed() }
        String string = directories.snapshotValue.text()
        !(string.equals(""))
    }

    def "check Export Overflow Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(10){ directories.exportOverflowValue.isDisplayed() }
        String string = directories.exportOverflowValue.text()
        !(string.equals(""))
    }

    def "check Command Logs Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(10){ directories.commandLogsValue.isDisplayed() }
        String string = directories.commandLogsValue.text()
        !(string.equals(""))
    }

    def "check Log Snapshot Value not empty"() {
        when:
        at AdminPage
        then:
        waitFor(10){ directories.commandLogSnapshotValue.isDisplayed() }
        String string = directories.commandLogSnapshotValue.text()
        !(string.equals(""))
    }

    // Overview

    def "check title"() {
        when:
        at AdminPage
        then:
		waitFor(30) { overview.title.isDisplayed() }
        overview.title.text().toLowerCase().equals("Overview".toLowerCase())
    }

    def "check Site Per Host"() {
        when:
        at AdminPage
        then:
		waitFor(30) { overview.sitePerHost.isDisplayed() }
        overview.sitePerHost.text().toLowerCase().equals("Sites Per Host".toLowerCase())
    }

    def "check K-safety"() {
        when:
        at AdminPage
        then:
		waitFor(30) { overview.ksafety.isDisplayed() }
        overview.ksafety.text().toLowerCase().equals("K-safety".toLowerCase())
    }

    def "check Partition Detection"() {
        when:
        at AdminPage
        then:
		waitFor(30) { overview.partitionDetection.isDisplayed() }
        overview.partitionDetection.text().toLowerCase().equals("Partition detection".toLowerCase())
    }

    def "check Security"() {
        when:
        at AdminPage
        then:
		waitFor(30) { overview.security.isDisplayed() }
        overview.security.text().toLowerCase().equals("Security".toLowerCase())
    }

    def "check HTTP Access"() {
        when:
        at AdminPage
        then:
		waitFor(30) { overview.httpAccess.isDisplayed() }
        overview.httpAccess.text().toLowerCase().equals("HTTP Access".toLowerCase())
    }

    def "check Auto Snapshots"() {
        when:
        at AdminPage
        then:
		waitFor(30) { overview.autoSnapshots.isDisplayed() }
        overview.autoSnapshots.text().toLowerCase().equals("Auto Snapshots".toLowerCase())
    }

    def "check Command Logging"() {
        when:
        at AdminPage
        then:
		waitFor(30) { overview.commandLogging.isDisplayed() }
        overview.commandLogging.text().toLowerCase().equals("Command Logging".toLowerCase())
    }

    def "check Export"() {
        when:
        at AdminPage
        then:
		waitFor(30) { overview.export.isDisplayed() }
        overview.export.text().toLowerCase().equals("Export".toLowerCase())
    }

    def "check Advanced"() {
        when:
        at AdminPage
        then:
		waitFor(30) { overview.advanced.isDisplayed() }
        overview.advanced.text().toLowerCase().equals("Advanced".toLowerCase())
    }

    //values

    def "check Site Per Host value"() {
        when:
        at AdminPage
        then:
        waitFor(10){ overview.sitePerHostValue.isDisplayed() }
        String string = overview.sitePerHostValue.text()
        !(string.equals(""))
    }

    def "check K-safety value"() {
        when:
        at AdminPage
        then:
        waitFor(10){ overview.ksafetyValue.isDisplayed() }
        String string = overview.ksafetyValue.text()
        !(string.equals(""))
    }

    def "check Partition Detection value"() {
        when:
        at AdminPage
        then:
        waitFor(10){ overview.partitionDetectionValue.isDisplayed() }
        String string = overview.partitionDetectionValue.text()
        !(string.equals(""))
    }

    def "check Security value"() {
        when:
        at AdminPage
        then: 
        waitFor(10){ overview.securityValue.isDisplayed() }
        String string = overview.securityValue.text()
        !(string.equals(""))
    }


    def "check HTTP Access value"() {
        when:
        at AdminPage
        then:
        waitFor(10){ overview.httpAccessValue.isDisplayed() }
        String string = overview.httpAccessValue.text()
        !(string.equals(""))
    }

    def "check Auto Snapshots value"() {
        when:
        at AdminPage
        then:
        waitFor(10){ overview.autoSnapshotsValue.isDisplayed() }
        String string = overview.autoSnapshotsValue.text()
        !(string.equals(""))
    }

    def "check Command Logging value"() {
        when:
        at AdminPage
        then:
        waitFor(10){ overview.commandLoggingValue.isDisplayed() }
        String string = overview.commandLoggingValue.text()
        !(string.equals(""))
    }

    // edit

    //--security

    def "click security button"(){
        when:
        at AdminPage
        waitFor(30) { overview.securityEdit.isDisplayed() }
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
        waitFor(30) { overview.securityEditOk.isDisplayed() }
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
        waitFor(30) { overview.securityEditOk.isDisplayed() }
        overview.securityEditOk.click()
        overview.securityPopupOk.click()
    }

    // --Auto snapshot

    def "check Auto Snapshots edit"() {
        when:
        at AdminPage
        then:
        waitFor(10){ overview.autoSnapshotsEdit.isDisplayed() }
        String string = overview.autoSnapshotsEdit.text()
        !(string.equals(""))
    }

    def "click edit Auto Snapshots and check"() {
        when:
        at AdminPage
        then:
        waitFor(10) {
            overview.autoSnapshotsEdit.isDisplayed()
        }
        overview.autoSnapshotsEdit.click()
		
		waitFor(30) {
		    overview.autoSnapshotsEditCheckbox.isDisplayed()
		    overview.autoSnapshotsEditOk.isDisplayed()
		    overview.autoSnapshotsEditCancel.isDisplayed()
		}
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
		waitFor(30) {
		    overview.autoSnapshotsEditOk.isDisplayed()
		    overview.autoSnapshotsEditCancel.isDisplayed()
		}

        when:
        overview.autoSnapshotsEditCancel.click()
        then:
		waitFor(30) {
		    !(overview.autoSnapshotsEditCancel.isDisplayed())
		    !(overview.autoSnapshotsEditOk.isDisplayed())
		}
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
