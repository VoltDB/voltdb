import geb.spock.GebReportingSpec
import org.junit.Test
import voltDBadmin
import voltDBhome


/**
 * Created by lavthaiba on 2/2/2015.
 */
class voltDBSpec extends GebReportingSpec {

   @Test
    def "popup test"() {

        given:
        to voltDBhome

        def $line
        new File("src/test/groovy/users.txt").withReader { $line = it.readLine() }



        when: "submitted"
       /* if ($line.startsWith('#'))
          //  $line.append.println('\n')
        Integer.parseInt($line.replaceAll("[\\(\\)(#)(//)]", ""))
        //$line.add.println('\n')*/

            loginBoxuser1.value($line)
            loginBoxuser2.value($line)
            loginbtn.click()


       //empty username and empty password
     /*if ( loginBoxuser1.value("") &&
       loginBoxuser2.value("")&&
       loginbtn.click()){println('empty username and empty password')}


       //correct username and wrong password
       if(loginBoxuser1.value("deerwalk")&&
       loginBoxuser2.value("deer")&&
       loginbtn.click()) {println('correct username and wrong password')}

       //correct username and empty password
       if(loginBoxuser1.value("deerwalk")&&
       loginBoxuser2.value("")&&
       loginbtn.click()){println('correct username and empty password')}

       //incorrect username and correct password
       if(loginBoxuser1.value("deer")&&
       loginBoxuser2.value("deerwalk")&&
       loginbtn.click()){println('incorrect username and correct password')}

       //empty username and correct password
       if(loginBoxuser1.value("")&&
       loginBoxuser2.value("deerwalk")&&
       loginbtn.click()){println('empty username and correct password')}

       //empty username and incorrect password
      if(loginBoxuser1.value("")&&
       loginBoxuser2.value("deer")&&
       loginbtn.click()){println('empty username and incorrect password')}


       //incorrect username and incorrect password
       if(loginBoxuser1.value("deer")&&
       loginBoxuser2.value("deer")&&
       loginbtn.click()){println('incorrect username and incorrect password')}

       //correct username and correct password
       if(loginBoxuser1.value("deerwalk")&&
       loginBoxuser2.value("deerwalk")&&
       loginbtn.click()){println('correct username and correct password')}
*/

        at voltDBadmin
       // showadmin.click()
        then:
        showadmin.click()
        //at voltDBadmin
    }

/*
       @Test
       def "popup test fail"() {

           given:
           to voltDBhome

           def $line
           new File("src/test/groovy/users.txt").withReader { $line = it.readLine() }

           when: "submitted"
               loginBoxuser1.value("not")
               loginBoxuser2.value($line)
               loginbtn.click()

           then:
               at voltDBhome
       }
*/

    // HEADER TESTS



  /*  @Test
    def "header exists at admin" () {
        when:
            at voltDBadmin
        then:
            header.headerBanner.isDisplayed();
    }
   
    @Test
    def "header image exists" () {
        when:
            at voltDBadmin
        then:
            header.headerImage.isDisplayed();
    }*/

 /*   @Test
    def "header username exists" () {
        when:
            at voltDBadmin
        then:
            header.headerUsername.isDisplayed();
    }*/
   
  /*  @Test
    def "header logout exists" () {
        when:
            at voltDBadmin
        then:
            header.headerLogout.isDisplayed();
    }*/

  /*  @Test
    def "header help exists" () {
        when:
            at voltDBadmin
        then:
            header.headerHelp.isDisplayed();
    }
*/


    // HEADER TAB TESTS


   /*
    @Test
    def "header tab dbmonitor exists" ()  {
        when:
            at voltDBadmin
        then:
            header.headerTabDBMonitor.isDisplayed();
            header.headerTabDBMonitor.text().toLowerCase().equals("DB Monitor".toLowerCase())
    }
   
    @Test
    def "header tab admin exists" ()  {
        when:
            at voltDBadmin
        then:
            header.headerTabAdmin.isDisplayed();
            header.headerTabAdmin.text().toLowerCase().equals("Admin".toLowerCase())
    }
   
    @Test
    def "header tab schema exists" ()  {
        when:
            at voltDBadmin
        then:
            header.headerTabSchema.isDisplayed();
            header.headerTabSchema.text().toLowerCase().equals("Schema".toLowerCase())
    }
   
    @Test
    def "header tab sql query exists" ()  {
        when:
            at voltDBadmin
        then:
            header.headerTabSQLQuery.isDisplayed();
            header.headerTabSQLQuery.text().toLowerCase().equals("SQL Query".toLowerCase())
    }*/



    // USERNAME TEST


   
 /*   @Test
    def "header username check" () {
        def $line
        new File("src/test/groovy/users.txt").withReader { $line = it.readLine() }
        
        when:
            at voltDBadmin
        then:
            header.headerUsername.text().equals($line);
    }
   */

  

    /*@Test
    def "header username click and close" () {
        when:
            at voltDBadmin
        then:
            header.headerUsername.click()
            header.headerLogoutPopup.isDisplayed()
            header.headerLogoutPopupTitle.isDisplayed()
            header.headerLogoutPopupOkButton.isDisplayed()
            header.headerLogoutPopupCancelButton.isDisplayed()
            header.headerPopupClose.isDisplayed()
            header.headerPopupClose.click()
    }*/
   
    /*@Test
    def "header username click and cancel" () {
        when:
            at voltDBadmin
        then:
            header.headerUsername.click()
            header.headerLogoutPopup.isDisplayed()
            header.headerLogoutPopupTitle.isDisplayed()
            header.headerLogoutPopupOkButton.isDisplayed()
            header.headerLogoutPopupCancelButton.isDisplayed()
            header.headerPopupClose.isDisplayed()
            header.headerLogoutPopupCancelButton.click()
    }*/



    // LOGOUT TEST


   /*
    @Test
    def "logout button test close" ()  {
        when:
            at voltDBadmin
        then:
            header.headerLogout.click()
            header.headerLogoutPopup.isDisplayed()
            header.headerLogoutPopupTitle.isDisplayed()
            header.headerLogoutPopupOkButton.isDisplayed()
            header.headerLogoutPopupCancelButton.isDisplayed()
            header.headerPopupClose.isDisplayed()
            header.headerPopupClose.click()
      
    }


    

    @Test
    def "logout button test cancel" ()  {
        when:
        at voltDBadmin
        then:
        header.headerLogout.click()
        header.headerLogoutPopup.isDisplayed()
        header.headerLogoutPopupTitle.isDisplayed()
        header.headerLogoutPopupOkButton.isDisplayed()
        header.headerLogoutPopupCancelButton.isDisplayed()
        header.headerPopupClose.isDisplayed()
        header.headerLogoutPopupCancelButton.click()

    }
    
*/

    // HELP POPUP TEST


    
  /*  @Test
    def "help popup existance" () {
        when:
            at voltDBadmin
            header.headerHelp.click()
        then:
            header.headerPopup.isDisplayed()
            header.headerPopupTitle.text().toLowerCase().equals("help".toLowerCase());
            header.headerPopupClose.click()
    }
*/


    // FOOTER TESTS


/*
   @Test
   def "footer exists at admin" () {
       when:
           at voltDBadmin
       then:
           footer.footerBanner.isDisplayed();
   }

   @Test
   def "footer text exists and valid at admin"() {

       when:
           at voltDBadmin
       then:
           footer.footerBanner.isDisplayed();
           footer.footerText.text().toLowerCase().contains("VoltDB. All rights reserved.".toLowerCase());
   }*/

    // LOGOUT


 /* @Test
  def "logout" () {
      when:
          at voltDBadmin
          header.headerLogout.click()
          header.headerLogoutPopupOkButton.click()
      then:
          at voltDBhome
          loginBoxuser1.isDisplayed()
          loginBoxuser2.isDisplayed()
          loginbtn.isDisplayed()
  }*/






    //           LARGER TEST
    /*@Test
    def "footer exists test at admin" () {
        given:
            to voltDBhome

        def $line
        new File("src/test/groovy/users.txt").withReader { $line = it.readLine() }

        when: "submitted"
            loginBoxuser1.value($line)
            loginBoxuser2.value($line)
            loginbtn.click()
            at voltDBadmin
            showadmin.click()

        then:
            at voltDBadmin
            footerBanner.footer.isDisplayed();
    }
*/



    //***********************************Network interfaces***************************//
/*

    @Test
    def "check Network Interfaces title" () {
        networkinterfacesTitle.isDisplayed()
        networkinterfacesTitle.text().toLowerCase.equals("Network Interfaces".toLowerCase())
    }


    @Test
    def "check port Name title" () {
        portnameTitle.isDisplayed()
        portnameTitle.text().portnameTitle.equals("Port Name".toLowerCase())
    }



    @Test
    def "check client port" () {
        clientportTitle.isDisplayed()
        clientportTitle.text().clientportTitle.equals("Client Port".toLowerCase())
    }

    @Test
    def "check admin port" () {
        adminportTitle.isDisplayed()
        adminportTitle.text().adminportTitle.equals("Admin Port".toLowerCase())
    }

    @Test
    def "check HTTP port" () {
        httpportTitle.isDisplayed()
        httpportTitle.text().httpportTitle.equals("HTTP Port".toLowerCase())
    }

    @Test
    def "check Internal port" () {
        internalportTitle.isDisplayed()
        internalportTitle.text().internalportTitle.equals("HTTP Port".toLowerCase())
    }

    @Test
    def "check Zookeeper port" () {
        zookeeperportTitle.isDisplayed()
        zookeeperportTitle.text().zookeeperportTitle.equals("Zookeeper Port".toLowerCase())
    }

    @Test
    def "check Replication port" () {
        replicationportTitle.isDisplayed()
        replicationportTitle.text().replicationportTitle.equals("Replication Port".toLowerCase())
    }

    @Test
   def "check Cluster Setting title" () {
       clustersettingTitle.isDisplayed()
       clustersettingTitle.text().clustersettingTitle.equals("Cluster Setting".toLowerCase())
   }

    @Test
   def "check Client port value" () {
       clientportTitlevalue.isDisplayed()
       clientportTitlevalue.text().clientportTitlevalue.equals(21212)
   }

    @Test
   def "check Admin port value" () {
       adminportTitlevalue.isDisplayed()
       adminportTitlevalue.text().adminportTitlevalue.equals(21211)
   }


    @Test
   def "check HTTP port value" () {
       httpportTitlevalue.isDisplayed()
       httpportTitlevalue.text().httpportTitlevalue.equals(8080)
   }

    @Test
  def "check Internal port value" () {
      internalportTitlevalue.isDisplayed()
      internalportTitlevalue.text().internalportTitlevalue.equals(3021)
  }

    @Test
  def "check Zookeeper port value" () {
      zookeeperportTitlevalue.isDisplayed()
      zookeeperportTitlevalue.text().zookeeperportTitlevalue.equals(2181)
  }

    @Test
def "check Replication port value" () {
    replicationportTitlevalue.isDisplayed()
    replicationportTitlevalue.text().replicationportTitlevalue.equals(5555)
}
*/

    // ***********************************DIRECTORIES*********************************//


   /*
    @Test
    def "check directories title" () {
        directoriesTitle.isDisplayed()
        directoriesTitle.text().toLowerCase.equals("directories".toLowerCase())
    }

    @Test
    def "check directories root(destination) title" () {
        directoriesRootTitle.isDisplayed()
        directoriesRootTitle.text().toLowerCase.equals("Root (Destination)".toLowerCase())
    }

    @Test
    def "check directories snapshot title" () {
        directoriesSnapshotTitle.isDisplayed()
        directoriesSnapshotTitle.text().toLowerCase.equals("Snapshot".toLowerCase())
    }

    @Test
    def "check directories export overflow title" () {
        directoriesExpertOverflowTitle.isDisplayed()
        directoriesExpertOverflowTitle.text().toLowerCase.equals("Export Overflow".toLowerCase())
    }

    @Test
    def "check directories command logs title" () {
        directoriesCommandLogsTitle.isDisplayed()
        directoriesCommandLogsTitle.text().toLowerCase.equals("Command Logs".toLowerCase())
    }

    @Test
    def "check directories command log snapshots title" () {
        directoriesCommandLogSnapshotTitle.isDisplayed()
        directoriesCommandLogSnapshotTitle.text().toLowerCase.equals("Command log Snapshots".toLowerCase())
    }

    @Test
    def "check directories root(destination) value" () {
        directoriesRootValue.isDisplayed()
        directoriesRootValue.text().toLowerCase.equals("voltdbroot".toLowerCase())
    }

    @Test
    def "check directories snapshots value" () {
        directoriesSnapshotsValue.isDisplayed()
        directoriesSnapshotsValue.text().toLowerCase.equals("snapshots".toLowerCase())
    }
    @Test
    def "check directories expert overflow value" () {
        directoriesExpertOverflowValue.isDisplayed()
        directoriesExpertOverflowValue.text().toLowerCase.equals("export_overflow".toLowerCase())
    }

    @Test
    def "check directories command logs value" () {
        directoriesCommandLogsValue.isDisplayed()
        directoriesCommandLogsValue.text().toLowerCase.equals("command_log".toLowerCase())
    }

    @Test
    def "check directories command log snapshot value" () {
        directoriesCommandLogSnapshot.isDisplayed()
        directoriesCommandLogSnapshot.text().toLowerCase.equals("command_log_snapshot".toLowerCase())
    }
*/


    //************************ CLUSTER****************************//


    @Test
    def "cluster title"(){
            clusterTitle.isDisplayed()
            clusterTitle.text().toLowerCase.equals("Cluster".toLowerCase())

    }


// RESUME and Pause Button

/*
    @Test
    def "check whether cluster is pause or resume"(){
        if (pausebutton.isDisplayed()){

                at voltDBadmin
                pausebutton.click()
                pausecancel.click()
            } else
        {

            at voltDBadmin
            resumebutton.click()
            resumeok.click()

        }
    }
*/

    /*// in case of pause for cancel
    @Test
    def "when cluster pause in cancel"() {

        when:
        at voltDBadmin
        pausebutton.click()
        then:
        pauseconfirmation.isDisplayed()
        pauseconfirmation.text().equals("Pause: Confirmation");
        pausecancel.click()
    }

    // in case of pause for ok
    @Test
    def "when cluster pause in ok"() {

        when:
        at voltDBadmin
        pausebutton.click()
        then:
        pauseconfirmation.isDisplayed()
        pauseconfirmation.text().equals("Pause: Confirmation");
        pauseok.click()
    }
*/
    //in case of resume for ok
   /* @Test
    def "when cluster resume in ok"() {

        when:
        at voltDBadmin
        resumebutton.click()
        then:
        resumeconfirmation.isDisplayed()
        resumeconfirmation.text().equals("Resume: Confirmation");
        resumeok.click()
    }
    // in case of resume for cancel
    @Test
    def "when cluster resume"() {

        when:
        at voltDBadmin
        resumebutton.click()
        then:
        resumeconfirmation.isDisplayed()
        resumeconfirmation.text().equals("Resume: Confirmation");
        resumecancel.click()
    }*/





    // when cancel
    /*@Test
    def "when Resume cancel"(){
        when:
        at voltDBadmin
        resumebutton.click()
        then:
        resumeconfirmation.isDisplayed()
        resumeconfirmation.text().toLowerCase().equals("Do you want to resume the cluster and exit admin mode?".toLowerCase());
        resumeconfirmation.resumecancel.click()
    }

            // when resume ok
    @Test
    def "when Resume ok"(){
        when:
        at voltDBadmin
        resumebutton.click()
        then:
        resumeconfirmation.isDisplayed()
        resumeconfirmation.text().toLowerCase().equals("Do you want to resume the cluster and exit admin mode?".toLowerCase());
        resumeconfirmation.resumeok.click()
    }*/

  /*  @Test
    def "when save for close"(){
        when:
        at voltDBadmin
        savebutton.click()
        then:
        saveconfirmation.isDisplayed()
        saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
        saveclose.click()
    }
*/
/*
   @Test
    def "when save for cancel"(){
        when:
        at voltDBadmin
        savebutton.click()
        then:
        saveconfirmation.isDisplayed()
        saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
        savecancel.click()
    }*/


/*
   @Test
    def "when save for ok"(){
        when:
        at voltDBadmin
        savebutton.click()
        then:
        saveconfirmation.isDisplayed()
        saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
       savedirectory.value("aa")
        saveok.click()
    }*/

 /*   @Test
    def "when save for close if wanted to save"(){
        when:
        at voltDBadmin
        savebutton.click()
        then:
        saveconfirmation.isDisplayed()
        saveconfirmation.text().toLowerCase().equals("Save".toLowerCase());
        savedirectory.value("aa")
        saveok.click()
        wanttosavecloseconfirm.text().toLowerCase().equals("Save".toLowerCase());
        wantotosaveclosebutton.click()
    }*/

  /*  @Test
    def "when restore and cancel"(){
        when:
        at voltDBadmin
        restorebutton.click()
        then:
        restoreconfirmation.isDisplayed()
        restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase());
        restorecancelbutton.click()

    }


    @Test
    def "when restore and close"(){
        when:
        at voltDBadmin
        restorebutton.click()
        then:
        restoreconfirmation.isDisplayed()
        restoreconfirmation.text().toLowerCase().equals("Restore".toLowerCase());
        restoreclosebutton.click()

    }*/
/*
    @Test
    def "when shutdown cancel button"(){
        when:
        at voltDBadmin
        shutdownbutton.click()
        then:
        shutdownconfirmation.isDisplayed()
        shutdownconfirmation.text().toLowerCase().equals("Shutdown: Confirmation".toLowerCase())
        shutdowncancelbutton.click()
    }

    @Test
    def "when shutdown close button"(){
        when:
        at voltDBadmin
        shutdownbutton.click()
        then:
        shutdownconfirmation.isDisplayed()
        shutdownconfirmation.text().toLowerCase().equals("Shutdown: Confirmation".toLowerCase())
        shutdownclosebutton.click()
    }*/

/*
   @Test
    def "when download configuration is clicked"(){
        when:
        at voltDBadmin
       downloadconfigurationbutton.isDisplayed()
       then:
       downloadconfigurationbutton.click()
   }
*/

  /*  @Test
    def "when server is clicked"() {
        when:
        at voltDBadmin
        serverbutton.isDisplayed()
        then:
        serverbutton.click()
        serverconfirmation.text().toLowerCase().equals("Servers".toLowerCase())
        serverbutton.click()
    }*/
}