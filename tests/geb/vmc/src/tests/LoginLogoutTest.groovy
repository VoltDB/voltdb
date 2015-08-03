/**
 * Created by anrai on 2/12/15.
 */
package vmcTest.tests

import vmcTest.pages.*
import geb.Page;

class LoginLogoutTest extends TestBase {

    def "Login Test Valid Username and Password"() {
        int count = 0
        boolean status = false
        setup: 'Open VMC page'
        to VoltDBManagementCenterPage
        expect: 'to be on VMC page'
        at VoltDBManagementCenterPage

        when: 'click the Admin link (if needed)'
        page.openAdminPage()
        then: 'should be on Admin page'
        at AdminPage

        when:'Check Security Enabled'
        waitFor(waitTime) { page.overview.securityValue.isDisplayed() }
        String security = page.overview.securityValue.text();
        then:
        if(page.overview.securityValue.text().equals("Off"))
        {
            println("PASS")
        }



       if(security=="On")
       {
           while(count<numberOfTrials) {
               count++
               try {
                   when:
                   waitFor(waitTime) {
                       page.header.logout.click()
                       page.header.logoutPopupOkButton.isDisplayed()
                   }

                   waitFor(waitTime) {
                       page.header.logoutPopupOkButton.click()
                       !page.header.logoutPopupOkButton.isDisplayed()
                   }

                   page.loginValid()

                   then:
                   at VoltDBManagementCenterPage
                   status = true
                   break
               } catch(geb.waiting.WaitTimeoutException e) {
                   to VoltDBManagementCenterPage
                   println("Wait Timeout Exception: Retrying")
                   status = false
               } catch(org.openqa.selenium.StaleElementReferenceException e) {
                   to VoltDBManagementCenterPage
                   println("Stale Element Exception: Retrying")
                   status = false
               }
           }

           if(status == true) {
               println("Login Test Valid Username and Password:PASS")
               assert true
           }
           else {
               println("Login Test Valid Username and Password:FAIL")
               assert false
           }
       }
//        then: 'should be on DB Monitor page'
//        at DbMonitorPage
//        if(security=="On") {
//            waitFor(30) {  header.logout.isDisplayed() }
//        }
//        setup: 'Open VMC page'
//        to VoltDBManagementCenterPage
//        expect: 'to be on VMC page'
//        at VoltDBManagementCenterPage
        

    }
    
	def "Login Test Invalid Username and Password"() {
        int count = 0
        boolean status = false

        setup: 'Open VMC page'
        to VoltDBManagementCenterPage
        expect: 'to be on VMC page'
        at VoltDBManagementCenterPage

        when: 'click the Admin link (if needed)'
        page.openAdminPage()
        then: 'should be on Admin page'
        at AdminPage

        when:'Check Security Enabled'
        waitFor(waitTime) { page.overview.securityValue.isDisplayed() }
        String security = page.overview.securityValue.text();
        then:
        if(page.overview.securityValue.text().equals("Off"))
        {
            println("PASS")
        }



        if(security=="On")
        {
            while(count<numberOfTrials) {
                count++
                try {
                    when:
                    waitFor(waitTime) {
                        page.header.logout.click()
                        page.header.logoutPopupOkButton.isDisplayed()
                    }

                    waitFor(waitTime) {
                        page.header.logoutPopupOkButton.click()
                        !page.header.logoutPopupOkButton.isDisplayed()
                    }

                    try{
                        page.loginInvalid()
                    }
                    catch (Exception e) {
                        assert true
                    }

                    then:
                    at VoltDBManagementCenterPage
                    status = true
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    to VoltDBManagementCenterPage
                    println("Wait Timeout Exception: Retrying")
                    status = false
                } catch(org.openqa.selenium.StaleElementReferenceException e) {
                    to VoltDBManagementCenterPage
                    println("Stale Element Exception: Retrying")
                    status = false
                }
            }

            if(status == true) {
                println("Login Test Valid Username and Password:PASS")
                assert true
            }
            else {
                println("Login Test Valid Username and Password:FAIL")
                assert false
            }
        }
    }

    def "Login Test Blank Username and Password"() {
        int count = 0
        boolean status = false

        setup: 'Open VMC page'
        to VoltDBManagementCenterPage
        expect: 'to be on VMC page'
        at VoltDBManagementCenterPage

        when: 'click the Admin link (if needed)'
        page.openAdminPage()
        then: 'should be on Admin page'
        at AdminPage

        when:'Check Security Enabled'
        waitFor(waitTime) { page.overview.securityValue.isDisplayed() }
        String security = page.overview.securityValue.text();
        then:
        if(page.overview.securityValue.text().equals("Off"))
        {
            println("PASS")
        }



        if(security=="On")
        {
            while(count<numberOfTrials) {
                count++
                try {
                    when:
                    waitFor(waitTime) {
                        page.header.logout.click()
                        page.header.logoutPopupOkButton.isDisplayed()
                    }

                    waitFor(waitTime) {
                        page.header.logoutPopupOkButton.click()
                        !page.header.logoutPopupOkButton.isDisplayed()
                    }

                    try{
                        page.loginEmpty()
                    }
                    catch (Exception e) {
                        assert true
                    }

                    then:
                    at VoltDBManagementCenterPage
                    status = true
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    to VoltDBManagementCenterPage
                    println("Wait Timeout Exception: Retrying")
                    status = false
                } catch(org.openqa.selenium.StaleElementReferenceException e) {
                    to VoltDBManagementCenterPage
                    println("Stale Element Exception: Retrying")
                    status = false
                }
            }

            if(status == true) {
                println("Login Test Valid Username and Password:PASS")
                assert true
            }
            else {
                println("Login Test Valid Username and Password:FAIL")
                assert false
            }
        }


    }

}
