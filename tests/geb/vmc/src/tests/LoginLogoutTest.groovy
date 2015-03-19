/**
 * Created by anrai on 2/12/15.
 */
package vmcTest.tests

import vmcTest.pages.*
import geb.Page;

class LoginLogoutTest extends TestBase {

    def "Login Test Valid Username and Password"() {
        setup: 'Open VMC page'
        to VoltDBManagementCenterPage
        expect: 'to be on VMC page'
        at VoltDBManagementCenterPage
        
        when:
        waitFor(30) {
        	page.header.logout.click()
        	page.header.logoutPopupOkButton.isDisplayed()
        }
        
        waitFor(30) {
        	page.header.logoutPopupOkButton.click()
        	!page.header.logoutPopupOkButton.isDisplayed()
        }
        
        
       	page.login()
       	
       	then:
       	at VoltDBManagementCenterPage
    }
	
	def "Login Test Blank Username and Password"() {
        setup: 'Open VMC page'
        to VoltDBManagementCenterPage
        expect: 'to be on VMC page'
        at VoltDBManagementCenterPage
        
        when:
        waitFor(30) {
        	page.header.logout.click()
        	page.header.logoutPopupOkButton.isDisplayed()
        }
        
        waitFor(30) {
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
    }
    
    def "Login Test Blank Username and Password"() {
        setup: 'Open VMC page'
        to VoltDBManagementCenterPage
        expect: 'to be on VMC page'
        at VoltDBManagementCenterPage
        
        when:
        waitFor(30) {
        	page.header.logout.click()
        	page.header.logoutPopupOkButton.isDisplayed()
        }
        
        waitFor(30) {
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
    }

}
