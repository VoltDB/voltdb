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

class AdminSecurityUser extends TestBase {
	int insideCount = 0
	boolean loopStatus = false
	
    def setup() { // called before each test
		int count = 0
		
		
		while(count<numberOfTrials) {
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

	def "Admin Page: Add users in security"() {
		testStatus 				= false
		int count 				= 0
		int countForFiveTrials	= 0
		int five 				= 5
		loopStatus 				= false
		boolean created 		= false
		boolean login 			= false 
		boolean createSameUser 	= false
		boolean editUser 		= false
		
		String usernameOne 	= page.overview.getUsernameOneForSecurity()
		String usernameTwo 	= page.overview.getUsernameTwoForSecurity()
		String passwordOne	= page.overview.getPasswordOneForSecurity()
		String passwordTwo 	= page.overview.getPasswordTwoForSecurity()
		String roleOne		= page.overview.getRoleOneForSecurity()
		String roleTwo		= page.overview.getRoleTwoForSecurity()
		
		String username		= page.header.getUsername()
		String password		= page.header.getPassword()
		
		expect: 'at Admin Page'
		at AdminPage
		
		while (count < numberOfTrials) {
			count ++
			
			try {
				insideCount = 0
				
				if( created == false ) {
					while (insideCount < numberOfTrials) {
						insideCount ++
					
						when: 'Security button is clicked'
						page.overview.expandSecurity()
						then: 'Security expand'
						page.overview.checkIfSecurityIsExpanded()
				
						when: 'Security add button is clicked'
						page.overview.openSecurityAdd()
						then: 'Popup is displayed'
						page.overview.checkSecurityAddOpen()
				
						when: 'Username, Password and Role is given'
						page.overview.enterUserCredentials(usernameOne, passwordOne, roleOne)
						then:
						if(overview.checkListForUsers(usernameOne) == true) {
							loopStatus = true
							created = true
							break
						}
					}
				
					if (loopStatus == false) {
						println("The username wasn't created in " + numberOfTrials + " trials")
						assert false
					}
				}
				
				// LOGOUT AND THEN LOGIN AS usernameOne
				
				
				if(login == false) {
					when: 'logout button is clicked and popup is displayed'
					waitFor(30) {
						page.header.logout.click()
						page.header.logoutPopupOkButton.isDisplayed()
					}
				
					then: 'logout is confirmed and popup is removed'
					waitFor(30) {
						page.header.logoutPopupOkButton.click()
						!page.header.logoutPopupOkButton.isDisplayed()
					}
					at LoginLogoutPage
				
					insideCount = 0
					while(insideCount < numberOfTrials) {
						try {
							insideCount++
							when: 'at Login Page'
							at LoginLogoutPage
							then: 'enter as the new user'
							page.loginBoxuser1.value(usernameOne)
							page.loginBoxuser2.value(passwordOne)
							page.loginbtn.click()
							at VoltDBManagementCenterPage
							break
						
						} catch (org.openqa.selenium.ElementNotVisibleException e) {
							println("ElementNotVisibleException: Unable to Start the test")
							println("Retrying")
						}
					}
			
					// LOGOUT AND LOGIN AS admin
					when: 'logout button is clicked and popup is displayed'
					waitFor(30) {
						page.header.logout.click()
						page.header.logoutPopupOkButton.isDisplayed()
					}
				
					then: 'logout is confirmed and popup is removed'
					waitFor(30) {
						page.header.logoutPopupOkButton.click()
						!page.header.logoutPopupOkButton.isDisplayed()
					}
					to LoginLogoutPage
				
					insideCount = 0
					while(insideCount < numberOfTrials) {
						try {
							insideCount++
							when: 'at Login Page'
							at LoginLogoutPage
							then: 'enter as the admin'
							page.loginBoxuser1.value(username)
							page.loginBoxuser2.value(password)
							page.loginbtn.click()
							at VoltDBManagementCenterPage
				
							break
						} catch (org.openqa.selenium.ElementNotVisibleException e) {
							println("ElementNotVisibleException: Unable to Start the test")
							println("Retrying")
						}
					}
				
					// GO TO ADMIN PAGE
					when: 'click the Admin link (if needed)'
					page.openAdminPage()
					then: 'should be on Admin page'
					at AdminPage
					login = true
				}
				// TRY TO CREATE NEW USER WITH THE SAME username AS usernameOne
				
				if(createSameUser == false) {
					when: 'Security button is clicked'
					page.overview.expandSecurity()
					then: 'Check if Security expanded or nots'
					page.overview.checkIfSecurityIsExpanded()
				
					when: 'Security add button is clicked'
					page.overview.openSecurityAdd()
					then: 'Popup is displayed'
					page.overview.checkSecurityAddOpen()
					
					when: 'Username, Password and Role is given'
					page.overview.enterUserCredentials(usernameOne, passwordOne, roleOne)
					then: 'Error message is displayed'
					page.overview.userPopupSave.click()
					waitFor(waitTime) {
						page.overview.errorUsernameMessage.isDisplayed()
						page.overview.errorUsernameMessage.text().equals("This username already exists.")
					}
					println("Duplicate username wasn't allowed with success")
					page.overview.userPopupCancel.click()
					createSameUser = true
				}		

				// EDIT THE USER usernameOne AND CHANGE IT TO usernameTwo		
				
				if(editUser == false) {
					insideCount = 0
					while(insideCount < numberOfTrials) {
						insideCount ++
					
						when: 'Click Edit User button'
						page.overview.openEditUser()
						then: 'Edit User popup is displayed'
						page.overview.checkSecurityEditOpen()
				
						when: 'Username, Password and Role is given'
						page.overview.enterUserCredentials(usernameTwo, passwordTwo, roleTwo)
						then:
						if(overview.checkListForUsers(usernameTwo) == true) {
							loopStatus = true
							editUser = true
							break
						}
					}
				
					if (loopStatus == false) {
						println("The username wasn't edited in " + numberOfTrials + " trials")
						assert false
					}
				}

				// DELETE THE USER
				insideCount = 0
				while(insideCount < numberOfTrials) {
					insideCount ++
					
					when: 'Security button is clicked'
					page.overview.expandSecurity()
					then: 'Check if Security expanded or nots'
					page.overview.checkIfSecurityIsExpanded()
					
					when: 'Click Edit User button'
					page.overview.openEditUserNext()
					then: 'Edit User popup is displayed'
					page.overview.checkSecurityEditOpen()
				
					when: 'User was deleted'
					page.overview.deleteUserSecurityPopup()
					then: 'check for the user'
					if(overview.checkListForUsers(usernameTwo) == false) {
						println("deletion successful")
						loopStatus = true
						break
					}
				}
				
				if (loopStatus == false) {
					println("The username wasn't deleted in " + numberOfTrials + " trials")
					assert false
				}
				
				testStatus = true
				break
			} catch(geb.waiting.WaitTimeoutException e) {
				println("Wait Timeout Exception Occurred: Retrying")
				at AdminPage
				testStatus = false
			} catch(org.openqa.selenium.StaleElementReferenceException e) {
				println("Stale Element Reference Exception Occurred: Retrying")
				at AdminPage
				testStatus = false
			}
		}
		
		if(testStatus == true) {
			println("PASS")
		}
		else {
			println("FAIL")
			assert false
		}
	}
    /*
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

        when: 'Security button is clicked'
        page.overview.expandSecurity()
        then: 'Check if Security expanded or nots'
        page.overview.checkIfSecurityIsExpanded()


        try {
            insideCount = 0
            while(insideCount < numberOfTrials) {
                insideCount ++

                when: 'Security button is clicked'
                page.overview.expandSecurity()
                then: 'Check if Security expanded or nots'
                page.overview.checkIfSecurityIsExpanded()

                when: 'Click Edit User button'
                page.overview.openEditUserNext()
                then: 'Edit User popup is displayed'
                page.overview.checkSecurityEditOpen()

                when: 'User was deleted'
                page.overview.deleteUserSecurityPopup()
                then: 'check for the user'
                if(overview.checkListForUsers(usernameTwo) == false) {
                    println("deletion successful")
                    loopStatus = true
                    break
                }
            }
        } catch(geb.error.RequiredPageContentNotPresent e) {
            println("Already deleted")
        }
    }
	*/
}
