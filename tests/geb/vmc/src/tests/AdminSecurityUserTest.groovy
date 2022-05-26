/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

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

class AdminSecurityUserTest extends TestBase {
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

    def addUsersInSecurity() {
        insideCount = 0
        loopStatus              = false
        boolean created         = false
        String usernameOne  = page.overview.getUsernameOneForSecurity()

        String passwordOne  = page.overview.getPasswordOneForSecurity()

        String roleOne      = page.overview.getRoleOneForSecurity()


        String username     = page.header.getUsername()
        String password     = page.header.getPassword()

        expect: 'at Admin Page'
        at AdminPage

        when:'Check Security Enabled'
        at AdminPage
        waitFor(waitTime) { page.overview.securityValue.isDisplayed() }
        then:
        if(page.overview.securityValue.text().equals("Off")) {
            println("PASS")
        }
        else if (page.overview.securityValue.text().equals("On")) {
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
        }
    }

    def logoutAndThenLoginAsUsernameOne() {
        String usernameOne  = page.overview.getUsernameOneForSecurity()

        String passwordOne  = page.overview.getPasswordOneForSecurity()

        when:'Check Security Enabled'
        at AdminPage
        waitFor(waitTime) { page.overview.securityValue.isDisplayed() }
        then:
        if(page.overview.securityValue.text().equals("Off")) {
            println("PASS")
        }
        else if (page.overview.securityValue.text().equals("On")) {
            when: 'logout button is clicked and popup is displayed'
            waitFor(waitTime) {
                page.header.logout.click()
                page.header.logoutPopupOkButton.isDisplayed()
            }
            then: 'logout is confirmed and popup is removed'
            waitFor(waitTime) {
                page.header.logoutPopupOkButton.click()
                !page.header.logoutPopupOkButton.isDisplayed()
            }
            at LoginLogoutPage

            // insideCount = 0
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
        }
    }

    def logoutAsUsernameOneAndLoginAsAdmin() {
        when:'Check Security Enabled'
        at AdminPage
        waitFor(waitTime) { page.overview.securityValue.isDisplayed() }
        then:
        if(page.overview.securityValue.text().equals("Off")) {
            println("PASS")
        }
        else if (page.overview.securityValue.text().equals("On")) {
            when: 'logout button is clicked and popup is displayed'
            waitFor(waitTime) {
                page.header.logout.click()

            }
            waitFor(10){page.header.logoutPopupOkButton.isDisplayed()}
            then: 'logout is confirmed and popup is removed'
            waitFor(waitTime) {
                page.header.logoutPopupOkButton.click()
                !page.header.logoutPopupOkButton.isDisplayed()
            }
            //to LoginLogoutPage

            while(insideCount < numberOfTrials) {
                try {
                    insideCount++
                    when: 'at Login Page'
                    at LoginLogoutPage
                    then: 'enter as the admin'
                    page.loginBoxuser1.value("admin")
                    page.loginBoxuser2.value("voltdb")
                    page.loginbtn.click()
                    at VoltDBManagementCenterPage

                    break
                } catch (org.openqa.selenium.ElementNotVisibleException e) {
                    println("ElementNotVisibleException: Unable to Start the test")
                    println("Retrying")
                }
            }
        }
    }

    def tryToCreateNewUserWithTheSameUsernameAsUsernameOne() {
    String usernameOne  = page.overview.getUsernameOneForSecurity()
    String roleOne      = page.overview.getRoleOneForSecurity()
    String passwordOne  = page.overview.getPasswordOneForSecurity()

    //              // TRY TO CREATE NEW USER WITH THE SAME username AS usernameOne

        when:'Check Security Enabled'
        at AdminPage
        waitFor(waitTime) { page.overview.securityValue.isDisplayed() }
        then:
        if(page.overview.securityValue.text().equals("Off")) {
            println("PASS")
        }
        else if (page.overview.securityValue.text().equals("On")) {
            when: 'Security button is clicked'
            page.overview.expandSecurity()
            then: 'Check if Security expanded or notes'
            try {
                waitFor(10) { page.overview.securityExpanded.isDisplayed() }
            } catch (geb.error.RequiredPageContentNotPresent e) {
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
            }
            //waitFor(20){ page.overview.checkIfSecurityIsExpanded()}

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
        }
    }

    def editTheUserUsernameoneAndChangeItToUsernametwo(){

        String usernameTwo  = page.overview.getUsernameTwoForSecurity()
        String passwordTwo  = page.overview.getPasswordTwoForSecurity()
        String roleTwo      = page.overview.getRoleTwoForSecurity()

        // EDIT THE USER usernameOne AND CHANGE IT TO usernameTwo
        when:'Check Security Enabled'
        at AdminPage
        waitFor(waitTime) { page.overview.securityValue.isDisplayed() }
        then:
        if(page.overview.securityValue.text().equals("Off")) {
            println("PASS")
        }
        else if (page.overview.securityValue.text().equals("On")) {
            when: 'Security button is clicked'
            page.overview.expandSecurity()
            then: 'Check if Security expanded or notes'

            when: 'Click Edit User button'
            page.overview.openEditUser()
            then: 'Edit User popup is displayed'
            //page.overview.checkSecurityEditOpen()
            try {
                waitFor(waitTime) {
                    page.overview.userPopupUsernameField.isDisplayed()
                    page.overview.userPopupPasswordField.isDisplayed()
                    page.overview.userPopupSave.isDisplayed()
                }
            } catch(geb.error.RequiredPageContentNotPresent e) {

            } catch(geb.waiting.WaitTimeoutException e) {

            }

            when: 'Username, Password and Role is given'
            page.overview.enterUserCredentials(usernameTwo, passwordTwo, roleTwo)
            then:
            if(overview.checkListForUsers(usernameTwo) == true) {
                loopStatus = true
            }
            if (loopStatus == false) {
                println("The username wasn't edited in " + numberOfTrials + " trials")
                assert false
            }
        }
    }

    def deleteTheUser() {
        String usernameTwo  = page.overview.getUsernameTwoForSecurity()

        // DELETE THE USER
        when:'Check Security Enabled'
        at AdminPage
        waitFor(waitTime) { page.overview.securityValue.isDisplayed() }
        then:
        if(page.overview.securityValue.text().equals("Off")) {
            println("PASS")
        }
        else if (page.overview.securityValue.text().equals("On")) {
            when: 'Security button is clicked'
            page.overview.expandSecurity()
            then: 'Check if Security expanded or notes'

            when: 'Open Edit'
            page.overview.openEditUserNext()
            then: 'Edit User popup is displayed'
            page.overview.checkSecurityEditOpen()

            when: 'User was deleted'
            page.overview.deleteUserSecurityPopup()
            then: 'check for the user'
            if(overview.checkListForUsers(usernameTwo) == false) {
                println("deletion successful")
            }
        }
    }
}
