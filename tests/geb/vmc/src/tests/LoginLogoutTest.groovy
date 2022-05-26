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

import vmcTest.pages.*
import geb.Page;

class LoginLogoutTest extends TestBase {

    def verifyLoginWithValidUsernameAndPassword() {
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
        if(page.overview.securityValue.text().equals("Off")) {
            println("PASS")
        }
        if(security=="On") {
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

    def verityLoginWithInvalidUsernameAndPassword() {
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
        if(page.overview.securityValue.text().equals("Off")) {
            println("PASS")
        }

        if(security=="On") {
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

    def verifyLoginWithBlankUsernameAndPassword() {
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
        if(page.overview.securityValue.text().equals("Off")) {
            println("PASS")
        }

        if(security=="On") {
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
