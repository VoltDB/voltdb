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

class AdminSnmpTest extends TestBase {
    def setup() { // called before each test
        int count = 0

        while(count<numberOfTrials) {
            count ++
            try {
                setup: 'Open VMC page'
                to VoltDBManagementCenterPage
                page.loginIfNeeded()
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

    def checkSnmpTitles() {
        int count = 0
        testStatus = false

        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.snmpTitle.isDisplayed()
                    page.snmpTitle.text().toLowerCase().equals("SNMP".toLowerCase())
                }
                then:
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }

        when:'click title'
            page.snmpTitle.click()
        then:
            page.snmpTarget.isDisplayed()
            page.snmpTarget.text().toLowerCase().equals("Target".toLowerCase())
            page.snmpCommunity.isDisplayed()
            page.snmpCommunity.text().toLowerCase().equals("Community".toLowerCase())
            page.snmpUsername.isDisplayed()
            page.snmpUsername.text().toLowerCase().equals("username".toLowerCase())
            page.snmpAuthenticationProtocol.isDisplayed()
            page.snmpAuthenticationProtocol.text().toLowerCase().equals("Authentication Protocol".toLowerCase())
            page.snmpAuthenticationKey.isDisplayed()
            page.snmpAuthenticationKey.text().toLowerCase().equals("Authentication Key".toLowerCase())
            page.snmpPrivacyProtocol.isDisplayed()
            page.snmpPrivacyProtocol.text().toLowerCase().equals("Privacy Protocol".toLowerCase())
            page.snmpPrivacyKey.isDisplayed()
            page.snmpPrivacyKey.text().toLowerCase().equals("Privacy Key".toLowerCase())
    }

    def checkSnmpButtons() {
        int count = 0
        testStatus = false
        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    page.snmpEditButton.isDisplayed()
                }
                then:
                page.snmpEditButton.click()
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }

    }

    def targetValueNotEmpty() {
        int count = 0
        testStatus = false
        boolean isPro = false
        boolean snmpEnabled = false

        expect: 'at Admin Page'

        when: "check Pro version"
        if (waitFor(10){page.snmpTitle.isDisplayed()}) {
            isPro = true
        }
        else{
            assert false
        }
        then: "check SNMP enabled"
        if (isPro == true) {
            if (page.snmpEnabled.text().toLowerCase().equals("On")) {
                snmpEnabled = true
            }
        }
        when: "check edit snmp button displayed"
        if (page.editSnmpButton.isDisplayed()) {
            page.editSnmpButton.click()
        }
        if (waitFor(10) { page.editSnmpOkButton.isDisplayed() }) {
            page.editSnmpOkButton.click()
        }

        then: "check target validation"
        if (snmpEnabled == true) {
            if (page.errorTarget.isDisplayed()) {
                println("PASS")
            } else {
                println("FAIL: Test didn't pass")
                assert false
            }

        }

    }

    def checkCommunityDefaultValue(){
        int count = 0
        testStatus = false
        expect: 'at Admin Page'
        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    if (page.editSnmpButton.isDisplayed()) {
                        page.editSnmpButton.click()
                    }
                }
                then:
                if(page.txtCommunity.value().equals("public")){
                    assert true
                }
                else{
                    println("default value for community is not set")
                    assert false
                }
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
//        when: "click edit button"
//            if (page.editSnmpButton.isDisplayed()) {
//                page.editSnmpButton.click()
//            }
//        then:
//            if(page.txtCommunity.value().equals("public")){
//                assert true
//            }
//            else{
//                println("default value for community is not set")
//                assert false
//            }
    }

    def checkAuthKeyDefaultValue(){
        int count = 0
        testStatus = false
        expect: 'at Admin Page'
        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    if (page.editSnmpButton.isDisplayed()) {
                        page.editSnmpButton.click()
                    }
                }
                then:
                if(page.txtAuthkey.value().equals("voltdbauthkey")){
                    assert true
                }
                else{
                    println("default value for authkey is not set")
                    assert false
                }
                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
    }

    def checkPrivKeyDefaultValue(){
        int count = 0
        testStatus = false
        expect: 'at Admin Page'

        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    if (page.editSnmpButton.isDisplayed()) {
                        page.editSnmpButton.click()
                    }
                }
                then:
                if(page.txtPrivkey.value().equals("voltdbprivacykey")){
                    assert true
                }
                else{
                    println("default value for privkey is not set")
                    assert false
                }

                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }
        if(testStatus == true) {
            println("PASS")
        }
        else {
            println("FAIL: Test didn't pass in " + numberOfTrials + " trials")
            assert false
        }
    }

    def checkInvalidTarget(){
        int count = 0
        testStatus = false
        boolean isPro = false
        boolean snmpEnabled = false

        expect: 'at Admin Page'

        when: "check Pro version"

        if (waitFor(10){page.snmpTitle.isDisplayed()}) {
            isPro = true
        }
        else{
            assert false
        }
        then: "check SNMP enabled"
        if (isPro == true) {
            if (page.snmpEnabled.text().toLowerCase().equals("On")) {
                snmpEnabled = true
            }
        }
        when: "check edit snmp button displayed"
        if (page.editSnmpButton.isDisplayed()) {
            page.editSnmpButton.click()

        }

        then:
        if(page.chkSNMPDiv.isDisplayed()){
            page.chkSNMPDiv.click()
            page.snmpEnabled.text().toLowerCase().equals("On")
        }
        if (waitFor(10) { page.editSnmpOkButton.isDisplayed() }) {
            page.txtTarget.value("asdfasdf")
            page.editSnmpOkButton.click()
        }

        when: "check target validation"
        report "target"
        if (snmpEnabled == true) {
            if (page.errorTarget.isDisplayed()) {
                page.errorTarget.text().toLowerCase().equals("This field is required")
                println("PASS")
            } else {
                println("FAIL: Test didn't pass")
                assert false
            }

        }
        then:
            assert true
    }

    def checkInvalidAuthKey(){
        int count = 0
        testStatus = false
        boolean isPro = false
        boolean snmpEnabled = false

        expect: 'at Admin Page'

        when: "check Pro version"

        if (waitFor(10){page.snmpTitle.isDisplayed()}) {
            isPro = true
        }
        else{
            assert false
        }
        then: "check SNMP enabled"
        if (isPro == true) {
            if (page.snmpEnabled.text().toLowerCase().equals("On")) {
                snmpEnabled = true
            }
        }
        when: "check edit snmp button displayed"
        if (page.editSnmpButton.isDisplayed()) {
            page.editSnmpButton.click()

        }

        then:
        if(page.chkSNMPDiv.isDisplayed()){
            page.chkSNMPDiv.click()
            page.snmpEnabled.text().toLowerCase().equals("On")
        }
        if (waitFor(10) { page.editSnmpOkButton.isDisplayed() }) {
            page.txtAuthkey.value("")
            page.editSnmpOkButton.click()
        }

        when: "check auth key validation"
        if (snmpEnabled == true) {
            if (page.errorAuthkey.isDisplayed()) {
                page.errorAuthkey.text().toLowerCase().equals("This field is required")
                println("PASS")
            } else {
                println("FAIL: Test didn't pass")
                assert false
            }
        }
        then:
        assert true

        when: "check no auth condition"
        page.ddlAuthProtocol.value("NoAuth")
        then: "click edit ok button"
        if (waitFor(10) { page.editSnmpOkButton.isDisplayed() }) {
            page.txtAuthkey.value("")
            page.txtTarget.value("10.10.1.2:89")
            page.editSnmpOkButton.click()
        }
        if (snmpEnabled == true) {
            if (page.errorAuthkey.isDisplayed()) {
                assert false
                println("No validation required")
            } else {
                println("FAIL: Test didn't pass")
                assert true
            }
        }

        when: "check auth key with username given and auth given"
        page.txtUsername.value("sfd")
        then: "click edit ok button"
        if (waitFor(10) { page.editSnmpOkButton.isDisplayed() }) {
            page.txtAuthkey.value("")
            page.txtTarget.value("10.10.1.2:89")
            page.editSnmpOkButton.click()
        }
        if (snmpEnabled == true) {
            if (page.errorAuthkey.isDisplayed()) {
                assert true
                page.errorAuthkey.text().toLowerCase().equals("This field is required")
            } else {
                println("FAIL: Test didn't pass")
                assert false
            }
        }

        when: "check max 6 character auth key validation with username given and auth given"
        page.txtUsername.value("sfd")
        then: "click edit ok button"
        if (waitFor(10) { page.editSnmpOkButton.isDisplayed() }) {
            page.txtAuthkey.value("df")
            page.txtTarget.value("10.10.1.2:89")
            page.editSnmpOkButton.click()
        }
        if (snmpEnabled == true) {
            if (page.errorAuthkey.isDisplayed()) {
                assert true
                page.errorAuthkey.text().toLowerCase().equals("Please enter at least 8 characters.")
            } else {
                println("FAIL: Test didn't pass")
                assert false
            }
        }

    }

    def checkInvalidPrivKey(){
        int count = 0
        testStatus = false
        boolean isPro = false
        boolean snmpEnabled = false

        expect: 'at Admin Page'

        when: "check Pro version"

        if (waitFor(10){page.snmpTitle.isDisplayed()}) {
            isPro = true
        }
        else{
            assert false
        }
        then: "check SNMP enabled"
        if (isPro == true) {
            if (page.snmpEnabled.text().toLowerCase().equals("On")) {
                snmpEnabled = true
            }
        }
        when: "check edit snmp button displayed"
        if (page.editSnmpButton.isDisplayed()) {
            page.editSnmpButton.click()

        }

        then:
        if(page.chkSNMPDiv.isDisplayed()){
            page.chkSNMPDiv.click()
            page.snmpEnabled.text().toLowerCase().equals("On")
        }
        if (waitFor(10) { page.editSnmpOkButton.isDisplayed() }) {
            page.txtPrivkey.value("")
            page.editSnmpOkButton.click()
        }

        when: "check auth key validation"
        if (snmpEnabled == true) {
            if (page.errorPrivkey.isDisplayed()) {
                page.errorPrivkey.text().toLowerCase().equals("This field is required")
                println("PASS")
            } else {
                println("FAIL: Test didn't pass")
                assert false
            }
        }
        then:
        assert true

        when: "check no auth condition"
        page.ddlAuthProtocol.value("NoAuth")
        then: "click edit ok button"
        if (waitFor(10) { page.editSnmpOkButton.isDisplayed() }) {
            page.txtPrivkey.value("")
            page.txtTarget.value("10.10.1.2:89")
            page.editSnmpOkButton.click()
        }
        if (snmpEnabled == true) {
            if (page.errorPrivkey.isDisplayed()) {
                assert false
                println("No validation required")
            } else {
                println("FAIL: Test didn't pass")
                assert true
            }
        }

        when: "check auth key with username given and auth given"
        page.txtUsername.value("sfd")
        then: "click edit ok button"
        if (waitFor(10) { page.editSnmpOkButton.isDisplayed() }) {
            page.txtPrivkey.value("")
            page.txtTarget.value("10.10.1.2:89")
            page.editSnmpOkButton.click()
        }
        if (snmpEnabled == true) {
            if (page.errorPrivkey.isDisplayed()) {
                assert true
                page.errorPrivkey.text().toLowerCase().equals("This field is required")
            } else {
                println("FAIL: Test didn't pass")
                assert false
            }
        }

        when: "check max 6 character auth key validation with username given and auth given"
        page.txtUsername.value("sfd")
        then: "click edit ok button"
        if (waitFor(10) { page.editSnmpOkButton.isDisplayed() }) {
            page.txtPrivkey.value("df")
            page.txtTarget.value("10.10.1.2:89")
            page.editSnmpOkButton.click()
        }
        if (snmpEnabled == true) {
            if (page.errorPrivkey.isDisplayed()) {
                assert true
                page.errorPrivkey.text().toLowerCase().equals("Please enter at least 8 characters.")
            } else {
                println("FAIL: Test didn't pass")
                assert false
            }
        }

    }

    def editSNMPWhenOff(){
        int count = 0
        testStatus = false
        boolean isPro = false
        boolean snmpEnabled = false

        expect: 'at Admin Page'

        when: "check Pro version"

        if (waitFor(10){page.snmpTitle.isDisplayed()}) {
            isPro = true
        }
        else{
            assert false
        }
        then: "check SNMP enabled"
        if (isPro == true) {
            if (page.snmpEnabled.text().toLowerCase().equals("On")) {
                snmpEnabled = true
            }
            else{
                println("SNMP is Off")
            }
        }
        when: "check edit snmp button displayed"
        if (page.editSnmpButton.isDisplayed()) {
            page.editSnmpButton.click()

        }
        then: "click edit ok button"
        if (waitFor(10) { page.editSnmpOkButton.isDisplayed() }) {
            page.editSnmpOkButton.click()
            println("must save SNMP config")
        }
        when:"click confirm ok button"
        if(waitFor(10){page.btnSaveSnmp.isDisplayed()}){
            page.btnSaveSnmp.click()
        }
        then:"check save status"
            if(page.loadingSnmp.isDisplayed()){
                //need to check saved data here
                println("save is working properly")
            }


    }

    def editSNMPWhenOn(){
        int count = 0
        testStatus = false
        boolean isPro = false
        boolean snmpEnabled = false

        expect: 'at Admin Page'

        when: "check Pro version"

        if (waitFor(10){page.snmpTitle.isDisplayed()}) {
            isPro = true
        }
        else{
            assert false
        }
        then: "check SNMP enabled"
        if (isPro == true) {
            if (page.snmpEnabled.text().toLowerCase().equals("On")) {
                snmpEnabled = true
            }
        }
        when: "check edit snmp button displayed"
        if (page.editSnmpButton.isDisplayed()) {
            page.editSnmpButton.click()

        }

        then:
        if(page.chkSNMPDiv.isDisplayed()){
            page.chkSNMPDiv.click()
            page.snmpEnabled.text().toLowerCase().equals("On")
        }

        when: "check edit snmp button displayed"
        if (page.editSnmpButton.isDisplayed()) {
            page.editSnmpButton.click()

        }
        then: "click edit ok button"
        if (waitFor(10) { page.editSnmpOkButton.isDisplayed() }) {
            page.txtTarget.value("10.10.1.2:89")
            page.editSnmpOkButton.click()
            println("must save SNMP config")
        }
        when:"click confirm ok button"
        if(waitFor(10){page.btnSaveSnmp.isDisplayed()}){
            page.btnSaveSnmp.click()
        }
        then:"check save status"
        if(page.loadingSnmp.isDisplayed()){
            //need to check saved data here
            println(page.targetSpan.text())
            waitFor(10){page.targetSpan.text().toLowerCase().equals("10.10.1.2:89")}
            println("save is working properly")
        }

        when: "check Pro version"

        if (waitFor(10){page.snmpTitle.isDisplayed()}) {
            isPro = true
        }
        else{
            assert false
        }
        then: "check SNMP enabled"
        if (isPro == true) {
            if (page.snmpEnabled.text().toLowerCase().equals("On")) {
                snmpEnabled = true
            }
        }
        when: "check edit snmp button displayed"
        if (page.editSnmpButton.isDisplayed()) {
            page.editSnmpButton.click()
        }

        then:
        if(page.chkSNMPDiv.isDisplayed()){
            page.chkSNMPDiv.click()
            page.snmpEnabled.text().toLowerCase().equals("Off")
        }

        when: "click edit ok button"
        if (waitFor(10) { page.editSnmpOkButton.isDisplayed() }) {
            waitFor(10){page.txtTarget.value("")}
            page.editSnmpOkButton.click()
            println("must save SNMP config")
        }
        then:"click confirm ok button"
        while(count<numberOfTrials) {
            count ++
            try {
                when:
                waitFor(waitTime) {
                    if(page.btnSaveSnmp.isDisplayed()){
                        page.btnSaveSnmp.click()
                    }
                }
                then:
                if(page.loadingSnmp.isDisplayed()){
                    //need to check saved data here
                    waitFor(10){page.txtTarget.text().toLowerCase().equals("")}
                    println("save is working properly")
                }

                testStatus = true
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("RETRYING: WaitTimeoutException occured")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("RETRYING: StaleElementReferenceException occured")
            }
        }




    }
}
