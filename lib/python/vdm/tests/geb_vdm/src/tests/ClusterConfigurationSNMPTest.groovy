/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

class ClusterConfigurationSNMPTest extends TestBase {

    def setup() { // called before each test
        count = 0

        while(count<numberOfTrials) {
            count ++
            try {
                setup: 'Open Cluster Settings page'
                to ClusterSettingsPage
                expect: 'to be on Cluster Settings page'
                at ClusterSettingsPage

                break
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                println("ElementNotVisibleException: Unable to Start the test")
                println("Retrying")
            }
        }
    }

    def CheckSNMPPanel(){
        int countNext = 0
        int indexOfNewDatabase = 0
        String newDatabaseName = ""
        when: 'Create database'
        for(count=0; count<numberOfTrials; count++) {
            try {
                indexOfNewDatabase = createNewDatabase(create_DatabaseTest_File)
                break
            } catch(Exception e) {
                deleteDatabase(create_DatabaseTest_File)
            } catch(org.codehaus.groovy.runtime.powerassert.PowerAssertionError e) {
                //Do Nothing
            }
        }
        newDatabaseName = "name_src"
        then: 'Select new database'
        for (count = 0; count < numberOfTrials; count++) {
            try {
                for(countNext=0; countNext<numberOfTrials; countNext++) {
                    try {
                        waitFor { buttonAddDatabase.isDisplayed() }
                        break
                    } catch(geb.waiting.WaitTimeoutException exception) {
                        currentDatabase.click()
                    }
                }
                $(id:getIdOfDatabase(String.valueOf(indexOfNewDatabase))).click()
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }

        //Check if SNMP configurations are displayed
        Boolean result = false
        try{
            waitFor{ snmp.lblTarget.isDisplayed() }
            result = true
        } catch(geb.waiting.WaitTimeoutException exception){
            result = false
        }

        if(result){
            snmp.tdTargetVal.isDisplayed()
            snmp.tdCommunityVal.isDisplayed()
            snmp.tdUsernameVal.isDisplayed()
            snmp.tdAuthProtocolVal.isDisplayed()
            snmp.tdAuthKeyVal.isDisplayed()
            snmp.tdPrivacyProtocolVal.isDisplayed()
            snmp.tdPrivacyKeyVal.isDisplayed()

        } else {
            when:
            waitFor(20){snmp.snmpTitle.isDisplayed()}
            snmp.snmpTitle.click()
            then:
            snmp.tdTargetVal.isDisplayed()
            snmp.tdCommunityVal.isDisplayed()
            snmp.tdUsernameVal.isDisplayed()
            snmp.tdAuthProtocolVal.isDisplayed()
            snmp.tdAuthKeyVal.isDisplayed()
            snmp.tdPrivacyProtocolVal.isDisplayed()
            snmp.tdPrivacyKeyVal.isDisplayed()
        }

    }

    def CheckSNMPConfigValidation() {
        int countNext = 0
        int indexOfNewDatabase = 0
        String newDatabaseName = ""
        when: 'Create database'
        for(count=0; count<numberOfTrials; count++) {
            try {
                indexOfNewDatabase = createNewDatabase(create_DatabaseTest_File)
                break
            } catch(Exception e) {
                deleteDatabase(create_DatabaseTest_File)
            } catch(org.codehaus.groovy.runtime.powerassert.PowerAssertionError e) {
                //Do Nothing
            }
        }
        newDatabaseName = "name_src"
        then: 'Select new database'
        for (count = 0; count < numberOfTrials; count++) {
            try {
                for(countNext=0; countNext<numberOfTrials; countNext++) {
                    try {
                        waitFor { buttonAddDatabase.isDisplayed() }
                        break
                    } catch(geb.waiting.WaitTimeoutException exception) {
                        currentDatabase.click()
                    }
                }
                $(id:getIdOfDatabase(String.valueOf(indexOfNewDatabase))).click()
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }

        // Create a SNMP configuration
        when: 'Open popup for SNMP'
        waitFor(20){ snmp.btnUpdateSNMP.isDisplayed() }
        snmp.btnUpdateSNMP.click()
        waitFor(20) { snmp.btnSaveSNMP.isDisplayed() }

        then: 'Check if id is displayed.'
        waitFor {
            snmp.txtTarget.isDisplayed()
            snmp.txtUsername.isDisplayed()
            snmp.selectAuthProtocol.isDisplayed()
            snmp.txtAuthKey.isDisplayed()
            snmp.selectPrivacyProtocol.isDisplayed()
            snmp.txtPrivacyKey.isDisplayed()
        }

        if(snmp.chkSNMPDivClass.hasClass('checked')){
            println('status: Checked')
            //1 Check validation for target.
            //1.1 When target is empty.
            when: 'Fill the form'
            snmp.txtTarget.value('')
            snmp.btnSaveSNMP.click()
            then:
            waitFor{ snmp.errorTarget.isDisplayed() }
            assert snmp.errorTarget.text() == "This field is required."

            //1.2 When target is invalid.
            when: 'Fill the form'
            snmp.txtTarget.value('test@@')
            snmp.btnSaveSNMP.click()
            then:
            waitFor{ snmp.errorTarget.isDisplayed() }
            assert snmp.errorTarget.text() == "Please enter a valid target."

            //2 Check validation for authkey and privacykey when username is not specified.
            //2.1 When authprotocol is other than "NoAUth" and privacyprotocol is other than "NoPriv"
            when: 'Fill the form'
            waitFor {
                snmp.txtTarget.value('127.0.0.1:8080')
                snmp.txtUsername.value('')
                snmp.txtAuthKey.value('')
                snmp.txtPrivacyKey.value('')
                snmp.btnSaveSNMP.click()
            }
            then:
            snmp.errorAuthKey.isDisplayed()
            assert snmp.errorAuthKey.text() == "This field is required."
            snmp.errorPrivacyKey.isDisplayed()
            assert snmp.errorPrivacyKey.text() == "This field is required."

            //3 Check validation for authkey and privacykey when username is specified
            //3.1 When authkey and privacykey are not specified.
            when: 'Fill the form'
            snmp.txtUsername.value('test')
            snmp.btnSaveSNMP.click()
            then:
            snmp.errorAuthKey.isDisplayed()
            assert snmp.errorAuthKey.text() == "This field is required."
            snmp.errorPrivacyKey.isDisplayed()
            assert snmp.errorPrivacyKey.text() == "This field is required."

            //3.1 When authkey and privacykey have length smaller than 8
            when: 'Fill the form'
            waitFor {
                snmp.txtTarget.value('127.0.0.1:8080')
                snmp.txtUsername.value('test')
                snmp.txtAuthKey.value('test1')
                snmp.txtPrivacyKey.value('test2')
                snmp.btnSaveSNMP.click()
            }
            then:
            snmp.errorAuthKey.isDisplayed()
            assert snmp.errorAuthKey.text() == "Please enter at least 8 characters."
            snmp.errorPrivacyKey.isDisplayed()
            assert snmp.errorPrivacyKey.text() == "Please enter at least 8 characters."

        } else {
            println('status: Unchecked')
            snmp.chkSNMPDiv.click()
            //1 Check validation for target.
            //1.1 When target is empty.
            when: 'Fill the form'
            snmp.txtTarget.value('')
            snmp.btnSaveSNMP.click()
            then:
            waitFor{ snmp.errorTarget.isDisplayed() }
            assert snmp.errorTarget.text() == "This field is required."

            //1.2 When target is invalid.
            when: 'Fill the form'
            snmp.txtTarget.value('test@@')
            snmp.btnSaveSNMP.click()
            then:
            waitFor{ snmp.errorTarget.isDisplayed() }
            assert snmp.errorTarget.text() == "Please enter a valid target."

            //2 Check validation for authkey and privacykey when username is not specified.
            //2.1 When authprotocol is other than "NoAUth" and privacyprotocol is other than "NoPriv"
            when: 'Fill the form'
            snmp.txtTarget.value('127.0.0.1:8080')
            snmp.txtUsername.value('')
            snmp.txtAuthKey.value('')
            snmp.txtPrivacyKey.value('')
            snmp.btnSaveSNMP.click()
            then:
            snmp.errorAuthKey.isDisplayed()
            assert snmp.errorAuthKey.text() == "This field is required."
            snmp.errorPrivacyKey.isDisplayed()
            assert snmp.errorPrivacyKey.text() == "This field is required."

            //3 Check validation for authkey and privacykey when username is specified
            //3.1 When authkey and privacykey are not specified.
            when: 'Fill the form'
            snmp.txtUsername.value('test')
            snmp.btnSaveSNMP.click()
            then:
            snmp.errorAuthKey.isDisplayed()
            assert snmp.errorAuthKey.text() == "This field is required."
            snmp.errorPrivacyKey.isDisplayed()
            assert snmp.errorPrivacyKey.text() == "This field is required."
            //3.1 When authkey and privacykey have length smaller than 8
            when: 'Fill the form'
            waitFor {
                snmp.txtTarget.value('127.0.0.1:8080')
                snmp.txtUsername.value('test')
                snmp.txtAuthKey.value('test1')
                snmp.txtPrivacyKey.value('test2')
                snmp.btnSaveSNMP.click()
            }
            then:
            snmp.errorAuthKey.isDisplayed()
            assert snmp.errorAuthKey.text() == "Please enter at least 8 characters."
            snmp.errorPrivacyKey.isDisplayed()
            assert snmp.errorPrivacyKey.text() == "Please enter at least 8 characters."
        }
    }

    def DisableSNMPConfigAndUpdate(){
        int countNext = 0
        int indexOfNewDatabase = 0
        String newDatabaseName = ""
        when: 'Create database'
        for(count=0; count<numberOfTrials; count++) {
            try {
                indexOfNewDatabase = createNewDatabase(create_DatabaseTest_File)
                break
            } catch(Exception e) {
                deleteDatabase(create_DatabaseTest_File)
            } catch(org.codehaus.groovy.runtime.powerassert.PowerAssertionError e) {
                //Do Nothing
            }
        }
        newDatabaseName = "name_src"
        then: 'Choose new database'
        //chooseDatabase(indexOfNewDatabase, "name_src")
        for (count = 0; count < numberOfTrials; count++) {
            try {
                for(countNext=0; countNext<numberOfTrials; countNext++) {
                    try {
                        waitFor { buttonAddDatabase.isDisplayed() }
                        break
                    } catch(geb.waiting.WaitTimeoutException exception) {
                        currentDatabase.click()
                    }
                }
                $(id:getIdOfDatabase(String.valueOf(indexOfNewDatabase))).click()
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }

        // Create a SNMP configuration
        when: 'Open popup for SNMP'
        waitFor(20){ snmp.btnUpdateSNMP.isDisplayed() }
        snmp.btnUpdateSNMP.click()
        waitFor(20) { snmp.btnSaveSNMP.isDisplayed() }
        then: 'Check if id is displayed.'
        waitFor {
            snmp.chkSNMP.isDisplayed()
            snmp.txtTarget.isDisplayed()
            snmp.txtUsername.isDisplayed()
            snmp.selectAuthProtocol.isDisplayed()
            snmp.txtAuthKey.isDisplayed()
            snmp.selectPrivacyProtocol.isDisplayed()
            snmp.txtPrivacyKey.isDisplayed()
        }

        if(snmp.chkSNMPDivClass.hasClass('checked')){
            snmp.chkSNMPDiv.click()
            when: 'Fill the form'
            snmp.txtTarget.value('')
            snmp.txtAuthKey.value('')
            snmp.txtPrivacyKey.value('')
            snmp.btnSaveSNMP.click()
            then:
            assert checkSaveMessage()
        } else {
            when: 'Fill the form'
            snmp.txtTarget.value('')
            snmp.txtAuthKey.value('')
            snmp.txtPrivacyKey.value('')
            snmp.btnSaveSNMP.click()
            then:
            assert checkSaveMessage()
        }

    }

    def EnableSNMPConfigAndUpdate(){
        int countNext = 0
        int indexOfNewDatabase = 0
        String newDatabaseName = ""
        when: 'Create database'
        for(count=0; count<numberOfTrials; count++) {
            try {
                indexOfNewDatabase = createNewDatabase(create_DatabaseTest_File)
                break
            } catch(Exception e) {
                deleteDatabase(create_DatabaseTest_File)
            } catch(org.codehaus.groovy.runtime.powerassert.PowerAssertionError e) {
                //Do Nothing
            }
        }
        newDatabaseName = "name_src"
        then: 'Choose new database'
        //chooseDatabase(indexOfNewDatabase, "name_src")
        for (count = 0; count < numberOfTrials; count++) {
            try {
                for(countNext=0; countNext<numberOfTrials; countNext++) {
                    try {
                        waitFor { buttonAddDatabase.isDisplayed() }
                        break
                    } catch(geb.waiting.WaitTimeoutException exception) {
                        currentDatabase.click()
                    }
                }
                $(id:getIdOfDatabase(String.valueOf(indexOfNewDatabase))).click()
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }

        // Update SNMP configuration
        when: 'Open popup for SNMP'
        waitFor(20){ snmp.btnUpdateSNMP.isDisplayed() }
        snmp.btnUpdateSNMP.click()
        waitFor(20) { snmp.btnSaveSNMP.isDisplayed() }
        then: 'Check if id is displayed.'
        waitFor {
            snmp.chkSNMP.isDisplayed()
            snmp.txtTarget.isDisplayed()
            snmp.txtUsername.isDisplayed()
            snmp.selectAuthProtocol.isDisplayed()
            snmp.txtAuthKey.isDisplayed()
            snmp.selectPrivacyProtocol.isDisplayed()
            snmp.txtPrivacyKey.isDisplayed()
        }

        if(snmp.chkSNMPDivClass.hasClass('checked')){
            when: 'Fill the form'
            snmp.txtTarget.value('127.0.0.1:8080')
            snmp.txtUsername.value('testUser')
            snmp.txtAuthKey.value('test1234')
            snmp.txtPrivacyKey.value('test5678')
            snmp.btnSaveSNMP.click()
            then:
            assert checkSaveMessage()
        } else {
            snmp.chkSNMPDiv.click()
            when: 'Fill the form'
            snmp.txtTarget.value('127.0.0.1:8080')
            snmp.txtUsername.value('testUser')
            snmp.txtAuthKey.value('test1234')
            snmp.txtPrivacyKey.value('test5678')
            snmp.btnSaveSNMP.click()
            then:
            assert checkSaveMessage()
            Boolean result = false
            try{
                waitFor{ snmp.lblTarget.isDisplayed() }
                result = true
            } catch(geb.waiting.WaitTimeoutException exception){
                result = false
            }
            if(result){
                snmp.tdTargetVal.isDisplayed()
                assert snmp.tdTargetVal.text() == "127.0.0.1:8080"
                snmp.tdCommunityVal.isDisplayed()
                assert snmp.tdCommunityVal.text() == "public"
                snmp.tdUsernameVal.isDisplayed()
                assert snmp.tdUsernameVal.text() == "testUser"
                snmp.tdAuthProtocolVal.isDisplayed()
                assert snmp.tdAuthProtocolVal.text() == "SHA"
                snmp.tdAuthKeyVal.isDisplayed()
                assert snmp.tdAuthKeyVal.text() == "AES"
                snmp.tdPrivacyProtocolVal.isDisplayed()
                assert snmp.tdPrivacyProtocolVal.text() == "test1234"
                snmp.tdPrivacyKeyVal.isDisplayed()
                assert snmp.tdPrivacyKeyVal.text() == "test5678"

            } else {
                when:
                waitFor(20){snmp.snmpTitle.isDisplayed()}
                snmp.snmpTitle.click()
                then:
                snmp.tdTargetVal.isDisplayed()
                assert snmp.tdTargetVal.text() == "127.0.0.1:8080"
                snmp.tdCommunityVal.isDisplayed()
                assert snmp.tdCommunityVal.text() == "public"
                snmp.tdUsernameVal.isDisplayed()
                assert snmp.tdUsernameVal.text() == "testUser"
                snmp.tdAuthProtocolVal.isDisplayed()
                assert snmp.tdAuthProtocolVal.text() == "SHA"
                snmp.tdAuthKeyVal.isDisplayed()
                assert snmp.tdAuthKeyVal.text() == "test1234"
                snmp.tdPrivacyProtocolVal.isDisplayed()
                assert snmp.tdPrivacyProtocolVal.text() == "AES"
                snmp.tdPrivacyKeyVal.isDisplayed()
                assert snmp.tdPrivacyKeyVal.text() == "test5678"
            }

        }

        when: 'Open popup for SNMP'
        waitFor(20){ snmp.btnUpdateSNMP.isDisplayed() }
        snmp.btnUpdateSNMP.click()
        waitFor(20) { snmp.btnSaveSNMP.isDisplayed() }
        then: 'Check if id is displayed.'
        waitFor {
            snmp.chkSNMP.isDisplayed()
            snmp.txtTarget.isDisplayed()
            snmp.txtUsername.isDisplayed()
            snmp.selectAuthProtocol.isDisplayed()
            snmp.txtAuthKey.isDisplayed()
            snmp.selectPrivacyProtocol.isDisplayed()
            snmp.txtPrivacyKey.isDisplayed()
        }

        if(snmp.chkSNMPDivClass.hasClass('checked')){
            when: 'Fill the form'
            snmp.txtTarget.value('127.0.0.1:8080')
            snmp.txtUsername.value('testUser')
            snmp.selectAuthProtocol.click()
            snmp.selectAuthProtocol.find("option").find{ it.value() == "NoAuth" }.click()
            snmp.txtAuthKey.value('')
            snmp.selectPrivacyProtocol.click()
            snmp.selectPrivacyProtocol.find("option").find{ it.value() == "NoPriv" }.click()
            snmp.txtPrivacyKey.value('')
            snmp.btnSaveSNMP.click()
            then:
            assert checkSaveMessage()
        } else {
            snmp.chkSNMPDiv.click()
            when: 'Fill the form'
            snmp.txtTarget.value('127.0.0.1:8080')
            snmp.txtUsername.value('testUser')
            snmp.selectAuthProtocol.click()
            snmp.selectAuthProtocol.find("option").find{ it.value() == "NoAuth" }.click()
            snmp.txtAuthKey.value('')
            snmp.selectPrivacyProtocol.click()
            snmp.selectPrivacyProtocol.find("option").find{ it.value() == "NoAuth" }.click()
            snmp.txtPrivacyKey.value('')
            snmp.btnSaveSNMP.click()
            then:
            assert checkSaveMessage()
        }

    }

    def DeleteSNMPConfig(){
        int countNext = 0
        int indexOfNewDatabase = 0
        String newDatabaseName = ""
        when: 'Create database'
        for(count=0; count<numberOfTrials; count++) {
            try {
                indexOfNewDatabase = createNewDatabase(create_DatabaseTest_File)
                break
            } catch(Exception e) {
                deleteDatabase(create_DatabaseTest_File)
            } catch(org.codehaus.groovy.runtime.powerassert.PowerAssertionError e) {
                //Do Nothing
            }
        }
        newDatabaseName = "name_src"
        then: 'Choose new database'
        //chooseDatabase(indexOfNewDatabase, "name_src")
        for (count = 0; count < numberOfTrials; count++) {
            try {
                for(countNext=0; countNext<numberOfTrials; countNext++) {
                    try {
                        waitFor { buttonAddDatabase.isDisplayed() }
                        break
                    } catch(geb.waiting.WaitTimeoutException exception) {
                        currentDatabase.click()
                    }
                }
                $(id:getIdOfDatabase(String.valueOf(indexOfNewDatabase))).click()
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }

        // Update SNMP configuration
        when: 'Open popup for SNMP'
        waitFor(20){ snmp.btnUpdateSNMP.isDisplayed() }
        snmp.btnUpdateSNMP.click()
        waitFor(20) { snmp.btnSaveSNMP.isDisplayed() }
        then: 'Check if id is displayed.'
        waitFor {
            snmp.chkSNMP.isDisplayed()
            snmp.txtTarget.isDisplayed()
            snmp.txtUsername.isDisplayed()
            snmp.selectAuthProtocol.isDisplayed()
            snmp.txtAuthKey.isDisplayed()
            snmp.selectPrivacyProtocol.isDisplayed()
            snmp.txtPrivacyKey.isDisplayed()
        }

        if(snmp.chkSNMPDivClass.hasClass('checked')){
            when: 'Fill the form'
            snmp.txtTarget.value('127.0.0.1:8080')
            snmp.txtUsername.value('testUser')
            snmp.txtAuthKey.value('test1234')
            snmp.txtPrivacyKey.value('test5678')
            snmp.btnSaveSNMP.click()
            then:
            assert checkSaveMessage()
        } else {
            snmp.chkSNMPDiv.click()
            when: 'Fill the form'
            snmp.txtTarget.value('127.0.0.1:8080')
            snmp.txtUsername.value('testUser')
            snmp.txtAuthKey.value('test1234')
            snmp.txtPrivacyKey.value('test5678')
            snmp.btnSaveSNMP.click()
            then:
            assert checkSaveMessage()
        }

        when: 'Open popup for SNMP'
        waitFor(20){ snmp.btnUpdateSNMP.isDisplayed() }
        snmp.btnUpdateSNMP.click()
        waitFor(20) { snmp.btnSaveSNMP.isDisplayed() }
        then: 'Check if id is displayed.'
        waitFor {
            snmp.chkSNMP.isDisplayed()
            snmp.txtTarget.isDisplayed()
            snmp.txtUsername.isDisplayed()
            snmp.selectAuthProtocol.isDisplayed()
            snmp.txtAuthKey.isDisplayed()
            snmp.selectPrivacyProtocol.isDisplayed()
            snmp.txtPrivacyKey.isDisplayed()
        }

        when:
        snmp.btnDeleteSNMP.click()
        then:
        assert checkSaveMessage()

    }

    def cleanup() { // called after each test
        to ClusterSettingsPage
        int indexToDelete = 2
        indexOfNewDatabase = 1
        chooseDatabase(indexOfNewDatabase, "Database")
        deleteNewDatabase(indexToDelete, "name_src")
    }
}
