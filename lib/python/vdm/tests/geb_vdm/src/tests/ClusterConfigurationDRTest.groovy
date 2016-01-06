/*
This file is part of VoltDB.

Copyright (C) 2008-2015 VoltDB Inc.

This file contains original code and/or modifications of original code.
Any modifications made by VoltDB Inc. are licensed under the following
terms and conditions:

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.
*/

class ClusterConfigurationDRTest extends TestBase {
    String saveMessage              = "Changes have been saved."
    String masterId                 = "1"
    String master                   = "Master"
    String on                       = "On"
    String off                      = "Off"

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

    def createAndDelete() {
        // Create a DR configuration
        when: 'Open popup for DR'
        for(count=0; count<numberOfTrials; count++) {
            try {
                dr.editButton.click()
                waitFor { dr.editPopupSave.isDisplayed() }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { dr.editPopupSave.isDisplayed() }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            } catch(org.openqa.selenium.WebDriverException exception) {
                try {
                    waitFor { dr.editPopupSave.isDisplayed() }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }
        then: 'Check the box to enable the DR configuration'
        for(count=0; count<numberOfTrials; count++) {
            try {
                dr.enabledCheckbox.click()
                waitFor { dr.idField.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            }
        }

        when: 'Fill the form for master'
        dr.idField.value(masterId)
        dr.typeSelect.click()
        dr.masterSelect.click()
        then: 'Save the master configuration'
        for(count=0; count<numberOfTrials; count++) {
            try {
                dr.editPopupSave.click()
                waitFor(60) { saveStatus.text().equals(saveMessage) }
                break
            } catch(geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                if(count==1) {
                    println("not found")
                    assert false
                }
            }
        }
        report "created"

        // Check the created DR configuration
        try {
            waitFor { dr.status.text().equals(on) }
            waitFor { dr.type.text().equals(master) }
            waitFor { dr.id.text().equals(masterId) }
            println("Test Pass: The DR configuration for master is created")
        } catch(geb.waiting.WaitTimeoutException exception) {
            println("Test Fail: The DR configuration for master is not create")
            assert false
        }

        // Delete the DR configuration
        when: 'Open popup for DR'
        for(count=0; count<numberOfTrials; count++) {
            try {
                dr.editButton.click()
                waitFor { dr.editPopupSave.isDisplayed() }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { dr.editPopupSave.isDisplayed() }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            } catch(org.openqa.selenium.WebDriverException exception) {
                try {
                    waitFor { dr.editPopupSave.isDisplayed() }
                    break
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }
        then:
        for(count=0; count<numberOfTrials; count++) {
            try {
                dr.delete.click()
                waitFor(60) { saveStatus.text().equals(saveMessage) }
                break
            } catch(geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            }
        }

        // Check if the DR configuration is deleted
        try {
            waitFor { dr.status.text().equals(off) }
            waitFor { !dr.type.isDisplayed() }
            waitFor { !dr.id.isDisplayed() }
            println("Test Pass: The DR configuration is deleted")
        } catch(geb.waiting.WaitTimeoutException exception) {
            println("Test Fail: The DR configuration is not deleted")
            assert false
        }
    }
}
