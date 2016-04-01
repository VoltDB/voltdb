/*
This file is part of VoltDB.

Copyright (C) 2008-2016 VoltDB Inc.

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
    String portValue                = "22"
    String on                       = "On"
    String off                      = "Off"
    String create_DatabaseTest_File = "src/resources/create_DatabaseTest.csv"

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
        when: 'Create database'
        for(count=0; count<numberOfTrials; count++) {
            try {
                indexOfNewDatabase = createNewDatabase(create_DatabaseTest_File)
                break
            } catch(Exception e) {
                deleteDatabase(create_DatabaseTest_File)
            } catch(org.codehaus.groovy.runtime.powerassert.PowerAssertionError e) {
                deleteDatabase(create_DatabaseTest_File)
            }
        }
        then: 'Choose new database'
        chooseDatabase(indexOfNewDatabase, "name_src")

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
        then: 'Check if id is displayed.'
        for(count=0; count<numberOfTrials; count++) {
            try {
                //dr.enabledCheckbox.click()
                waitFor { dr.idField.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            }
        }

        when: 'Fill the form for master'
        for(count=0; count<numberOfTrials; count++) {
            try {
                dr.idField.value(masterId)
                dr.databasePort.value(portValue)
                break
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
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
        and: ''
        waitFor {dr.addConnectionSource.isDisplayed()}
        dr.addConnectionSource.click()
        then: ''
        for(count=0; count<numberOfTrials; count++) {
            try {
                dr.sourceField.value("something")
                dr.editPopupSave.click()
                break
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            } catch(geb.waiting.WaitTimeoutException e) {

            }
        }

        when: 'Check the value'
        dr.displayedId.equals("1")
        dr.displayedPort.equals("22")
        dr.displayedSource.equals("something")
        and: 'Open popup for DR'
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
        then: ''
        for(count=0; count<numberOfTrials; count++) {
            try {
                dr.delete.click()
                dr.status.equals("Off")
                break
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }

        when: 'Choose the database with index 1'
        openDatabase()
        chooseDatabase(indexOfLocal, "local")
        and: 'Click delete for the required database'
        openDatabase()
        then: 'Delete the database'
        deleteNewDatabase(indexOfNewDatabase, "name_src")
        println()
    }


    def cleanup() { // called after each test
        count = 0

        while (count < numberOfTrials) {
            count++
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

        String databaseName = nameOfDatabaseInCSV(create_DatabaseTest_File)
        int numberOfDatabases = $('.btnDbList').size()
        buttonDatabase.click()
        int indexOfDatabaseToDelete = returnTheDatabaseIndexToDelete(numberOfDatabases, databaseName)
        if (indexOfDatabaseToDelete == 0) {
            println("Cleanup: Database wasn't found")
        } else {
            try {
                waitFor { buttonDatabase.isDisplayed() }
            }
            catch (geb.waiting.WaitTimeoutException exception) {
                openDatabase()
            }

            chooseDatabase(indexOfLocal, "local")
            openDatabase()

            for (count = 0; count < numberOfTrials; count++) {
                try {
                    $(returnCssPathOfDatabaseDelete(indexOfDatabaseToDelete)).click()
                    waitFor { popupDeleteDatabaseButtonOk.isDisplayed() }
                    break
                } catch (geb.waiting.WaitTimeoutException exception) {

                }
            }
            for (count = 0; count < numberOfTrials; count++) {
                try {
                    popupDeleteDatabaseButtonOk.click()
                    if (checkIfDatabaseExists(numberOfDatabases, databaseName, false) == false) {
                        println("Cleanup: Database was deleted")
                    }
                } catch (Exception e) {

                }
            }
            println()
        }
    }
}
