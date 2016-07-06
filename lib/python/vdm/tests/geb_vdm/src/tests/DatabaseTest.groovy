/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.io.BufferedReader
import java.io.FileNotFoundException
import java.io.FileReader
import java.io.IOException

class DatabaseTest extends TestBase {
    String create_DatabaseTest_File = "src/resources/create_DatabaseTest.csv"
    String edit_DatabaseTest_File   = "src/resources/edit_DatabaseTest.csv"
    String cvsSplitBy = ","

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

    def addEditDelteDatabase() {
        BufferedReader br = null
        String line = ""
        String[] extractedValue = ["random_input", "random_input"]
        int newValue = 0
        boolean foundStatus = false

        expect: 'Expect the add database button'
        waitFor { buttonDatabase.isDisplayed() }

        when: 'Open Database Popup'
        for(count=0; count<numberOfTrials; count++) {
            try {
                buttonDatabase.click()
                waitFor { buttonAddDatabase.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println(" - Retrying")
            }
        }
        and:'Calculate the number of database that exists initially'
        for(count=1; count<numberOfTrials; count++) {
            try {
                waitFor { $(id:page.getIdOfDatabaseEditButton(count.toString())).isDisplayed() }
            } catch(geb.waiting.WaitTimeoutException e) {
                break
            }
        }
        newValue = count
        then: 'The numbver of databases initially'
        println("The count is " + newValue)

        when: 'Click add database button to open popup'
        for(count=0; count<numberOfTrials; count++) {
            try {
                buttonAddDatabase.click()
                waitFor { popupAddDatabase.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to find the popup - Retrying")
            } catch(geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find the add button - Retrying")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element - Retrying")
            }
        }
        and: 'Extract Values from create_DatabaseTest.csv'
        try {
            br = new BufferedReader(new FileReader(create_DatabaseTest_File))
            for (count=0; (line = br.readLine()) != null; count++) {
                String[] extractedValues = line.split(cvsSplitBy)
                extractedValue[count] = extractedValues[1]
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace()
        } catch (IOException e) {
            e.printStackTrace()
        } finally {
            if (br != null) {
                try {
                    br.close()
                } catch (IOException e) {
                    e.printStackTrace()
                }
            }
        }
        String nameValue        = extractedValue[0].substring(1, extractedValue[0].length() - 1)
        String deploymentValue  = extractedValue[1].substring(1, extractedValue[1].length() - 1)
        then: 'Provide value to the textfields'
        for(count=0; count<numberOfTrials; count++) {
            try {
                popupAddDatabaseNameField.value(nameValue)
                //popupAddDatabaseDeploymentField.value(deploymentValue)
                break
            } catch (geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find the text fields - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element - Retrying")
            }
        }

        when: 'Save Database'
        int nextCount = 0
        String new_string = ""
        for(count=0; count<numberOfTrials; count++) {
            try {
                popupAddDatabaseButtonOk.click()
                for(nextCount=0; nextCount<=newValue; nextCount++){
                    new_string = $(".btnDbList", nextCount).text()
                    if(new_string.equals(nameValue)) {
                        foundStatus = true
                        break
                    }
                }
            } catch (geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find the Ok button - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException e) {
                break
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            }
        }
        and: 'Verify the New Database is created'
        if (foundStatus == true)
            println("The Database was created")
        else {
            println("The Database wasn't created")
            assert false
        }
        then: 'Get the required data-id'
        println(nextCount)
        def newId = $(".btnDbList")[nextCount].parent().attr("data-id")

        when: 'EDIT'
        String editId    = page.getIdOfDatabaseEditButton(newId.toString())
        String deleteId  = page.getIdOfDatabaseDeleteButton(newId.toString())
        for(count=0; count<numberOfTrials; count++) {
            try {

                try {
                    waitFor(5) { 1==0 }
                } catch(geb.waiting.WaitTimeoutException e) {

                }
                $(id:editId).click()
                waitFor { popupEditDatabaseButtonOk.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
                println("Unable to find the Edit popup - Retrying")
            } catch(geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find the Edit button - Retrying")
            }
        }
        and: 'Extract Values from edit_DatabaseTest.csv'
        try {
            br = new BufferedReader(new FileReader(edit_DatabaseTest_File))
            for (count=0; (line = br.readLine()) != null; count++) {
                String[] extractedValues = line.split(cvsSplitBy)
                extractedValue[count] = extractedValues[1]
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace()
        } catch (IOException e) {
            e.printStackTrace()
        } finally {
            if (br != null) {
                try {
                    br.close()
                } catch (IOException e) {
                    e.printStackTrace()
                }
            }
        }
        nameValue        = extractedValue[0].substring(1, extractedValue[0].length() - 1)
        deploymentValue  = extractedValue[1].substring(1, extractedValue[1].length() - 1)
        then: 'Provide value to the textfields'
        for(count=0; count<numberOfTrials; count++) {
            try {
                popupAddDatabaseNameField.value(nameValue)
                //popupAddDatabaseDeploymentField.value(deploymentValue)
                break
            } catch (geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find the text fields - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element - Retrying")
            }
        }

        when:
        for(count=0; count<numberOfTrials; count++) {
            try {
                popupEditDatabaseButtonOk.click()
                for(nextCount=0; nextCount<=newValue; nextCount++){
                    new_string = $(".btnDbList", nextCount).text()
                    if(new_string.equals(nameValue)) {
                        foundStatus = true
                        break
                    }
                }
                break
            } catch (geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find the ok button - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element - Retrying")
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                println("Element not visible - Retrying")
            }
        }
        then: 'Verify the New Database is created'
        if (foundStatus == true)
            println("The Database was created")
        else {
            println("The Database wasn't created")
            assert false
        }

        when: 'Click delete for the required database'
        for(count=0; count<numberOfTrials; count++) {
            try {
                try {
                    waitFor(5) { 1==0 }
                } catch(geb.waiting.WaitTimeoutException e) {

                }
                $("#deleteDatabase_2").click()
                waitFor { popupDeleteDatabaseButtonOk.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
               println("Unable to find the Delete popup - Retrying")
            } catch(geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find the delete button - Retrying")
            }
        }
        and: 'Confirm the deletion of the database and check if it was deleted'
        foundStatus = false
        for(count=0; count<numberOfTrials; count++) {
            try {
                popupDeleteDatabaseButtonOk.click()
                for(nextCount=0; nextCount<=newValue; nextCount++){
                    new_string = $(".btnDbList", nextCount).text()
                    if(new_string.equals(nameValue)) {
                        foundStatus = true
                        break
                    }
                }
            } catch (geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find the Ok button - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException e) {
                break
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
        then: 'Assert'
        if(foundStatus == false) {
            println("The Database was Deleted")
        }
        else {
            println("The Database wasn't Deleted")
            assert false
        }
    }
}
