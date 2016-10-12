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

import geb.Page

class ClusterSettingsPage extends Page {
    static content = {
        // Tabs
        clusterSettingsTab { $("#dbManager") }
        serverSettingsTab { $("#serverSetting") }

        // DB
        startCluster { $("#divDbManager > div.clusterContent > div.clusterStartStop > div > a") }

        // Servers
        buttonAddServer { $("#btnAddServer") }
        btnAddServerOption { $("#btnAddServerOption") }

        // Add Server Popup
        popupAddServer { $("#addServer > div > div") }
        popupAddServerDetails { $("#accordion > div > div.panel-heading > h4 > a") }
        popupAddServerNameField { $("#serverName") }
        popupAddServerHostNameField { $("#txtHostName") }
        popupAddServerDescriptionField { $("#txtDescription") }
        popupAddServerClientListenerField { $("#txtClientPort") }
        popupAddServerAdminListenerField { $("#txtAdminPort") }
        popupAddServerHttpListenerField { $("#txtHttpPort") }
        popupAddServerInternalListenerField { $("#txtInternalPort") }
        popupAddServerZookeeperListenerField { $("#txtZookeeper") }
        popupAddServerReplicationListenerField { $("#txtReplicationPort") }
        popupAddServerInternalInterfaceField { $("#txtInternalInterface") }
        popupAddServerExternalInterfaceField { $("#txtExternalInterface") }
        popupAddServerPublicInterfaceField { $("#txtPublicInterface") }
        popupAddServerPlacementGroupField { $("#txtPlacementGroup") }

        popupAddServerButtonOk { $("#btnCreateServerOk") }
        popupAddServerButtonCancel { $("#addServer > div > div > div.modal-footer > button.btn.btn-gray") }

        // Delete Server
        deleteServer { $("#serverList > tbody > tr:nth-child(5) > td:nth-child(2) > a > div") }
        popupDeleteServer { $("#deleteConfirmation > div > div") }
        popupDeleteServerButtonOk { $("#deleteServerOk") }

        testingPath(required: false) { $("#serverList > tbody > tr:nth-child(5) > td:nth-child(1)") }
        errorServerName { $("#errorServerName") }
        errorHostName { $("#errorHostName") }
        errorClientPort { $("#errorClientPort") }
        errorInternalInterface { $("#errorInternalInterface") }

        // Database
        firstDatabase { $("#dbInfo_1") }
        secondDatabase { $("#dbInfo_2") }
        currentDatabase { $("#clusterName") }

        buttonDatabase { $(id: "btnDatabaseLink") }
        buttonAddDatabase { $("#btnAddDatabase") }

        popupAddDatabase { $("#txtDbName") }
        popupAddDatabaseNameField { $("#txtDbName") }
        popupAddDatabaseDeploymentField { $("#txtDeployment") }
        popupAddDatabaseButtonOk(required: false) { $("#btnAddDatabaseOk") }
        popupAddDatabaseButtonCancel { $("#addDatabase > div > div > div.modal-footer > button.btn.btn-gray") }
        popupEditDatabaseButtonOk { $("#btnAddDatabaseOk") }
        popupDeleteDatabaseButtonOk { $("#btnDeleteDatabaseOk") }

        // Change Save Status
        saveStatus { $(id: "changeSaveStatus") }

        //
        logPopupTitle           { $("#errorHeader") }
        logPopupOk              { $("#divServerLogPopup > div > div > div.modal-footer > button") }

        // MODULES - The elements of Cluster Configuration are separated into modules
        overview { module OverviewModule }
        directories { module DirectoriesModule }
        dr { module DatabaseReplicationModule }
        selectServer                 {$("#serverOption")}
        selectOption                    {"#divDbManager > div:nth-child(7) > div > div > div.rightSideServ > form > select > option:nth-child(2)"}
    }

    static at = {
//        waitFor(30) { clusterSettingsTab.isDisplayed() }
        //      waitFor(30) { serverSettingsTab.isDisplayed() }
    }


    /*
     *  Return the id of Server with index as input
     */

    String getIdOfServer(int index) {
        return ("tdHostname_" + String.valueOf(index))
    }
    /*
     *  Return the id of delete button of Server with index as input
     */

    String getIdOfDeleteButton(int index) {
        return ("deleteServer_" + String.valueOf(index))
    }

    /*
     *  Return the id of edit button of Server with index as input
     */

    String getIdOfEditButton(int index) {
        return ("editServer_" + String.valueOf(index))
    }

    /*
     *  Return the id of delete button for Database with index as input
     */

    String getIdOfDatabaseDeleteButton(String index) {
        return ("deleteDatabase_" + index)
    }

    /*
     *  Return the id of edit button for Database with index as input
     */

    String getIdOfDatabaseEditButton(String index) {
        return ("editDatabase_" + index)
    }

    String getIdOfDatabase(String index) {
        return ("dbInfo_" + index)
    }

    static String cvsSplitBy = ","
    int numberOfTrials = 5
    BufferedReader br = null
    String line = ""
    String[] extractedValue = ["random_input", "random_input"]
    String newValueDatabase = 0
    String saveMessage = "Changes have been saved."
    boolean foundStatus = false
    int count = 0

    def boolean chooseSelectOption( String choice ) {
        selectServer.value(choice)
    }

    /*
     *  Create a new Database
     */

    int createNewDatabase(String create_DatabaseTest_File) {
        newValueDatabase = 0
        foundStatus = false

        for (count = 0; count < numberOfTrials; count++) {
            try {
                buttonDatabase.click()
                waitFor { buttonAddDatabase.isDisplayed() }
                break
            } catch (geb.waiting.WaitTimeoutException e) {
                println("Waiting - Retrying")
            }
        }

        for (count = 1; count < numberOfTrials; count++) {
            try {
                waitFor { $(id: page.getIdOfDatabaseEditButton(count.toString())).isDisplayed() }
            } catch (geb.waiting.WaitTimeoutException e) {
                break
            }
        }
        newValueDatabase = count

        println("The count is " + newValueDatabase)
        for (count = 0; count < numberOfTrials; count++) {
            try {
                buttonAddDatabase.click()
                waitFor { popupAddDatabase.isDisplayed() }
                break
            } catch (geb.waiting.WaitTimeoutException e) {
                println("Unable to find the popup - Retrying")
            } catch (geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find the add button - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element - Retrying")
            }
        }

        try {
            br = new BufferedReader(new FileReader(create_DatabaseTest_File))
            for (count = 0; (line = br.readLine()) != null; count++) {
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
        String nameValue = nameOfDatabaseInCSV(create_DatabaseTest_File)
        String deploymentValue = extractedValue[1].substring(1, extractedValue[1].length() - 1)

        for (count = 0; count < numberOfTrials; count++) {
            try {
                try {
                    waitFor { 1 == 0 }
                } catch (geb.waiting.WaitTimeoutException e) {

                }
                popupAddDatabaseNameField.value(nameValue)
                //popupAddDatabaseDeploymentField.value(deploymentValue)
                break
            } catch (geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find the text fields - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element - Retrying")
            }
        }

        int nextCount = 0
        String new_string = ""
        for (count = 0; count < numberOfTrials; count++) {
            try {
                popupAddDatabaseButtonOk.click()
                for (nextCount = 0; nextCount <= newValueDatabase; nextCount++) {
                    new_string = $(".btnDbList", nextCount).text()
                    if (new_string.equals(nameValue)) {
                        foundStatus = true
                        break
                    }
                }
            } catch (geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find the Ok button - Retrying")
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                break
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch (org.openqa.selenium.WebDriverException e) {
                println("Not Clickable at the moment - Retrying")
            }

        }

        if (foundStatus == true)
            println("The Database was created")
        else {
            println("The Database wasn't created")
            assert false
        }

        println(nextCount)
        def newId = $(".btnDbList")[nextCount].parent().attr("data-id")

        return (Integer.valueOf(newValueDatabase) + 1)
    }

    /*
     *  Delete the database with the given index and name
     */

    boolean deleteNewDatabase(int indexOfNewDatabase, String databaseName) {
        int nextCount = 0
        String new_string = ""
        String deleteId = getIdOfDatabaseDeleteButton(String.valueOf(indexOfNewDatabase))

        for (count = 0; count < numberOfTrials; count++) {
            try {
                for (nextCount = 0; nextCount < numberOfTrials; nextCount++) {
                    try {
                        waitFor { buttonAddDatabase.isDisplayed() }
                        break
                    } catch (geb.waiting.WaitTimeoutException exception) {
                        currentDatabase.click()
                    }
                }
                $(id: deleteId).click()
                waitFor { popupDeleteDatabaseButtonOk.isDisplayed() }
                break
            } catch (geb.waiting.WaitTimeoutException e) {
                println("Unable to find the Delete popup - Retrying")
            } catch (geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find the delete button - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                try {
                    waitFor { popupDeleteDatabaseButtonOk.isDisplayed() }
                    break
                } catch (geb.waiting.WaitTimeoutException ex) {
                    println("Unable to find the Delete popup - Retrying")
                }
            }
        }

        foundStatus = false
        for (count = 0; count < numberOfTrials; count++) {
            try {
                popupDeleteDatabaseButtonOk.click()
                for (nextCount = 0; nextCount <= indexOfNewDatabase; nextCount++) {
                    new_string = $(".btnDbList", nextCount).text()
                    if (new_string.equals(databaseName)) {
                        foundStatus = true
                        break
                    }
                }
            } catch (geb.error.RequiredPageContentNotPresent e) {
                println("Unable to find the Ok button - Retrying")
            } catch (org.openqa.selenium.ElementNotVisibleException e) {
                break
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            }
        }

        if (foundStatus == false) {
            println("The Database was Deleted")
            return true
        } else {
            println("The Database wasn't Deleted")
            return false
        }
    }

    /*
     *  Choose the database with given index and name
     */

    boolean chooseDatabase(int indexOfNewDatabase, String newDatabaseName) {
        int countNext = 0
        for (count = 0; count < numberOfTrials; count++) {
            try {
                for (countNext = 0; countNext < numberOfTrials; countNext++) {
                    try {
                        waitFor { buttonAddDatabase.isDisplayed() }
                        break
                    } catch (geb.waiting.WaitTimeoutException exception) {
                        currentDatabase.click()
                    }
                }
                $(id: getIdOfDatabase(String.valueOf(indexOfNewDatabase))).click()
                waitFor { currentDatabase.text().equals(newDatabaseName) }
                return true
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch (org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { currentDatabase.text().equals(newDatabaseName) }
                    return true
                } catch (geb.waiting.WaitTimeoutException exc) {
                    println("Waiting - Retrying")
                }
            }
        }
        return false
    }

    /*
     *  Check the Save Message
     */

    boolean checkSaveMessage() {
        if (waitFor(60) { saveStatus.text().equals(saveMessage) }) {
            return true
        } else {
            println("Test Fail: The required text is not displayed")
            return false
        }
    }

    /*
     *
     */

    boolean openDatabase() {
        for (count = 0; count < numberOfTrials; count++) {
            try {
                currentDatabase.click()
                waitFor { buttonAddDatabase.isDisplayed() }
                break
            } catch (geb.waiting.WaitTimeoutException exception) {
                println("Waiting - Retrying")
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element exception - Retrying")
            } catch (org.openqa.selenium.ElementNotVisibleException exception) {
                if (count > 1) {
                    try {
                        waitFor { buttonAddDatabase.isDisplayed() }
                        break
                    } catch (geb.waiting.WaitTimeoutException exc) {
                        println("Waiting - Retrying")
                    }
                } else if (count == 0) {
                    try {
                        waitFor(30) { !saveStatus.isDisplayed() }
                    } catch (geb.waiting.WaitTimeoutException exc) {
                        break
                    }
                }
            }
        }
    }

    /*
     *  Return the value of Database name in CSV file
     */

    String nameOfDatabaseInCSV(String create_DatabaseTest_File) {
        try {
            br = new BufferedReader(new FileReader(create_DatabaseTest_File))
            for (count = 0; (line = br.readLine()) != null; count++) {
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
        return (extractedValue[0].substring(1, extractedValue[0].length() - 1))
    }

    /*
     *  Return the CSS path of database delete button with index as input
     */

    String returnCssPathOfDatabaseDelete(int index) {
        return "#tblDatabaseList > tbody > tr:nth-child(" + index + ") > td:nth-child(3) > a"
    }

    /*
     *  Return the CSS path of database with index as input
     */

    String returnCssPathOfDatabase(int index) {
        return "#tblDatabaseList > tbody > tr:nth-child(" + index + ") > td:nth-child(1) > a"
    }

    /*
     *  Return the index of the databaseName
     */

    int returnTheDatabaseIndexToDelete(int numberOfDatabases, String databaseName) {
        if (checkIfDatabaseExists(numberOfDatabases, databaseName, false) == false) {
            return 0
        } else {
            for (count = 0; count < numberOfDatabases; count++) {
                if ($(".btnDbList", count).text().equals(databaseName)) {
                    break
                }
            }
            return (count + 1)
        }
    }

    /*
     *  Check if the name of Database provided exists
     *  The status of displayMessage decides if the message is to be displayed or not
     */

    boolean checkIfDatabaseExists(int numberOfDatabases, String databaseName, boolean displayMessage) {
        for (count = 0; count < numberOfDatabases; count++) {
            if ($(".btnDbList", count).text().equals(databaseName)) {
                if (displayMessage == true) {
                    println("The database to delete was found")
                }
                return true
            }
        }
        if (displayMessage == true) {
            println("The database to delete wasn't found")
        }
        return false
    }

    boolean deleteDatabase(String create_DatabaseTest_File) {
        String databaseName = nameOfDatabaseInCSV(create_DatabaseTest_File)
        int numberOfDatabases = $('.btnDbList').size()
        //buttonDatabase.click()
        int indexOfDatabaseToDelete = returnTheDatabaseIndexToDelete(numberOfDatabases, databaseName)

        if (indexOfDatabaseToDelete == 0) {
            println("Database not found")
        } else {
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
                        println("Database was deleted")
                    }
                } catch (Exception e) {

                }
            }
            println()
        }
    }

    /// Trial of new methods to return css path
    /// If the index is 1 then there must not be other servers
    def String getCssPathOfServer(int index) {
        String cssPath

        if (index == 1) {
            cssPath = "#serverList > tbody > tr > td:nth-child(1)"
        } else {
            cssPath = "#serverList > tbody > tr:nth-child(" + String.valueOf(index) + ") > td:nth-child(1)"
        }
        return cssPath
    }

    def String getCssPathOfEditServer(int index) {
        String cssPath

        if (index == 1) {
            cssPath = "#serverList > tbody > tr > td:nth-child(2) > a > div"
        } else {
            cssPath = "#serverList > tbody > tr:nth-child(" + String.valueOf(index) + ") > td:nth-child(2) > a > div"
        }
        return cssPath
    }

    def String getCssPathOfDeleteServer(int index) {
        String cssPath

        if (index == 1) {
            cssPath = "#serverList > tbody > tr > td:nth-child(3) > a > div"
        } else {
            cssPath = "#serverList > tbody > tr:nth-child(" + String.valueOf(index) + ") > td:nth-child(3) > a > div"
        }
        return cssPath
    }

    def String getCssPathOfLog(int index) {
        String cssPath = "#editServer_" + String.valueOf(index) + " > span"

        return cssPath
    }
}