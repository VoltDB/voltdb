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
import geb.Module
import org.openqa.selenium.Keys

class OverviewModule extends Module {
    static content = {
        // Site Per Host
        sitePerHostText                 { $("#adminTbl > tbody > tr:nth-child(1) > td.configLabel") }
        sitePerHostField                { $(id:"txtSitePerHost") }

        // K-Safety
        ksafetyText                     { $("#adminTbl > tbody > tr:nth-child(2) > td.configLabel") }
        ksafetyField                    { $(id:"txtKSafety") }


        // Partition Detection
        partitionDetectionText          { $("#adminTbl > tbody > tr.security > td.configLabel") }
        partitionDetectionCheckbox      { $("#adminTbl > tbody > tr.security > td:nth-child(2) > div > ins") }
        partitionDetectionStatus        { $(id:"txtPartitionDetection") }


        // Security
        securityText                    { $("#row-6 > td.configLabel > a > span") }
        securityCheckbox                (required: false){ $("#row-6 > td:nth-child(2) > div > ins") }
        securityStatus                  { $("#txtSecurity") }

        usernameTitleText               { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigLeft > div > div.mainTbl > table > tbody > tr.child-row-6.subLabelRow.thead.secTbl1 > td.configLabel") }
        roleTitleText                   { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigLeft > div > div.mainTbl > table > tbody > tr.child-row-6.subLabelRow.thead.secTbl1 > td:nth-child(2)") }
        addUserButton                   { $("#btnAddSecurity > span") }

        addSecurityButton               { $("#btnAddSecurity") }
        userField                       { $("#txtUser") }
        passwordField                   { $("#txtPassword") }
        roleField                       { $("#txtUserRole-tokenfield") }
        selectAdminRole                 { $("#selectRole > option:nth-child(1)") }
        saveUserOkButton                { $("#btnSaveUserOk") }
        securityLabel                   { $("tr.securityList").find("td.configLabel") }
        updateSecurityButton            { $("a.btnUpdateSecurity") }
        deleteUserButton                { $("#deleteUser") }
        errorUser                       { $("#errorUser") }
        errorPassword                   { $("#errorPassword") }
        cancelUserButton                { $("#btnCancelUser") }
        noSecurityAvailable             { $("#trSecurity > td.configLabel") }

        editSecurityOne (required: false)   { $("#adminTbl > tbody > tr.child-row-6.subLabelRow.securityList > td:nth-child(4) > a") }
        usernameOne (required: false)       { $("#adminTbl > tbody > tr.child-row-6.subLabelRow.securityList > td.configLabel") }
        roleOne (required: false)           { $("#adminTbl > tbody > tr.child-row-6.subLabelRow.securityList > td:nth-child(2)") }

        // HTTP Access
        httpAccessText                  { $("#row-1 > td.configLabel > a > span") }
        httpAccessCheckbox              { $("#row-1 > td:nth-child(2) > div > ins") }
        httpAccessStatus                { $(id:"txtHttpAccess") }

        jsonApiText                     {$("#adminTbl > tbody > tr.child-row-1.subLabelRow > td.configLabel")}
        jsonApiCheckbox                 { $(id:"chkAutoSnapshot") }

        // Auto Snapshots
        autoSnapshotsText               { $("#row-2 > td.configLabel > a > span") }
        autoSnapshotsCheckbox           { $("#row-2 > td:nth-child(2) > div > ins") }
        autoSnapshotsStatus             { $(id:"txtAutoSnapshot") }

        filePrefixText                  { $("#adminTbl1 > tbody > tr:nth-child(2) > td.configLabel") }
        frequencyText                   { $("#adminTbl1 > tbody > tr:nth-child(3) > td.configLabel") }
        retainedText                    { $("#adminTbl1 > tbody > tr:nth-child(4) > td.configLabel") }

        filePrefixField                 { $(id:"txtFilePrefix") }
        frequencyField                  { $(id:"txtFrequency") }
        retainedField                   { $(id:"txtRetained") }

        // Command Logging
        commandLoggingText              { $("#row-3 > td.configLabel > a > span") }
        commandLoggingCheckbox          { $("#row-3 > td:nth-child(2) > div > ins") }
        commandLoggingStatus            { $(id:"txtCommandLog") }

        logFrequencyTimeText            { $("#adminTbl1 > tbody > tr:nth-child(6) > td.configLabel") }
        logFrequencyTransactionsText    { $("#adminTbl1 > tbody > tr:nth-child(7) > td.configLabel") }
        logSegmentSizeText              { $("#adminTbl1 > tbody > tr:nth-child(8) > td.configLabel") }

        logFrequencyTimeField           { $(id:"txtLogFrequencyTime") }
        logFrequencyTransactionsField   { $(id:"txtLogFreqTransaction") }
        logSegmentSizeField             { $(id:"txtLogSegmentSize") }

        // Export
        exportText                      { $("#row-4 > td.configLabel > a > span") }
        exportAddButton                 { $(id:"btnAddExportProperty") }

        exportAddPopupStreamField       { $(id:"txtExportStream") }
        exportAddPopupType              { $(id:"txtExportType") }
        exportAddPopupSaveButton        { $(id:"btnSaveExportOk") }
        exportAddPopupDeleteButton      { $(id:"deleteExportConfig") }

        exportAddPopupKafka             { $("#txtExportType > option:nth-child(1)") }
        exportAddPopupElasticSearch     { $("#txtExportType > option:nth-child(2)") }
        exportAddPopupHttp              { $("#txtExportType > option:nth-child(3)") }
        exportAddPopupFile              { $("#txtExportType > option:nth-child(4)") }
        exportAddPopupRabbitMq          { $("#txtExportType > option:nth-child(5)") }
        exportAddPopupJdbc              { $("#txtExportType > option:nth-child(6)") }
        exportAddPopupCustom            { $("#txtExportType > option:nth-child(7)") }
        exportAddPopupKafkaMetadata     { $(id:"txtMetadataBrokerListValue") }
        exportAddPopupEndpointValue     { $(id:"txtEndpointESValue") }

        configTbl                       { $("#adminTbl") }
        exportPropertyCreatedRow        { configTbl.find('.parentprop').last() }
        exportPropertyName              { exportPropertyCreatedRow.find('span') }
        exportPropertyNameOption        (required: false){ exportPropertyCreatedRow.find('span')}
        exportPorpertyEdit              { exportPropertyCreatedRow.find('td').last().find('a') }
        txtExportConnectorClass         { $("#txtExportConnectorClass") }
        exportPropertyTxtName           { $("#txtName1") }
        exportPropertyTxtValue          { $("#txtValue1") }
        exportPropertyTxtNameError      { $("#errorName1") }
        exportPropertyTxtValueError     { $("#errorValue1") }
        lnkAddNewProperty               { $("#lnkAddNewProperty") }
        deleteFirstProperty             { $("#deleteFirstProperty") }

        errorExportStream               { $("#errorExportStream") }
        errorExportConnectorClass       { $("#errorExportConnectorClass") }
        errorMetadataBrokerListValue    { $("#errorMetadataBrokerListValue") }
        errorEndpointESValue            { $("#errorEndpointESValue") }

        // Import
        //importText                      { $(class:"fontFamily", text: "Import") }
        importText                      { $("#row-7 > td.configLabel > a > span") }
        importCheckbox                  { $("") }
        btnAddImportProperty            { $("#btnAddImportProperty") }
        txtImportStream                 { $("#txtImportStream") }
        selectImportType                { $("#txtImportType") }
        importTypeKafka                 { $("#txtImportType > option:nth-child(1)") }
        importTypeElasticSearch         { $("#txtImportType > option:nth-child(2)") }
        importTypeHttp                  { $("#txtImportType > option:nth-child(3)") }
        importTypeFile                  { $("#txtImportType > option:nth-child(4)") }
        importTypeRabbitMq              { $("#txtImportType > option:nth-child(5)") }
        importTypeJdbc                  { $("#txtImportType > option:nth-child(6)") }
        importTypeCustom                { $("#txtImportType > option:nth-child(7)") }

        importKafkaMetadata             { $("#txtMetadataBrokerListValue") }
        importElasticEndpointValue      { $("#txtEndpointESValue") }

        btnSaveImportOk                 { $("#btnSaveImportOk") }
        btnSaveImportCancel             { $("#btnSaveImportCancel") }
        deleteImportConfig              { $("#deleteImportConfig") }

        importPropertyCreatedRow        { configTbl.find('.importParentProp').last() }
        importPropertyName              { importPropertyCreatedRow.find('span') }
        importPorpertyEdit              { importPropertyCreatedRow.find('td').last().find('a') }
        lnkAddNewImportProperty         { $("#lnkAddNewImportProperty") }
        txtImportConnectorClass         { $("#txtImportConnectorClass")}
        importPropertyTxtName           { $("#txtName1") }
        importPropertyTxtValue          { $("#txtValue1") }
        importPropertyTxtNameError      { $("#errorName1") }
        importPropertyTxtValueError     { $("#errorValue1") }

        errorImportStream               { $("#errorImportStream") }
        errorImportConnectorClass       { $("#errorImportConnectorClass") }

        // Advanced
        advancedText                    { $(class:"fontFamily", text: "Advanced") }

        maxJavaHeapText                 { $("#adminTbl > tbody > tr:nth-child(21) > td.configLabel") }
        maxJavaHeapField                { $(id:"txtMaxJavaHeap") }

        heartbeatTimeoutText            { $("#adminTbl1 > tbody > tr:nth-child(14) > td.configLabel") }
        heartbeatTimeoutField           { $(id:"txtHeartbeatTimeout") }

        queryTimeoutText                { $("#adminTbl1 > tbody > tr:nth-child(15) > td.configLabel") }
        queryTimeoutField               { $(id:"txtQueryTimeout") }

        maxTempTableMemoryText          { $("#adminTbl1 > tbody > tr:nth-child(16) > td.configLabel") }
        maxTempTableMemoryField         { $(id:"txtMaxTempTableMemory") }

        snapshotPriorityText            { $("#adminTbl1 > tbody > tr:nth-child(17) > td.configLabel") }
        snapshotPriorityField           { $(id:"txtSnapshotPriority") }



        memoryLimitText                 { $("#adminTbl1 > tbody > tr:nth-child(18) > td.configLabel") }
        memoryLimitField                { $("#txtMemoryLimit") }
        memoryLimitType                 { $(id:"selMemoryLimitUnit") }
        memoryLimitOptionGB             { $("#selMemoryLimitUnit > option:nth-child(1)") }
    }

    int count
    static int numberOfTrials = 10

    def String getIdOfExportText(int index) {
        return ("exportList_" + String.valueOf(index))
    }

    def String getIdOfExportEdit(int index) {
        return ("btnUpdateExport_" + String.valueOf(index))
    }

    def boolean CheckIfSecurityChkExist(){
        boolean result = false
        try{
            waitFor(30){securityCheckbox.isDisplayed() }
            println("Checkbox is displayed.")
            result = true
        } catch(geb.waiting.WaitTimeoutException e){
            println("Checkbox is not displayed")
        }
        return result;
    }

    // Edit for VDM-37 starts from here

    def void expandSecurity() {
        for(count=0; count<numberOfTrials; count++) {
            try {
                securityText.click()
                waitFor { addSecurityButton.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException exception) {
                println("Unable to find the add security button - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { addSecurityButton.isDisplayed() }
                    break
                } catch(geb.waiting.WaitTimeoutException e) {

                }
            }
        }
    }

    def addUser() {
        for(count=0; count<numberOfTrials; count++) {
            try {
                addSecurityButton.click()
                waitFor { saveUserOkButton.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException exception) {
                println("Unable to find the add security button - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { saveUserOkButton.isDisplayed() }
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("Unable to find the add security button - Retrying")
                }
            } catch (org.openqa.selenium.StaleElementReferenceException e) {
                println("Stale Element Exception - Retrying")
            }
        }
    }

    def provideValueForUser(String username, String password, String role) {
        userField.value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        userField.value(username)

        passwordField.value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        passwordField.value(password)

        roleField.value(Keys.chord(Keys.CONTROL, "A") + Keys.BACK_SPACE)
        roleField.value(role + ",")
    }

    def saveUser() {
        for(count=0; count<numberOfTrials; count++) {
            try {
                saveUserOkButton.click()
                waitFor { !saveUserOkButton.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException exception) {
                println("Unable to find the add security button - Retrying")
            } catch(org.openqa.selenium.ElementNotVisibleException exception) {
                try {
                    waitFor { !saveUserOkButton.isDisplayed() }
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    println("Unable to find the add security button - Retrying")
                }
            }
        }
    }

}
