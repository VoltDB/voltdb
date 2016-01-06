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

import geb.Page
import geb.Module

class OverviewModule extends Module {
    static content = {
        // Site Per Host
        sitePerHostText                 { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigLeft > div > div.mainTbl > table > tbody > tr:nth-child(1) > td.configLabel") }
        sitePerHostField                { $(id:"txtSitePerHost") }

        // K-Safety
        ksafetyText                     { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigLeft > div > div.mainTbl > table > tbody > tr:nth-child(2) > td.configLabel") }
        ksafetyField                    { $(id:"txtKSafety") }

        // Partition Detection
        partitionDetectionText          { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigLeft > div > div.mainTbl > table > tbody > tr.security > td.configLabel") }
        partitionDetectionCheckbox      { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigLeft > div > div.mainTbl > table > tbody > tr.security > td:nth-child(2) > div > ins") }
        partitionDetectionStatus        { $(id:"txtPartitionDetection") }

        // Security
        securityText                    { $("#row-6 > td.configLabel > a > span") }
        securityCheckbox                (required: false){ $("#row-6 > td:nth-child(2) > div > ins") }
        securityStatus                  { $("#txtSecurity") }

        usernameTitleText               { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigLeft > div > div.mainTbl > table > tbody > tr.child-row-6.subLabelRow.thead.secTbl1 > td.configLabel") }
        roleTitleText                   { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigLeft > div > div.mainTbl > table > tbody > tr.child-row-6.subLabelRow.thead.secTbl1 > td:nth-child(2)") }
        addUserButton                   { $("#btnAddSecurity > span") }

        // HTTP Access
        httpAccessText                  { $("#row-1 > td.configLabel > a > span") }
        httpAccessCheckbox              { $("#row-1 > td:nth-child(2) > div > ins") }
        httpAccessStatus                { $(id:"txtHttpAccess") }

        jsonApiText                     {$("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigLeft > div > div.mainTbl > table > tbody > tr.child-row-1.subLabelRow > td.configLabel")}
        jsonApiCheckbox                 { $(id:"chkAutoSnapshot") }

        // Auto Snapshots
        autoSnapshotsText               { $("#row-2 > td.configLabel > a > span") }
        autoSnapshotsCheckbox           { $("#row-2 > td:nth-child(2) > div > ins") }
        autoSnapshotsStatus             { $(id:"txtAutoSnapshot") }

        filePrefixText                  { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigLeft > div > div.mainTbl > table > tbody > tr:nth-child(10) > td.configLabel") }
        frequencyText                   { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigLeft > div > div.mainTbl > table > tbody > tr:nth-child(11) > td.configLabel") }
        retainedText                    { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigLeft > div > div.mainTbl > table > tbody > tr:nth-child(12) > td.configLabel") }

        filePrefixField                 { $(id:"txtFilePrefix") }
        frequencyField                  { $(id:"txtFrequency") }
        retainedField                   { $(id:"txtRetained") }

        // Command Logging
        commandLoggingText              { $(class:"fontFamily", text:"Command Logging") }
        commandLoggingCheckbox          { $("#row-3 > td:nth-child(2) > div > ins") }
        commandLoggingStatus            { $(id:"txtCommandLog") }

        logFrequencyTimeText            { $("td", class:"configLabel", 13) }
        logFrequencyTransactionsText    { $("td", class:"configLabel", 14) }
        logSegmentSizeText              { $("td", class:"configLabel", 15) }

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

        maxJavaHeapText                 { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigLeft > div > div.mainTbl > table > tbody > tr:nth-child(24) > td.configLabel") }
        maxJavaHeapField                { $(id:"txtMaxJavaHeap") }

        heartbeatTimeoutText            { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigLeft > div > div.mainTbl > table > tbody > tr:nth-child(25) > td.configLabel") }
        heartbeatTimeoutField           { $(id:"txtHeartbeatTimeout") }

        queryTimeoutText                { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigLeft > div > div.mainTbl > table > tbody > tr:nth-child(26) > td.configLabel") }
        queryTimeoutField               { $(id:"txtQueryTimeout") }

        maxTempTableMemoryText          { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigLeft > div > div.mainTbl > table > tbody > tr:nth-child(27) > td.configLabel") }
        maxTempTableMemoryField         { $(id:"txtMaxTempTableMemory") }

        snapshotPriorityText            { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigLeft > div > div.mainTbl > table > tbody > tr:nth-child(28) > td.configLabel") }
        snapshotPriorityField           { $(id:"txtSnapshotPriority") }

        memoryLimitText                 { $("#divDbManager > div:nth-child(6) > div > div.col-md-6.clusterConfigLeft > div > div.mainTbl > table > tbody > tr:nth-child(29) > td.configLabel") }
        memoryLimitField                { $(id:"txtMemoryLimit") }

        // security
        securityText               { $("#row-6 > td.configLabel") }

        addSecurityButton              {$("#btnAddSecurity")}

        userField                     {$("#txtUser")}

        passwordField                 {$("#txtPassword")}

        selectAdminRole             { $("#selectRole > option:nth-child(1)") }

        SaveUserOkButton               {$("#btnSaveUserOk")}

        securityLabel               {$("tr.securityList").find("td.configLabel")}


        updateSecurityButton           {$("a.btnUpdateSecurity")}

        deleteUserButton               {$("#deleteUser")}

        errorUser                   {$("#errorUser")}

        errorPassword               {$("#errorPassword")}

        cancelUserButton               {$("#btnCancelUser")}

    }
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

}
