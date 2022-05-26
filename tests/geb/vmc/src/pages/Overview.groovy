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

package vmcTest.pages

import geb.Module
import java.util.List;
import java.util.Map;

import geb.*
import geb.navigator.Navigator
import geb.waiting.WaitTimeoutException

import org.openqa.selenium.JavascriptExecutor

/**
 * Created by anrai on 2/16/15.
 */

class Overview extends Module {

    static content = {
        title               { $("h1", text:"Overview") }
        sitePerHost         { $("#admin > div.adminContainer > div.adminContentLeft > div.overviewTbl > table > tbody:nth-child(1) > tr:nth-child(1) > td.configLabel") }
        ksafety             { $("#admin > div.adminContainer > div.adminContentLeft > div.overviewTbl > table > tbody:nth-child(1) > tr:nth-child(2) > td.configLabel") }
        partitionDetection  { $("#admin > div.adminContainer > div.adminContentLeft > div.overviewTbl > table > tbody:nth-child(1) > tr:nth-child(3) > td.configLabel") }
        security            { $(class:"labelCollapsed", text:"Security") }
        httpAccess          { $(class:"labelCollapsed", text:"HTTP Access") }
        autoSnapshots       { $(class:"labelCollapsed", text:"Auto Snapshots") }
        commandLogging      { $(class:"labelCollapsed", text:"Command Logging") }
        export              { $(class:"labelCollapsed", text:"Export") }
        advanced            { $(class:"labelCollapsed", text:"Advanced") }

        sitePerHostValue            { $("#sitePerHost") }
        ksafetyValue                { $("#kSafety") }
        partitionDetectionValue     { $("#partitionDetectionLabel") }
        securityValue               { $("#spanSecurity") }
        httpAccessValue             { $("#httpAccessLabel") }
        autoSnapshotsValue          { $("#txtAutoSnapshot") }
        commandLoggingValue         { $("#commandLogLabel") }

        securityEdit                { $("#securityEdit", class:"edit")}

        securityEditCheckbox        { $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-6.hasSubLabel.parent.security td.securitytd div.icheckbox_square-aero.checked.customCheckbox ins.iCheck-helper") }
        securityEditOk              { $("#btnEditSecurityOk") }
        securityEditCancel          { $("#btnEditSecurityCancel") }

        autoSnapshotsEdit           { $("#autoSnapshotEdit") }
        autoSnapshotsEditCheckbox1  { $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-2.hasSubLabel.parent td#autoSnapshotOption.snapshottd div.icheckbox_square-aero.customCheckbox ins.iCheck-helper") }

        autoSnapshotsEditOk         { $("#btnEditAutoSnapshotOk") }
        autoSnapshotsEditCancel     { $("#btnEditAutoSnapshotCancel") }

        filePrefixField             { $(id:"txtPrefix") }
        frequencyField              { $(id:"txtFrequency") }
        frequencyUnitField          { $(id:"ddlfrequencyUnit") }
        retainedField               { $(id:"txtRetained") }

        filePrefix                  { $(id:"prefixSpan") }
        frequency                   { $(id:"frequencySpan") }
        frequencyUnit               { $(id:"spanFrequencyUnit") }
        retained                    { $(id:"retainedSpan") }

        hour                        { $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#snapshotFrequencyRow.child-row-2.subLabelRow td.tdSnapshotFrequency form#frmSnapshotFrequency select#ddlfrequencyUnit.valid option") }
        minute                      { $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#snapshotFrequencyRow.child-row-2.subLabelRow td.tdSnapshotFrequency form#frmSnapshotFrequency select#ddlfrequencyUnit.valid option") }
        second                      { $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#snapshotFrequencyRow.child-row-2.subLabelRow td.tdSnapshotFrequency form#frmSnapshotFrequency select#ddlfrequencyUnit.valid option") }

        // SECURITY POPUP
        securityPopupOk             { $(id:"btnSecurityOk") }
        securityPopupCancel         { $(id:"btnPopupSecurityCancel") }

        // AUTO SNAPSHOTS EDIT POPUP

        autoSnapshotsPopup          { $("html body div.popup_cont div.popup div.popup_content") }
        autoSnapshotsPopupTitle     { $("html body div.popup_cont div.popup div.popup_content div.overlay-title.icon-alert") }
        autoSnapshotsPopupDisplay   { $("html body div.popup_cont div.popup div.popup_content div.overlay-content.confirmationHeight p.txt-bold") }
        autoSnapshotsPopupClose     { $("html body div.popup_cont div.popup_close") }
        autoSnapshotsPopupOk        { $(id:"btnSaveSnapshot", text:"Ok") }
        autoSnapshotsPopupCancel    { $("html body div.popup_cont div.popup div.popup_content div.overlay-btns a#btnPopupAutoSnapshotCancel.btn.btn-gray") }

        // SECURITY EXPANSION

        securityExpanded            { $(class:"labelCollapsed labelExpanded", text:"Security") }
        securityUsername            { $("th", text:"Username") }
        securityRole                { $("th", text:"Role") }
        securityAdd                 { $("#addNewUserLink1") }

        securityUserList            { $("#UsersList") }

        errorUsernameMessage        { $("#errorUser") }
        errorPasswordMessage        { $("#errorPassword") }

        userPopupUsernameField      { $("#txtUser") }
        userPopupPasswordField      { $("#txtPassword") }
        userPopupSelectRole         { $("#selectRole") }
        userPopupSave               { $("#btnSaveUser") }
        userPopupCancel             { $("#btnCancelUser") }
        userPopupDelete             { $("#deleteSecUser") }

        editUser                    { $(class:"edit", onclick:onClickValue) }
        editUserNext                { $(class:"edit", onclick:onClickValueNext) }
        saveDelete                  { $("#userSaveDelete") }
        userPopupConfirmYes         { $("#btnSaveSecUser") }
        userPopupConfirmNo          { $("btnCancelSaveSecUser") }

        // HTTP ACCESS EXPANSION

        jsonApi         { $(class:"configLabel", text:"JSON API") }
        jsonApiStatus   { $("#txtJsonAPI") }

        // AUTO SNAPSHOTS EXPANSION



        // COMMAND LOGGING EXPANSION

        logFrequencyTime            { $(class:"configLabel", text:"Log Frequency Time") }
        logFrequencyTransactions    { $(class:"configLabel", text:"Log Frequency Transactions") }
        logSize                     { $(class:"configLabel", text:"Log Size") }

        logFrequencyTimeValue           { $("#commandLogFrequencyTime") }
        logFrequencyTransactionsValue   { $("#commandLogFrequencyTxns") }
        logSizeValue                    { $("#commandLogSegmentSize") }

        // EXPORT EXPANSION

        export                      { $(class:"labelCollapsed", text:"Export") }
        exportExpanded              { $(class:"labelCollapsed labelExpanded", text:"Export") }
        exportTablesText            { $(class:"configLabel", text:"Export Tables") }
        listOfExport                { $("#lstExportTbl") }
        exportNoConfigAvailable     { $("#noConfigExport") }
        exportConfiguration         { $("#exportConfiguration") }
        exportConfig                { $(class:"configLabel expoStream", title:"Click to expand/collapse", number) }

        editExportConfiguration     { $("#exportEdit0") }

        // EXPORT POPUP
        stream                      { $("#txtStream") }
        exportAddConfigPopupTitle   { $("#addConfigHeader") }
        addProperty                 { $("#lnkAddNewProperty") }
        save                        { $("#btnAddConfigSave", text:"Save") }
        cancel                      { $("#btnAddConfigCancel", text:"Cancel") }

        textType                    { $("#txtType")}

        errorStream                 { $("#errorStream") }

        fileName                    { $(class:"labelCollapsed", text:fileTest) }
        jdbcName                    { $(class:"labelCollapsed", text:jdbcTest) }
        kafkaName                   { $(class:"labelCollapsed").first() }
        httpName                    { $(class:"labelCollapsed", text:httpTest) }
        rabbitMqBrokerName          { $(class:"labelCollapsed", text:rabbitMqBrokerTest) }
        rabbitMqAmqpName            { $(class:"labelCollapsed", text:rabbitMqAmqpTest) }
        customName                  { $(class:"labelCollapsed", text:customTest) }

        elasticSearchName           { $(class:"labelCollapsed", text:elasticSearchTest) }

        confirmyesbtn               { $("#btnSaveConfigOk") }

        deleteConfiguration         { $("#deleteAddConfig > a") }

        newTextField                { $("#txtName1") }
        newValueField               { $("#txtValue1") }
        deleteFirstProperty         { $("#deleteFirstProperty") }


        // EXPORT POPUP: ELASTICSEARCH
        endpointES                      { $("#txtEndpointES") }
        endpointESValue                   { $("#txtEndpointESValue") }
        errorEndpointESValue          { $("#errorEndpointESValue") }

        // EXPORT POPUP: FILE
        type                        { $("#txtFileType") }
        nonce                       { $("#txtnonce") }
        outdir                      { $("#txtOutdir") }

        typeValue                   { $("#txtFileTypeValue") }
        nonceValue                  { $("#txtnonceValue") }
        outdirValue                 { $("#txtOutdirValue") }

        errorFileTypeValue          { $("#errorFileTypeValue") }
        errornonceValue             { $("#errornonceValue") }
        errorOutdirValue            { $("#errorOutdirValue") }

        // EXPORT POPUP: JDBC
        jdbcdriver                  { $("#txtJdbcDriver") }
        jdbcurl                     { $("#txtJdbcUrl") }

        jdbcdriverValue             { $("#txtJdbcDriverValue") }
        jdbcurlValue                { $("#txtJdbcUrlValue") }

        errorJdbcDriverValue        { $("#errorJdbcDriverValue") }
        errorJdbcUrlValue           { $("#errorJdbcUrlValue") }

        // EXPORT POPUP: KAFKA
        metadatabroker              { $("#txtMetadataBrokerList") }

        metadatabrokerValue         { $("#txtMetadataBrokerListValue") }

        errorMetadataBrokerListValue{ $("#errorMetadataBrokerListValue") }

        // EXPORT POPUP: HTTP
        endpoint                    { $("#txtEndpoint") }

        endpointValue               { $("#txtEndpointValue") }

        errorEndpointValue          { $("#errorEndpointValue") }

        // EXPORT POPUP: RABBITMQ
        rabbitMq                    { $("#selectRabbitMq") }

        rabbitMqValue               { $("#txtRabbitMqValue") }

        errorRabbitMqValue          { $("#errorRabbitMqValue") }

        // EXPORT POPUP: CUSTOM
        exportConnectorClass        { $("#txtExportConnectorClass") }

        errorExportConnectorClass   { $("#errorExportConnectorClass") }


        //Import
        textImportType              { $("#txtImportType")}
        txtImportFormat             { $("#txtImportFormat") }
        importAddConfigPopupTitle   { $("#addImportConfigHeader") }
        addImportProperty           { $("#lnkAddNewImportProperty") }
        saveImport                  { $("#btnAddImportConfigSave", text:"Save") }
        cancelImport                { $("#btnAddImportConfigCancel", text:"Cancel") }

        errorFormat                 { $("#errorImportFormat") }

        //KAFKA
        txtTopics                   { $("#txtTopics") }
        txtProcedure                { $("#txtProcedure") }
        txtBrokers                  { $("#txtBrokers") }

        txtTopicsValue              { $("#txtTopicsValue") }
        txtProcedureValue           { $("#txtProcedureValue") }
        txtBrokersValue             { $("#txtBrokersValue") }

        errorTopicValue             { $("#errorTopicValue") }
        errorProcedureValue         { $("#errorProcedureValue") }
        errorBrokersValue           { $("#errorBrokersValue") }

        //KINESIS
        txtRegion                   { $("#txtRegion") }
        txtSecretKey                { $("#txtSecretKey") }
        txtAccessKey                { $("#txtAccessKey") }
        txtStreamName               { $("#txtStreamName") }
        txtProcedureKi              { $("#txtProcedureKi") }
        txtAppName                  { $("#txtAppName") }

        txtRegionValue              { $("#txtRegionValue") }
        txtSecretKeyValue           { $("#txtSecretKeyValue") }
        txtAccessKeyValue           { $("#txtAccessKeyValue") }
        txtStreamNameValue          { $("#txtStreamNameValue") }
        txtProcedureKiValue         { $("#txtProcedureKiValue") }
        txtAppNameValue             { $("#txtAppNameValue") }

        errorRegionValue            { $("#errorRegionValue") }
        errorSecretKeyValue         { $("#errorSecretKeyValue") }
        errorAccessKeyValue         { $("#errorAccessKeyValue") }
        errorStreamNameValue        { $("#errorStreamNameValue") }
        errorProcedureKiValue       { $("#errorProcedureKiValue") }
        errorAppNameValue           { $("#errorAppNameValue") }


        btnSaveImportConfigOk       { $("#btnSaveImportConfigOk") }
        btnSaveImportConfigCancel   { $("#btnSaveImportConfigCancel") }

        KafkaImportName             { $(class:"labelCollapsed", text: "KAFKA") }
        KinesisImportName           { $(class:"labelCollapsed", text: "KINESIS") }
        deleteImportConfiguration   { $("#deleteImportConfig > a") }

        newImportTextField          { $("#txtImportName1") }
        newImportValueField         { $("#txtImportValue1") }
        deleteFirstImportProperty   { $("#deleteFirstImportProperty") }

        errorImportName1            { $("#errorImportName1") }
        errorImportValue1           { $("#errorImportValue1") }
        // EXPORT EXPANSION

        importConfig                { $(class:"labelCollapsed", text:"Import") }
        importExpanded              { $(class:"labelCollapsed labelExpanded", text:"Import") }
        importNoConfigAvailable     { $("#noConfigImport") }
        importConfiguration         { $("#importConfiguration") }

        editImportConfiguration     { $("#importEdit0") }

        //

        // ADVANCED EXPANSION

        maxJavaHeap             { $(class:"configLabel", text:"Max Java Heap") }
        heartbeatTimeout        { $(class:"configLabel", text:"Heartbeat Timeout") }
        queryTimeout            { $(class:"configLabel", text:"Query Timeout") }
        maxTempTableMemory      { $(class:"configLabel", text:"Max Temp Table Memory") }
        snapshotPriority        { $(class:"configLabel", text:"Snapshot Priority") }
        memoryLimitSize         { $(class:"configLabel", text:"Memory Limit") }
        diskLimit               {$("#diskLimit")}


        maxJavaHeapValue        { $("#maxJavaHeap") }
        heartbeatTimeoutValue   { $("#formHeartbeatTimeout") }
        queryTimeoutValue       { $("#queryTimeOutUnitSpan") }
        maxTempTableMemoryValue { $("#temptablesmaxsizeUnit") }
        snapshotPriorityValue   { $("#snapshotpriority") }
        memoryLimitSizeValue    { $("#memoryLimitSize") }
        memoryLimitSizeUnit     { $("#memoryLimitSizeUnit") }

        // heartbeat timeout
        heartTimeoutEdit        { $("#btnEditHrtTimeOut") }
        heartTimeoutValue       { $("#hrtTimeOutSpan") }
        heartTimeoutField       { $("#txtHrtTimeOut") }
        heartTimeoutOk          { $("#btnEditHeartbeatTimeoutOk") }
        heartTimeoutCancel      { $("#btnEditHeartbeatTimeoutCancel") }
        heartTimeoutPopupOk     { $("#btnPopupHeartbeatTimeoutOk") }
        heartTimeoutPopupCancel { $("#btnPopupHeartbeatTimeoutCancel") }

        // query timeout
        queryTimeoutEdit        { $("#btnEditQueryTimeout") }
        queryTimeoutValue       { $("#queryTimeOutSpan") }
        queryTimeoutField       { $("#txtQueryTimeout") }
        queryTimeoutOk          { $("#btnEditQueryTimeoutOk") }
        queryTimeoutCancel      { $("#btnEditQueryTimeoutCancel") }
        queryTimeoutPopupOk     { $("#btnPopupQueryTimeoutOk") }
        queryTimeoutPopupCancel { $("#btnPopupQueryTimeoutCancel") }

        //Memory Limit
        memoryLimitEdit         { $("#btnEditMemorySize") }
        memoryLimitValue        { $("#memoryLimitSize") }
        memoryLimitField        { $("#txtMemoryLimitSize") }
        memoryLimitUnit         { $("#memoryLimitSizeUnit") }
        memoryLimitError        { $("#errorMemorySize") }
        memoryLimitOk           { $("#btnEditMemorySizeOk") }
        memoryLimitCancel       { $("#btnEditMemorySizeCancel") }
        memoryLimitPopupOk      { $("#btnPopupMemoryLimitOk") }
        memoryLimitPopupCancel  { $("#btnPopupMemoryLimitCancel") }
        memoryLimitDdlUnit      { $("#ddlMemoryLimitUnit") }
        memoryLimitDelete       (required:false) { $("#btnDeleteMemory") }
        memoryLimitDeleteOk     { $("#btnDelPopupMemoryLimitOk") }

        //Disk Limit


        lnkAddNewFeature {$("#lnkAddNewFeature")}
        btnAddDiskLimitSave {$("#btnAddDiskLimitSave")}
        btnAddDiskLimitCancel {$("#btnAddDiskLimitCancel")}
        btnSaveDiskLimitOk {$("#btnSaveDiskLimitOk")}
        btnSaveDiskLimitCancel {$("#btnSaveDiskLimitCancel")}
        addDiskLimitHeader {$("#addDiskLimitHeader")}
        featureName1 {$("#txtNameDL1")}
        featureValue1 {$("#txtValueDL1")}
        featureUnit1 {$("#txtUnitDL1")}
        featureName2 {$("#txtNameDL2")}
        featureValue2 {$("#txtValueDL2")}
        featureUnit2 {$("#txtUnitDL2")}
        deleteFirstFeature {$("#deleteFirstFeature")}
        errorValue1 {$("#errorValueDL1")}
        errortxtName2 {$("#error_txtNameDL2")}
        errortxtName1 {$("#error_txtNameDL1")}
        diskLimitEdit {$("#btnEditDiskLimit") }


            //$(class:"labelCollapsed labelExpanded", text:"Disk Limit") }





        // error message
        errorMsgHeartbeat       { $("#errorHeartbeatTimeout") }
        errorQuery              { $("#errorQueryTimeout") }

        //error message for Add Property

        errorPropertyName1 {$("#errorName1")}
        errorPropertyValue1 {$("#errorValue1")}

        addconfig           { $("#addNewConfigLink")}
        addImportConfig     { $("#addNewImportConfigLink") }
    }

    int numberOfTrials = 10
    int waitTime = 30
    int five = 5
    int count = 0
    String onClickValue     = getName()
    String onClickValueNext = getNameNext()

    String fileTest             = getFileTest()
    String jdbcTest             = getJdbcTest()
    String kafkaTest            = getKafkaTest()
    String httpTest             = getHttpTest()
    String rabbitMqBrokerTest   = getRabbitmqBrokerTest()
    String rabbitMqAmqpTest     = getRabbitmqAmqpTest()
    String customTest           = getCustomTest()
    String elasticSearchTest             = getElasticSearchTest()

    def String getName() {
        return "addUser(1,'" + getUsernameOneForSecurity() + "','" + getRoleOneForSecurity() + "');"
    }

    def String getNameNext() {
        return "addUser(1,'" + getUsernameTwoForSecurity() + "','" + getRoleTwoForSecurity() + "');"
    }

    def String getFileTest() {
        return getFileTestName() + " (FILE)"
    }

    def String getElasticSearchTest() {
        return getElasticSearchTestName() + " (ELASTICSEARCH)"
    }

    def String getJdbcTest() {
        return getJdbcTestName() + " (JDBC)"
    }

    def String getKafkaTest() {
        return getKafkaTestName() + " (KAFKA)"
    }

    def String getHttpTest() {
        return getHttpTestName() + " (HTTP)"
    }

    def String getRabbitmqBrokerTest() {
        return getRabbitmqBrokerTestName() + " (RABBITMQ)"
    }

    def String getRabbitmqAmqpTest() {
        return getRabbitmqAmqpTestName() + " (RABBITMQ)"
    }

    def String getCustomTest() {
        return getCustomTestName() + " (CUSTOM)"
    }

    // Get usernameone for security
    def String getUsernameOneForSecurity() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/securityUsers.txt"))
        String username
        while((username = br.readLine()) != "#usernameOne") {}
        username = br.readLine()
        return username
    }

    // Get usernametwo for security
    def String getUsernameTwoForSecurity() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/securityUsers.txt"))
        String username
        while((username = br.readLine()) != ("#usernameTwo")) {}
        username = br.readLine()
        return username
    }

    // Get passwordone for security
    def String getPasswordOneForSecurity() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/securityUsers.txt"))
        String password
        while((password = br.readLine()) != "#passwordOne") {}
        password = br.readLine()
        return password
    }

    // Get passwordtwo for security
    def String getPasswordTwoForSecurity() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/securityUsers.txt"))
        String password
        while((password = br.readLine()) != ("#passwordTwo")) {}
        password = br.readLine()
        return password
    }

    // Get roleone for security
    def String getRoleOneForSecurity() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/securityUsers.txt"))
        String role
        while((role = br.readLine()) != ("#roleOne")) {}
        role = br.readLine()
        return role
    }

    // Get roletwo for security
    def String getRoleTwoForSecurity() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/securityUsers.txt"))
        String role
        while((role = br.readLine()) != ("#roleTwo")) {}
        role = br.readLine()
        return role
    }

    /**
     * Click security to expand, if already it is not expanded
     */
     def boolean expandSecurity() {
        if (checkIfSecurityIsExpanded() == false)
            security.click()
     }


     /**
     * Click security to collapse, if already it is expanded
     */
     def boolean collapseSecurity() {
        if (checkIfSecurityIsExpanded() == true)
            security.click()
     }

     /**
     * Check if security is expanded or not
     */
     def boolean checkIfSecurityIsExpanded() {
        try {
            securityExpanded.isDisplayed()
            return true
        } catch(geb.error.RequiredPageContentNotPresent e) {
            return false
        } catch(org.openqa.selenium.StaleElementReferenceException e) {
            return true
        }
     }

     /**
     * Click and open security Add popup
     */
     def boolean openSecurityAdd() {
        try {
            waitFor(waitTime) { securityAdd.isDisplayed() }
            securityAdd.click()
            checkSecurityAddOpen()
            return true
        } catch(geb.error.RequiredPageContentNotPresent e) {
            return false
        } catch(geb.waiting.WaitTimeoutException e) {
            return false
        }
     }

     /**
     * Check if Security Add is open or not
     */
     def boolean checkSecurityAddOpen() {
        try {
            waitFor(waitTime) {
                userPopupUsernameField.isDisplayed()
                userPopupPasswordField.isDisplayed()
                userPopupSave.isDisplayed()
            }
            return true
        } catch(geb.error.RequiredPageContentNotPresent e) {
            return false
        } catch(geb.waiting.WaitTimeoutException e) {
            return false
        }
     }

     /**
     * Enter the username, password, and role save it
     */
     def boolean enterUserCredentials(String username, String password, String role) {
        try {
            checkSecurityAddOpen()
            waitFor(waitTime) {
                userPopupUsernameField.value(username)
                userPopupPasswordField.value(password)
                userPopupSelectRole.value(role)
            }
            userPopupSave.click()

            waitFor(waitTime) {
                saveDelete.text().equals("save")
                userPopupConfirmYes.isDisplayed()
            }

            count = 0
            while(count < five) {
                count++
                try {
                    waitFor(waitTime) {
                        userPopupConfirmYes.click()
                    }
                } catch(geb.error.RequiredPageContentNotPresent e) {
                    break
                } catch(org.openqa.selenium.ElementNotVisibleException e) {
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    break
                }
            }

            return true
        } catch(geb.error.RequiredPageContentNotPresent e) {
            return false
        } catch(geb.waiting.WaitTimeoutException e) {
            return false
        }
    }


    /**
    * Returns the list of all the users in Security
    */
    def String getListOfUsers() {
        String list = ""
        try {
            //expandSecurity()

            waitFor(waitTime) { securityUserList.isDisplayed() }

            list = securityUserList.text()
            return list
        } catch(geb.error.RequiredPageContentNotPresent e) {
            println("Error when getting list of users")
            return ""
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Error when getting list of users")
            return ""
        }
    }


    def boolean checkListForUsers(String username) {
        try {
            String list = getListOfUsers()
            if(list.contains(username)) {
                println("The user " + username + " was created" )
                return true
            }
            else {
                return false
            }
        } catch(geb.error.RequiredPageContentNotPresent e) {
            println("Error when getting list of users")
            return false
        } catch(geb.waiting.WaitTimeoutException e) {
            println("Error when getting list of users")
            return false
        }
    }


    /**
    * Check if Security Edit is open or not
    */
    def boolean checkSecurityEditOpen() {
        try {
            waitFor(waitTime) {
                userPopupUsernameField.isDisplayed()
                userPopupPasswordField.isDisplayed()
                userPopupSave.isDisplayed()
            }
            return true
        } catch(geb.error.RequiredPageContentNotPresent e) {
            return false
        } catch(geb.waiting.WaitTimeoutException e) {
            return false
        }
    }


    def boolean openEditUser() {
        try {
            waitFor(waitTime) { editUser.isDisplayed() }
            editUser.click()
            checkSecurityAddOpen()
            return true
        } catch(geb.error.RequiredPageContentNotPresent e) {
            return false
        } catch(geb.waiting.WaitTimeoutException e) {
            return false
        }
    }

    def boolean openEditUserNext() {
        try {
            waitFor(waitTime) { editUserNext.isDisplayed() }
            editUserNext.click()
            checkSecurityAddOpen()
            return true
        } catch(geb.error.RequiredPageContentNotPresent e) {
            return false
        } catch(geb.waiting.WaitTimeoutException e) {
            return false
        }
    }

    def boolean cancelPopup() {
        try {
            waitFor(waitTime) { userPopupCancel.click() }
        } catch(geb.error.RequiredPageContentNotPresent e) {
            println("Cancel button isn't present")
            return true
        }
    }

    def boolean deleteUserSecurityPopup() {
        try {
            waitFor(waitTime) { userPopupDelete.click() }

            waitFor(waitTime) {
                saveDelete.text().equals("delete")
                userPopupConfirmYes.isDisplayed()
            }

            count = 0
            while(count < five) {
                count++
                try {
                    waitFor(waitTime) {
                        page.overview.userPopupConfirmYes.click()
                    }
                } catch(geb.error.RequiredPageContentNotPresent e) {
                    break
                } catch(org.openqa.selenium.ElementNotVisibleException e) {
                    break
                } catch(geb.waiting.WaitTimeoutException e) {
                    break
                }
            }
        } catch(geb.error.RequiredPageContentNotPresent e) {
            println("Delete button isn't present")
            return true
        } catch(geb.waiting.WaitTimeoutException e) {
            return false
        }
    }

    def openAddConfigurationPopup() {
        waitFor(waitTime) { addconfig.isDisplayed() }
        int count = 0
        while(count<numberOfTrials) {
            count++
            try {
                addconfig.click()
                waitFor(waitTime) { imp.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
    }

    def openAddImportConfigurationPopup() {
        waitFor(waitTime) { addImportConfig.isDisplayed() }
        int count = 0
        while(count<numberOfTrials) {
            count++
            try {
                addImportConfig.click()
                waitFor(waitTime) { importAddConfigPopupTitle.isDisplayed() }
                break
            } catch(geb.waiting.WaitTimeoutException e) {
            }
        }
    }

    def openEditDiskLimitPopup() {
        waitFor(waitTime) { diskLimitEdit.isDisplayed() }
        diskLimitEdit.click()
        waitFor(waitTime) { addDiskLimitHeader.isDisplayed() }
//        int count = 0
//        while(count<numberOfTrials) {
//            count++
//            try {
//
//                break
//            } catch(geb.waiting.WaitTimeoutException e) {
//            }
//        }
    }

    // For Export

    def String getFileTestName() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportDetails.txt"))
        String fileTestName
        while((fileTestName = br.readLine()) != ("#fileTestName")) {}
        fileTestName = br.readLine()
        return fileTestName
    }

    def String getJdbcTestName() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportDetails.txt"))
        String jdbcTestName
        while((jdbcTestName = br.readLine()) != ("#jdbcTestName")) {}
        jdbcTestName = br.readLine()
        return jdbcTestName
    }

    def String getKafkaTestName() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportDetails.txt"))
        String kafkaTestName
        while((kafkaTestName = br.readLine()) != ("#kafkaTestName")) {}
        kafkaTestName = br.readLine()
        return kafkaTestName
    }

    def String getHttpTestName() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportDetails.txt"))
        String httpTestName
        while((httpTestName = br.readLine()) != ("#httpTestName")) {}
        httpTestName = br.readLine()
        return httpTestName
    }

    def String getRabbitmqBrokerTestName() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportDetails.txt"))
        String rabbitmqBrokerTestName
        while((rabbitmqBrokerTestName = br.readLine()) != ("#rabbitmqBrokerTest")) {}
        rabbitmqBrokerTestName = br.readLine()
        return rabbitmqBrokerTestName
    }

    def String getRabbitmqAmqpTestName() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportDetails.txt"))
        String rabbitmqAmqpTestName
        while((rabbitmqAmqpTestName = br.readLine()) != ("#rabbitmqAmqpTest")) {}
        rabbitmqAmqpTestName = br.readLine()
        return rabbitmqAmqpTestName
    }

    def String getCustomTestName() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportDetails.txt"))
        String customTestName
        while((customTestName = br.readLine()) != ("#customTestName")) {}
        customTestName = br.readLine()
        return customTestName
    }

    def String getElasticSearchTestName() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportDetails.txt"))
        String elasticSearchTestName
        while((elasticSearchTestName = br.readLine()) != ("#elasticSearchTestName")) {}
        elasticSearchTestName = br.readLine()
        return elasticSearchTestName
    }

    def void clickSave() {
        int count = 0
        while(count<30) {
            count++
            try {
                waitFor(waitTime) { save.click() }
                waitFor(waitTime) { confirmyesbtn.isDisplayed() }
                println("new")
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
                println("new1")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("new2")
            } catch(geb.waiting.WaitTimeoutException e) {
                println("old")
            }
        }

        count = 0
        while(count<30) {
            count++
            try {
                waitFor(waitTime) { confirmyesbtn.click() }
                waitFor(waitTime) { !confirmyesbtn.isDisplayed() }
                println("new3")
                break
            } catch(geb.error.RequiredPageContentNotPresent e) {
                println("new4")
            } catch(org.openqa.selenium.StaleElementReferenceException e) {
                println("new5")
            } catch(geb.waiting.WaitTimeoutException e) {
                println("old1")
            }
        }
    }

    def void deleteExportConfiguration() {
        int count = 0
        while(count<numberOfTrials) {
            count++
            try {
                deleteConfiguration.click()
                !deleteConfiguration.isDisplayed()
            } catch(geb.error.RequiredPageContentNotPresent e) {

            } catch(org.openqa.selenium.StaleElementReferenceException e) {

            } catch(org.openqa.selenium.ElementNotVisibleException e) {

            }
        }

        count = 0
        while(count<numberOfTrials) {
            count++
            try {
                confirmyesbtn.click()
                !confirmyesbtn.isDisplayed()
            } catch(geb.error.RequiredPageContentNotPresent e) {

            } catch(org.openqa.selenium.StaleElementReferenceException e) {

            } catch(org.openqa.selenium.ElementNotVisibleException e) {

            }
        }
    }

    /**
     * Click export to expand, if already it is not expanded
     */
    def boolean expandExport() {
        if (checkIfExportIsExpanded() == false)
            export.click()
    }

    /**
     * Click import to expand, if already it is not expanded
     */
    def boolean expandImport() {
        if (checkIfImportIsExpanded() == false)
            importConfig.click()
    }

     /**
     * Click export to collapse, if already it is expanded
     */
     def boolean collapseExport() {
        if (checkIfExportIsExpanded() == true)
            export.click()
     }

     /**
     * Check if export is expanded or not
     */
     def boolean checkIfExportIsExpanded() {
        try {
            importExpanded.isDisplayed()
            return true
        } catch(geb.error.RequiredPageContentNotPresent e) {
            return false
        } catch(org.openqa.selenium.StaleElementReferenceException e) {
            return true
        }
     }

    /**
     * Check if import is expanded or not
     */
    def boolean checkIfImportIsExpanded() {
        try {
            importExpanded.isDisplayed()
            return true
        } catch(geb.error.RequiredPageContentNotPresent e) {
            return false
        } catch(org.openqa.selenium.StaleElementReferenceException e) {
            return true
        }
    }

     def boolean checkIfDiskLimitIsExpanded() {
        try {
            diskLimitExpanded.isDisplayed()
            return true
        } catch(geb.error.RequiredPageContentNotPresent e) {
            return false
        } catch(org.openqa.selenium.StaleElementReferenceException e) {
            return true
        }
     }

    /**
     * Click export to expand, if already it is not expanded
     */
    def boolean expandDiskLimit() {
        if (checkIfDiskLimitIsExpanded() == false)
            diskLimitExpanded.click()
    }

     def String getFileValueOne() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportDetails.txt"))
        String fileValueOne
        while((fileValueOne = br.readLine()) != ("#typeValue")) {}
        fileValueOne = br.readLine()
        return fileValueOne
     }

     def String getFileValueTwo() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportDetails.txt"))
        String fileValueTwo
        while((fileValueTwo = br.readLine()) != ("#nonceValue")) {}
        fileValueTwo = br.readLine()
        return fileValueTwo
     }

     def String getFileValueThree() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportDetails.txt"))
        String fileValueThree
        while((fileValueThree = br.readLine()) != ("#outdirValue")) {}
        fileValueThree = br.readLine()
        return fileValueThree
     }

     def String getJdbcValueOne() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportDetails.txt"))
        String jdbcValueOne
        while((jdbcValueOne = br.readLine()) != ("#jdbcdriverValue")) {}
        jdbcValueOne = br.readLine()
        return jdbcValueOne
     }

     def String getJdbcValueTwo() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportDetails.txt"))
        String jdbcValueTwo
        while((jdbcValueTwo = br.readLine()) != ("#jdbcurlValue")) {}
        jdbcValueTwo = br.readLine()
        return jdbcValueTwo
     }

     def String getMetadataValue() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportDetails.txt"))
        String metadataValue
        while((metadataValue = br.readLine()) != ("#metadataValue")) {}
        metadataValue = br.readLine()
        return metadataValue
     }

     def String getEndValue() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportDetails.txt"))
        String endValue
        while((endValue = br.readLine()) != ("#endValue")) {}
        endValue = br.readLine()
        return endValue
     }

     def String getBrokerValue() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportDetails.txt"))
        String brokerValue
        while((brokerValue = br.readLine()) != ("#brokerValue")) {}
        brokerValue = br.readLine()
        return brokerValue
     }

     def String getAmqpValue() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportDetails.txt"))
        String amqpValue
        while((amqpValue = br.readLine()) != ("#amqpValue")) {}
        amqpValue = br.readLine()
        return amqpValue
    }

     def String getCustomConnectorClass() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportDetails.txt"))
        String customConnectorClass
        while((customConnectorClass = br.readLine()) != ("#customConnectorClass")) {}
        customConnectorClass = br.readLine()
        return customConnectorClass
    }

    def String getElasticSearchValueOne() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportDetails.txt"))
        String elasticSearchValue
        while((elasticSearchValue = br.readLine()) != ("#endpointValue")) {}
        elasticSearchValue = br.readLine()
        return elasticSearchValue
    }


}

