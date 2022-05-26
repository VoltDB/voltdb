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

import geb.navigator.Navigator

class AdminPage extends VoltDBManagementCenterPage {
    static content = {
        cluster                     { $('#admin > div.adminWrapper') }
        overview                    { $('#admin > div.adminContainer > div.adminContentLeft') }
        networkInterfaces           { $('#admin > div.adminContainer > div.adminContentRight > div.adminPorts') }
        directories                 { $('#admin > div.adminContainer > div.adminContentRight > div.adminDirectories') }
        networkInterfaces           { module NetworkInterfaces }
        directories                 { module Directories }
        overview                    { module Overview }
        cluster                     { module Cluster }
        header                      { module Header }
        footer                      { module Footer }
        server                      { module voltDBclusterserver }
        downloadbtn                 { module downloadconfigbtn }
        schema                      { module schemaTab}

        serverbutton                { $("#serverName") }
        serverconfirmation          { $("#serverConfigAdmin > div > div.slide-pop-title > div.icon-server.searchLeft.searchLeftAdmin") }

        servernamelist              { $("#serverListWrapperAdmin > table > tbody > tr > td.configLabel > a") }
        servername                  { $("#serverListWrapperAdmin > table > tbody > tr > td.configLabel > a") }
        mainservername              { $("#serverListWrapperAdmin") }

        serverresumebuttonmain      { $(class:"shutdown", text:"Resume") }
        serverstopbtndisable        { $(class:"disableServer").find(".stopDisable") }
        serverstopbtnenable         { $(class:"shutdown", text:"Stop") }

        serverstopbuttonmain        { $("#stopServer_voltdbserver", text:"Paused") }

        shutdownServerPause         { $(class:"shutdownServerPause").first() }

        shutdownServerStop          { $(class:"shutdownServer").first() }

        servernamelist1             { $("#serverListWrapperAdmin > table > tbody > tr.activeHostMonitoring > td.configLabel > a") }
        servernamelist2             { $("#serverListWrapperAdmin > table > tbody > tr:nth-child(2) > td.configLabel > a") }
        servernamelist3             { $("#serverListWrapperAdmin > table > tbody > tr:nth-child(3) > td.configLabel > a") }

        serverstopbtn1              { $("#stopServer_deerwalk3", class:"disableServer", text:"Stop") }
        serverstopbtn2              { $("#stopServer_deerwalk2", text:"Stop") }
        serverstopbtn3              { $("#stopServer_deerwalk4", text:"Stop") }
        serverstopcancel            { $("#StopConfirmCancel",    text:"Cancel") }
        serverstopok                { $("#StopConfirmOK",        text:"Ok") }
        serverstoppopup             { $("body > div.popup_content34", class:"popup") }
        downloadconfigurationbutton { $("#downloadAdminConfigurations") }
        //DBmonitor part for server
        dbmonitorbutton             { $("#navDbmonitor > a") }
        clusterserverbutton         { $("#btnPopServerList") }
        servernamefourthbtn         { $("#serversList > li:nth-child(1) > a") }
        servernamesecondbtn         { $("#serversList > li:nth-child(2) > a") }
        servernamethirdbtn          { $("#serversList > li:nth-child(3) > a") }
        serveractivechk             { $("#serversList > li.active.monitoring > a") }
        serversearch                { $("input", type: "text", id: "popServerSearch") }
        checkserverTitle            { $("#popServerList > div > div.slide-pop-title > div.icon-server.searchLeft") }
        setthreshhold               { $("#threshold") }
        clickthreshholdset          { $("#saveThreshold") }

        // dbmonitor graph
        servercpudaysmin            { $("#visualisationCpu > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text") }
        servercpudaysmax            { $("#visualisationCpu > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text") }
        servercpuminutesmin         { $("#visualisationCpu > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text") }
        servercpuminutemax          { $("#visualisationCpu > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text") }
        servercpusecondmin          { $("#visualisationCpu > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text") }
        servercpusecondmax          { $("#visualisationCpu > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text") }

        selecttypeindrop                { $("#graphView") }
        selecttypedays                  { $("#graphView > option:nth-child(3)") }
        selecttypemin                   { $("#graphView > option:nth-child(2)") }
        selecttypesec                   { $("#graphView > option:nth-child(1)") }

        serverramdaysmin                { $("#visualisationRam > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text") }
        serverramdaysmax                { $("#visualisationRam > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text") }
        serverramsecondmin              { $("#visualisationRam > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text") }
        serverramsecondmax              { $("#visualisationRam > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text") }
        serverramminutesmin             { $("#visualisationRam > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text") }
        serverramminutesmax             { $("#visualisationRam > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text") }

        clusterlatencydaysmin           { $("#visualisationLatency > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text") }
        clusterlatencydaysmax           { $("#visualisationLatency > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text") }
        clusterlatencysecondmin         { $("#visualisationLatency > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text") }
        clusterlatencysecondmax         { $("#visualisationLatency > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text") }
        clusterlatencyminutesmin        { $("#visualisationLatency > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text") }
        clusterlatencyminutesmax        { $("#visualisationLatency > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text") }

        clustertransactiondaysmin       { $("#visualisationTransaction > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text") }
        clustertransactiondaysmax       { $("#visualisationTransaction > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text") }
        clustertransactionsecondmin     { $("#visualisationTransaction > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text") }
        clustertransactionsecondmax     { $("#visualisationTransaction > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text") }
        clustertransactionminutesmin    { $("#visualisationTransaction > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text") }
        clustertransactionminutesmax    { $("#visualisationTransaction > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text") }

        // Network interfaces
        title1                          { $(text:"Overview") }
        sitePerHost                     { $(class:"configLabel", text:"Sites Per Host") }
        ksafety                         { $(class:"configLabel", text:"K-Safety") }
        partitionDetection              { $(class:"configLabel", text:"Partition Detection") }
        security                        { $(class:"labelCollapsed", text:"Security") }
        httpAccess                      { $(class:"labelCollapsed", text:"HTTP Access") }
        autoSnapshots                   { $(class:"labelCollapsed", text:"Auto Snapshots") }
        commandLogging                  { $(class:"labelCollapsed", text:"Command Logging") }
        export                          { $(class:"labelCollapsed", text:"Export") }
        advanced                        { $(class:"labelCollapsed", text:"Advanced") }

        sitePerHostValue                { $("#sitePerHost") }
        ksafetyValue                    { $("#kSafety") }
        partitionDetectionValue         { $("#partitionDetectionLabel") }
        securityValue                   { $("#spanSecurity") }
        httpAccessValue                 { $("#httpAccessLabel") }
        autoSnapshotsValue              { $("#txtAutoSnapshot") }
        commandLoggingValue             { $("#commandLogLabel") }

        fileprefixEdit                  {$("#txtPrefix", name:"txtPrefix")}
        frequencyEdit                   {$("#txtFrequency", name:"txtFrequency")}
        retainedEdit                    {$("#txtRetained", name:"txtRetained")}

        securityEdit                    { $("#securityEdit") }
        securityEditCheckbox            { $(class:"iCheck-helper") }
        securityEditOk                  { $("#btnEditSecurityOk") }
        securityEditCancel              { $("#btnEditSecurityCancel") }

        autoSnapshotsEdit               { $("#autoSnapshotEdit") }
        autoSnapshotsEditCheckbox1      { $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-2.hasSubLabel.parent td#autoSnapshotOption.snapshottd div.icheckbox_square-aero.customCheckbox ins.iCheck-helper") }
        autoSnapshotsEditCheckbox       { $("#autoSnapshotOption > div.icheckbox_square-aero.customCheckbox > ins") }

        autoSnapshotsEditOk             { $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-2.hasSubLabel.parent td a#btnEditAutoSnapshotOk.editOk") }
        autoSnapshotsEditCancel         { $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-2.hasSubLabel.parent td a#btnEditAutoSnapshotCancel.editCancel") }
        autosnapshotsconfirmok          { $("#btnSaveSnapshot", text:"Ok")}


        filePrefixField                 { $(id:"txtPrefix") }
        frequencyField                  { $(id:"txtFrequency") }
        frequencyUnitField              { $(id:"ddlfrequencyUnit") }
        retainedField                   { $(id:"txtRetained") }

        filePrefix                      { $(id:"prefixSpan") }
        frequency                       { $(id:"frequencySpan") }
        frequencyUnit                   { $(id:"spanFrequencyUnit") }
        retained                        { $(id:"retainedSpan") }

        hour                            { $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#snapshotFrequencyRow.child-row-2.subLabelRow td.tdSnapshotFrequency form#frmSnapshotFrequency select#ddlfrequencyUnit.valid option") }
        minute                          { $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#snapshotFrequencyRow.child-row-2.subLabelRow td.tdSnapshotFrequency form#frmSnapshotFrequency select#ddlfrequencyUnit.valid option") }
        second                          { $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#snapshotFrequencyRow.child-row-2.subLabelRow td.tdSnapshotFrequency form#frmSnapshotFrequency select#ddlfrequencyUnit.valid option") }

        // Admin Export

        exportbtn                       { $("#row-4 > td.configLabel > a", text:"Export")}
        noconfigtxt                     { $("#exportConfiguration > tr > td.configLabel")}
        //addconfig                     { $("#addNewConfigLink")}
        addconfigpopup                  { $("#addConfigInnerPopup")}
        addconfigtxt                    { $("#addConfigHeader")}
        inputstream                     { $("#txtStream")}
        inputtype                       { $("#txtType")}
        inputnamefrst                   { $("#txtName0")}
        inputvaluefrst                  { $("#txtValue0")}
        inputnamescnd                   { $("#txtName1")}
        inputvaluescnd                  { $("#txtValue1")}
        addproperty                     { $("#lnkAddNewProperty")}
        deleteproperty                  { $("#delRow0")}
        deletescndproperty              { $("#deleteFirstProperty")}
        saveconfig                      { $("#btnAddConfigSave", text:"Save")}
        cancelconfig                    { $("#btnAddConfigCancel", text:"Cancel")}
        streamchkbox                    { $("#Tr1 > td:nth-child(3) > div > ins")}
        reqfielderror                   { $(class:"error", text:"This field is required")}
        streamtxt                       { $("#Tr1 > td:nth-child(1)", text:"Stream")}
        typetxt                         { $("#addConfigWrapper > table:nth-child(1) > tbody > tr:nth-child(2) > td:nth-child(1)", text:"Type")}
        propertiestxt                   { $("#addConfigWrapper > table:nth-child(2) > tbody > tr:nth-child(1) > td > div > div.proLeft", text:"Properties")}
        nametxt                         { $("#tblAddNewProperty > tbody > tr:nth-child(1) > th:nth-child(1)", text:"Name")}
        valuetxt                        { $("#tblAddNewProperty > tbody > tr:nth-child(1) > th:nth-child(2)", text:"Value")}
        deletetxt                       { $("#tblAddNewProperty > tbody > tr:nth-child(1) > th:nth-child(3)", text:"Delete")}
        confirmpopupask                 { $("#saveConfigConfirmation > div.overlay-content > div")}
        confirmnobtn                    { $("#btnSaveConfigCancel", text:"No")}
        confirmyesbtn                   { $("#btnSaveConfigOk")}
        belowexportbtn                  { $("#row-40 > td.configLabel.expoStream > a")}
        belowexportnametxt              { $("#exportConfiguration > tr:nth-child(2) > td.configLabe2")}
        belowexportvaluetxt             { $("#exportConfiguration > tr:nth-child(2) > td:nth-child(2)")}
        belowexportnamescndtxt          { $("#exportConfiguration > tr:nth-child(3) > td.configLabe2")}
        belowexportvaluescndtxt         { $("#exportConfiguration > tr:nth-child(3) > td:nth-child(2)")}
        clickdbmonitorerrormsg          { $("#btnUpdateErrorOk", text:"Ok")}
        dbmonitorerrormsgpopup          { $("#updateInnerErrorPopup")}
        onstatetxt                      { $("#row-40 > td:nth-child(3)", text:"On")}
        offstatetxt                     { $("#row-40 > td:nth-child(3)", text:"Off")}
        exportsametxterr                { $("#btnUpdateErrorOk", text:"Ok")}
        updateerrormsgexport            { $("#updateInnerErrorPopup > div.overlay-contentError.errorMsg > p")}
        exporteditbtn                   { $("#exportEdit0")}
        addconfigcheckonbox             { $("input[type='checkbox']",class:"chkStream")}
        checkboxtest                    { $("div.icheckbox_square-aero:nth-child(1) > ins:nth-child(2)")}
        //addconfigcheckoffbox          { $(id:"chkStream", class:"chkStream")}
        checkboxofftxt                  { $("#chkStreamValue", text:"Off")}
        checkboxontxt                   { $("#chkStreamValue", text:"On")}
        deleteconfigurations            { $("#deleteAddConfig > a", text:"Delete this Configuration")}
        deleteconfirmation              { $("#saveConfigConfirmation > div.overlay-content > div")}
        deleteYes                       { $("#btnSaveConfigOk")}
        deleteNo                        { $("#btnSaveConfigCancel", text:"No")}
        samestreamnameerrorpopup        { $("#updateInnerErrorPopup")}
        samestreamnameerrorOk           { $("#btnUpdateErrorOk", text:"Ok")}

        //Admin Import
        importbtn                       { $("#row-5 > td.configLabel > a", text:"Import")}
        noimportconfigtxt               { $("#importConfiguration > tr > td.configLabel")}


        // SECURITY POPUP
        securityPopup                   { $(class:"popup_content14") }
        securityPopupOk                 { $("#btnSecurityOk") }
        securityPopupCancel             { $("#btnPopupSecurityCancel") }

        // AUTO SNAPSHOTS EDIT POPUP

        autoSnapshotsPopup              { $("html body div.popup_cont div.popup div.popup_content") }
        autoSnapshotsPopupTitle         { $("html body div.popup_cont div.popup div.popup_content div.overlay-title.icon-alert") }
        autoSnapshotsPopupDisplay       { $("html body div.popup_cont div.popup div.popup_content div.overlay-content.confirmationHeight p.txt-bold") }
        autoSnapshotsPopupClose         { $("html body div.popup_cont div.popup_close") }
        autoSnapshotsPopupOk            { $(id:"btnSaveSnapshot", text:"Ok") }
        autoSnapshotsPopupCancel        { $("html body div.popup_cont div.popup div.popup_content div.overlay-btns a#btnPopupAutoSnapshotCancel.btn.btn-gray") }

        //Database Replication Section
        divDrWrapper                    { $("#divDrWrapperAdmin")}
        DrTitle                         { $(class:"drAdminHeaderLeft",text:"Database Replication (DR)") }
        drMode                          { $("#drMode") }
        drId                            { $(class:"configLabel", text:"ID") }
        drIdValue                       { $("#drId") }
        master                          { $(class:"configLabel", text:"Master") }
        masterValue                     { $("#txtDrMaster")}
        replicSource                    { $("#replicaSource")}
        replica                         { $(class:"configLabel", text:contains("Replica")) }
        replicaSourceValue              { $("#txtDrReplica") }
        drTables                        { $(class:"configLabel", text:"DR Tables") }
        btnListDrTables                 { $("#lstDrTbl")}
        drTableListOk                   { $(id:"A2", text:"Ok") }
        drTableList                     { $("#drTableBody") }
        drMasterEdit                    { $("#drMasterEdit") }
        btnEditDrMasterOk               { $("#btnEditDrMasterOk") }
        btnEditDrMasterCancel           { $("#btnEditDrMasterCancel") }
        btnSaveDrMaster                 { $("#btnSaveDrMaster") }
        btnPopupDrMasterCancel          { $("#btnPopupDrMasterCancel") }
        drTablePopup                    { $(class:"overlay-title icon-alert", text:"DR Tables") }
        chkDrMaster                     { $("#row-DrConfig > td.tdDrConfig > div.icheckbox_square-aero.checked.customCheckbox > ins") }

        diskLimitEdit                   { $("#btnEditDiskLimit") }
        diskLimitExpanded               { $("#diskLimitConfiguration").find(".configLabel").first() }
        snapShotName                    { $("#diskLimitConfiguration").find(".configLabe2").first() }
        noFeaturestxt                   { $("#diskLimitConfiguration > tr.childprop-row-60 > td.configLabel") }
        updateInnerErrorPopup           { $("#updateInnerErrorPopup") }

        /* Snmp */
        snmpTitle                       { $("#row-7 > td.configLabel") }
        snmpEditButton                  { $("#snmpEdit") }

        snmpTarget                      { $("tr.child-row-7:nth-child(2) > td:nth-child(1)") }
        snmpCommunity                   { $("tr.child-row-7:nth-child(3) > td:nth-child(1)") }
        snmpUsername                    { $("tr.child-row-7:nth-child(4) > td:nth-child(1)") }
        snmpAuthenticationProtocol      { $("tr.child-row-7:nth-child(5) > td:nth-child(1)") }
        snmpAuthenticationKey           { $("tr.child-row-7:nth-child(6) > td:nth-child(1)") }
        snmpPrivacyProtocol             { $("tr.child-row-7:nth-child(7) > td:nth-child(1)") }
        snmpPrivacyKey                  { $("tr.child-row-7:nth-child(8) > td:nth-child(1)") }

        snmpEnabled                     { $("#txtSnmp")}
        editSnmpButton                  { $("#snmpEdit")}
        editSnmpOkButton                { $("#btnEditSnmpOk")}
        errorTarget                     {$("#errorTarget")}
        txtCommunity                    {$("#txtCommunity")}
        txtAuthkey                      {$("#txtAuthkey")}
        txtPrivkey                      {$("#txtPrivKey")}
        txtTarget                       {$("#txtTarget")}

        chkSNMPDiv                      {$("#snmpOption > div.icheckbox_square-aero.customCheckbox > ins")}
        errorAuthkey                    {$("#errorAuthkey")}
        ddlAuthProtocol                 {$("#ddlAuthProtocol")}
        txtUsername                     {$("#txtUsername")}
        btnSaveSnmp                     {$("#btnSaveSnmp")}
        loadingSnmp                     {$("#loadingSnmp")}
        targetSpan                      {$("#targetSpan")}

    }
    static at = {
        adminTab.displayed
        adminTab.attr('class') == 'active'
        cluster.displayed
        overview.displayed
        networkInterfaces.displayed
        directories.displayed
    }


    /**
     * Returns true if the "Overview"
     * currently exists (displayed).
     * @return true if the "Overview" area currently exists.
     */
    def boolean doesOverviewExist() {
        return overview.displayed
    }

    /**
     * Returns true if the "Overview"
     * currently exists (displayed).
     * @return true if the "Network Interfaces" area currently exists.
     */
    def boolean doesNetworkInterfacesExist() {
        return networkInterfaces.displayed
    }

    /**
     * Returns true if the "Directories"
     * currently exists (displayed).
     * @return true if the "Directories" area currently exists.
     */
    def boolean doesDirectoriesExist() {
        return directories.displayed
    }

    def String getValidPath() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/snapshotpath.txt"))
        String validPath

        while((validPath = br.readLine()) != "#valid_path") {
        }

        validPath = br.readLine()

        return validPath
    }

    def String getInvalidPath() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/snapshotpath.txt"))
        String invalidPath

        while((invalidPath = br.readLine()) != "#invalid_path") {
        }

        invalidPath = br.readLine()

        return invalidPath
    }

    def String getEmptyPath() {
        return ""
    }

    def String getUsername() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/users.txt"))
        String username

        while((username = br.readLine()) != "#username") {
        }

        username = br.readLine()

        return username
    }

    //for export

    def String getStream() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportpath.txt"))
        String stream

        while((stream = br.readLine()) != "#stream") {
        }

        stream = br.readLine()

        return stream
    }

    def String getType() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportpath.txt"))
        String type

        while((type = br.readLine()) != "#type") {
        }

        type = br.readLine()

        return type
    }


    def String getExportName() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportpath.txt"))
        String name

        while((name = br.readLine()) != "#name") {
        }

        name = br.readLine()

        return name
    }

    def String getValue() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportpath.txt"))
        String value

        while((value = br.readLine()) != "#value") {
        }

        value = br.readLine()

        return value
    }


    def String getExportNamescnd() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportpath.txt"))
        String name

        while((name = br.readLine()) != "#name_2") {
        }

        name = br.readLine()

        return name
    }

    def String getExportValuescnd() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportpath.txt"))
        String value

        while((value = br.readLine()) != "#value_2") {
        }

        value = br.readLine()

        return value
    }


    def String getStreamNxt() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/exportpath.txt"))
        String streamNxt

        while((streamNxt = br.readLine()) != "#streamNext") {
        }

        streamNxt = br.readLine()

        return streamNxt
    }

    def boolean CheckIfDREnabled(){
        boolean result = false
        try{
            waitFor(30){divDrWrapper.isDisplayed() }
            println("DR section is displayed.")
            result = true
        } catch(geb.waiting.WaitTimeoutException e){
            println("DR section is not displayed")
        }
        return result;
    }

}
