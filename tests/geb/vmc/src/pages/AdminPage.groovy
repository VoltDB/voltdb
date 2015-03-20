/**
 * Created by anrai on 2/12/15.
 */

package vmcTest.pages

import geb.navigator.Navigator

class AdminPage extends VoltDBManagementCenterPage {
    static content = {
        cluster             { $('#admin > div.adminWrapper') }
        overview            { $('#admin > div.adminContainer > div.adminContentLeft') }
        networkInterfaces   { $('#admin > div.adminContainer > div.adminContentRight > div.adminPorts') }
        directories         { $('#admin > div.adminContainer > div.adminContentRight > div.adminDirectories') }
        networkInterfaces   { module NetworkInterfaces }
        directories		 	{ module Directories }
        overview			{ module Overview }
        cluster				{ module Cluster }
        header              { module Header }
        footer              { module Footer }
        server              { module voltDBclusterserver}
        downloadbtn         { module downloadconfigbtn}
        schema              { module schemaTab}

        serverbutton				    { $("#serverName") }
        serverconfirmation			    { $("#serverConfigAdmin > div > div.slide-pop-title > div.icon-server.searchLeft.searchLeftAdmin") }
        deeerwalkservercancelbutton 	{ $("#StopConfirmCancel") }
        deerwalkserverstopok        	{ $("#StopConfirmOK") }
        servernamelist			{$("#serverListWrapperAdmin > table > tbody > tr > td.configLabel > a")}
        servername			{$("#serverListWrapperAdmin > table > tbody > tr > td.configLabel > a")}
        mainservername			{$("#serverListWrapperAdmin > table > tbody")}
        serverstopbuttonmain		{$("#stopServer_voltdbserver", text:"Stop")}

        serverresumebuttonmain		{$("#stopServer_voltdbserver", text:"Resume")}
        serverstopbtndisable   		{$(class:"disableServer", text:"Stop") }
        serverstopbtnenable			{$(class:"shutdown", text:"Stop")}



        //servernamelist			    {$("#serverListWrapperAdmin > table > tbody > tr > td.configLabel > a")}
        servername			        {$("#serverListWrapperAdmin > table > tbody > tr > td.configLabel > a")}
        serverstopbuttonmain		{$("#stopServer_voltdbserver", text:"Stop")}

        servernamelist1			{$("#serverListWrapperAdmin > table > tbody > tr.activeHostMonitoring > td.configLabel > a")}
        servernamelist2			{$("#serverListWrapperAdmin > table > tbody > tr:nth-child(2) > td.configLabel > a")}
        servernamelist3			{$("#serverListWrapperAdmin > table > tbody > tr:nth-child(3) > td.configLabel > a")}

        serverstopbtn1   		{$("#stopServer_deerwalk3", class:"disableServer", text:"Stop") }
        serverstopbtn2			{$("#stopServer_deerwalk2", text:"Stop")}
        serverstopbtn3			{$("#stopServer_deerwalk4", text:"Stop")}
        serverstopcancel		{$("#StopConfirmCancel",    text:"Cancel")}
        serverstopok			{$("#StopConfirmOK",        text:"Ok")}
        serverstoppopup  		{$("body > div.popup_content34", class:"popup")}

        downloadconfigurationbutton	{ $("#downloadAdminConfigurations") }
        //DBmonitor part for server
        dbmonitorbutton{$("#navDbmonitor > a")}
        clusterserverbutton{$("#btnPopServerList")}
        servernamefourthbtn{$("#serversList > li:nth-child(1) > a")}
        servernamesecondbtn{$("#serversList > li:nth-child(2) > a")}
        servernamethirdbtn{$("#serversList > li:nth-child(3) > a")}
        serveractivechk    {$("#serversList > li.active.monitoring > a")}
        serversearch{$("input", type: "text", id: "popServerSearch")}
        checkserverTitle{$("#popServerList > div > div.slide-pop-title > div.icon-server.searchLeft")}
        setthreshhold{$("#threshold")}
        clickthreshholdset{$("#saveThreshold")}

        // dbmonitor graph
        servercpudaysmin{$("#visualisationCpu > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text")}
        servercpudaysmax{$("#visualisationCpu > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text")}
        servercpuminutesmin{$("#visualisationCpu > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text")}
        servercpuminutemax{$("#visualisationCpu > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text")}
        servercpusecondmin{$("#visualisationCpu > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text")}
        servercpusecondmax{$("#visualisationCpu > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text")}

        selecttypeindrop{$("#graphView")}
        selecttypedays{$("#graphView > option:nth-child(3)")}
        selecttypemin{$("#graphView > option:nth-child(2)")}
        selecttypesec{$("#graphView > option:nth-child(1)")}

        serverramdaysmin{$("#visualisationRam > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text")}
        serverramdaysmax{$("#visualisationRam > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text")}
        serverramsecondmin{$("#visualisationRam > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text")}
        serverramsecondmax{$("#visualisationRam > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text")}
        serverramminutesmin{$("#visualisationRam > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text")}
        serverramminutesmax{$("#visualisationRam > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text")}

        clusterlatencydaysmin{$("#visualisationLatency > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text")}
        clusterlatencydaysmax{$("#visualisationLatency > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text")}
        clusterlatencysecondmin{$("#visualisationLatency > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text")}
        clusterlatencysecondmax{$("#visualisationLatency > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text")}
        clusterlatencyminutesmin{$("#visualisationLatency > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text")}
        clusterlatencyminutesmax{$("#visualisationLatency > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text")}

        clustertransactiondaysmin{$("#visualisationTransaction > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text")}
        clustertransactiondaysmax{$("#visualisationTransaction > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text")}
        clustertransactionsecondmin{$("#visualisationTransaction > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text")}
        clustertransactionsecondmax{$("#visualisationTransaction > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text")}
        clustertransactionminutesmin{$("#visualisationTransaction > g > g > g.nv-x.nv-axis > g > g:nth-child(2) > text")}
        clustertransactionminutesmax{$("#visualisationTransaction > g > g > g.nv-x.nv-axis > g > g:nth-child(3) > text")}


        // Network interfaces
        title1				{ $(text:"Overview") }
        sitePerHost			{ $(class:"configLabel", text:"Sites Per Host") }
        ksafety				{ $(class:"configLabel", text:"K-Safety") }
        partitionDetection	{ $(class:"configLabel", text:"Partition Detection") }
        security			{ $(class:"labelCollapsed", text:"Security") }
        httpAccess			{ $(class:"labelCollapsed", text:"HTTP Access") }
        autoSnapshots		{ $(class:"labelCollapsed", text:"Auto Snapshots") }
        commandLogging		{ $(class:"labelCollapsed", text:"Command Logging") }
        export				{ $(class:"labelCollapsed", text:"Export") }
        advanced			{ $(class:"labelCollapsed", text:"Advanced") }


        sitePerHostValue			{ $("#sitePerHost") }
        ksafetyValue				{ $("#kSafety") }
        partitionDetectionValue		{ $("#partitionDetectionLabel") }
        securityValue				{ $("#spanSecurity") }
        httpAccessValue				{ $("#httpAccessLabel") }
        autoSnapshotsValue			{ $("#txtAutoSnapshot") }
        commandLoggingValue			{ $("#commandLogLabel") }

        fileprefixEdit				{$("#txtPrefix", name:"txtPrefix")}
        frequencyEdit				{$("#txtFrequency", name:"txtFrequency")}
        retainedEdit				{$("#txtRetained", name:"txtRetained")}

        securityEdit				{ $("#securityEdit") }
        securityEditCheckbox		{ $(class:"iCheck-helper") }
        securityEditOk				{ $("#btnEditSecurityOk") }
        securityEditCancel			{ $("#btnEditSecurityCancel") }

        autoSnapshotsEdit			{ $("#autoSnapshotEdit") }
        autoSnapshotsEditCheckbox1 	{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-2.hasSubLabel.parent td#autoSnapshotOption.snapshottd div.icheckbox_square-aero.customCheckbox ins.iCheck-helper") }
        autoSnapshotsEditCheckbox 	{ $(class:"icheckbox_square-aero customCheckbox") }

        autoSnapshotsEditOk 		{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-2.hasSubLabel.parent td a#btnEditAutoSnapshotOk.editOk") }
        autoSnapshotsEditCancel 	{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-2.hasSubLabel.parent td a#btnEditAutoSnapshotCancel.editCancel") }
        autosnapshotsconfirmok      {$("#btnSaveSnapshot", text:"Ok")}


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
        securityPopup				{ $(class:"popup_content14") }
        securityPopupOk             { $("#btnSecurityOk") }
        securityPopupCancel         { $("#btnPopupSecurityCancel") }

        // AUTO SNAPSHOTS EDIT POPUP

        autoSnapshotsPopup			{ $("html body div.popup_cont div.popup div.popup_content") }
        autoSnapshotsPopupTitle		{ $("html body div.popup_cont div.popup div.popup_content div.overlay-title.icon-alert") }
        autoSnapshotsPopupDisplay	{ $("html body div.popup_cont div.popup div.popup_content div.overlay-content.confirmationHeight p.txt-bold") }
        autoSnapshotsPopupClose		{ $("html body div.popup_cont div.popup_close") }
        autoSnapshotsPopupOk		{ $(id:"btnSaveSnapshot", text:"Ok") }
        autoSnapshotsPopupCancel	{ $("html body div.popup_cont div.popup div.popup_content div.overlay-btns a#btnPopupAutoSnapshotCancel.btn.btn-gray") }

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


}
