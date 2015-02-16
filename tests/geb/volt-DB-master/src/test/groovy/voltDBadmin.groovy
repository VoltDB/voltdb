import geb.Page

/**
 * Created by lavthaiba on 2/4/2015.
 */
class voltDBadmin extends Page {

    static at = { waitFor { showadmin.isDisplayed() }


    }

    static content = {
        showadmin {
            $("a", text: "Admin")
        }


        // cluster

        clusterTitle{
            $("#admin > div.adminWrapper > div.adminLeft > h1")
        }

        pausebutton{
            $("#pauseConfirmation")
        }
        resumebutton{
            $("#resumeConfirmation")
        }
        resumeok{
            $("#btnResumeConfirmationOk")
        }
        pauseok{
            $("#btnPauseConfirmationOk")
        }
        pausecancel{
            $("#btnPauseConfirmationCancel")
        }

        resumecancel{
            $("#btnResumeConfirmationCancel")
        }
        resumeconfirmation{
                $("body > div.popup_cont > div.popup > div > div.overlay-title.icon-alert")
            }
        pauseconfirmation{
            $("body > div.popup_cont > div.popup > div > div.overlay-title.icon-alert")
        }


        savebutton{
            $("#saveConfirmation")
        }

        saveconfirmation{
            $("body > div.popup_cont > div.popup > div > div.overlay-title.icon-alert")
        }

        saveclose{
            $("body > div.popup_cont > div.popup_close")
        }
        savecancel{
            $("#btnSaveSnapshotCancel")
        }
        saveok{
            $("#btnSaveSnapshots")
        }

        savedirectory{
           // $("#txtSnapshotDirectory")
            $("input",type:"text",name:"txtSnapshotDirectory",id:"txtSnapshotDirectory")
        }

        wanttosavecloseconfirm{
            $("body > div.popup_cont > div.popup > div > div.overlay-title.icon-alert")
        }
        wantotsaveclosebutton{
            $("body > div.popup_cont > div.popup_close")
        }

        restorebutton{
            $("#restoreConfirmation")
        }
        restoreconfirmation{
            $("#restoredPopup > div.overlay-title.icon-alert")
        }

        restorecancelbutton{
            $("#btnRestoreCancel")
        }

        restoreclosebutton{
            $("body > div.popup_cont > div.popup_close")
        }
        shutdownbutton{
            $("#shutDownConfirmation")
        }
        shutdownconfirmation{
            $("body > div.popup_cont > div.popup > div > div.overlay-title.icon-alert")
        }
        shutdowncancelbutton{
            $("#btnShutdownConfirmationCancel")
        }
        shutdownclosebutton{
            $("body > div.popup_cont > div.popup_close")
        }
        downloadconfigurationbutton{
            $("#downloadAdminConfigurations")
        }

        serverbutton{
            $("#serverName")
        }
        serverconfirmation{
            $("#serverConfigAdmin > div > div.slide-pop-title > div.icon-server.searchLeft.searchLeftAdmin")
        }
        //Network Interfaces

        networkinterfacesTitle {
            $("#admin > div.adminContainer > div.adminContentRight > div.adminPorts > div.headerAdminContent > h1")
        }

        portnameTitle{
            $("#admin > div.adminContainer > div.adminContentRight > div.adminPorts > div:nth-child(2) > table > tbody > tr:nth-child(1) > th:nth-child(1)")
        }

        clientportTitle{
            $("#admin > div.adminContainer > div.adminContentRight > div.adminPorts > div:nth-child(2) > table > tbody > tr:nth-child(2) > td:nth-child(1)")

        }

        adminportTitle{
            $("#admin > div.adminContainer > div.adminContentRight > div.adminPorts > div:nth-child(2) > table > tbody > tr:nth-child(3) > td:nth-child(1)")
        }

        httpportTitle{
            $("#admin > div.adminContainer > div.adminContentRight > div.adminPorts > div:nth-child(2) > table > tbody > tr:nth-child(4) > td:nth-child(1)")
        }

        internalportTitle{
            $("#admin > div.adminContainer > div.adminContentRight > div.adminPorts > div:nth-child(2) > table > tbody > tr:nth-child(5) > td:nth-child(1)")
        }

        zookeeperportTitle{
            $("#admin > div.adminContainer > div.adminContentRight > div.adminPorts > div:nth-child(2) > table > tbody > tr:nth-child(6) > td:nth-child(1)")
        }

        replicationportTitle{
            $("#admin > div.adminContainer > div.adminContentRight > div.adminPorts > div:nth-child(2) > table > tbody > tr:nth-child(7) > td:nth-child(1)")
        }

        clustersettingTitle{
            $("#admin > div.adminContainer > div.adminContentRight > div.adminPorts > div:nth-child(2) > table > tbody > tr:nth-child(1) > th:nth-child(2)")
        }

        clientportTitlevalue{
            $("#clientport")
        }
        adminportTitlevalue{
            $("#adminport")
        }
        httpportTitlevalue{
            $("#httpport")
        }
        internalportTitlevalue{
            $("#internalPort")
        }

        zookeeperportTitlevalue{
            $("#zookeeperPort")
        }

        replicationportTitlevalue{
            $("#replicationPort")
        }

        serversettingTitle{
            $("#admin > div.adminContainer > div.adminContentRight > div.adminPorts > div:nth-child(2) > table > tbody > tr:nth-child(1) > th:nth-child(3)")
        }

        // DIRECTORIES

        directoriesTitle {
            $("#admin > div.adminContainer > div.adminContentRight > div.adminDirectories > div.headerAdminContent > h1")
        }

        directoriesRootTitle {
            $("#admin > div.adminContainer > div.adminContentRight > div.adminDirectories > div.adminDirect > table > tbody > tr:nth-child(1) > td:nth-child(1)")
        }
        directoriesSnapshotTitle {
            $("#admin > div.adminContainer > div.adminContentRight > div.adminDirectories > div.adminDirect > table > tbody > tr:nth-child(2) > td:nth-child(1)")
        }
        directoriesExportOverflowTitle {
            $("#admin > div.adminContainer > div.adminContentRight > div.adminDirectories > div.adminDirect > table > tbody > tr:nth-child(3) > td:nth-child(1)")
        }
        directoriesCommandLogsTitle {
            $("#admin > div.adminContainer > div.adminContentRight > div.adminDirectories > div.adminDirect > table > tbody > tr:nth-child(4) > td:nth-child(1)")
        }
        directoriesCommandLogSnapshotTitle {
            $("#admin > div.adminContainer > div.adminContentRight > div.adminDirectories > div.adminDirect > table > tbody > tr:nth-child(5) > td:nth-child(1)")
        }

        directoriesRootValue {
            $("#voltdbroot")
        }
        directoriesSnapshotValue {
            $("#snapshotpath")
        }
        directoriesExportOverflowValue {
            $("#exportOverflow")
        }
        directoriesCommandLogsValue {
            $("#commandlogpath")
        }
        directoriesCommandLogSnapshotValue {
            $("#commandlogsnapshotpath")
        }

        // OVERVIEW

        overviewTitle {
            $("#admin > div.adminContainer > div.adminContentLeft > div.headerAdminContent > h1")
        }

        overviewSitePerHostTitle {
            $("#admin > div.adminContainer > div.adminContentLeft > div.overviewTbl > table > tbody > tr:nth-child(1) > td.configLabel")
        }
        overviewKSafetyTitle {
            $("#admin > div.adminContainer > div.adminContentLeft > div.overviewTbl > table > tbody > tr:nth-child(2) > td.configLabel")
        }
        overviewPartitionDetectionTitle {
            $("#admin > div.adminContainer > div.adminContentLeft > div.overviewTbl > table > tbody > tr:nth-child(3) > td.configLabel")
        }
        overviewSecurityTitle {
            $("#row-6 > td.configLabel > a")
        }
        overviewHTTPAccessTitle {
            $("#row-1 > td.configLabel > a")
        }
        overviewAutoSnapshotsTitle {
            $("#row-2 > td.configLabel > a")
        }
        overviewCommandLoggingTitle {
            $("#row-3 > td.configLabel > a")
        }
        overviewExportTitle {
            $("#row-4 > td.configLabel > a")
        }
        overviewAdvancedTitle {
            $("#row-5 > td.configLabel > a")
        }
        overviewSitePerHostValue {
            $("#sitePerHost")
        }
        overviewKSafetyValue {
            $("#kSafety")
        }
        overviewPartitionDetectionValue {
            $("#partitionDetectionLabel")
        }
        overviewSecurityValue {
            $("#spanSecurity")
        }
        overviewHTTPAccessValue {
            $("#httpAccessLabel")
        }
        overviewAutoSnapshotsValue {
            $("#txtAutoSnapshot")
        }
        overviewCommandLoggingValue {
            $("#commandLogLabel")
        }
        overviewExportValue {
            $("#txtExportLabel")
        }

        // Security Edit

        overviewSecurityEditButton {
            $("#securityEdit")
        }
        overviewSecurityEditCheckbox {
            $("#row-6 > td.securitytd > div.icheckbox_square-aero.checked.customCheckbox > ins")
        }
        overviewSecurityEditOk {
            $("#btnEditSecurityOk")
        }
        overviewSecurityEditCancel {
            $("#btnEditSecurityCancel")
        }

        // Auto Snapshots Edit

        overviewAutoSnapshotsEditButton {
            $("#autoSnapshotEdit")
        }
        overviewAutoSnapshotEditCheckbox {
            $("#autoSnapshotOption > div.icheckbox_square-aero.customCheckbox > ins")
        }
        overviewAutoSnapshotEditOk {
            $("#btnEditAutoSnapshotOk")
        }
        overviewAutoSnapshotEditCancel {
            $("#btnEditAutoSnapshotCancel")
        }


        //
        header { module voltDBHeader }
        footer { module voltDBFooter }
    }
}

