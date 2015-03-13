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
        title				{ $("h1", text:"Overview") }
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


        securityEdit				{ $("#securityEdit") }
        securityEditCheckbox		{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-6.hasSubLabel.parent.security td.securitytd div.icheckbox_square-aero.checked.customCheckbox ins.iCheck-helper") }
        securityEditOk				{ $("#btnEditSecurityOk") }
        securityEditCancel			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-6.hasSubLabel.parent.security td a#btnEditSecurityCancel.editCancel") }


        autoSnapshotsEdit			{ $("#autoSnapshotEdit") }
        autoSnapshotsEditCheckbox1 	{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-2.hasSubLabel.parent td#autoSnapshotOption.snapshottd div.icheckbox_square-aero.customCheckbox ins.iCheck-helper") }
        autoSnapshotsEditCheckbox 	{ $(class:"icheckbox_square-aero customCheckbox") }

        autoSnapshotsEditOk 		{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-2.hasSubLabel.parent td a#btnEditAutoSnapshotOk.editOk") }
        autoSnapshotsEditCancel 	{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#row-2.hasSubLabel.parent td a#btnEditAutoSnapshotCancel.editCancel") }


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

        autoSnapshotsPopup			{ $("html body div.popup_cont div.popup div.popup_content") }
        autoSnapshotsPopupTitle		{ $("html body div.popup_cont div.popup div.popup_content div.overlay-title.icon-alert") }
        autoSnapshotsPopupDisplay	{ $("html body div.popup_cont div.popup div.popup_content div.overlay-content.confirmationHeight p.txt-bold") }
        autoSnapshotsPopupClose		{ $("html body div.popup_cont div.popup_close") }
        autoSnapshotsPopupOk		{ $(id:"btnSaveSnapshot", text:"Ok") }
        autoSnapshotsPopupCancel	{ $("html body div.popup_cont div.popup div.popup_content div.overlay-btns a#btnPopupAutoSnapshotCancel.btn.btn-gray") }

        // HTTP ACCESS EXPANSION

        jsonApi 					{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-1.subLabelRow td.configLabel") }
        jsonApiValue 				{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-1.subLabelRow td#txtJsonAPI") }

        // AUTO SNAPSHOTS EXPANSION



        // COMMAND LOGGING EXPANSION

        lftime 						{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-3.subLabelRow td.configLabel") }
        lftransactions 				{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-3.subLabelRow td.configLabel") }
        lssize						{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-3.subLabelRow td.configLabel") }

        lftimeValue 				{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-3.subLabelRow td span#commandLogFrequencyTime") }
        lftransactionsValue 		{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-3.subLabelRow td#commandLogFrequencyTxns") }
        lssizeValue					{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-3.subLabelRow td span#commandLogSegmentSize") }



        // EXPORT EXPANSION

        target 						{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-4.subLabelRow td.configLabel") }
        properties 					{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-4.subLabelRow td.configLabel") }
        propertiesSub 				{ $("tbody tr.propertyLast td") }

        // ADVANCED EXPANSION

        maxJavaHeap 				{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-5.subLabelRow td.configLabel") }
        heartbeatTimeOut			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#heartbeatTimeoutRow.child-row-5.subLabelRow td.configLabel") }
        queryTimeOut				{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#queryTimoutRow.child-row-5.subLabelRow td.configLabel") }
        maxTempTableMemory			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-5.subLabelRow td.configLabel") }
        snapshotPriority			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-5.subLabelRow td.configLabel") }

        maxJavaHeapValue			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-5.subLabelRow td") }
        heartbeatTimeOutValue		{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#heartbeatTimeoutRow.child-row-5.subLabelRow td.heartbeattd form#formHeartbeatTimeout span#hrtTimeOutSpan.unit") }
        queryTimeOutValue			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#queryTimoutRow.child-row-5.subLabelRow td.queryTimeOut form#formQueryTimeout") }
        maxTempTableMemoryValue		{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-5.subLabelRow td span#temptablesmaxsize") }
        snapshotPriorityValue		{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr.child-row-5.subLabelRow td#snapshotpriority") }

        heartbeatTimeOutEdit		{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#heartbeatTimeoutRow.child-row-5.subLabelRow td a#btnEditHrtTimeOut.edit") }
        heartbeatTimeOutBox			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#heartbeatTimeoutRow.child-row-5.subLabelRow td.heartbeattd form#formHeartbeatTimeout input#txtHrtTimeOut.exitableTxtBx.valid") }
        heartbeatTimeOutOk			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#heartbeatTimeoutRow.child-row-5.subLabelRow td a#btnEditHeartbeatTimeoutOk.editOk") }
        heartbeatTimeOutCancel		{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#heartbeatTimeoutRow.child-row-5.subLabelRow td a#btnEditHeartbeatTimeoutCancel.editCancel") }

        queryTimeOutEdit			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#queryTimoutRow.child-row-5.subLabelRow td a#btnEditQueryTimeout.edit") }
        queryTimeOutBox				{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#queryTimoutRow.child-row-5.subLabelRow td.queryTimeOut form#formQueryTimeout input#txtQueryTimeout.exitableTxtBx.valid") }
        queryTimeOutOk				{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#queryTimoutRow.child-row-5.subLabelRow td a#btnEditQueryTimeoutOk.editOk") }
        queryTimeOutCancel			{ $("html body div.page-wrap div#wrapper div#admin.container.contents div.adminContainer div.adminContentLeft div.overviewTbl table.adminTbl1 tbody tr#queryTimoutRow.child-row-5.subLabelRow td a#btnEditQueryTimeoutCancel.editCancel") }
    }
}

