package vmcTest.pages

import geb.Module
import java.util.List;
import java.util.Map;

import geb.*
import geb.navigator.Navigator
import geb.waiting.WaitTimeoutException

import org.openqa.selenium.JavascriptExecutor

/**
 * Created by lavthaiba on 2/16/15.
 */

class Cluster extends Module {
    static content = {
        clusterTitle                { $("#admin > div.adminWrapper > div.adminLeft > h1") }
        promotebutton				{ $("#promoteConfirmation") }
        promoteconfirmation			{ $("body > div.popup_cont > div.popup > div > div.overlay-title.icon-alert") }
        promotecancel				{ $("#promoteConfirmCancel") }
        promoteok					{ $("#promoteConfirmOk") }
        title						{ $("#admin > div.adminWrapper > div.adminLeft > h1") }
        pausebutton					{ $("#pauseConfirmation", class:"resume", text:"Pause") }
        resumebutton				{ $("#resumeConfirmation", class:"pause", text:"Resume") }
        resumeok					{ $("#btnResumeConfirmationOk") }
        pauseok						{ $("#btnPauseConfirmationOk") }
        pausecancel					{ $("#btnPauseConfirmationCancel") }
        resumecancel				{ $("#btnResumeConfirmationCancel") }
        resumeconfirmation			{ $("body > div.popup_cont > div.popup > div > div.overlay-title.icon-alert") }
        pauseconfirmation			{ $("body > div.popup_cont.14 div.popup div.popup_content14 div.overlay-title.icon-alert") }
        savebutton					{ $("#saveConfirmation") }
        saveconfirmation			{ $("body > div.popup_cont > div.popup > div > div.overlay-title.icon-alert") }
        saveclose					{ $("body > div.popup_cont > div.popup_close") }
        savecancel					{ $("#btnSaveSnapshotCancel") }
        saveok						{ $("#btnSaveSnapshots") }
        savesuccessok               { $("#btnSaveSnapshotStatus", text:"Ok")}
        saveerrormsg                { $("#errorSnapshotDirectoryPath")}

        emptysearchrestore          {$("#errorSearchSnapshotDirectory")}
        restorefinalclosepopup      {("body > div.popup_cont.\\34 > div.popup_close")}
        buttonrestore               {$("#btnRestore", text:"Restore")}
        restorepopupno              {$("#divConfirm > a.btn.btn-gray.confirmNoRestore", text:"No")}
        restorepopupyes             {$("#btnRestoreSnapshotOk", text:"Yes")}
        restorestatus               {$(class:"restore")}
        restoredirectory			{$("#txtSearchSnapshots")}
        restoreerrormsg             {$("#tblSearchList > tbody > tr:nth-child(2) > td")}
        restoresearch				{$("#btnSearchSnapshots")}
        savedirectory 				{ $("input", type:"text", name:"txtSnapshotDirectory", id:"txtSnapshotDirectory") }
        wanttosavecloseconfirm		{ $("body > div.popup_cont > div.popup > div > div.overlay-title.icon-alert") }
        wantotsaveclosebutton		{ $("body > div.popup_cont > div.popup_close") }
        restorebutton				{ $("#restoreConfirmation", text:"Restore") }
        restoreconfirmation			{ $("#restoredPopup > div.overlay-title.icon-alert", text:"Restore") }
        restorecancelbutton			{ $("#btnRestoreCancel", text:"Cancel") }
        restoreclosebutton			{ $(class:"popup_close") }
        shutdownbutton				{ $("#shutDownConfirmation") }
        shutdownconfirmation		{ $("body > div.popup_cont > div.popup > div > div.overlay-title.icon-alert") }
        shutdowncancelbutton		{ $("#btnShutdownConfirmationCancel") }
        shutdownclosebutton			{ $("body > div.popup_cont > div.popup_close") }
        saveno                      {$("body > div.popup_cont > div.popup > div > div.saveConfirmation > div.overlay-btns > a.btn.btn-gray.confirmNoSave")}
        saveyes                     {$("body > div.popup_cont > div.popup > div > div.saveConfirmation > div.overlay-btns > a.btn.closeBtn")}
        failedsaveok                { $("#btnSaveSnapshotStatus", text:"Ok")}
        downloadconfigurationbutton	{ $("#downloadAdminConfigurations") }

    }

}
