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
 * Created by lavthaiba on 2/16/15.
 */

class Cluster extends Module {
    static content = {
        clusterTitle                { $("#admin > div.adminWrapper > div.adminLeft > h1") }
        promotebutton               { $("#promoteConfirmation") }
        promoteconfirmation         { $("body > div.popup_cont > div.popup > div > div.overlay-title.icon-alert") }
        promotecancel               { $("#promoteConfirmCancel") }
        promoteok                   { $("#promoteConfirmOk") }
        title                       { $("#admin > div.adminWrapper > div.adminLeft > h1") }
        pausebutton                 { $("#pauseConfirmation") }
        resumebutton                { $("#resumeConfirmation") }
        resumeok                    { $("#btnResumeConfirmationOk") }
        pauseok                     { $("#btnPauseConfirmationOk") }
        pausecancel                 { $("#btnPauseConfirmationCancel") }
        resumecancel                { $("#btnResumeConfirmationCancel") }
        resumeconfirmation          { $("body > div.popup_cont > div.popup > div > div.overlay-title.icon-alert") }
        pauseconfirmation           { $("body > div.popup_cont.14 div.popup div.popup_content14 div.overlay-title.icon-alert") }
        savebutton                  { $("#saveConfirmation") }
        saveconfirmation            { $("body > div.popup_cont > div.popup > div > div.overlay-title.icon-alert") }
        saveclose                   { $("body > div.popup_cont > div.popup_close") }
        savecancel                  { $("#btnSaveSnapshotCancel") }
        saveok                      { $("#btnSaveSnapshots") }
        savesuccessok               { $("#btnSaveSnapshotStatus", text:"Ok")}
        saveerrormsg                { $("#errorSnapshotDirectoryPath")}

        emptysearchrestore          {$("#errorSearchSnapshotDirectory")}
        restorefinalclosepopup      {("body > div.popup_cont.\\34 > div.popup_close")}
        buttonrestore               {$("#btnRestore", text:"Restore")}
        restorepopupno              {$("#divConfirm > a.btn.btn-gray.confirmNoRestore", text:"No")}
        restorepopupyes             {$("#btnRestoreSnapshotOk", text:"Yes")}
        restorestatus               {$(class:"restore")}
        restoredirectory            {$("#txtSearchSnapshots")}
        restoreerrormsg             {$("#tblSearchList > tbody > tr:nth-child(2) > td")}
        restoresearch               {$("#btnSearchSnapshots")}
        savedirectory               { $("input", type:"text", name:"txtSnapshotDirectory", id:"txtSnapshotDirectory") }
        wanttosavecloseconfirm      { $("body > div.popup_cont > div.popup > div > div.overlay-title.icon-alert") }
        wantotsaveclosebutton       { $("body > div.popup_cont > div.popup_close") }
        restorebutton               { $("#restoreConfirmation", text:"Restore") }
        restoreconfirmation         { $("#restoredPopup > div.overlay-title.icon-alert", text:"Restore") }
        restorecancelbutton         { $("#btnRestoreCancel", text:"Cancel") }
        restoreclosebutton          { $(class:"popup_close") }
        shutdownbutton              { $("#shutDownConfirmation") }
        shutdownconfirmation        { $("body > div.popup_cont > div.popup > div > div.overlay-title.icon-alert") }
        shutdowncancelbutton        { $("#btnShutdownConfirmationCancel") }
        shutdownclosebutton         { $("body > div.popup_cont > div.popup_close") }
        saveno                      {$("body > div.popup_cont > div.popup > div > div.saveConfirmation > div.overlay-btns > a.btn.btn-gray.confirmNoSave")}
        saveyes                     {$("body > div.popup_cont > div.popup > div > div.saveConfirmation > div.overlay-btns > a.btn.closeBtn")}
        failedsaveok                { $("#btnSaveSnapshotStatus", text:"Ok")}
        downloadconfigurationbutton { $("#downloadAdminConfigurations") }
    }
}
