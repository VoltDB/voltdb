package vmcTest.pages

import geb.Module
import java.util.List;
import java.util.Map;

import geb.*
import geb.navigator.Navigator
import geb.waiting.WaitTimeoutException

import org.openqa.selenium.JavascriptExecutor

/**
 * Created by anrai on 2/12/15.
 */

class Header extends Module {
    static content = {
        banner                    { $('#headerMain') }
        image                     { $("#headerMain > div.headLeft > div.logo > img") }
        tabDBMonitor              { $("#navDbmonitor > a") }
        tabAdmin                  { $("#navAdmin > a") }
        tabSchema                 { $("#navSchema > a") }
        tabSQLQuery               { $("#navSqlQuery > a") }
        username                  { $("#btnlogOut > div") }
        logout                    { $("#logOut > div") }
        help                      { $("#showMyHelp") }
        popup                     { $("body > div.popup_cont > div.popup > div") }
        popupTitle                { $("body > div.popup_cont > div.popup > div > div.overlay-title.helpIcon") }
        popupClose                { $("body > div.popup_cont > div.popup_close") }
        logoutPopup               { $("body > div.popup_cont") }
        logoutPopupTitle          { $("body > div.popup_cont > div.popup > div > div.overlay-title") }
        logoutPopupOkButton       { $("#A1") }
        logoutPopupCancelButton   { $("#btnCancel") }
        noheader                        { $("#noheader") }   // This is for testing the test results
    }

}
