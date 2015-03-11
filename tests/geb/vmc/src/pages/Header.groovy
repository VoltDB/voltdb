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
        banner                    { $("#headerMain") }
        image                     { $("html body div.page-wrap div#wrapper div#headerMain.header div.headLeft div.logo img") }
        tabDBMonitor              { $("#navDbmonitor") }
        tabAdmin                  { $("#navAdmin") }
        tabSchema                 { $("#navSchema") }
        tabSQLQuery               { $("#navSqlQuery") }
        username                  { $(class:"userN") }
        logout                    { $("#logOut > div") }
        help                      { $("#showMyHelp") }
        popup                     { $(class:"popup_content10") }
        popupTitle                { $(class:"overlay-title helpIcon ", text:"Help") }
        popupClose                { $(class:"popup_close") }
        logoutPopup               { $(class:"popup_content2") }
        logoutPopupTitle          { $("html body div.popup_cont.2 div.popup div.popup_content2 div.overlay-title img.imgError") }
        logoutPopupOkButton       { $("#A1") }
        logoutPopupCancelButton   { $("#btnCancel") }
    }

}
