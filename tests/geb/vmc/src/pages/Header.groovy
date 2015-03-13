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
        image                     { $(class:"logo") }
        tabDBMonitor              { $("#navDbmonitor") }
        tabAdmin                  { $("#navAdmin") }
        tabSchema                 { $("#navSchema") }
        tabSQLQuery               { $("#navSqlQuery") }
        username                  { $("#btnlogOut") }
        logout                    { $(class:"user", title:"Log Out") }
        help                      { $("#showMyHelp") }
        popup                     { $(class:"popup_content10") }
        popupTitle                { $(class:"overlay-title helpIcon ", text:"Help") }
        popupClose                { $(class:"popup_close") }
        logoutPopup               { $(class:"popup_content2") }
        logoutPopupTitle          { $(class:"overlay-contentError errorMsg") }
        logoutPopupOkButton       { $("#A1") }
        logoutPopupCancelButton   { $("#btnCancel") }
    }

}
