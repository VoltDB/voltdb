import geb.Module

/**
 * Created by anrai on 2/10/15.
 */
class voltDBHeader extends Module{

    static content = {
        headerBanner {
            $("#headerMain")
        }

        headerImage {
            $("#headerMain > div.headLeft > div.logo > img")
        }

        headerTabDBMonitor{
            $("#navDbmonitor > a")
        }

        headerTabAdmin{
            $("#navAdmin > a")
        }
        headerTabSchema{
            $("#navSchema > a")
        }

        headerTabSQLQuery{
            $("#navSqlQuery > a")
        }

        headerUsername {
            $("#btnlogOut > div")
        }

        headerLogout {
            $("#logOut > div")
        }

        headerHelp {
            $("#showMyHelp")
        }

        headerPopup {
            $("body > div.popup_cont > div.popup > div")
        }

        headerPopupTitle {
            $("body > div.popup_cont > div.popup > div > div.overlay-title.helpIcon")
        }

        headerPopupClose {
            $("body > div.popup_cont > div.popup_close")
        }

        headerLogoutPopup {
            $("body > div.popup_cont")
        }

        headerLogoutPopupTitle {
            $("body > div.popup_cont > div.popup > div > div.overlay-title")
        }

        headerLogoutPopupOkButton {
            $("#A1")
        }

        headerLogoutPopupCancelButton {
            $("#btnCancel")
        }

        noheader {
            $("#noheader")
        }
    }


}
