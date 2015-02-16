import geb.Module

/**
 * Created by anrai on 2/10/15.
 */
class voltDBFooter extends Module {

    static content = {
        footerBanner {
            $("#mainFooter")
        }
        footerText {
            $("#mainFooter > p")
        }
        nofooter {
            $("#nofooter")
        }
    }
}
