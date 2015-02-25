package vmcTest.pages

import geb.Module

import java.util.List;
import java.util.Map;

import geb.*
import geb.navigator.Navigator
import geb.waiting.WaitTimeoutException

import org.openqa.selenium.JavascriptExecutor


/**
 * Created by lavthaiba on 2/24/2015.
 */
class schemaTab extends Module{

    static content = {

        // system overview
        schemaTabbutton { $("#navSchema > a") }
        systemoverviewTitle { $("#o > div:nth-child(1) > div > h4") }
        modeTitle { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(1) > td:nth-child(1)") }
        voltdbversion { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(2) > td:nth-child(1)") }
        buildstring { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(3) > td:nth-child(1)") }
        clustercomposition { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(4) > td:nth-child(1)") }
        runningsince { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(5) > td:nth-child(1)") }
        modevalue { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(1) > td:nth-child(2)") }
        versionvalue { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(2) > td:nth-child(2)") }
        buildstringvalue { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(3) > td:nth-child(2)") }
        clustercompositionvalue { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(4) > td:nth-child(2)") }
        runningsincevalue { $("#o > div:nth-child(1) > div > table > tbody > tr:nth-child(5) > td:nth-child(2)") }

        // catalog overview

        catalogoverviewstatistic { $("#o > div:nth-child(2) > div.dataBlockHeading > h3") }
        compiledversion {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(1) > td:nth-child(1)")
        }
        compiledonTitle {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(2) > td:nth-child(1)")
        }
        tablecount {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(3) > td:nth-child(1)")
        }
        materializedviewcount {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(4) > td:nth-child(1)")
        }
        indexcount {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(5) > td:nth-child(1)")
        }
        procedurecount {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(6) > td:nth-child(1)")
        }
        sqlstatementcount {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(7) > td:nth-child(1)")
        }
        compiledversionvalue {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(1) > td:nth-child(2)")
        }
        compiledonTitlevalue {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(2) > td:nth-child(2)")
        }
        tablecountvalue {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(3) > td:nth-child(2)")
        }
        materializedviewcountvalue {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(4) > td:nth-child(2)")
        }
        indexcountvalue {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(5) > td:nth-child(2)")
        }
        procedurecountvalue {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(6) > td:nth-child(2)")
        }
        sqlstatementcountvalue {
            $("#o > div:nth-child(2) > div.dataBlockContent > table > tbody > tr:nth-child(7) > td:nth-child(2)")
        }

        //  documentation
        documentationurl { $("#iconDoc") }
        documentationrightlabel { $("#catalogContainer > div.documentation > span") }

        //refreshbutton
        refreshbutton { $("#MenuCatalog > div > button") }

        //DDL source page
        ddlsourcebutton { $("#d-nav > a") }
        ddlsourceTitle {$("#d > div > div.dataBlockHeading > h1")}
        ddlsourcequeries{$("#d > div > div.dataBlockContent > pre")}
        ddlsourcedownload{$("#downloadDDL")}
    }

}
