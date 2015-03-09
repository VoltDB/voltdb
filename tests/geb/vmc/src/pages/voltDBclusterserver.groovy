package vmcTest.pages

import geb.Module
import java.util.List;
import java.util.Map;

import geb.*
import geb.navigator.Navigator
import geb.waiting.WaitTimeoutException

import org.openqa.selenium.JavascriptExecutor

/**
 * Created by lavthaiba on 2/19/2015.
 */
class voltDBclusterserver extends Module {
    static content = {
        serverbutton				{ $("#serverName") }
        serverconfirmation			{ $("#serverConfigAdmin > div > div.slide-pop-title > div.icon-server.searchLeft.searchLeftAdmin") }
        deerwalkserver3stopbutton   {$("#stopServer_deerwalk3")}
        deeerwalkservercancelbutton {$("#StopConfirmCancel")}
        deerwalkserverstopok        { $("#StopConfirmOK")}

        deerwalkserver4stopbutton   {$("#stopServer_deerwalk4")}

        //DBmonitor part for server
        dbmonitorbutton{$("#navDbmonitor > a")}
        clusterserverbutton{$("#btnPopServerList")}
        servernamefourthbtn{$("#serversList > li:nth-child(1) > a")}
        servernamesecondbtn{$("#serversList > li:nth-child(2) > a")}
        servernamethirdbtn{$("#serversList > li:nth-child(3) > a")}
        serveractivechk    {$("#serversList > li.active.monitoring > a")}
        serversearch{$("input", type: "text", id: "popServerSearch")}
        checkserverTitle{$("#popServerList > div > div.slide-pop-title > div.icon-server.searchLeft")}
        setthreshhold{$("#threshold")}
        clickthreshholdset{$("#saveThreshold")}

    }
}
