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

class NetworkInterfaces extends Module {
    static content = {
 	title			{ $("#admin > div.adminContainer > div.adminContentRight > div.adminPorts > div.headerAdminContent > h1") }
	portNameTitle		{ $("#admin > div.adminContainer > div.adminContentRight > div.adminPorts > div:nth-child(2) > table > tbody > tr:nth-child(1) > th:nth-child(1)") }

	clusterSettingTitle	{ $("#admin > div.adminContainer > div.adminContentRight > div.adminPorts > div:nth-child(2) > table > tbody > tr:nth-child(1) > th:nth-child(2)") }

	serverSettingTitle	{ $("#admin > div.adminContainer > div.adminContentRight > div.adminPorts > div:nth-child(2) > table > tbody > tr:nth-child(1) > th:nth-child(3)") }

	clientPortTitle		{ $("#admin > div.adminContainer > div.adminContentRight > div.adminPorts > div:nth-child(2) > table > tbody > tr:nth-child(2) > td:nth-child(1)") }
	adminPortTitle		{ $("#admin > div.adminContainer > div.adminContentRight > div.adminPorts > div:nth-child(2) > table > tbody > tr:nth-child(3) > td:nth-child(1)") }
	httpPortTitle 		{ $("#admin > div.adminContainer > div.adminContentRight > div.adminPorts > div:nth-child(2) > table > tbody > tr:nth-child(4) > td:nth-child(1)") }
	internalPortTitle	{ $("#admin > div.adminContainer > div.adminContentRight > div.adminPorts > div:nth-child(2) > table > tbody > tr:nth-child(5) > td:nth-child(1)") }
	zookeeperPortTitle	{ $("#admin > div.adminContainer > div.adminContentRight > div.adminPorts > div:nth-child(2) > table > tbody > tr:nth-child(6) > td:nth-child(1)") }
	replicationPortTitle	{ $("#admin > div.adminContainer > div.adminContentRight > div.adminPorts > div:nth-child(2) > table > tbody > tr:nth-child(7) > td:nth-child(1)") }
	
	clientPortValue		{ $("#clientport") }
	adminPortValue		{ $("#adminport") }
	httpPortValue		{ $("#httpport") }
	internalPortValue	{ $("#internalPort") }
	zookeeperPortValue	{ $("#zookeeperPort") }
	replicationPortValue	{ $("#replicationPort") }	     
    }

}
