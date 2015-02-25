/**
 * Created by anrai on 2/12/15.
 */

package vmcTest.pages

import geb.navigator.Navigator

class AdminPage extends VoltDBManagementCenterPage {
    static content = {
        cluster             { $('#admin > div.adminWrapper') }
        overview            { $('#admin > div.adminContainer > div.adminContentLeft') }
        networkInterfaces   { $('#admin > div.adminContainer > div.adminContentRight > div.adminPorts') }
        directories         { $('#admin > div.adminContainer > div.adminContentRight > div.adminDirectories') }
		networkInterfaces   { module NetworkInterfaces } 
		directories		 	{ module Directories }	
		overview			{ module Overview }
		cluster				{ module Cluster }       
		header              { module Header }
        footer              { module Footer }
        server              { module voltDBclusterserver}
        downloadbtn         { module downloadconfigbtn}
        schema              { module schemaTab}


        
    }
    static at = {
        adminTab.displayed
        adminTab.attr('class') == 'active'
        cluster.displayed
        overview.displayed
        networkInterfaces.displayed
        directories.displayed
    }


    /**
     * Returns true if the "Overview"
     * currently exists (displayed).
     * @return true if the "Overview" area currently exists.
     */
    def boolean doesOverviewExist() {
        return overview.displayed
    }

    /**
     * Returns true if the "Overview"
     * currently exists (displayed).
     * @return true if the "Network Interfaces" area currently exists.
     */
    def boolean doesNetworkInterfacesExist() {
        return networkInterfaces.displayed
    }
    
    /**
     * Returns true if the "Directories"
     * currently exists (displayed).
     * @return true if the "Directories" area currently exists.
     */
    def boolean doesDirectoriesExist() {
        return directories.displayed
    }


}
