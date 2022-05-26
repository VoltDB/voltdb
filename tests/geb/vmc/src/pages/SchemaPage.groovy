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

/**
 * This class represents the 'Schema' tab of the VoltDB Management Center
 * page, which is the VoltDB web UI (replacing the old Catalog Report).
 */
class SchemaPage extends VoltDBManagementCenterPage {
    static content = {
        schemaTabs  { $('#catalogNavlist') }
        overviewTab { schemaTabs.find('#o-nav') }
        schemaSubTab    { schemaTabs.find('#s-nav') }
        proceduresTab   { schemaTabs.find('#p-nav') }
        sizeTab     { schemaTabs.find('#z-nav') }
        ddlTab      { schemaTabs.find('#d-nav') }
        overviewLink    (to: SchemaPageOverviewTab) { overviewTab.find('a') }
        schemaSubLink   (to: SchemaPageSchemaTab)   { schemaSubTab.find('a') }
        proceduresLink  (to: SchemaPageProceduresAndSqlTab) { proceduresTab.find('a') }
        sizeLink    (to: SchemaPageSizeWorksheetTab){ sizeTab.find('a') }
        ddlLink     (to: SchemaPageDdlSourceTab)    { ddlTab.find('a') }
        voltDbDocumentationLink { $('a#iconDoc') }
        refreshbutton       { $(class:"refreshBtn schm", text:"Refresh") }
    }


    static at = {
        schemaTab.displayed
        schemaTab.attr('class') == 'active'
        overviewLink.displayed
        schemaSubLink.displayed
        proceduresLink.displayed
        sizeLink.displayed
        ddlLink.displayed
    }

    /**
     * Returns true if the current page is a SchemaPage and the current tab
     * open on that page is the "Overview" tab (i.e., the "Overview" tab of
     * the "Schema" tab of the VoltDB Management Center page is currently
     * open).
     * @return true if a SchemaPage's "Overview" tab is currently open.
     */
    def boolean isSchemaPageOverviewTabOpen() {
        int count = 0
        while (count<10) {
            count++
            try {
                if (isSchemaPageOpen() && overviewTab.attr('class') == 'active') {
                    return true
                } else {
                    return false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {

            }
        }
    }

    /**
     * Returns true if the current page is a SchemaPage and the current tab
     * open on that page is the "Schema" tab (i.e., the "Schema" tab of the
     * "Schema" tab of the VoltDB Management Center page is currently open).
     * @return true if a SchemaPage's "Schema" tab is currently open.
     */
    def boolean isSchemaPageSchemaTabOpen() {
        int count = 0
        while (count<10) {
            count++
            try {
                if (isSchemaPageOpen() && schemaSubTab.attr('class') == 'active') {
                    return true
                } else {
                    return false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {

            }
        }
    }

    /**
     * Returns true if the current page is a SchemaPage and the current tab
     * open on that page is the "Procedures & SQL" tab (i.e., the "Procedures
     * & SQL" tab of the "Schema" tab of the VoltDB Management Center page is
     * currently open).
     * @return true if a SchemaPage's "Procedures & SQL" tab is currently open.
     */
    def boolean isSchemaPageProceduresAndSqlTabOpen() {
        int count = 0
        while (count<10) {
            count++
            try {
                if (isSchemaPageOpen() && proceduresTab.attr('class') == 'active') {
                    return true
                } else {
                    return false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {

            }
        }
    }

    /**
     * Returns true if the current page is a SchemaPage and the current tab
     * open on that page is the "Size Worksheet" tab (i.e., the "Size Worksheet"
     * tab of the "Schema" tab of the VoltDB Management Center page is currently
     * open).
     * @return true if a SchemaPage's "Size Worksheet" tab is currently open.
     */
    def boolean isSchemaPageSizeWorksheetTabOpen() {
        int count = 0
        while (count<10) {
            count++
            try {
                if (isSchemaPageOpen() && sizeTab.attr('class') == 'active') {
                    return true
                } else {
                    return false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {

            }
        }
    }

    /**
     * Returns true if the current page is a SchemaPage and the current tab
     * open on that page is the "DDL Source" tab (i.e., the "DDL Source" tab
     * of the "Schema" tab of the VoltDB Management Center page is currently
     * open).
     * @return true if a SchemaPage's "DDL Source" tab is currently open.
     */
    def boolean isSchemaPageDdlSourceTabOpen() {
        int count = 0
        while (count<10) {
            count++
            try {
                if (isSchemaPageOpen() && ddlTab.attr('class') == 'active') {
                    return true
                } else {
                    return false
                }
            } catch(org.openqa.selenium.StaleElementReferenceException e) {

            }
        }
    }

    /**
     * Clicks the "Overview" link, opening the "Overview" tab, on the "Schema"
     * page (or tab); if the "Overview" tab is already open, no action is taken.
     */
    def void openSchemaPageOverviewTab() {
        if (!isSchemaPageOverviewTabOpen()) {
            overviewLink.click()
        }
    }

    /**
     * Clicks the "Schema" (sub-)link, opening the "Schema" tab, on the "Schema"
     * page (or tab); if the "Schema" tab is already open, no action is taken.
     */
    def void openSchemaPageSchemaTab() {
        if (!isSchemaPageSchemaTabOpen()) {
            schemaSubLink.click()
        }
    }

    /**
     * Clicks the "Procedures & SQL" link, opening the "Procedures & SQL" tab,
     * on the "Schema" page (or tab); if the "Procedures & SQL" tab is already
     * open, no action is taken.
     */
    def void openSchemaPageProceduresAndSqlTab() {
        if (!isSchemaPageProceduresAndSqlTabOpen()) {
            proceduresLink.click()
        }
    }

    /**
     * Clicks the "Size Worksheet" link, opening the "Size Worksheet" tab, on
     * the "Schema" page (or tab); if the "Size Worksheet" tab is already open,
     * no action is taken.
     */
    def void openSchemaPageSizeWorksheetTab() {
        if (!isSchemaPageSizeWorksheetTabOpen()) {
            sizeLink.click()
        }
    }

    /**
     * Clicks the "DDL Source" link, opening the "DDL Source" tab, on the
     * "Schema" page (or tab); if the "DDL Source" tab is already open, no
     * action is taken.
     */
    def void openSchemaPageDdlSourceTab() {
        if (!isSchemaPageDdlSourceTabOpen()) {
            ddlLink.click()
        }
    }



    /*
     * get query to create a table
     */
    def String getQueryToCreateTable() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryDbMonitor.txt"));
        String line;
        String query = ""

        while((line = br.readLine()) != "#create") {
        }

        while ((line = br.readLine()) != "#delete") {
            // process the line.
            query = query + line + "\n"
        }

        return query
    }

    /*
     * get query to delete a table
     */
    def String getQueryToDeleteTable() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryDbMonitor.txt"));
        String line;
        String query = ""

        while((line = br.readLine()) != "#delete") {
        }

        while ((line = br.readLine()) != "#deleteOnly") {
            // process the line.
            query = query + line + "\n"
        }

        return query
    }

    /*
     * get query to delete a table
     */
    def String getQueryToDeleteTableOnly() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryDbMonitor.txt"));
        String line;
        String query = ""

        while((line = br.readLine()) != "#deleteOnly") {
        }

        while ((line = br.readLine()) != "#name") {
            // process the line.
            query = query + line + "\n"
        }

        return query
    }

    /*
     * get tablename that is created and deleted
     */

    def String getTablename() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryDbMonitor.txt"));
        String line;
        String query = ""

        while((line = br.readLine()) != "#name") {
        }

        while ((line = br.readLine()) != "#index") {
            query = query + line + "\n"
        }

        return query
    }

    /*
     * get query to create index
     */
    def String getQueryToCreateIndex() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryDbMonitor.txt"));
        String line;
        String query = ""

        while((line = br.readLine()) != "#index") {
        }

        while ((line = br.readLine()) != "#view") {
            query = query + line + "\n"
        }

        return query
    }

    /*
    * get query to create index
    */
    def String getQueryToCreateView() {
        BufferedReader br = new BufferedReader(new FileReader("src/resources/sqlQueryDbMonitor.txt"));
        String line;
        String query = ""

        while((line = br.readLine()) != "#view") {
        }

        while ((line = br.readLine()) != "#createStoredProcedure") {
            query = query + line + "\n"
        }

        return query
    }

    /*
     * click SQL query to go to SQL query
     */

    def boolean gotoSqlQuery() {
        header.tabSQLQuery.click()
    }

    /*
     * click schema tab to go to Schema tab
     */

    def boolean gotoSchemaSubTab() {
        schemaSubLink.click()
    }

    public String removeLastTwoChar(String s) {
        if (s == null || s.length() == 0) {
            return s;
        }
        return s.substring(0, s.length()-2);
    }

    public String returnIdOfRowCount(int index) {
        return "s-size-" + String.valueOf(index) + "-count"
    }
}
