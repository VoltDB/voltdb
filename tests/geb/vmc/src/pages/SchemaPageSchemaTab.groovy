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

import java.util.List;
import java.util.Map;
import vmcTest.pages.VoltDBManagementCenterPage.ColumnHeaderCase;

/**
 * This class represents the 'Schema' tab of the 'Schema' page (or tab) of
 * the VoltDB Management Center (page), which is the VoltDB web UI (replacing
 * the old Catalog Report).
 */
class SchemaPageSchemaTab extends SchemaPage {
    static content = {
        requiredId      { $(checkId) }
        schemaTable { $('#schematable') }

        ascending       { $("html body div.page-wrap div#wrapper div#schema.contents div#catalogContainer.catalogContainer div#containerCatalog.container div#s.reportpage div.dataBlock div.dataBlockContent div.tblScroll table#schematable.table.tableL1.tablesorter.table-bordered.hasFilters.hasStickyHeaders.tablesorter-bootstrap thead tr.tablesorter-headerRow th.tablesorter-header.bootstrap-header.tablesorter-headerAsc") }
        descending      { $("html body div.page-wrap div#wrapper div#schema.contents div#catalogContainer.catalogContainer div#containerCatalog.container div#s.reportpage div.dataBlock div.dataBlockContent div.tblScroll table#schematable.table.tableL1.tablesorter.table-bordered.hasFilters.hasStickyHeaders.tablesorter-bootstrap thead tr.tablesorter-headerRow th.tablesorter-header.bootstrap-header.tablesorter-headerDesc") }

        row                 { $(class:"primaryrow") }


        viewDdlSource       { $("#ddlSource") }

        searchName          { $(class:"tablesorter-filter", 0) }

        test            { $("html body div.page-wrap div#wrapper div#schema.contents div#catalogContainer.catalogContainer div#containerCatalog.container div#s.reportpage div.dataBlock div.dataBlockContent div.tblScroll table#schematable.table.tableL1.tablesorter.table-bordered.hasFilters.hasStickyHeaders.tablesorter-bootstrap tbody") }

        name            { $(class:"tablesorter-header-inner", text:"Name" ) }
        type            { $(class:"tablesorter-header-inner", text:"Type" ) }
        partitioning    { $(class:"tablesorter-header-inner", text:"Partitioning" ) }
        columns         { $(class:"tablesorter-header-inner", text:"Columns" ) }
        indexes         { $(class:"tablesorter-header-inner", text:"Indexes" ) }
        pkey            { $(class:"tablesorter-header-inner", text:"PKey" ) }
        tuplelimit      { $(class:"tablesorter-header-inner", text:"TupleLimit" ) }
        expandallcheck  { $("#s > div > div.dataBlockContent > div > div > label", text:"Expand All")}
        expandedcheck   {$("#s > div > div.dataBlockContent > div")}
        documentationLink { $("#iconDoc") }
        generatedbytxt  {$("#catalogContainer > div.documentation > span")}
        expandedlist    { $(class:"togglex")}
        expandedlistbox { $(class:"invert")}
        expandedlist1   { $(class:"togglex",1)}
        expandedlist2   { $(class:"togglex",2)}
        refreshtableschema      { $("#MenuCatalog > div > button", text:"Refresh")}


        header              { module Header }
        footer              { module Footer }
    }
    static at = {
        schemaSubTab.displayed
        schemaSubTab.attr('class') == 'active'
        schemaTable.displayed
    }

    String checkId = "#s-"+ getTablename()

    /**
     * Returns the contents of the "Schema" table, on the Schema tab of the
     * Schema page of the VMC, by column, as a Map<String,List<String>>.
     * @param colHeaderFormat - the case in which you want the table's column
     * headers returned: converted to lower case, to upper case, or as-is.
     * @return the contents of the "Schema" table, listed by column.
     */

    Map<String,List<String>> getSchemaTableByColumn(ColumnHeaderCase colHeaderFormat=ColumnHeaderCase.AS_IS) {
        return getTableByColumn(schemaTable, colHeaderFormat)
    }

    /**
     * Returns the contents of the "Schema" table, on the Schema tab of the
     * Schema page of the VMC, by row, as a List<List<String>>.
     * @param colHeaderFormat - the case in which you want the table's column
     * headers returned: converted to lower case, to upper case, or as-is.
     * @return the contents of the "Schema" table, listed by row.
     */

    List<List<String>> getSchemaTableByRow(ColumnHeaderCase colHeaderFormat=ColumnHeaderCase.AS_IS) {
        return getTableByRow(schemaTable, colHeaderFormat)
    }


    /*
     * refresh and send tablename into the search tablename box
     */
    def boolean sendTablename(String tablename) {
        refresh.click()
        waitFor(20,10) { searchName.displayed }
        searchName.value(tablename)
    }

    /*
     * check if row is displayed
     */
    def boolean rowDisplayed() {
        row.displayed
    }

    /*
     * check ascending class
     */
    def boolean checkAscending() {
        ascending.displayed
    }

    /*
     * check descending class
     */
    def boolean checkDescending() {
        descending.displayed
    }
}
