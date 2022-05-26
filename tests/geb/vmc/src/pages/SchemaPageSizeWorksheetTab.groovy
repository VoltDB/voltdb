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
 * This class represents the 'Size Worksheet' tab of the 'Schema' page (or tab)
 * of the VoltDB Management Center (page), which is the VoltDB web UI (replacing
 * the old Catalog Report).
 */

class SchemaPageSizeWorksheetTab extends SchemaPage {
    static content = {
        name        { $(class:"tablesorter-header-inner", text:"Name") }
        type        { $(class:"tablesorter-header-inner", text:"Type") }
        count       { $(class:"tablesorter-header-inner", text:"Count") }
        rowMin      { $(class:"tablesorter-header-inner", text:"Row Min") }
        rowMax      { $(class:"tablesorter-header-inner", text:"Row Max") }
        indexMin    { $(class:"tablesorter-header-inner", text:"Index Min") }
        indexMax    { $(class:"tablesorter-header-inner", text:"Index Max") }
        tableMin    { $(class:"tablesorter-header-inner", text:"Table Min") }
        tableMax    { $(class:"tablesorter-header-inner", text:"Table Max") }

        //ascending     { $(class:"tablesorter-icon icon-chevron-up") }
        ascending       { $("#sizetable > thead > tr.tablesorter-headerRow > th.tablesorter-header.bootstrap-header.tablesorter-headerAsc") }
        //descending        { $(class:"tablesorter-icon icon-chevron-down") }
        descending      { $("#sizetable > thead > tr.tablesorter-headerRow > th.tablesorter-header.bootstrap-header.tablesorter-headerDesc") }

        sizeAnalysisSummary { $("#z > div:nth-child(1) > div.dataBlockHeading > h1") }

        sizeTableMin        { $("#s-size-summary-table-min") }
        sizeTableMax        { $("#s-size-summary-table-max") }
        sizeViewMin         { $("#s-size-summary-view-min") }
        sizeIndexMin        { $("#s-size-summary-index-min") }
        sizeTotalMin        { $("#s-size-summary-total-min") }
        sizeTotalMax        { $("#s-size-summary-total-max") }

        textTable           { $("#z > div:nth-child(3) > div.dataBlockContent > div > table > tbody > tr:nth-child(1) > td:nth-child(2)") }
        textView            { $("#z > div:nth-child(3) > div.dataBlockContent > div > table > tbody > tr:nth-child(2) > td:nth-child(2)") }
        textIndex           { $("#z > div:nth-child(3) > div.dataBlockContent > div > table > tbody > tr:nth-child(3) > td:nth-child(2)") }
        textTotal           { $("#z > div:nth-child(2) > div.dataBlockContent > div > table > tbody > tr:nth-child(5) > td:nth-child(1) > b") }

        tablenamePresent    (required:false) { $(class:"table-view-name", text:"voters") }

        searchName          { $("#sizetable > thead > tr.tablesorter-filter-row > td:nth-child(1) > input") }
        documentationLink   { $("#iconDoc") }
        generatedbytxt      {$("#catalogContainer > div.documentation > span")}
        refreshtableworksheet   { $("#MenuCatalog > div > button", text:"Refresh")}
    }

    static at = {
        sizeTab.displayed
        sizeTab.attr('class') == 'active'
    }
}
