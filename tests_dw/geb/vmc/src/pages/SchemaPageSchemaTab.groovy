/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

/**
 * This class represents the 'Schema' tab of the 'Schema' page (or tab) of
 * the VoltDB Management Center (page), which is the VoltDB web UI (replacing
 * the old Catalog Report).
 */
class SchemaPageSchemaTab extends SchemaPage {
    static content = {
        schemaTable { $('#schematable') }
    }
    static at = {
        schemaSubTab.displayed
        schemaSubTab.attr('class') == 'active'
        schemaTable.displayed
    }
    
    /**
     * Returns the contents of the "Schema" table, on the Schema tab of the
     * Schema page of the VMC.
     * @param columnWise - if true, returns a Map<String,List<String>>, with
     * each key being a column header, and its associated List contains each
     * of the values in that column; if false, returns a List<List(String)>,
     * representing the table by row, with the columnn headers in the first
     * row.
     * @return the contents of the "Schema" table.
     */
    def getSchemaTable(boolean columnWise) {
        return getTableContents(schemaTable, false, columnWise)
    }

}
