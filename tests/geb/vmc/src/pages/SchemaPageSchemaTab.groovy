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

}
