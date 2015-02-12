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

/**
 * This class represents the 'DDL Source' tab of the 'Schema' page (or tab) of
 * the VoltDB Management Center (page), which is the VoltDB web UI (replacing
 * the old Catalog Report).
 */
class SchemaPageDdlSourceTab extends SchemaPage {
    static content = {
        mainContent { $('#d') }
        downloadButton  { mainContent.find('.dataBlockHeading').find('a') }
        sourceText  { mainContent.find('.dataBlockContent') }
    }
    static at = {
        ddlTab.displayed
        ddlTab.attr('class') == 'active'
        //downloadButton.displayed
        sourceText.displayed
    }

    /**
     * Returns the DDL Source, as displayed on the "DDL Source" tab of the
     * "Schema" page (or tab).
     * @return the displayed DDL Source (as a String).
     */
    def String getDdlSource() {
        return sourceText.first().text()
    }

    /**
     * Returns the DDL Source, as displayed on the "DDL Source" tab of the
     * "Schema" page (or tab), returning each line as a separate String.
     * @return the displayed DDL Source (as a List<String>).
     */
    def List<String> getDdlSourceLines() {
        List<String> lines = []  // supports remove() method (unlike Arrays.asList)
        Arrays.asList(getDdlSource().split("\n")).each { lines.add(it) }
        return lines
    }

}
