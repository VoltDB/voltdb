/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb;

import java.io.IOException;
import java.util.TreeMap;

import org.voltdb.compiler.VoltProjectBuilder;

public class QuickCatalog {

    TreeMap<String, TableShorthand> m_tables = new TreeMap<String, TableShorthand>();
    VoltProjectBuilder m_project = new VoltProjectBuilder();
    String m_pathToCatalog = null;

    public QuickCatalog addDDL(String ddl) throws IOException {
        m_project.addLiteralSchema(ddl);
        return this;
    }

    /*public QuickCatalog addTable(String shorthand) {
        TableShorthand table = new TableShorthand(shorthand);
        String name = table.getName();
        while (m_tables.containsKey(name)) {
            name += "X";
        }
        m_tables.put(name, table);
        return this;
    }

    public QuickCatalog addIndex(String tableName, String shorthand) {
        TableShorthand table = m_tables.get(tableName);
        // might throw NPE... don't care
        table.addIndex(shorthand);
        return this;
    }

    public QuickCatalog compile(String jarname) throws IOException {
        for (Entry<String, TableShorthand> e : m_tables.entrySet()) {
            m_project.addLiteralSchema(e.getValue().getDDL(e.getKey()));
        }
        m_pathToCatalog = Configuration.getPathToCatalogForTest(jarname + ".jar");
        m_project.compile(m_pathToCatalog);

        return this;
    }*/

    public String getPath() {
        return m_pathToCatalog;
    }

}
