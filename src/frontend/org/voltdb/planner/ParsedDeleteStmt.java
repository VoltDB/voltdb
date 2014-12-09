/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.planner;

import java.util.ArrayList;
import java.util.List;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.Database;

/**
 *
 *
 */
public class ParsedDeleteStmt extends AbstractParsedStmt {

    private List<ParsedColInfo> m_orderColumns = null;

    /**
    * Class constructor
    * @param paramValues
    * @param db
    */
    public ParsedDeleteStmt(String[] paramValues, Database db) {
        super(paramValues, db);
    }

    private void parseOrderColumns(VoltXMLElement orderColumnsXml) {
        m_orderColumns = new ArrayList<>();

        for (VoltXMLElement orderColXml : orderColumnsXml.children) {
            m_orderColumns.add(ParsedColInfo.fromOrderByXml(this, orderColXml));
        }
    }

    @Override
    void parse(VoltXMLElement stmtNode) {
        assert(m_tableList.size() == 1);

        for (VoltXMLElement elem : stmtNode.children) {
            if (elem.name.equalsIgnoreCase("ordercolumns")) {
                parseOrderColumns(elem);

            }
        }
    }

}
