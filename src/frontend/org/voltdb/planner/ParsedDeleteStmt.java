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
import java.util.Collections;
import java.util.List;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.Database;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.LimitPlanNode;

/**
 *
 *
 */
public class ParsedDeleteStmt extends AbstractParsedStmt {

    private final List<ParsedColInfo> m_orderColumns = new ArrayList<>();
    private LimitPlanNode m_limitPlanNode = null;

    /**
    * Class constructor
    * @param paramValues
    * @param db
    */
    public ParsedDeleteStmt(String[] paramValues, Database db) {
        super(paramValues, db);
    }

    private void parseOrderColumns(VoltXMLElement orderColumnsXml) {
        assert(m_orderColumns.size() == 0);
        if (orderColumnsXml == null)
            return;

        for (VoltXMLElement orderColXml : orderColumnsXml.children) {
            m_orderColumns.add(ParsedColInfo.fromOrderByXml(this, orderColXml));
        }
    }

    @Override
    void parse(VoltXMLElement stmtNode) {
        assert(m_tableList.size() == 1);

        VoltXMLElement limitXml = null;
        VoltXMLElement offsetXml = null;
        for (VoltXMLElement elem : stmtNode.children) {
            if (elem.name.equalsIgnoreCase("ordercolumns")) {
                parseOrderColumns(elem);
            }
            else if (elem.name.equalsIgnoreCase("limit")) {
                limitXml = elem;
            }
            else if(elem.name.equalsIgnoreCase("offset")) {
                offsetXml = elem;
            }
        }

        m_limitPlanNode = limitPlanNodeFromXml(limitXml, offsetXml);
    }

    @Override
    public boolean hasOrderByColumns() {
        return m_orderColumns.size() > 0;
    }

    @Override
    public List<ParsedColInfo> orderByColumns() {
        return Collections.unmodifiableList(m_orderColumns);
    }

    public AbstractPlanNode handleLimit(AbstractPlanNode root) {
        if (m_limitPlanNode != null) {
            // XXX fail here if no ORDER BY present
            assert (m_limitPlanNode.getChildCount() == 0);
            m_limitPlanNode.addAndLinkChild(root);
            return m_limitPlanNode;
        }

        return root;
    }
}
