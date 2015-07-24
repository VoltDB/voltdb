/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.compiler;

import java.util.ArrayList;
import java.util.List;
import org.voltdb.planner.ParsedColInfo;
import org.voltdb.compiler.VoltXMLElementHelper;

import org.hsqldb_voltpatches.VoltXMLElement;

/**
 * Tune XMLs for materialized view min/max recalculation queries.
 * ENG-8641
 */
public class MatViewFallbackQueryXMLGenerator {

    private VoltXMLElement m_xml;
    private int m_maxElementId;
    private ArrayList<ParsedColInfo> m_groupByColumnsParsedInfo;
    private ArrayList<ParsedColInfo> m_displayColumnsParsedInfo;
    private ArrayList<VoltXMLElement> m_fallbackQueryXMLs;

    public MatViewFallbackQueryXMLGenerator(VoltXMLElement xmlquery,
                                            ArrayList<ParsedColInfo> groupByColumnsParsedInfo,
                                            ArrayList<ParsedColInfo> displayColumnsParsedInfo) {
        m_xml = xmlquery.duplicate();
        m_groupByColumnsParsedInfo = groupByColumnsParsedInfo;
        m_displayColumnsParsedInfo = displayColumnsParsedInfo;
        m_maxElementId = VoltXMLElementHelper.findMaxElementId( m_xml );
        m_fallbackQueryXMLs = new ArrayList<VoltXMLElement>();
        generateFallbackQueryXMLs();
    }

    private String nextElementId() {
        ++m_maxElementId;
        return String.valueOf( m_maxElementId );
    }

    private String lastElementId() {
        return String.valueOf( m_maxElementId );
    }

    private void generateFallbackQueryXMLs() {
        /*********************************************************************************************************
         * This function will turn the XML for materialized view definitions like:
         *     SELECT d1, d2, COUNT(*), MIN(abs(v1)) AS vmin, MAX(abs(v1)) AS vmax FROM ENG6511 GROUP BY d1, d2;
         * into fallback query XMLs like:
         *     SELECT min(v1) FROM ENG6511 WHERE d1=? AND d2=?;
         *     SELECT max(v1) FROM ENG6511 WHERE d1=? AND d2=?;
         ********************************************************************************************************/
        List<VoltXMLElement> columns = VoltXMLElementHelper.getFirstChild(m_xml, "columns").children;
        List<VoltXMLElement> parameters = VoltXMLElementHelper.getFirstChild(m_xml, "parameters").children;
        VoltXMLElement groupcolumnsElement = VoltXMLElementHelper.getFirstChild(m_xml, "groupcolumns");

        // 1. Turn groupby into joincond (WHERE) ================================================================
        if (groupcolumnsElement != null) {
            // If there is no group by clause, then nothing needs to be transformed.
            List<VoltXMLElement> tablescans = VoltXMLElementHelper.getFirstChild(m_xml, "tablescans").children;
            VoltXMLElement tablescanForJoinCond = tablescans.get(tablescans.size() - 1);
            // Add the joincond to the last table scan element.
            List<VoltXMLElement> joincond = VoltXMLElementHelper.getFirstChild(tablescanForJoinCond, "joincond", true).children;
            List<VoltXMLElement> groupcolumns = groupcolumnsElement.children;
            VoltXMLElement joincondFromGroupby = null;
            for (int i=0; i<m_groupByColumnsParsedInfo.size(); ++i) {
                String index = String.valueOf(i);
                String valueType = m_groupByColumnsParsedInfo.get(i).expression.getValueType().getName();
                VoltXMLElement column = groupcolumns.get(i);
                // Add the column to the parameter list.
                parameters.add( VoltXMLElementHelper.buildParamElement( nextElementId(), index, valueType ) );
                // Put together the where conditions for the groupby columns.
                VoltXMLElement first = joincondFromGroupby;
                VoltXMLElement second = VoltXMLElementHelper.buildColumnParamJoincondElement( "equal", column, lastElementId(), nextElementId() );
                // If there is the first groupby column, first == null
                // mergeTwoElementsUsingOperator will directly return the second.
                joincondFromGroupby = VoltXMLElementHelper.mergeTwoElementsUsingOperator( "and", nextElementId(), first, second );
            }
            if (joincond.size() == 0) {
                // If there was no where condition, use the joincondFromGroupby directly.
                joincond.add(joincondFromGroupby);
            }
            else {
                // If there was an existing condition, connect the exsiting one and joincondFromGroupby with an "and".
                joincond.set(0, VoltXMLElementHelper.mergeTwoElementsUsingOperator("and", nextElementId(), joincond.get(0), joincondFromGroupby));
            }
            // Remove the group by columns, they are now in the form of where conditions.
            m_xml.children.remove(groupcolumnsElement);
        }

        // 2. Process aggregation columns =====================================================================
        List<VoltXMLElement> originalColumns = new ArrayList<VoltXMLElement>();
        originalColumns.addAll(columns);
        columns.clear();

        // Add one min/max columns at a time as a new fallback query XML.
        for (int i=m_groupByColumnsParsedInfo.size()+1; i<m_displayColumnsParsedInfo.size(); ++i) {
            VoltXMLElement column = originalColumns.get(i);
            String optype = column.attributes.get("optype");
            if ( optype.equals("min") || optype.equals("max") ) {
                columns.add(column);
                m_fallbackQueryXMLs.add(m_xml.duplicate());
                // System.out.println(m_xml.toString());
                columns.clear();
            }
        }
    }

    public List<VoltXMLElement> getFallbackQueryXMLs() {
        return m_fallbackQueryXMLs;
    }
}
