/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.planner.ParsedColInfo;

/**
 * Tune XMLs for materialized view min/max recalculation queries.
 * ENG-8641
 */
public class MatViewFallbackQueryXMLGenerator {

    private VoltXMLElement m_xml;
    private int m_maxElementId;
    private List<ParsedColInfo> m_groupByColumnsParsedInfo;
    private List<ParsedColInfo> m_displayColumnsParsedInfo;
    private ArrayList<VoltXMLElement> m_fallbackQueryXMLs;
    private final boolean m_isMultiTableView;

    public MatViewFallbackQueryXMLGenerator(VoltXMLElement xmlquery,
                                            List<ParsedColInfo> groupByColumnsParsedInfo,
                                            List<ParsedColInfo> displayColumnsParsedInfo,
                                            boolean isMultiTableView) {
        m_xml = xmlquery.duplicate();
        m_groupByColumnsParsedInfo = groupByColumnsParsedInfo;
        m_displayColumnsParsedInfo = displayColumnsParsedInfo;
        m_isMultiTableView = isMultiTableView;
        m_maxElementId = VoltXMLElementHelper.findMaxElementId( m_xml );
        m_fallbackQueryXMLs = new ArrayList<>();
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
         *     SELECT min(v1) FROM ENG6511 WHERE d1=? AND d2=? AND abs(v1)>=?;
         *     SELECT max(v1) FROM ENG6511 WHERE d1=? AND d2=? AND abs(v1)<=?;
         ********************************************************************************************************/
        List<VoltXMLElement> columns = VoltXMLElementHelper.getFirstChild(m_xml, "columns").children;
        List<VoltXMLElement> parameters = VoltXMLElementHelper.getFirstChild(m_xml, "parameters").children;
        VoltXMLElement groupcolumnsElement = VoltXMLElementHelper.getFirstChild(m_xml, "groupcolumns");
        List<VoltXMLElement> tablescans = VoltXMLElementHelper.getFirstChild(m_xml, "tablescans").children;
        VoltXMLElement tablescanForJoinCond = tablescans.get(tablescans.size() - 1);
        // Add the joincond to the last table scan element.
        List<VoltXMLElement> joincondList = VoltXMLElementHelper.getFirstChild(tablescanForJoinCond, "joincond", true).children;
        VoltXMLElement joincond = joincondList.size() == 0 ? null : joincondList.get(0);
        joincondList.clear();

        // 1. Turn groupby into joincond (WHERE) ================================================================
        if (groupcolumnsElement != null) {
            // If there is no group by clause, then nothing needs to be transformed.
            List<VoltXMLElement> groupcolumns = groupcolumnsElement.children;
            for (int i=0; i<m_groupByColumnsParsedInfo.size(); ++i) {
                String index = String.valueOf(i);
                String valueType = m_groupByColumnsParsedInfo.get(i).m_expression.getValueType().getName();
                VoltXMLElement column = groupcolumns.get(i);
                // Add the column to the parameter list.
                parameters.add( VoltXMLElementHelper.buildParamElement( nextElementId(), index, valueType ) );

                // Put together the where conditions for the groupby columns.
                //
                // Note that due to ENG-11080 we need to use NOT DISTINCT for
                // multi-table views, due to the possibility of GB columns being NULL.
                // For single-table views, we can catch NULL GB columns at runtime and
                // fall back to a manual scan.
                String comparisonOp = m_isMultiTableView ? "notdistinct" : "equal";
                VoltXMLElement columnParamJoincond = VoltXMLElementHelper.buildColumnParamJoincondElement(comparisonOp, column, lastElementId(), nextElementId() );
                joincond = VoltXMLElementHelper.mergeTwoElementsUsingOperator( "and", nextElementId(), joincond, columnParamJoincond );
            }
            // Remove the group by columns, they are now in the form of where conditions.
            m_xml.children.remove(groupcolumnsElement);
        }

        // 2. Process aggregation columns =====================================================================
        List<VoltXMLElement> originalColumns = new ArrayList<>();
        originalColumns.addAll(columns);
        columns.clear();
        // Parameter index for min/max column
        String paramIndex = String.valueOf(m_groupByColumnsParsedInfo.size());
        // Add one min/max columns at a time as a new fallback query XML - not needed since COUNT(*) can be anywhere
        for (int i=m_groupByColumnsParsedInfo.size(); i<m_displayColumnsParsedInfo.size(); ++i) {
            VoltXMLElement column = originalColumns.get(i);
            String optype = column.attributes.get("optype");
            if ( optype.equals("min") || optype.equals("max") ) {
                columns.add(column);
                VoltXMLElement aggArg = column.children.get(0);
                String operator = optype.equals("min") ? "greaterthanorequalto" : "lessthanorequalto";
                String valueType = m_displayColumnsParsedInfo.get(i).m_expression.getValueType().getName();
                parameters.add( VoltXMLElementHelper.buildParamElement(nextElementId(), paramIndex, valueType) );
                VoltXMLElement aggArgJoincond = VoltXMLElementHelper.buildColumnParamJoincondElement(operator, aggArg, lastElementId(), nextElementId());
                joincondList.add( VoltXMLElementHelper.mergeTwoElementsUsingOperator("and", nextElementId(), joincond, aggArgJoincond) );
                m_fallbackQueryXMLs.add(m_xml.duplicate());
                // For debug:
                // System.out.println(m_xml.toString());
                columns.clear();
                joincondList.clear();
                parameters.remove(m_groupByColumnsParsedInfo.size());
            }
        }
    }

    public List<VoltXMLElement> getFallbackQueryXMLs() {
        return m_fallbackQueryXMLs;
    }
}
