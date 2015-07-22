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
         *     SELECT abs(v1) FROM ENG6511 WHERE d1=? AND d2=? AND abs(v1)>=? ORDER BY abs(v1) LIMIT 2;
         *     SELECT abs(v1) FROM ENG6511 WHERE d1=? AND d2=? AND abs(v1)<=? ORDER BY abs(v1) DESC LIMIT 2;
         ********************************************************************************************************/
        VoltXMLElement groupcolumnsElement = VoltXMLElementHelper.getFirstChild(m_xml, "groupcolumns");
        List<VoltXMLElement> parameters = VoltXMLElementHelper.getFirstChild(m_xml, "parameters").children;
        List<VoltXMLElement> columns = VoltXMLElementHelper.getFirstChild(m_xml, "columns").children;
        List<VoltXMLElement> ordercolumns = VoltXMLElementHelper.getFirstChild(m_xml, "ordercolumns", true).children;
        List<VoltXMLElement> tablescans = VoltXMLElementHelper.getFirstChild(m_xml, "tablescans").children;
        // Add the joincond to the last table scan element.
        VoltXMLElement tablescanForJoinCond = tablescans.get(tablescans.size() - 1);
        List<VoltXMLElement> joincond = VoltXMLElementHelper.getFirstChild(tablescanForJoinCond, "joincond", true).children;

        // 1. Add XMLElements for "LIMIT" =======================================================================
        //    LIMIT 2
        m_xml.children.addAll( 0, VoltXMLElementHelper.buildLimitElements( 2, nextElementId() ) );

        // 2. Turn groupby into joincond (WHERE) ================================================================
        if (groupcolumnsElement != null) {
            // If there is no group by clause, then nothing needs to be transformed.
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

        // 3. Process aggregation columns =====================================================================
        List<VoltXMLElement> originalColumns = new ArrayList<VoltXMLElement>();
        originalColumns.addAll(columns);
        // If the materialized view definition doesn't have groupby column, joincond.size() == 0
        VoltXMLElement joincondTemplate = joincond.size() > 0 ? joincond.get(0) : null;
        columns.clear();
        String index = String.valueOf(m_groupByColumnsParsedInfo.size());
        for (int i=m_groupByColumnsParsedInfo.size()+1; i<m_displayColumnsParsedInfo.size(); ++i) {
            VoltXMLElement column = originalColumns.get(i);
            String optype = column.attributes.get("optype");
            if ( optype.equals("min") || optype.equals("max") ) {
                String valueType = m_displayColumnsParsedInfo.get(i).expression.getValueType().getName();
                // Add the min/max argument to the display column list.
                VoltXMLElement aggArg = column.children.get(0);
                if ( ! aggArg.name.equals("columnref") ) {
                    // If the argument is not a columnref, i.e. it doesn't have a column name or an alias,
                    // we need to give it an alias in order to pass checkPlanColumnMatch() in ParsedSelectStmt.
                    String alias = column.attributes.get("alias");
                    aggArg.attributes.put("alias", alias);
                }
                columns.add( aggArg );
                parameters.add( VoltXMLElementHelper.buildParamElement( nextElementId(), index, valueType ) );
                String operator = "greaterthanorequalto"; // min
                boolean desc = false;
                if ( optype.equals("max") ) {
                    operator = "lessthanorequalto";
                    desc = true;
                }
                VoltXMLElement aggColumnParamJoincondElement = VoltXMLElementHelper.buildColumnParamJoincondElement( operator, aggArg, lastElementId(), nextElementId() );
                VoltXMLElement finalJoincond = VoltXMLElementHelper.mergeTwoElementsUsingOperator("and", nextElementId(), joincondTemplate, aggColumnParamJoincondElement);
                if (joincond.size() == 0) {
                    joincond.add( finalJoincond );
                }
                else {
                    joincond.set( 0, finalJoincond );
                }
                ordercolumns.add( VoltXMLElementHelper.buildOrderColumnsElement( aggArg, desc, nextElementId() ) );
                // finish for the current aggregation.
                m_fallbackQueryXMLs.add( m_xml.duplicate() );
                // reset for the next aggregation.
                columns.clear();
                ordercolumns.clear();
                parameters.remove(parameters.size() - 1);
            }
        }
    }

    public List<VoltXMLElement> getFallbackQueryXMLs() {
        return m_fallbackQueryXMLs;
    }
}
