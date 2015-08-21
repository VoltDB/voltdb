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
import java.util.Collections;
import org.voltdb.planner.ParsedColInfo;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.Column;
import org.voltdb.compiler.VoltXMLElementHelper;

import org.hsqldb_voltpatches.VoltXMLElement;

/**
 * Tune XMLs for materialized view min/max recalculation queries.
 * ENG-8641
 */
public class MatViewQueriesXMLGenerator {

    private Table m_destTable;
    private VoltXMLElement m_xml;
    private int m_maxElementId;
    private ArrayList<ParsedColInfo> m_groupByColumnsParsedInfo;
    private ArrayList<ParsedColInfo> m_displayColumnsParsedInfo;
    private ArrayList<VoltXMLElement> m_fallbackQueryXMLs;
    private VoltXMLElement m_buildUpQueryXML;

    public MatViewQueriesXMLGenerator(Table destTable, VoltXMLElement xmlquery,
                                      ArrayList<ParsedColInfo> groupByColumnsParsedInfo,
                                      ArrayList<ParsedColInfo> displayColumnsParsedInfo) {
        m_destTable = destTable;
        m_xml = xmlquery.duplicate();
        m_groupByColumnsParsedInfo = groupByColumnsParsedInfo;
        m_displayColumnsParsedInfo = displayColumnsParsedInfo;
        m_maxElementId = VoltXMLElementHelper.findMaxElementId( m_xml );
        m_fallbackQueryXMLs = new ArrayList<VoltXMLElement>();

        // The following tasks should be called in fixed order!
        adjustColumnAliases();
        generateBuildUpQueryXMLs();
        generateFallbackQueryXMLs();
    }

    private String nextElementId() {
        ++m_maxElementId;
        return String.valueOf( m_maxElementId );
    }

    private String lastElementId() {
        return String.valueOf( m_maxElementId );
    }

    private void adjustColumnAliases() {
        ArrayList<String> realColumnNames = new ArrayList<String>(Collections.nCopies(m_destTable.getColumns().size(), ""));
        List<VoltXMLElement> viewColumns = VoltXMLElementHelper.getFirstChild(m_xml, "columns").children;
        assert(viewColumns.size() == m_destTable.getColumns().size());
        for (Column info : m_destTable.getColumns()) {
            realColumnNames.set(info.getIndex(), info.getName());
        }
        for (int i=0; i<viewColumns.size(); ++i) {
            VoltXMLElement columnref = viewColumns.get(i);
            String realColumnName = realColumnNames.get(i);
            columnref.attributes.put("alias", realColumnName);
        }
    }

    private void generateBuildUpQueryXMLs() {
        // This function will generate the XML for materialized view build up query:
        // INSERT INTO target_table SELECT ...
        // 1. Create insert node.
        m_buildUpQueryXML = new VoltXMLElement("insert");
        m_buildUpQueryXML.attributes.put("table", m_destTable.getTypeName());
        // 2. Add columns
        VoltXMLElement columns = new VoltXMLElement("columns");
        m_buildUpQueryXML.children.add(columns);
        List<VoltXMLElement> viewColumns = VoltXMLElementHelper.getFirstChild(m_xml, "columns").children;
        for (VoltXMLElement columnref : viewColumns) {
            VoltXMLElement column = new VoltXMLElement("column");
            column.attributes.put("name", columnref.attributes.get("alias"));
            columns.children.add(column);
        }
        m_buildUpQueryXML.children.add(m_xml.duplicate());;
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
                String valueType = m_groupByColumnsParsedInfo.get(i).expression.getValueType().getName();
                VoltXMLElement column = groupcolumns.get(i);
                // Add the column to the parameter list.
                parameters.add( VoltXMLElementHelper.buildParamElement( nextElementId(), index, valueType ) );
                // Put together the where conditions for the groupby columns.
                VoltXMLElement columnParamJoincond = VoltXMLElementHelper.buildColumnParamJoincondElement( "equal", column, lastElementId(), nextElementId() );
                joincond = VoltXMLElementHelper.mergeTwoElementsUsingOperator( "and", nextElementId(), joincond, columnParamJoincond );
            }
            // Remove the group by columns, they are now in the form of where conditions.
            m_xml.children.remove(groupcolumnsElement);
        }

        // 2. Process aggregation columns =====================================================================
        List<VoltXMLElement> originalColumns = new ArrayList<VoltXMLElement>();
        originalColumns.addAll(columns);
        columns.clear();
        // Parameter index for min/max column
        String paramIndex = String.valueOf(m_groupByColumnsParsedInfo.size());
        // Add one min/max columns at a time as a new fallback query XML.
        for (int i=m_groupByColumnsParsedInfo.size()+1; i<m_displayColumnsParsedInfo.size(); ++i) {
            VoltXMLElement column = originalColumns.get(i);
            String optype = column.attributes.get("optype");
            if ( optype.equals("min") || optype.equals("max") ) {
                columns.add(column);
                VoltXMLElement aggArg = column.children.get(0);
                String operator = optype.equals("min") ? "greaterthanorequalto" : "lessthanorequalto";
                String valueType = m_displayColumnsParsedInfo.get(i).expression.getValueType().getName();
                parameters.add( VoltXMLElementHelper.buildParamElement(nextElementId(), paramIndex, valueType) );
                VoltXMLElement aggArgJoincond = VoltXMLElementHelper.buildColumnParamJoincondElement(operator, aggArg, lastElementId(), nextElementId());
                joincondList.add( VoltXMLElementHelper.mergeTwoElementsUsingOperator("and", nextElementId(), joincond, aggArgJoincond) );
                m_fallbackQueryXMLs.add(m_xml.duplicate());
                // For debug:
                // System.out.println(m_xml.toString());
                columns.clear();
                joincondList.clear();
            }
        }
    }

    public VoltXMLElement getBuildUpQueryXML() {
        return m_buildUpQueryXML;
    }

    public List<VoltXMLElement> getFallbackQueryXMLs() {
        return m_fallbackQueryXMLs;
    }
}