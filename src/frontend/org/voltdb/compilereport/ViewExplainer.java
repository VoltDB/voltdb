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

package org.voltdb.compilereport;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.IndexRef;
import org.voltdb.catalog.MaterializedViewHandlerInfo;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.types.ExpressionType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;

// Generate a report that can explain to the users how we maintain a view.
public class ViewExplainer {

    private static String getIndexNameUsedInStatement(Statement stmt) {
        // Get the name of the index used in a statement (query plan)
        // that is used for refreshing a min/max column in a single-table view.
        String[] indicesUsedInStatement = stmt.getIndexesused().split(",");
        assert(indicesUsedInStatement.length <= 1);
        for (String tableDotIndexPair : indicesUsedInStatement) {
            if (tableDotIndexPair.length() == 0) {
                continue;
            }
            String parts[] = tableDotIndexPair.split("\\.", 2);
            assert(parts.length == 2);
            if (parts.length != 2) {
                continue;
            }
            return parts[1];
        }
        return "";
    }

    public static ArrayList<String[]> explain(Table viewTable) throws Exception {
        String viewName = viewTable.getTypeName();
        MaterializedViewHandlerInfo mvHandlerInfo = viewTable.getMvhandlerinfo().get("mvHandlerInfo");
        MaterializedViewInfo mvInfo = null;
        CatalogMap<Statement> fallBackQueryStmts;
        List<Column> destColumnArray = CatalogUtil.getSortedCatalogItems(viewTable.getColumns(), "index");
        ArrayList<String[]> retval = new ArrayList<String[]>();

        // Is this view single-table?
        if (mvHandlerInfo == null) {
            // If this is not a multi-table view, we need to go to its source table for metadata.
            // (Legacy code for single table view uses a different model and code path)
            if (viewTable.getMaterializer() == null) {
                // If we cannot find view metadata from both locations, this table is not a materialized view.
                throw new Exception("Table " + viewName + " is not a view.");
            }
            mvInfo = viewTable.getMaterializer().getViews().get(viewName);
            fallBackQueryStmts = mvInfo.getFallbackquerystmts();
        }
        else {
            // For multi-table views we need to show the query plan for evaluating joins.
            Statement createQuery = mvHandlerInfo.getCreatequery().get("createQuery");
            retval.add(new String[]
                           {"Join Evaluation", Encoder.hexDecodeToString(createQuery.getExplainplan())});
            fallBackQueryStmts = mvHandlerInfo.getFallbackquerystmts();
        }
        // For each min/max column find out if an execution plan is used.
        int minMaxAggIdx = 0;
        for (int j = 0; j < destColumnArray.size(); j++) {
            Column destColumn = destColumnArray.get(j);
            ExpressionType aggType = ExpressionType.get(destColumn.getAggregatetype());
            if (aggType == ExpressionType.AGGREGATE_MIN || aggType == ExpressionType.AGGREGATE_MAX) {
                Statement fallBackQueryStmt = fallBackQueryStmts.get(String.valueOf(minMaxAggIdx));
                // How this min/max will be refreshed?
                String plan = "";
                // For single-table views, check if we uses:
                //   * built-in sequential scan
                //   * built-in index scan
                //   * execution plan
                if (mvHandlerInfo == null) {
                    CatalogMap<IndexRef> hardCodedIndicesForSingleTableView = mvInfo.getIndexforminmax();
                    String hardCodedIndexName = hardCodedIndicesForSingleTableView.get(String.valueOf(minMaxAggIdx)).getName();
                    String indexNameUsedInStatement = getIndexNameUsedInStatement(fallBackQueryStmt);
                    if (! indexNameUsedInStatement.equalsIgnoreCase(hardCodedIndexName)) {
                        plan = Encoder.hexDecodeToString(fallBackQueryStmt.getExplainplan());
                    }
                    // If we do not use execution plan, see which built-in method is used.
                    if (plan.equals("")) {
                        if (hardCodedIndexName.equals("")) {
                            plan = "Built-in sequential scan.";
                        }
                        else {
                            plan = "Built-in index scan \"" + hardCodedIndexName + "\".";
                        }
                    }
                }
                else {
                    plan = Encoder.hexDecodeToString(fallBackQueryStmt.getExplainplan());
                }
                retval.add(new String[]
                           {"Refresh " + (aggType == ExpressionType.AGGREGATE_MIN ? "MIN" : "MAX") + " column \"" +
                            destColumn.getName() + "\"", plan});
                minMaxAggIdx++;
            }
        }
        return retval;
    }
}
