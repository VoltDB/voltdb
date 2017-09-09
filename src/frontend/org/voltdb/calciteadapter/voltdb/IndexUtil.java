/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.calciteadapter.voltdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.json_voltpatches.JSONException;
import org.voltdb.calciteadapter.RexConverter;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.planner.AccessPath;
import org.voltdb.planner.SubPlanAssembler;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.utils.CatalogUtil;

public class IndexUtil {

    /**
     * Given a table, a set of predicate expressions and a specific index, find the best way to
     * access the data using the given index, or return null if no good way exists.
     *
     * @param table The table we want data from.
     * @param exprs The set of predicate expressions.
     * @param index The index we want to use to access the data.
     * @return A valid access path using the data or null if none found.
     */
    public static AccessPath getCalciteRelevantAccessPathForIndex(
            Table  table, List<Column> catColumns, RexProgram program, Index index) {
        // Get filter condition or NULL
        RexLocalRef condRef = program.getCondition();
        if (condRef == null) {
            // No filters to pick an index
            return null;
        }
        // Convert Calcite expressions to VoltDB ones
        List<RexNode> exprs = program.getExprList();
        AbstractExpression voltExpr = RexConverter.convertRefExpression(table.getTypeName(), catColumns, condRef, exprs);
        Collection<AbstractExpression> voltSubExprs = ExpressionUtil.uncombineAny(voltExpr);

        StmtTableScan tableScan = new StmtTargetTableScan(table, table.getTypeName(), 0);
        AccessPath accessPath = SubPlanAssembler.getRelevantAccessPathForIndexForCalcite(tableScan, voltSubExprs, index);

        // Partial Index Check
        accessPath = SubPlanAssembler.processPartialIndex(index, tableScan, accessPath, voltSubExprs, null, null);
        return accessPath;
    }

    /**
     * Add access path details to an index scan. Delegates the real work to the SubPlanAssembler
     *
     * @param scanNode Initial index scan plan.
     * @param path The access path to access the data in the table (index/scan/etc).
     * @return An index scan plan node
     */
    public static AbstractPlanNode buildIndexAccessPlanForTable(IndexScanPlanNode scanNode, AccessPath path) {
        return SubPlanAssembler.buildIndexAccessPlanForTable(scanNode, path);
    }

    /**
     * For a given index return either list of index expressions or index's columns ordinal
     *
     * @param tableScan
     * @param index
     * @return Pair<List<AbstractExpression>, List<Integer>>
     */
    public static List<RelFieldCollation> getIndexCollationFields(StmtTableScan tableScan, Index index, RexProgram program) {
        String exprsjson = index.getExpressionsjson();
        List<RelFieldCollation> collationsList = new ArrayList<>();
        if (exprsjson.isEmpty()) {
            List<ColumnRef> indexedColRefs = CatalogUtil.getSortedCatalogItems(index.getColumns(), "index");
            for (ColumnRef cr : indexedColRefs) {
                collationsList.add(new RelFieldCollation(cr.getColumn().getIndex()));
            }
        } else {
            try {
                List<AbstractExpression> indexedExprs = AbstractExpression.fromJSONArrayString(exprsjson, tableScan);
                for (AbstractExpression indexExpr : indexedExprs) {
                    RexLocalRef indexLocalRef = RexConverter.convertAbstractExpression(indexExpr, program);
                    collationsList.add(new RelFieldCollation(indexLocalRef.getIndex()));
                }
            } catch (JSONException e) {
                e.printStackTrace();
                assert(false);
                return null;
            }
        }
        return collationsList;
    }

    public static List<AccessPath> generateOuterAccessPaths() {
        return null;
    }
}
