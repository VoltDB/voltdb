/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.calciteadapter.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.json_voltpatches.JSONException;
import org.voltdb.calciteadapter.converter.RexConverter;
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
import org.voltdb.types.SortDirectionType;
import org.voltdb.utils.CatalogUtil;

public class IndexUtil {

    /**
     * Given a table, a predicate expression, and an index, find
     * the best way to access the data using the given index, or return null if no good way exists.
     *
     * @param table The table we want data from.
     * @param catColumns The table columns
     * @param condRef RexNode representing the predicate expression.
     * @param program Program to resolve the condRef expression if its a reference expression
     * @param index The index we want to use to access the data.
     * @param sortDirection sort direction to use
     *
     * @return A valid access path using the data or null if none found.
     */
    public static AccessPath getCalciteRelevantAccessPathForIndex(Table table,
            List<Column> catColumns,
            RexNode condRef,
            RexProgram program,
            Index index,
            SortDirectionType sortDirection) {
        return getCalciteRelevantAccessPathForIndex(
                table, catColumns, condRef, program, index, sortDirection, -1);
    }

    /**
     * Given a table (stand alone or from a join), a predicate expression, and an index, find
     * the best way to access the data using the given index, or return null if no good way exists.
     * If the numLhsFieldsForJoin is specified and != -1 it means that this table is on the inner side
     * of a join.
     *
     * @param table The table we want data from.
     * @param catColumns The table columns
     * @param condRef RexNode representing the predicate expression.
     * @param program Program to resolve the condRef expression if its a reference expression
     * @param index The index we want to use to access the data.
     * @param sortDirection sort direction to use
     * @param numLhsFieldsForJoin number of fields that come from outer table (-1 if not a join)
     *
     * @return A valid access path using the data or null if none found.
     */
    public static AccessPath getCalciteRelevantAccessPathForIndex(Table table,
            List<Column> catColumns,
            RexNode condRef,
            RexProgram program,
            Index index,
            SortDirectionType sortDirection,
            int numLhsFieldsForJoin) {
        // Get filter condition or NULL
        if (condRef == null) {
            // No filters to pick an index
            return null;
        }

        // Convert Calcite expressions to VoltDB ones
        AbstractExpression voltExpr = RexConverter.convertRefExpression(
                condRef, table.getTypeName(), catColumns, program, numLhsFieldsForJoin);
        Collection<AbstractExpression> voltSubExprs = ExpressionUtil.uncombineAny(voltExpr);

        StmtTableScan tableScan = new StmtTargetTableScan(table, table.getTypeName(), 0);
        AccessPath accessPath = SubPlanAssembler.getRelevantAccessPathForIndexForCalcite(tableScan, voltSubExprs, index, sortDirection);

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
    public static List<RelFieldCollation> getIndexCollationFields(Table catTable, Index index, RexProgram program) {
        String exprsjson = index.getExpressionsjson();
        List<RelFieldCollation> collationsList = new ArrayList<>();
        if (exprsjson.isEmpty()) {
            List<ColumnRef> indexedColRefs = CatalogUtil.getSortedCatalogItems(index.getColumns(), "index");
            for (ColumnRef cr : indexedColRefs) {
                collationsList.add(new RelFieldCollation(cr.getColumn().getIndex()));
            }
        } else {
            try {
                StmtTableScan tableScan = new StmtTargetTableScan(catTable, catTable.getTypeName(), 0);
                List<Column> columns = CatalogUtil.getSortedCatalogItems(catTable.getColumns(), "index");
                // Convert each Calcite expression from Program to a VoltDB one and keep track of its index
                Map<AbstractExpression, Integer> convertedProgExprs = new HashMap<>();
                int exprIdx = 0;
                for (RexNode expr : program.getExprList()) {
                    AbstractExpression convertedExpr = RexConverter.convertRefExpression(
                            expr,
                            tableScan.getTableName(),
                            columns,
                            program,
                            -1);
                    convertedProgExprs.put(convertedExpr, exprIdx++);
                }

                // Build a collation based on index expressions
                List<AbstractExpression> indexedExprs = AbstractExpression.fromJSONArrayString(exprsjson, tableScan);
                for (AbstractExpression indexExpr : indexedExprs) {
                    Integer indexExprIdx = convertedProgExprs.get(indexExpr);
                    if (indexExprIdx != null) {
                        collationsList.add(new RelFieldCollation(indexExprIdx));
                    } else {
                        // Break on a first mismatch.
                        break;
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace();
                assert(false);
                return null;
            }
        }
        return collationsList;
    }

}
