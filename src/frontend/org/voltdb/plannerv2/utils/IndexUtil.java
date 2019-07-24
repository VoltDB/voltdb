/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.plannerv2.utils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google_voltpatches.common.collect.Lists;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.json_voltpatches.JSONException;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.planner.AccessPath;
import org.voltdb.planner.SubPlanAssembler;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.plannerv2.converter.RexConverter;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.types.SortDirectionType;
import org.voltdb.utils.CatalogUtil;

public class IndexUtil {

    /**
     * Given a table (stand alone or from a join), a predicate expression, and an index, find
     * the best way to access the data using the given index, or return null if no good way exists.
     * If numOuterFieldsForJoin != -1 then the expression belongs to a stand-alone table.
     *
     * @param table The table we want data from.
     * @param catColumns The table columns
     * @param condRef RexNode representing the predicate expression.
     * @param program Program to resolve the condRef expression if its a reference expression
     * @param index The index we want to use to access the data.
     * @param sortDirection sort direction to use
     * @param numOuterFieldsForJoin number of fields that come from outer table (-1 if not a join)
     * @param isInnerTable if the table is the inner relation
     *
     * @return A valid access path using the data or null if none found.
     */
    public static AccessPath getCalciteRelevantAccessPathForIndex(
            Table table, List<Column> catColumns, RexNode condRef, RexProgram program,
            Index index, SortDirectionType sortDirection, int numOuterFieldsForJoin, boolean isInnerTable) {
        // Get filter condition or NULL
        if (condRef == null) { // No filters to pick an index
            return null;
        } else { // Convert Calcite expressions to VoltDB ones
            final Collection<AbstractExpression> voltSubExprs = ExpressionUtil.uncombineAny(
                    RexConverter.convertRefExpression(condRef, table.getTypeName(), catColumns,
                            program, numOuterFieldsForJoin, isInnerTable));
            final StmtTableScan tableScan = new StmtTargetTableScan(table, table.getTypeName(), 0);
            // Partial Index Check
            return SubPlanAssembler.verifyIfPartialIndex(index, tableScan,
                    SubPlanAssembler.getRelevantAccessPathForIndexForCalcite(
                            tableScan, voltSubExprs, index, sortDirection),
                    voltSubExprs, null, null);
        }
    }

    /**
     * Add access path details to an index scan. Delegates the real work to the SubPlanAssembler
     *
     * @param scanNode Initial index scan plan.
     * @param path The access path to access the data in the table (index/scan/etc).
     * @param tableIdx - 1 if a scan is an inner scan of the NJIJ. 0 otherwise.
     *
     * @return An index scan plan node
     */
    public static AbstractPlanNode buildIndexAccessPlanForTable(
            IndexScanPlanNode scanNode, AccessPath path, int tableIdx) {
        return SubPlanAssembler.buildIndexAccessPlanForTable(scanNode, path, tableIdx);
    }

    /**
     * For a given index return either list of index expressions or index's columns ordinal
     *
     * @param tableScan
     * @param index
     * @return Pair<List<AbstractExpression>, List<Integer>>
     */
    static List<RelFieldCollation> getIndexCollationFields(
            Table table, Index index, RexProgram program) throws JSONException {
        final String json = index.getExpressionsjson();
        if (json.isEmpty()) {
            return Lists.transform(CatalogUtil.getSortedCatalogItems(index.getColumns(), "index"),
                    cr -> new RelFieldCollation(cr.getColumn().getIndex()));
        } else {
            final StmtTableScan tableScan = new StmtTargetTableScan(table, table.getTypeName(), 0);
            final List<Column> columns = CatalogUtil.getSortedCatalogItems(table.getColumns(), "index");
            // Convert each Calcite expression from Program to a VoltDB one and keep track of its index
            final AtomicInteger exprIndex = new AtomicInteger(0);
            final Map<AbstractExpression, Integer> convertedProgExprs = program.getExprList().stream()
                    .map(expr -> new AbstractMap.SimpleEntry<>(
                            RexConverter.convertRefExpression(
                                    expr, tableScan.getTableName(), columns, program, -1, false),
                            exprIndex.getAndIncrement()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b)-> b));
            // Build a collation based on index expressions
            return AbstractExpression.fromJSONArrayString(json, tableScan).stream()
                    .flatMap(expr -> {
                        final Integer indexExprIdx = convertedProgExprs.get(expr);
                        return indexExprIdx == null ? Stream.empty() : Stream.of(new RelFieldCollation(indexExprIdx));
                    }).collect(Collectors.toList());
        }
    }

}
