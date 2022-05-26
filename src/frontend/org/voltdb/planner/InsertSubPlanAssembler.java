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

package org.voltdb.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.parseinfo.StmtSubqueryScan;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.types.PlanNodeType;

public class InsertSubPlanAssembler extends SubPlanAssembler {

    private boolean m_bestAndOnlyPlanWasGenerated = false;
    final private boolean m_targetIsExportTable;

    InsertSubPlanAssembler(AbstractParsedStmt parsedStmt,
            StatementPartitioning partitioning, boolean targetIsExportTable) {
        super(parsedStmt, partitioning);
        m_targetIsExportTable = targetIsExportTable;
    }

    @Override
    AbstractPlanNode nextPlan() {
        if (m_bestAndOnlyPlanWasGenerated) {
            return null;
        }

        // We may generate a few different plans for the subquery, but by the time
        // we get here, we'll generate only one plan for the INSERT statement itself.
        // Mostly this method exists to check that we can find a valid partitioning
        // for the statement.

        m_bestAndOnlyPlanWasGenerated = true;
        ParsedInsertStmt insertStmt = (ParsedInsertStmt)m_parsedStmt;
        Table targetTable = insertStmt.m_tableList.get(0);
        targetTable.getTypeName();
        StmtSubqueryScan subquery = insertStmt.getSubqueryScan();
        boolean subqueryIsMultiFragment = subquery.getBestCostPlan().rootPlanGraph.hasAnyNodeOfType(PlanNodeType.SEND);

        if (targetTable.getIsreplicated()) {
            // must not be single-partition insert if targeting a replicated table
            // setUpForNewPlans already validates this
            assert(! m_partitioning.wasSpecifiedAsSingle() && ! m_partitioning.isInferredSingle());

            // Cannot access any partitioned tables in subquery for replicated table
            if (! subquery.getIsReplicated()) {
                throw new PlanningErrorException("Subquery in "+ getSqlType() +" INTO ... SELECT statement may not access " +
                                                 "partitioned data for insertion into replicated table " + targetTable.getTypeName() + ".");
            }
        }
        else if (! m_partitioning.wasSpecifiedAsSingle()) {
            //        [assume that c1 is the partitioning column]
            //        INSERT INTO t1 (c1, c2, c3, ...)
            //        SELECT e1, e2, e3, ... FROM ...
            //
            //        can be analyzed as if it was
            //
            //        SELECT COUNT(*)
            //        FROM t1
            //          INNER JOIN
            //            (SELECT e1, e2, e3, ... FROM ...) AS insert_subquery
            //            ON t1.c1 = insert_subquery.e1;
            //
            // Build the corresponding data structures for analysis by StatementPartitioning.

            if (subqueryIsMultiFragment) {
                // What is the appropriate level of detail for this message?
                m_recentErrorMsg = getSqlType() +" INTO ... SELECT statement subquery is too complex.  " +
                    "Please either simplify the subquery or use a SELECT followed by an INSERT.";
                return null;
            }

            Column partitioningCol = targetTable.getPartitioncolumn();
            if (partitioningCol == null) {
                assert (m_targetIsExportTable);
                m_recentErrorMsg = "The target table for an INSERT INTO ... SELECT statement is an "
                        + "stream with no partitioning column defined.  "
                        + "This is not currently supported.  Please define a "
                        + "partitioning column for this stream to use it with INSERT INTO ... SELECT.";
                return null;
            }

            List<StmtTableScan> tables = new ArrayList<>();
            StmtTargetTableScan stmtTargetTableScan = new StmtTargetTableScan(targetTable);
            tables.add(stmtTargetTableScan);
            tables.add(subquery);

            // Create value equivalence between the partitioning column of the target table
            // and the corresponding expression produced by the subquery.

            HashMap<AbstractExpression, Set<AbstractExpression>>  valueEquivalence = new HashMap<>();
            int i = 0;
            boolean setEquivalenceForPartitioningCol = false;
            for (Column col : insertStmt.m_columns.keySet()) {
                if (partitioningCol.compareTo(col) == 0) {
                    List<SchemaColumn> partitioningColumns = stmtTargetTableScan.getPartitioningColumns();
                    assert(partitioningColumns.size() == 1);
                    AbstractExpression targetPartitionColExpr = partitioningColumns.get(0).getExpression();
                    TupleValueExpression selectedExpr = subquery.getOutputExpression(i);
                    assert(!valueEquivalence.containsKey(targetPartitionColExpr));
                    assert(!valueEquivalence.containsKey(selectedExpr));

                    Set<AbstractExpression> equivSet = new HashSet<>();
                    equivSet.add(targetPartitionColExpr);
                    equivSet.add(selectedExpr);

                    valueEquivalence.put(targetPartitionColExpr,  equivSet);
                    valueEquivalence.put(selectedExpr,  equivSet);
                    setEquivalenceForPartitioningCol = true;

                }
                ++i;
            }

            if (!setEquivalenceForPartitioningCol) {
                // partitioning column of target table is not being set from value produced by the subquery.
                m_recentErrorMsg = "Partitioning column must be assigned a value " +
                    "produced by the subquery in an "+ getSqlType() +" INTO ... SELECT statement.";
                return null;
            }

            m_partitioning.analyzeForMultiPartitionAccess(tables, valueEquivalence);

            if (! m_partitioning.isJoinValid()) {
                m_recentErrorMsg = "Partitioning could not be determined for "+ getSqlType() +" INTO ... SELECT statement.  " +
                    "Please ensure that statement does not attempt to copy row data from one partition to another, " +
                    "which is unsupported.";
                return null;
            }
        }


        return subquery.getBestCostPlan().rootPlanGraph;
    }

    public String getSqlType() {
        if (m_parsedStmt.m_isUpsert) {
            return "UPSERT";
        }
        return "INSERT";
    }

}
