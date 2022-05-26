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

import java.util.ArrayDeque;
import java.util.Deque;

import org.voltdb.catalog.Table;
import org.voltdb.planner.parseinfo.JoinNode;
import org.voltdb.plannodes.AbstractPlanNode;

/**
 * For a delete or update plan, this class builds the part of the plan
 * which collects tuples from relations. Given the tables and the predicate
 * (and sometimes the output columns), this will build a plan that will output
 * matching tuples to a temp table. A delete, update or send plan node can then
 * be glued on top of it. In selects, aggregation and other projections are also
 * done on top of the result from this class.
 *
 */
public class WriterSubPlanAssembler extends SubPlanAssembler {

    /** The only table involved in this update or delete stmt */
    final Table m_targetTable;

    /** The list of generated plans. This allows generation in batches.*/
    final Deque<AbstractPlanNode> m_plans = new ArrayDeque<>();

    /** Only create access plans once - all are created in the first pass. */
    boolean m_generatedPlans = false;

    /**
     * @param parsedStmt The parsed and dissected statement object describing the sql to execute.
     * @param partitioning Describes the specified and inferred partition context.
     */
    WriterSubPlanAssembler(AbstractParsedStmt parsedStmt, StatementPartitioning partitioning) {
        super(parsedStmt, partitioning);
        assert(m_parsedStmt.m_tableList.size() == 1);
        m_targetTable = m_parsedStmt.m_tableList.get(0);
    }

    /**
     * Pull a join order out of the join orders deque, compute all possible plans
     * for that join order, then append them to the computed plans deque.
     */
    @Override
    AbstractPlanNode nextPlan() {
        if (!m_generatedPlans) {
            assert (m_parsedStmt.m_joinTree != null);
            // Clone the node to make make sure that analyze expression is called
            // only once on the node.
            JoinNode tableNode = (JoinNode) m_parsedStmt.m_joinTree.clone();
            // Analyze join conditions
            tableNode.analyzeJoinExpressions(m_parsedStmt);
            // these just shouldn't happen right?
            assert m_parsedStmt.m_noTableSelectionList.isEmpty();

            m_generatedPlans = true;
            // This is either UPDATE or DELETE statement. Consolidate all expressions
            // into the WHERE list.
            tableNode.m_whereInnerList.addAll(tableNode.m_joinInnerList);
            tableNode.m_joinInnerList.clear();
            tableNode.m_accessPaths.addAll(getRelevantAccessPathsForTable(tableNode.getTableScan(),
                    null, tableNode.m_whereInnerList, null));

            for (AccessPath path : tableNode.m_accessPaths) {
                tableNode.m_currentAccessPath = path;
                AbstractPlanNode plan = getAccessPlanForTable(tableNode);
                m_plans.add(plan);
            }
        }
        return m_plans.poll();
    }

}
