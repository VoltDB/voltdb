/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.planner;

import java.util.*;
import org.voltdb.catalog.*;
import org.voltdb.plannodes.*;

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
    Table m_targetTable;

    /** The list of generated plans. This allows generation in batches.*/
    ArrayDeque<AbstractPlanNode> m_plans = new ArrayDeque<AbstractPlanNode>();

    /** Only create access plans once - all are created in the first pass. */
    boolean m_generatedPlans = false;

    /**
     *
     * @param db The catalog's Database object.
     * @param parsedStmt The parsed and dissected statement object describing the sql to execute.
     * @param singlePartition Does this statement access one or multiple partitions?
     */
    WriterSubPlanAssembler(Database db, AbstractParsedStmt parsedStmt, boolean singlePartition,
                           int partitionCount)
    {
        super(db, parsedStmt, singlePartition, partitionCount);

        assert(m_parsedStmt.tableList.size() == 1);
        m_targetTable = m_parsedStmt.tableList.get(0);
    }

    /**
     * Pull a join order out of the join orders deque, compute all possible plans
     * for that join order, then append them to the computed plans deque.
     */
    @Override
    AbstractPlanNode nextPlan() {
        if (!m_generatedPlans) {
            m_generatedPlans = true;
            // for each table, just add the empty access path (the full table scan)
            Table nextTables[] = new Table[0];
            ArrayList<AccessPath> paths = getRelevantAccessPathsForTable(m_targetTable, nextTables);

            // for each access path
            for (AccessPath accessPath : paths) {
                // get a plan
                AbstractPlanNode scanPlan = getAccessPlanForTable(m_targetTable, accessPath);
                m_plans.add(scanPlan);
            }

        }
        return m_plans.poll();
    }

    /**
     * Determines whether a table will require a distributed scan.
     * @param table The table that may or may not require a distributed scan
     * @return true if the table requires a distributed scan, false otherwise
     */
    @Override
    protected boolean tableRequiresDistributedScan(Table table) {
        return m_singlePartition == false;
    }
}
