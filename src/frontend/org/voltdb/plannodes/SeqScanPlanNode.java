/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.plannodes;

import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.types.PlanNodeType;

public class SeqScanPlanNode extends AbstractScanPlanNode {

    public SeqScanPlanNode() {
        super();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.SEQSCAN;
    }

    /**
     * Accessor for flag marking the plan as guaranteeing an identical result/effect
     * when "replayed" against the same database state, such as during replication or CL recovery.
     * @return false
     */
    @Override
    public boolean isOrderDeterministic() {
        return false; // TODO: enhance to return true for any supportable cases of in-order storage
    }

    @Override
    public void computeCostEstimates(long childOutputTupleCountEstimate, Cluster cluster, Database db, DatabaseEstimates estimates, ScalarValueHints[] paramHints) {
        Table target = db.getTables().getIgnoreCase(m_targetTableName);
        assert(target != null);
        DatabaseEstimates.TableEstimates tableEstimates = estimates.getEstimatesForTable(target.getTypeName());
        // This maxTuples value estimates the number of tuples fetched from the sequential scan.
        // It's a vague measure of the cost of the scan.
        // Its accuracy depends a lot on what kind of post-filtering or projection needs to happen, if any.
        // The tuplesRead value is also used to estimate the number of RESULT rows, regardless of
        // how effective post-filtering might be -- as if all rows passed the filters.
        // This is at least semi-consistent with the ignoring of post-filter effects in IndexScanPlanNode.
        // In effect, though, it gives index scans an "unfair" advantage when they reduce the estimated result size
        // by taking into account the indexed filters -- follow-on plan steps, sorts (etc.), are costed lower
        // as if they are operating on fewer rows than would have come out of the seqscan,
        // though that's nonsense.
        // In any case, it's important to keep an eye on any changes (discounts) to SeqScanPlanNode's costing
        // here to make sure that SeqScanPlanNode never gains an unfair advantage over IndexScanPlanNode.
        m_estimatedProcessedTupleCount = tableEstimates.maxTuples;
        m_estimatedOutputTupleCount = tableEstimates.maxTuples;
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "SEQUENTIAL SCAN of \"" + m_targetTableName + "\"" + explainPredicate("\n" + indent + " filter by ");
    }
}
