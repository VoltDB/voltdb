/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.plannodes;

import java.util.*;
import org.json.JSONException;
import org.json.JSONStringer;
import org.json.JSONString;
import org.voltdb.expressions.*;
import org.voltdb.planner.PlanStatistics;
import org.voltdb.planner.PlannerContext;
import org.voltdb.planner.StatsField;
import org.voltdb.types.*;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;

public class IndexScanPlanNode extends AbstractScanPlanNode {

    public enum Members {
        TARGET_INDEX_NAME,
        END_EXPRESSION,
        SEARCHKEY_EXPRESSIONS,
        KEY_ITERATE,
        LOOKUP_TYPE,
        SORT_DIRECTION;
    }

    /**
     * Attributes
     * NOTE: The IndexScanPlanNode will use AbstractScanPlanNode's m_predicate
     * as the "Post-Scan Predicate Expression". When this is defined, the EE will
     * run a tuple through an additional predicate to see whether it qualifies.
     * This is necessary when we have a predicate that includes columns that are not
     * all in the index that was selected.
     */

    // The index to use in the scan operation
    protected String m_targetIndexName;

    // When this expression evaluates to true, we will stop scanning
    protected AbstractExpression m_endExpression;

    // This list of expressions corresponds to the values that we will use
    // at runtime in the lookup on the index
    protected List<AbstractExpression> m_searchkeyExpressions = new ArrayList<AbstractExpression>();

    // ???
    protected Boolean m_keyIterate = false;

    // The overall index lookup operation type
    protected IndexLookupType m_lookupType = IndexLookupType.EQ;

    // The sorting direction
    protected SortDirectionType m_sortDirection = SortDirectionType.INVALID;

    public IndexScanPlanNode(PlannerContext context) {
        super(context);
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.INDEXSCAN;
    }

    @Override
    public void validate() throws Exception {
        super.validate();

        // There needs to be at least one search key expression
        if (m_searchkeyExpressions.isEmpty()) {
            throw new Exception("ERROR: There were no search key expressions defined for " + this);
        }

        // Validate Expression Trees
        if (m_endExpression != null) {
            m_endExpression.validate();
        }
        for (AbstractExpression exp : m_searchkeyExpressions) {
            exp.validate();
        }
    }

    /**
     *
     * @param keyIterate
     */
    public void setKeyIterate(Boolean keyIterate) {
        m_keyIterate = keyIterate;
    }

    /**
     *
     * @return Does this scan iterate over values in the index.
     */
    public Boolean getKeyIterate() {
        return m_keyIterate;
    }

    /**
     *
     * @return The type of this lookup.
     */
    public IndexLookupType getLookupType() {
        return m_lookupType;
    }

    /**
     * @return The sorting direction.
     */
    public SortDirectionType getSortDirection() {
        return m_sortDirection;
    }

    /**
     *
     * @param lookupType
     */
    public void setLookupType(IndexLookupType lookupType) {
        m_lookupType = lookupType;
    }

    /**
     * @param sortDirection
     *            the sorting direction
     */
    public void setSortDirection(SortDirectionType sortDirection) {
        m_sortDirection = sortDirection;
    }

    /**
     * @return the target_index_name
     */
    public String getTargetIndexName() {
        return m_targetIndexName;
    }

    /**
     * @param targetIndexName the target_index_name to set
     */
    public void setTargetIndexName(String targetIndexName) {
        m_targetIndexName = targetIndexName;
    }

    /**
     * @return the post_predicate
     */
    public AbstractExpression getEndExpression() {
        return m_endExpression;
    }

    /**
     * @param endExpression the end expression to set
     */
    public void setEndExpression(AbstractExpression endExpression) {
        m_endExpression = endExpression;
    }

    /**
     * @return the searchkey_expressions
     */
    public List<AbstractExpression> getSearchKeyExpressions() {
        return m_searchkeyExpressions;
    }

    @Override
    public boolean computeEstimatesRecursively(PlanStatistics stats, Cluster cluster, Database db, DatabaseEstimates estimates, ScalarValueHints[] paramHints) {
        Table target = db.getTables().getIgnoreCase(m_targetTableName);
        assert(target != null);
        DatabaseEstimates.TableEstimates tableEstimates = estimates.getEstimatesForTable(target.getTypeName());
        stats.incrementStatistic(0, StatsField.TREE_INDEX_LEVELS_TRAVERSED, (long)(Math.log(tableEstimates.maxTuples)));
        stats.incrementStatistic(0, StatsField.TUPLES_READ, 1);
        m_estimatedOutputTupleCount = 1;
        return true;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.KEY_ITERATE.name()).value(m_keyIterate);
        stringer.key(Members.LOOKUP_TYPE.name()).value(m_lookupType.toString());
        stringer.key(Members.SORT_DIRECTION.name()).value(m_sortDirection.toString());
        stringer.key(Members.TARGET_INDEX_NAME.name()).value(m_targetIndexName);
        stringer.key(Members.END_EXPRESSION.name());
        stringer.value(m_endExpression);

        stringer.key(Members.SEARCHKEY_EXPRESSIONS.name()).array();
        for (AbstractExpression ae : m_searchkeyExpressions) {
            assert (ae instanceof JSONString);
            stringer.value(ae);
        }
        stringer.endArray();
    }
}
