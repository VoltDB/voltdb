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

package org.voltdb.plannodes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.PlanStatistics;
import org.voltdb.planner.StatsField;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.IndexType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

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

    // A reference to the Catalog index object which defined the index which
    // this index scan is going to use
    // XXX-IZZY use more specific data later, perhaps, rather than whole object
    protected Index m_catalogIndex = null;

    public IndexScanPlanNode() {
        super();
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

    public void setCatalogIndex(Index index)
    {
        m_catalogIndex = index;
    }

    public Index getCatalogIndex()
    {
        return m_catalogIndex;
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
    public void setEndExpression(AbstractExpression endExpression)
    {
        if (endExpression != null)
        {
            // PlanNodes all need private deep copies of expressions
            // so that the resolveColumnIndexes results
            // don't get bashed by other nodes or subsequent planner runs
            try
            {
                m_endExpression = (AbstractExpression) endExpression.clone();
            }
            catch (CloneNotSupportedException e)
            {
                // This shouldn't ever happen
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    public void addSearchKeyExpression(AbstractExpression expr)
    {
        if (expr != null)
        {
            // PlanNodes all need private deep copies of expressions
            // so that the resolveColumnIndexes results
            // don't get bashed by other nodes or subsequent planner runs
            try
            {
                m_searchkeyExpressions.add((AbstractExpression) expr.clone());
            }
            catch (CloneNotSupportedException e)
            {
                // This shouldn't ever happen
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    /**
     * @return the searchkey_expressions
     */
    // Please don't use me to add search key expressions.  Use
    // addSearchKeyExpression() so that the expression gets cloned
    public List<AbstractExpression> getSearchKeyExpressions() {
        return Collections.unmodifiableList(m_searchkeyExpressions);
    }

    @Override
    public void resolveColumnIndexes()
    {
        // IndexScanPlanNode has TVEs that need index resolution in:
        // m_searchkeyExpressions
        // m_endExpression

        // Collect all the TVEs in the end expression and search key expressions
        List<TupleValueExpression> index_tves =
            new ArrayList<TupleValueExpression>();
        index_tves.addAll(ExpressionUtil.getTupleValueExpressions(m_endExpression));
        for (AbstractExpression search_exp : m_searchkeyExpressions)
        {
            index_tves.addAll(ExpressionUtil.getTupleValueExpressions(search_exp));
        }
        // and update their indexes against the table schema
        for (TupleValueExpression tve : index_tves)
        {
            int index = m_tableSchema.getIndexOfTve(tve);
            tve.setColumnIndex(index);
        }
        // now do the common scan node work
        super.resolveColumnIndexes();
    }

    @Override
    public boolean computeEstimatesRecursively(PlanStatistics stats, Cluster cluster, Database db, DatabaseEstimates estimates, ScalarValueHints[] paramHints) {

        // HOW WE COST INDEXES
        // unique, covering index always wins
        // otherwise, pick the index with the most columns covered otherwise
        // count non-equality scans as -0.5 coverage
        // prefer array to hash to tree, all else being equal

        // FYI: Index scores should range between 1 and 48898 (I think)

        Table target = db.getTables().getIgnoreCase(m_targetTableName);
        assert(target != null);
        DatabaseEstimates.TableEstimates tableEstimates = estimates.getEstimatesForTable(target.getTypeName());
        stats.incrementStatistic(0, StatsField.TREE_INDEX_LEVELS_TRAVERSED, (long)(Math.log(tableEstimates.maxTuples)));

        // get the width of the index and number of columns used
        int colCount = m_catalogIndex.getColumns().size();
        int keyWidth = m_searchkeyExpressions.size();
        assert(keyWidth <= colCount);

        // need a double for math
        double keyWidthFl = keyWidth;
        // count a scan as a half cover
        if (m_lookupType != IndexLookupType.EQ)
            keyWidthFl -= 0.5;
        // when choosing between multiple matched indexes with 10 or more columns,
        // we won't always pick the optimal one.
        // if you hit this, you're probably in a silly use case
        final double MAX_INTERESTING_INDEX_WIDTH = 10.0;
        if (keyWidthFl > MAX_INTERESTING_INDEX_WIDTH)
            keyWidthFl = MAX_INTERESTING_INDEX_WIDTH;

        // estimate cost of scan
        int tuplesToRead = 0;

        // minor priorities for index types (tiebreakers)
        if (m_catalogIndex.getType() == IndexType.ARRAY.getValue())
            tuplesToRead = 1;
        if (m_catalogIndex.getType() == IndexType.HASH_TABLE.getValue())
            tuplesToRead = 2;
        if ((m_catalogIndex.getType() == IndexType.BALANCED_TREE.getValue()) ||
            (m_catalogIndex.getType() == IndexType.BTREE.getValue()))
            tuplesToRead = 3;
        assert(tuplesToRead > 0);

        // if not a unique, covering index, pick the choice with the most columns
        if (!m_catalogIndex.getUnique() || (colCount != keyWidth)) {
            assert(keyWidthFl <= MAX_INTERESTING_INDEX_WIDTH);
            // cost starts at 100 and goes down by 10 for each column used
            final double MAX_TUPLES_READ = MAX_INTERESTING_INDEX_WIDTH * 10.0;
            tuplesToRead += (int) (10.0 * (MAX_TUPLES_READ - keyWidthFl));
        }

        stats.incrementStatistic(0, StatsField.TUPLES_READ, tuplesToRead);
        m_estimatedOutputTupleCount = tuplesToRead;

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

    @Override
    protected String explainPlanForNode(String indent) {
        assert(m_catalogIndex != null);

        int indexSize = m_catalogIndex.getColumns().size();
        int keySize = m_searchkeyExpressions.size();

        String scanType = "unique-scan";
        if (m_lookupType != IndexLookupType.EQ)
            scanType = "range-scan";

        String cover = "covering";
        if (indexSize > keySize)
            cover = String.format("%d/%d cols", keySize, indexSize);

        String usageInfo = String.format("(%s %s)", scanType, cover);
        if (keySize == 0)
            usageInfo = "(for sort order only)";

        String retval = "INDEX SCAN of \"" + m_targetTableName + "\"";
        retval += " using \"" + m_targetIndexName + "\"";
        retval += " " + usageInfo;
        return retval;
    }
}
