/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.hsqldb_voltpatches.HSQLInterface;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.IndexType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;
import org.voltdb.utils.CatalogUtil;

public class IndexScanPlanNode extends AbstractScanPlanNode {

    public enum Members {
        TARGET_INDEX_NAME,
        END_EXPRESSION,
        SEARCHKEY_EXPRESSIONS,
        INITIAL_EXPRESSION,
        SKIP_NULL_PREDICATE,
        KEY_ITERATE,
        LOOKUP_TYPE,
        PURPOSE,
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
    protected final List<AbstractExpression> m_searchkeyExpressions = new ArrayList<AbstractExpression>();

    // for reverse scan LTE only.
    // The initial expression is needed to control a (short?) forward scan to adjust the start of a reverse
    // iteration after it had to initially settle for starting at "greater than a prefix key".
    private AbstractExpression m_initialExpression;

    // The predicate for underflow case using the index
    private AbstractExpression m_skip_null_predicate;

    // The overall index lookup operation type
    protected IndexLookupType m_lookupType = IndexLookupType.EQ;

    // The sorting direction
    protected SortDirectionType m_sortDirection = SortDirectionType.INVALID;

    // A reference to the Catalog index object which defined the index which
    // this index scan is going to use
    protected Index m_catalogIndex = null;

    private ArrayList<AbstractExpression> m_bindings = new ArrayList<AbstractExpression>();;

    private static final int FOR_SCANNING_PERFORMANCE_OR_ORDERING = 1;
    private static final int FOR_GROUPING = 2;
    private static final int FOR_DETERMINISM = 3;

    private int m_purpose = FOR_SCANNING_PERFORMANCE_OR_ORDERING;

    public IndexScanPlanNode() {
        super();
    }

    public IndexScanPlanNode(StmtTableScan tableScan, Index index) {
        super();
        setTableScan(tableScan);
        setCatalogIndex(index);
    }

    public IndexScanPlanNode(AbstractScanPlanNode srcNode, AggregatePlanNode apn, Index index, SortDirectionType sortDirection) {
        super(srcNode.m_targetTableName, srcNode.m_targetTableAlias);
        m_tableSchema = srcNode.m_tableSchema;
        m_predicate = srcNode.m_predicate;
        m_tableScanSchema = srcNode.m_tableScanSchema.clone();
        for (AbstractPlanNode inlineChild : srcNode.getInlinePlanNodes().values()) {
            addInlinePlanNode(inlineChild);
        }
        m_catalogIndex = index;
        m_targetIndexName = index.getTypeName();
        m_lookupType = IndexLookupType.GTE;    // a safe way
        m_sortDirection = sortDirection;
        if (apn != null) {
            m_outputSchema = apn.m_outputSchema.clone();
        }
        m_tableScan = srcNode.getTableScan();
    }

    public void setSkipNullPredicate() {
        // prepare position of non null key
        int searchKeySize = m_searchkeyExpressions.size();
        if (m_lookupType == IndexLookupType.EQ || searchKeySize == 0 || isReverseScan()) {
            m_skip_null_predicate = null;
            return;
        }

        int nextKeyIndex;
        if (m_endExpression != null &&
                searchKeySize < ExpressionUtil.uncombine(m_endExpression).size()) {
            nextKeyIndex = searchKeySize;
        } else {
            nextKeyIndex = searchKeySize - 1;
        }

        setSkipNullPredicate(nextKeyIndex);
    }

    public void setSkipNullPredicate(int nextKeyIndex) {

        String exprsjson = m_catalogIndex.getExpressionsjson();
        List<AbstractExpression> indexedExprs = null;
        if (exprsjson.isEmpty()) {
            indexedExprs = new ArrayList<AbstractExpression>();

            List<ColumnRef> indexedColRefs = CatalogUtil.getSortedCatalogItems(m_catalogIndex.getColumns(), "index");
            assert(nextKeyIndex < indexedColRefs.size());
            for (int i = 0; i <= nextKeyIndex; i++) {
                ColumnRef colRef = indexedColRefs.get(i);
                Column col = colRef.getColumn();
                TupleValueExpression tve = new TupleValueExpression(m_targetTableName, m_targetTableAlias,
                        col.getTypeName(), col.getTypeName());
                tve.setTypeSizeBytes(col.getType(), col.getSize(), col.getInbytes());

                indexedExprs.add(tve);
            }
        } else {
            try {
                indexedExprs = AbstractExpression.fromJSONArrayString(exprsjson, m_tableScan);
                assert(nextKeyIndex < indexedExprs.size());
            } catch (JSONException e) {
                e.printStackTrace();
                assert(false);
            }
        }

        List<AbstractExpression> exprs = new ArrayList<AbstractExpression>();
        for (int i = 0; i < nextKeyIndex; i++) {
            AbstractExpression idxExpr = indexedExprs.get(i);
            AbstractExpression expr = new ComparisonExpression(ExpressionType.COMPARE_EQUAL,
                    idxExpr, (AbstractExpression) m_searchkeyExpressions.get(i).clone());
            exprs.add(expr);
        }
        AbstractExpression nullExpr = indexedExprs.get(nextKeyIndex);
        AbstractExpression expr = new OperatorExpression(ExpressionType.OPERATOR_IS_NULL, nullExpr, null);
        exprs.add(expr);

        m_skip_null_predicate = ExpressionUtil.combine(exprs);
        m_skip_null_predicate.finalizeValueTypes();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.INDEXSCAN;
    }

    @Override
    public void getTablesAndIndexes(Map<String, StmtTargetTableScan> tablesRead,
            Collection<String> indexes)
    {
        super.getTablesAndIndexes(tablesRead, indexes);
        assert(m_targetIndexName.length() > 0);
        if (indexes != null) {
            assert(m_targetIndexName.length() > 0);
            indexes.add(m_targetIndexName);
        }
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
     * Accessor for flag marking the plan as guaranteeing an identical result/effect
     * when "replayed" against the same database state, such as during replication or CL recovery.
     * @return true for unique index scans
     */
    @Override
    public boolean isOrderDeterministic() {
        if (m_catalogIndex.getUnique()) {
            // Any unique index scan capable of returning multiple rows will return them in a fixed order.
            // XXX: This may not be strictly true if/when we support order-determinism based on a mix of columns
            // from different joined tables -- an equality filter based on a non-ordered column from the other table
            // would not produce predictably ordered results even when the other table is ordered by all of its display columns
            // but NOT the column used in the equality filter.
            return true;
        }
        // Assuming (?!) that the relative order of the "multiple entries" in a non-unique index can not be guaranteed,
        // the only case in which a non-unique index can guarantee determinism is for an indexed-column-only scan,
        // because it would ignore any differences in the entries.
        // TODO: return true for an index-only scan --
        // That would require testing for an inline projection node consisting solely
        // of (functions of?) the indexed columns.
        m_nondeterminismDetail = "index scan may provide insufficient ordering";
        return false;
    }

    private void setCatalogIndex(Index index)
    {
        m_catalogIndex = index;
        m_targetIndexName = index.getTypeName();
    }

    public Index getCatalogIndex()
    {
        return m_catalogIndex;
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
            m_endExpression = (AbstractExpression) endExpression.clone();
        }
    }

    public void addEndExpression(AbstractExpression newExpr)
    {
        if (newExpr != null)
        {
            List<AbstractExpression> newEndExpressions = ExpressionUtil.uncombine(m_endExpression);
            newEndExpressions.add((AbstractExpression)newExpr.clone());
            m_endExpression = ExpressionUtil.combine(newEndExpressions);
        }
    }

    public void clearSearchKeyExpression()
    {
        m_searchkeyExpressions.clear();
    }

    public void addSearchKeyExpression(AbstractExpression expr)
    {
        if (expr != null)
        {
            // PlanNodes all need private deep copies of expressions
            // so that the resolveColumnIndexes results
            // don't get bashed by other nodes or subsequent planner runs
            m_searchkeyExpressions.add((AbstractExpression) expr.clone());
        }
    }

    public void removeLastSearchKey()
    {
        int size = m_searchkeyExpressions.size();
        if (size <= 1) {
            clearSearchKeyExpression();
        } else {
            m_searchkeyExpressions.remove(size - 1);
        }
    }

    // CAUTION: only used in MIN/MAX optimization of reverse scan
    // we know this plan should be chosen, so we can change it safely
    public void resetPredicate()
    {
        m_predicate = null;
    }

    /**
     * @return the searchkey_expressions
     */
    // Please don't use me to add search key expressions.  Use
    // addSearchKeyExpression() so that the expression gets cloned
    public List<AbstractExpression> getSearchKeyExpressions() {
        return Collections.unmodifiableList(m_searchkeyExpressions);
    }

    public void setInitialExpression(AbstractExpression expr) {
        if (expr != null) {
            m_initialExpression = (AbstractExpression)expr.clone();
        }
    }

    public AbstractExpression getInitialExpression() {
        return m_initialExpression;
    }

    public AbstractExpression getSkipNullPredicate() {
        return m_skip_null_predicate;
    }

    public boolean isReverseScan() {
        return m_sortDirection == SortDirectionType.DESC ||
                m_lookupType == IndexLookupType.LT || m_lookupType == IndexLookupType.LTE;
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
        index_tves.addAll(ExpressionUtil.getTupleValueExpressions(m_initialExpression));
        index_tves.addAll(ExpressionUtil.getTupleValueExpressions(m_skip_null_predicate));
        for (AbstractExpression search_exp : m_searchkeyExpressions)
        {
            index_tves.addAll(ExpressionUtil.getTupleValueExpressions(search_exp));
        }
        // and update their indexes against the table schema
        for (TupleValueExpression tve : index_tves)
        {
            int index = tve.resolveColumnIndexesUsingSchema(m_tableSchema);
            tve.setColumnIndex(index);
        }
        // now do the common scan node work
        super.resolveColumnIndexes();
    }

    @Override
    public void computeCostEstimates(long childOutputTupleCountEstimate, Cluster cluster, Database db, DatabaseEstimates estimates, ScalarValueHints[] paramHints) {

        // HOW WE COST INDEXES
        // unique, covering index always wins
        // otherwise, pick the index with the most columns covered otherwise
        // count non-equality scans as -0.5 coverage
        // prefer array to hash to tree, all else being equal

        // FYI: Index scores should range between 2 and 800003 (I think)

        DatabaseEstimates.TableEstimates tableEstimates = estimates.getEstimatesForTable(m_targetTableName);

        // get the width of the index and number of columns used
        // need doubles for math
        double colCount = CatalogUtil.getCatalogIndexSize(m_catalogIndex);
        double keyWidth = m_searchkeyExpressions.size();
        assert(keyWidth <= colCount);

        // count a range scan as a half covered column
        if (keyWidth > 0.0 && m_lookupType != IndexLookupType.EQ) {
            keyWidth -= 0.5;
        }
        // When there is no start key, count an end-key as a single-column range scan key.
        else if (keyWidth == 0.0 && m_endExpression != null) {
            // TODO: ( (double) ExpressionUtil.uncombineAny(m_endExpression).size() ) - 0.5
            // might give a result that is more in line with multi-component start-key-only scans.
            keyWidth = 0.5;
        }


        // Estimate the cost of the scan (AND each projection and sort thereafter).
        // This "tuplesToRead" is not strictly speaking an expected count of tuples.
        // Its multiple uses are explained below.
        int tuplesToRead = 0;

        // Assign minor priorities for different index types (tiebreakers).
        if (m_catalogIndex.getType() == IndexType.HASH_TABLE.getValue()) {
            tuplesToRead = 2;
        }
        else if ((m_catalogIndex.getType() == IndexType.BALANCED_TREE.getValue()) ||
                 (m_catalogIndex.getType() == IndexType.BTREE.getValue())) {
            tuplesToRead = 3;
        }
        assert(tuplesToRead > 0);

        // If not a unique, covering index, favor (discount)
        // the choice with the most columns pre-filtered by the index.
        if (!m_catalogIndex.getUnique() || (colCount > keyWidth)) {
            // Cost starts at 90% of a comparable seqscan AND
            // gets scaled down by an additional factor of 0.1 for each fully covered indexed column.
            // One intentional benchmark is for a single range-covered
            // (i.e. half-covered, keyWidth == 0.5) column to have less than 1/3 the cost of a
            // "for ordering purposes only" index scan (keyWidth == 0).
            // This is to completely compensate for the up to 3X final cost resulting from
            // the "order by" and non-inlined "projection" nodes that must be added later to the
            // inconveniently ordered scan result.
            // Using a factor of 0.1 per FULLY covered (equality-filtered) column,
            // the effective scale factor for a single PARTIALLY covered (range-filtered) column
            // comes to SQRT(0.1) which is just under 32% FTW!
            tuplesToRead += (int) (tableEstimates.maxTuples * 0.90 * Math.pow(0.10, keyWidth));

            // With all this discounting, make sure that any non-"covering unique" index scan costs more
            // than any "covering unique" one, no matter how many indexed column filters get piled on.
            // It's theoretically possible to be wrong here -- that a not-strictly-unique combination of
            // indexed column filters statistically selects fewer (fractional) rows per scan
            // than a unique index, but we favor the unique index anyway because:
            // -- the "unique" declaration guarantees a worse-case upper limit of 1 row per scan.
            // -- the per-indexed-column selectivity factors used above are highly fictionalized
            //    -- actual cardinality for individual components of compound indexes MIGHT be very low,
            //       making them much less selective than estimated.
            if (tuplesToRead < 4) {
                tuplesToRead = 4; // i.e. costing 1 unit more than a covered unique btree.
            }
        }

        // This tuplesToRead value estimates the number of base table tuples
        // fetched from the index scan.
        // It's a vague measure of the cost of the scan whose accuracy depends a lot
        // on what kind of post-filtering needs to happen.
        // The tuplesRead value is also used here to estimate the number of RESULT rows.
        // This value is estimated without regard to any post-filtering effect there might be
        // -- as if all rows found in the index passed any additional post-filter conditions.
        // This ignoring of post-filter effects is at least consistent with the SeqScanPlanNode.
        // In effect, it gives index scans an "unfair" advantage
        // -- follow-on sorts (etc.) are costed lower as if they are operating on fewer rows
        // than would have come out of the seqscan, though that's nonsense.
        // It's just an artifact of how SeqScanPlanNode costing ignores ALL filters but
        // IndexScanPlanNode costing only ignores post-filters.
        // In any case, it's important to keep this code roughly in synch with any changes to
        // SeqScanPlanNode's costing to make sure that SeqScanPlanNode never gains an unfair advantage.
        m_estimatedOutputTupleCount = tuplesToRead;
        m_estimatedProcessedTupleCount = tuplesToRead;

        // special case a unique match for the output count
        if (m_catalogIndex.getUnique() && (colCount == keyWidth)) {
            m_estimatedOutputTupleCount = 1;
        }

        LimitPlanNode limit = (LimitPlanNode)m_inlineNodes.get(PlanNodeType.LIMIT);
        if (limit != null) {
            int limitInt = limit.getLimit();
            if (limitInt == -1) {
                // If Limit ?, it's likely to be a small number. So pick up 50 here.
                limitInt = 50;
            }

            m_estimatedOutputTupleCount = Math.min(m_estimatedOutputTupleCount, limitInt);
            int offsetInt = limit.getOffset();

            if (m_predicate == null && offsetInt == 0) {
                m_estimatedProcessedTupleCount = limitInt;
            }
        }
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.LOOKUP_TYPE.name()).value(m_lookupType.toString());
        stringer.key(Members.SORT_DIRECTION.name()).value(m_sortDirection.toString());
        if (m_purpose != FOR_SCANNING_PERFORMANCE_OR_ORDERING) {
            stringer.key(Members.PURPOSE.name()).value(m_purpose);
        }
        stringer.key(Members.TARGET_INDEX_NAME.name()).value(m_targetIndexName);
        if (m_searchkeyExpressions.size() > 0) {
            stringer.key(Members.SEARCHKEY_EXPRESSIONS.name()).array();
            for (AbstractExpression ae : m_searchkeyExpressions) {
                assert (ae instanceof JSONString);
                stringer.value(ae);
            }
            stringer.endArray();
        }
        if (m_endExpression != null) {
            stringer.key(Members.END_EXPRESSION.name());
            stringer.value(m_endExpression);
        }
        if (m_initialExpression != null) {
            stringer.key(Members.INITIAL_EXPRESSION.name()).value(m_initialExpression);
        }

        if (m_skip_null_predicate != null) {
            stringer.key(Members.SKIP_NULL_PREDICATE.name()).value(m_skip_null_predicate);
        }
    }

    //all members loaded
    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        super.loadFromJSONObject(jobj, db);
        m_lookupType = IndexLookupType.get( jobj.getString( Members.LOOKUP_TYPE.name() ) );
        m_sortDirection = SortDirectionType.get( jobj.getString( Members.SORT_DIRECTION.name() ) );
        m_purpose = jobj.has(Members.PURPOSE.name()) ?
                jobj.getInt(Members.PURPOSE.name()) : FOR_SCANNING_PERFORMANCE_OR_ORDERING;
        m_targetIndexName = jobj.getString(Members.TARGET_INDEX_NAME.name());
        m_catalogIndex = db.getTables().get(super.m_targetTableName).getIndexes().get(m_targetIndexName);
        //load end_expression
        m_endExpression = AbstractExpression.fromJSONChild(jobj, Members.END_EXPRESSION.name(), m_tableScan);
        // load initial_expression
        m_initialExpression = AbstractExpression.fromJSONChild(jobj, Members.INITIAL_EXPRESSION.name(), m_tableScan);
        //load searchkey_expressions
        AbstractExpression.loadFromJSONArrayChild(m_searchkeyExpressions, jobj,
                Members.SEARCHKEY_EXPRESSIONS.name(), m_tableScan);
        m_skip_null_predicate = AbstractExpression.fromJSONChild(jobj, Members.SKIP_NULL_PREDICATE.name(), m_tableScan);
    }

    @Override
    protected String explainPlanForNode(String indent) {
        assert(m_catalogIndex != null);

        int keySize = m_searchkeyExpressions.size();

        // When there is no start key, count a range scan key for each ANDed end condition.
        if (keySize == 0 && m_endExpression != null) {
            keySize = ExpressionUtil.uncombineAny(m_endExpression).size();
        }

        String usageInfo;
        String predicatePrefix;
        if (keySize == 0) {
            // The plan is easy to explain if it isn't using indexed expressions.
            // Just explain why an index scan was chosen
            // -- either for determinism or for an explicit ORDER BY requirement.
            if (m_purpose == FOR_DETERMINISM) {
                usageInfo = " (for deterministic order only)";
            }
            else if (m_purpose == FOR_GROUPING) {
                usageInfo = " (for optimized grouping only)";
            }
            else {
                usageInfo = " (for sort order only)";
            }
            // Introduce on its own indented line, any unrelated post-filter applied to the result.
            // e.g. " filter by OTHER_COL = 1"
            predicatePrefix = "\n" + indent + " filter by ";
        }
        else {
            int indexSize = CatalogUtil.getCatalogIndexSize(m_catalogIndex);
            String[] asIndexed = new String[indexSize];
            // Not really expecting to need these fall-back labels,
            // but in the case of an unexpected error accessing the catalog data,
            // they beat an NPE.
            for (int ii = 0; ii < keySize; ++ii) {
                asIndexed[ii] = "(index key " + ii + ")";
            }
            String jsonExpr = m_catalogIndex.getExpressionsjson();
            // if this is a pure-column index...
            if (jsonExpr.isEmpty()) {
                // grab the short names of the indexed columns in use.
                for (ColumnRef cref : m_catalogIndex.getColumns()) {
                    Column col = cref.getColumn();
                    asIndexed[cref.getIndex()] = col.getName();
                }
            }
            else {
                try {
                    List<AbstractExpression> indexExpressions =
                        AbstractExpression.fromJSONArrayString(jsonExpr, m_tableScan);
                    int ii = 0;
                    for (AbstractExpression ae : indexExpressions) {
                        asIndexed[ii++] = ae.explain(m_targetTableName);
                    }
                } catch (JSONException e) {
                    // If something unexpected went wrong,
                    // just fall back on the positional key labels.
                }
            }

            // Explain the search criteria that describe the start of the index scan, like
            // "(event_type = 1 AND event_start > x.start_time)"
            String start = explainSearchKeys(asIndexed, keySize);
            if (m_lookupType == IndexLookupType.EQ) {
                // qualify whether the equality matching is for a unique value.
                // " uniquely match (event_id = 1)" vs.
                // " scan matches for (event_type = 1) AND (event_location = x.region)"
                if (m_catalogIndex.getUnique()) {
                    usageInfo = "\n" + indent + " uniquely match " + start;
                }
                else {
                    usageInfo = "\n" + indent + " scan matches for " + start;
                }
            }
            else {
                usageInfo = "\n" + indent;
                if (isReverseScan()) {
                    usageInfo += "reverse ";
                }
                // qualify whether the inequality matching covers all or only some index key components
                // " " range-scan covering from (event_type = 1) AND (event_start > x.start_time)" vs
                // " " range-scan on 1 of 2 cols from event_type = 1"
                if (indexSize == keySize) {
                    usageInfo += "range-scan covering from " + start;
                }
                else {
                    usageInfo += String.format("range-scan on %d of %d cols from %s", keySize, indexSize, start);
                }
                // Explain the criteria for continuinuing the scan such as
                // "while (event_type = 1 AND event_start < x.start_time+30)"
                // or label it as a scan "to the end"
                usageInfo += explainEndKeys();
            }
            // Introduce any additional filters not related to the index
            // that could cause rows to be skipped.
            // e.g. "... scan ... from ... while ..., filter by OTHER_COL = 1"
            predicatePrefix = ", filter by ";
        }
        // Describe any additional filters not related to the index
        // e.g. "...filter by OTHER_COL = 1".
        String predicate = explainPredicate(predicatePrefix);
        // Describe the table name and either a user-provided name of the index or
        // its user-specified role ("primary key").
        String retval = "INDEX SCAN of \"" + m_targetTableName + "\"";
        String indexDescription = " using \"" + m_targetIndexName + "\"";
        // Replace ugly system-generated index name with a description of its user-specified role.
        if (m_targetIndexName.startsWith(HSQLInterface.AUTO_GEN_PRIMARY_KEY_PREFIX) ||
                m_targetIndexName.startsWith(HSQLInterface.AUTO_GEN_CONSTRAINT_WRAPPER_PREFIX) ||
                m_targetIndexName.equals(HSQLInterface.AUTO_GEN_MATVIEW_IDX) ) {
            indexDescription = " using its primary key index";
        }
        // Bring all the pieces together describing the index, how it is scanned,
        // and whatever extra filter processing is done to the result.
        retval += indexDescription;
        retval += usageInfo + predicate;
        return retval;
    }

    /// Explain that this index scan begins at the "start" of the index
    /// or at a particular key, possibly compound.
    private String explainSearchKeys(String[] asIndexed, int nCovered)
    {
        // By default, indexing starts at the start of the index.
        if (m_searchkeyExpressions.isEmpty()) {
            return "start";
        }
        String conjunction = "";
        String result = "(";
        int prefixSize = nCovered - 1;
        for (int ii = 0; ii < prefixSize; ++ii) {
            result += conjunction + asIndexed[ii] + " = " +
                    m_searchkeyExpressions.get(ii).explain(m_targetTableName);
            conjunction = ") AND (";
        }
        // last element
        result += conjunction +
                asIndexed[prefixSize] + " " + m_lookupType.getSymbol() + " " +
                m_searchkeyExpressions.get(prefixSize).explain(m_targetTableName) + ")";
        return result;
    }

    /// Explain that this index scans "to end" of the index
    /// or only "while" an end expression involving indexed key values remains true.
    private String explainEndKeys()
    {
        // By default, indexing starts at the start of the index.
        if (m_endExpression == null) {
            return " to end";
        }
        return " while " + m_endExpression.explain(m_targetTableName);
    }

    public void setBindings(ArrayList<AbstractExpression> bindings) {
        m_bindings  = bindings;
    }

    public ArrayList<AbstractExpression> getBindings() {
        return m_bindings;
    }

    public void addBindings(List<AbstractExpression> bindings) {
        m_bindings.addAll(bindings);
    }

    public void setForDeterminismOnly() {
        m_purpose = FOR_DETERMINISM;
    }

    public void setForGroupingOnly() {
        m_purpose = FOR_GROUPING;
    }

    public boolean isForGroupingOnly() {
        return m_purpose == FOR_GROUPING;
    }

    // Called by ReplaceWithIndexLimit and ReplaceWithIndexCounter
    // only apply those optimization if it has no (post-)predicates
    // except those (post-)predicates are artifact predicates we
    // added for reverse scan purpose only
    public boolean isPredicatesOptimizableForAggregate() {
        // for reverse scan, need to examine "added" predicates
        List<AbstractExpression> predicates = ExpressionUtil.uncombine(m_predicate);
        // if the size of predicates doesn't equal 1, can't be our added artifact predicates
        if (predicates.size() != 1) {
            return false;
        }
        // examin the possible "added" predicates: NOT NULL expr.
        AbstractExpression expr = predicates.get(0);
        if (expr.getExpressionType() != ExpressionType.OPERATOR_NOT) {
            return false;
        }
        if (expr.getLeft().getExpressionType() != ExpressionType.OPERATOR_IS_NULL) {
            return false;
        }
        // Not reverse scan.
        if (m_lookupType != IndexLookupType.LT && m_lookupType != IndexLookupType.LTE) {
            return false;
        }

        return true;
    }
}
