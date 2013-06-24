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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
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
        DETERMINISM_ONLY,
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

    // ???
    protected Boolean m_keyIterate = false;

    // The overall index lookup operation type
    protected IndexLookupType m_lookupType = IndexLookupType.EQ;

    // The sorting direction
    protected SortDirectionType m_sortDirection = SortDirectionType.INVALID;

    // A reference to the Catalog index object which defined the index which
    // this index scan is going to use
    protected Index m_catalogIndex = null;

    private ArrayList<AbstractExpression> m_bindings = null;

    private boolean m_forDeterminismOnly = false;

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
            m_endExpression = (AbstractExpression) endExpression.clone();
        }
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
    public void computeCostEstimates(long childOutputTupleCountEstimate, Cluster cluster, Database db, DatabaseEstimates estimates, ScalarValueHints[] paramHints) {

        // HOW WE COST INDEXES
        // unique, covering index always wins
        // otherwise, pick the index with the most columns covered otherwise
        // count non-equality scans as -0.5 coverage
        // prefer array to hash to tree, all else being equal

        // FYI: Index scores should range between 2 and 800003 (I think)

        Table target = db.getTables().getIgnoreCase(m_targetTableName);
        assert(target != null);
        DatabaseEstimates.TableEstimates tableEstimates = estimates.getEstimatesForTable(target.getTypeName());

        // get the width of the index and number of columns used
        // need doubles for math
        double colCount = m_catalogIndex.getColumns().size();
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
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.KEY_ITERATE.name()).value(m_keyIterate);
        stringer.key(Members.LOOKUP_TYPE.name()).value(m_lookupType.toString());
        stringer.key(Members.SORT_DIRECTION.name()).value(m_sortDirection.toString());
        if (m_forDeterminismOnly) {
            stringer.key(Members.DETERMINISM_ONLY.name()).value(true);
        }
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

    //all members loaded
    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        super.loadFromJSONObject(jobj, db);
        m_keyIterate = jobj.getBoolean( Members.KEY_ITERATE.name() );
        m_lookupType = IndexLookupType.get( jobj.getString( Members.LOOKUP_TYPE.name() ) );
        m_sortDirection = SortDirectionType.get( jobj.getString( Members.SORT_DIRECTION.name() ) );
        m_forDeterminismOnly = jobj.optBoolean(Members.DETERMINISM_ONLY.name());
        m_targetIndexName = jobj.getString(Members.TARGET_INDEX_NAME.name());
        m_catalogIndex = db.getTables().get(super.m_targetTableName).getIndexes().get(m_targetIndexName);
        JSONObject tempjobj = null;
        //load end_expression
        if( !jobj.isNull( Members.END_EXPRESSION.name() ) ) {
            tempjobj = jobj.getJSONObject( Members.END_EXPRESSION.name() );
            m_endExpression = AbstractExpression.fromJSONObject( tempjobj, db);
        }
        //load searchkey_expressions
        if( !jobj.isNull( Members.SEARCHKEY_EXPRESSIONS.name() ) ) {
            JSONArray jarray = jobj.getJSONArray( Members.SEARCHKEY_EXPRESSIONS.name() );
            int size = jarray.length();
            for( int i = 0 ; i < size; i++ ) {
                tempjobj = jarray.getJSONObject( i );
                m_searchkeyExpressions.add( AbstractExpression.fromJSONObject(tempjobj, db));
            }
        }
    }

    @Override
    protected String explainPlanForNode(String indent) {
        assert(m_catalogIndex != null);

        int indexSize = m_catalogIndex.getColumns().size();
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
            if (m_forDeterminismOnly) {
                usageInfo = " (for deterministic order only)";
            } else {
                usageInfo = " (for sort order only)";
            }
            // Introduce on its own indented line, any unrelated post-filter applied to the result.
            // e.g. " filter by OTHER_COL = 1"
            predicatePrefix = "\n" + indent + " filter by ";
        }
        else {
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
                        AbstractExpression.fromJSONArrayString(jsonExpr, null);
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
                // qualify whether the inequality matching covers all or only some index key components
                // " " range-scan covering from (event_type = 1) AND (event_start > x.start_time)" vs
                // " " range-scan on 1 of 2 cols from event_type = 1"
                if (indexSize == keySize) {
                    usageInfo = "\n" + indent + " range-scan covering from " + start;
                }
                else {
                    usageInfo = String.format("\n%s range-scan on %d of %d cols from %s", indent, keySize, indexSize, start);
                }
                // Explain the criteria for continuinuing the scan such as
                // "while (event_type = 1 AND event_start < x.start_time+30)"
                // or label it as a scan "to the end"
                usageInfo += explainEndKeys(asIndexed);
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
        if (m_targetIndexName.startsWith("SYS_IDX_PK_") ||
            m_targetIndexName.startsWith("SYS_IDX_SYS_PK_") ||
            m_targetIndexName.startsWith("MATVIEW_PK_INDEX") ) {
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
            result += conjunction +
                asIndexed[ii] + " = " + m_searchkeyExpressions.get(ii).explain(m_targetTableName);
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
    private String explainEndKeys(String[] asIndexed)
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

    public void setForDeterminismOnly() {
        m_forDeterminismOnly = true;
    }
}
