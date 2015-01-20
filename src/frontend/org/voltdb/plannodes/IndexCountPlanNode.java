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
import org.voltdb.catalog.Table;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.utils.CatalogUtil;

public class IndexCountPlanNode extends AbstractScanPlanNode {

    public enum Members {
        TARGET_INDEX_NAME,
        SEARCHKEY_EXPRESSIONS,
        ENDKEY_EXPRESSIONS,
        SKIP_NULL_PREDICATE,
        LOOKUP_TYPE,
        END_TYPE;
    }

    /**
     * Attributes
     */

    // The index to use in the scan operation
    protected String m_targetIndexName;

    //
    protected List<AbstractExpression> m_endkeyExpressions = new ArrayList<AbstractExpression>();

    // This list of expressions corresponds to the values that we will use
    // at runtime in the lookup on the index
    protected List<AbstractExpression> m_searchkeyExpressions = new ArrayList<AbstractExpression>();

    // The overall index lookup operation type
    protected IndexLookupType m_lookupType = IndexLookupType.EQ;

    // The overall index lookup operation type
    protected IndexLookupType m_endType = IndexLookupType.EQ;

    // A reference to the Catalog index object which defined the index which
    // this index scan is going to use
    protected Index m_catalogIndex = null;

    private ArrayList<AbstractExpression> m_bindings;

    private AbstractExpression m_skip_null_predicate;

    public IndexCountPlanNode() {
        super();
    }

    public IndexCountPlanNode(String tableName, String tableAlias) {
        super(tableName, tableAlias);
        assert(tableName != null && tableAlias != null);
    }

    private IndexCountPlanNode(IndexScanPlanNode isp, AggregatePlanNode apn,
                               IndexLookupType endType, List<AbstractExpression> endKeys)
    {
        super(isp.m_targetTableName, isp.m_targetTableAlias);

        m_catalogIndex = isp.m_catalogIndex;

        m_estimatedOutputTupleCount = 1;
        m_tableSchema = isp.m_tableSchema;
        m_tableScanSchema = isp.m_tableScanSchema.clone();

        m_targetIndexName = isp.m_targetIndexName;

        m_tableScan = isp.getTableScan();

        m_predicate = null;
        m_bindings = isp.getBindings();

        m_outputSchema = apn.getOutputSchema().clone();
        m_hasSignificantOutputSchema = true;

        if (!isp.isReverseScan()) {
            m_lookupType = isp.m_lookupType;
            m_searchkeyExpressions = isp.m_searchkeyExpressions;

            m_endType = endType;
            m_endkeyExpressions.addAll(endKeys);

            setSkipNullPredicate(false);
        } else {
            // for reverse scan, swap everything of searchkey and endkey
            // because we added the last < / <= to searchkey but not endExpr
            assert(endType == IndexLookupType.EQ);
            m_lookupType = endType;     // must be EQ, but doesn't matter, since previous lookup type is not GT
            m_searchkeyExpressions.addAll(endKeys);
            m_endType = isp.m_lookupType;
            m_endkeyExpressions = isp.getSearchKeyExpressions();

            setSkipNullPredicate(true);
        }
    }

    private void setSkipNullPredicate(boolean isReverseScan) {

        int nextKeyIndex = -1;

        if (isReverseScan) {
            if (m_searchkeyExpressions.size() < m_endkeyExpressions.size()) {
                assert(m_endType == IndexLookupType.LT || m_endType == IndexLookupType.LTE);
                assert( m_endkeyExpressions.size() - m_searchkeyExpressions.size() == 1);
                nextKeyIndex = m_searchkeyExpressions.size();
            }
        } else {
            // useful for underflow case to eliminate nulls
            if (m_searchkeyExpressions.size() >= m_endkeyExpressions.size()) {
                if (m_lookupType == IndexLookupType.GT || m_lookupType == IndexLookupType.GTE) {
                    assert(m_searchkeyExpressions.size() > 0);
                    nextKeyIndex = m_searchkeyExpressions.size() - 1;
                }
            }
        }
        if (nextKeyIndex < 0) {
            return ;
        }

        List<AbstractExpression> exprs = new ArrayList<AbstractExpression>();

        String exprsjson = m_catalogIndex.getExpressionsjson();
        List<AbstractExpression> indexedExprs = null;
        if (exprsjson.isEmpty()) {
            indexedExprs = new ArrayList<AbstractExpression>();

            List<ColumnRef> indexedColRefs = CatalogUtil.getSortedCatalogItems(m_catalogIndex.getColumns(), "index");
            for (int i = 0; i <= nextKeyIndex; i++) {
                ColumnRef colRef = indexedColRefs.get(i);
                Column col = colRef.getColumn();
                TupleValueExpression tve = new TupleValueExpression(m_targetTableName, m_targetTableAlias,
                        col.getTypeName(), col.getTypeName());

                tve.setTypeSizeBytes(col.getType(), col.getSize(), col.getInbytes());

                tve.resolveForTable((Table)m_catalogIndex.getParent());
                indexedExprs.add(tve);
            }
        } else {
            try {
                indexedExprs = AbstractExpression.fromJSONArrayString(exprsjson, m_tableScan);
            } catch (JSONException e) {
                e.printStackTrace();
                assert(false);
            }

        }
        AbstractExpression expr;
        for (int i = 0; i < nextKeyIndex; i++) {
            AbstractExpression idxExpr = indexedExprs.get(i);
            expr = new ComparisonExpression(ExpressionType.COMPARE_EQUAL,
                    idxExpr, (AbstractExpression) m_searchkeyExpressions.get(i).clone());
            exprs.add(expr);
        }
        AbstractExpression nullExpr = indexedExprs.get(nextKeyIndex);
        expr = new OperatorExpression(ExpressionType.OPERATOR_IS_NULL, nullExpr, null);
        exprs.add(expr);
        m_skip_null_predicate = ExpressionUtil.combine(exprs);
        m_skip_null_predicate.finalizeValueTypes();
    }

    // Create an IndexCountPlanNode that replaces the parent aggregate and chile indexscan
    // UNLESS the indexscan's end expressions aren't a form that can be modeled with an end key.
    // The supported forms for end expression are:
    //   - null
    //   - one filter expression per index key component (ANDed together) as "combined" for the IndexScan.
    //   - fewer filter expressions than index key components with one of them (the last) being a LT comparison.
    //   - 1 fewer filter expressions than index key components, but all ANDed equality filters
    // The LT restriction comes because when index key prefixes are identical to the prefix-only end key,
    // the entire index key sorts greater than the prefix-only end-key, because it is always longer.
    // These prefix-equal cases would be missed in an EQ or LTE filter, causing undercounts.
    // A prefix-only LT filter is intended to discard prefix-equal cases, so it is allowed.
    // @return the IndexCountPlanNode or null if one is not possible.
    public static IndexCountPlanNode createOrNull(IndexScanPlanNode isp, AggregatePlanNode apn)
    {
        // add support for reverse scan
        // for ASC scan, check endExpression; for DESC scan, need to check searchkeys
        List<AbstractExpression> endKeys = new ArrayList<AbstractExpression>();
        // Initially assume that there will be an equality filter on all key components.
        IndexLookupType endType = IndexLookupType.EQ;
        List<AbstractExpression> endComparisons = ExpressionUtil.uncombine(isp.getEndExpression());
        for (AbstractExpression ae: endComparisons) {
            // There should be no more end expressions after an LT or LTE has reset the end type.
            assert(endType == IndexLookupType.EQ);

            if (ae.getExpressionType() == ExpressionType.COMPARE_LESSTHAN) {
                endType = IndexLookupType.LT;
            }
            else if (ae.getExpressionType() == ExpressionType.COMPARE_LESSTHANOREQUALTO) {
                endType = IndexLookupType.LTE;
            } else {
                assert(ae.getExpressionType() == ExpressionType.COMPARE_EQUAL);
            }

            // PlanNodes all need private deep copies of expressions
            // so that the resolveColumnIndexes results
            // don't get bashed by other nodes or subsequent planner runs
            endKeys.add((AbstractExpression)ae.getRight().clone());
        }

        int indexSize = 0;
        String jsonstring = isp.getCatalogIndex().getExpressionsjson();
        List<ColumnRef> indexedColRefs = null;
        List<AbstractExpression> indexedExprs = null;
        if (jsonstring.isEmpty()) {
            indexedColRefs = CatalogUtil.getSortedCatalogItems(isp.getCatalogIndex().getColumns(), "index");
            indexSize = indexedColRefs.size();
        } else {
            try {
                indexedExprs = AbstractExpression.fromJSONArrayString(jsonstring, isp.getTableScan());
                indexSize = indexedExprs.size();
            } catch (JSONException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        // decide whether to pad last endKey to solve
        // SELECT COUNT(*) FROM T WHERE C1 = ? AND C2 > / >= ?
        if (!isp.isReverseScan() &&
                endType == IndexLookupType.EQ &&
                endKeys.size() > 0 &&
                endKeys.size() == indexSize - 1 &&
                isp.getSearchKeyExpressions().size() == indexSize) {

            VoltType missingKeyType = VoltType.INVALID;
            boolean canPadding = true;

            // need to check the filter we are missing is the last indexable expr
            // and find out the missing key
            if (jsonstring.isEmpty()) {
                int lastIndex = indexedColRefs.get(indexSize - 1).getColumn().getIndex();
                for (AbstractExpression expr : endComparisons) {
                    if (((TupleValueExpression)(expr.getLeft())).getColumnIndex() == lastIndex) {
                        canPadding = false;
                        break;
                    }
                }
                if (canPadding) {
                    missingKeyType = VoltType.get((byte)(indexedColRefs.get(indexSize - 1).getColumn().getType()));
                }
            } else {
                AbstractExpression lastIndexableExpr = indexedExprs.get(indexSize - 1);
                for (AbstractExpression expr : endComparisons) {
                    if (expr.getLeft().bindingToIndexedExpression(lastIndexableExpr) != null) {
                        canPadding = false;
                        break;
                    }
                }
                if (canPadding) {
                    missingKeyType = lastIndexableExpr.getValueType();
                }
            }
            if (canPadding && missingKeyType.isMaxValuePaddable()) {
                ConstantValueExpression missingKey = new ConstantValueExpression();
                missingKey.setValueType(missingKeyType);
                missingKey.setValue(String.valueOf(VoltType.getPaddedMaxTypeValue(missingKeyType)));
                missingKey.setValueSize(missingKeyType.getLengthInBytesForFixedTypes());
                endType = IndexLookupType.LTE;
                endKeys.add(missingKey);
            } else {
                return null;
            }
        }

        // check endkey for ASC or searchkey for DESC case separately

        // Avoid the cases that would cause undercounts for prefix matches.
        // A prefix-only key exists and does not use LT.
        if (!isp.isReverseScan() &&
            (endType != IndexLookupType.LT) &&
            (endKeys.size() > 0) &&
            (endKeys.size() < indexSize)) {
            return null;
        }

        // DESC case
        if ((isp.getSearchKeyExpressions().size() > 0) &&
                (isp.getSearchKeyExpressions().size() < indexSize)) {
            return null;
        }
        return new IndexCountPlanNode(isp, apn, endType, endKeys);
    }

    @Override
    public void getTablesAndIndexes(Map<String, StmtTargetTableScan> tablesRead,
            Collection<String> indexes)
    {
        super.getTablesAndIndexes(tablesRead, indexes);
        if (indexes != null) {
            assert(m_targetIndexName.length() > 0);
            indexes.add(m_targetIndexName);
        }
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.INDEXCOUNT;
    }

    @Override
    public void validate() throws Exception {
        super.validate();

        // There needs to be at least one search key expression
        if (m_searchkeyExpressions.isEmpty()) {
            throw new Exception("ERROR: There were no search key expressions defined for " + this);
        }

        for (AbstractExpression exp : m_searchkeyExpressions) {
            exp.validate();
        }
    }

    /**
     * Should just return true -- there's only one order for a single row
     * @return true
     */
    @Override
    public boolean isOrderDeterministic() {
        return true;
    }

    @Override
    public void generateOutputSchema(Database db){}

    @Override
    public void resolveColumnIndexes(){}

    @Override
    public void computeCostEstimates(long childOutputTupleCountEstimate, Cluster cluster, Database db, DatabaseEstimates estimates, ScalarValueHints[] paramHints) {
        // Cost counting index scans as constant, almost negligible work.
        // This might be unfair, as the tree has O(logn) complexity, but we
        // really want to pick this kind of search over others.
        m_estimatedProcessedTupleCount = 1;
        m_estimatedOutputTupleCount = 1;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.LOOKUP_TYPE.name()).value(m_lookupType.toString());
        stringer.key(Members.END_TYPE.name()).value(m_endType.toString());
        stringer.key(Members.TARGET_INDEX_NAME.name()).value(m_targetIndexName);

        stringer.key(Members.ENDKEY_EXPRESSIONS.name());
        if ( m_endkeyExpressions.isEmpty()) {
            stringer.value(null);
        } else {
            stringer.array();
            for (AbstractExpression ae : m_endkeyExpressions) {
                assert (ae instanceof JSONString);
                stringer.value(ae);
            }
            stringer.endArray();
        }

        stringer.key(Members.SEARCHKEY_EXPRESSIONS.name()).array();
        for (AbstractExpression ae : m_searchkeyExpressions) {
            assert (ae instanceof JSONString);
            stringer.value(ae);
        }
        stringer.endArray();

        if (m_skip_null_predicate != null) {
            stringer.key(Members.SKIP_NULL_PREDICATE.name()).value(m_skip_null_predicate);
        }
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        super.loadFromJSONObject(jobj, db);

        m_lookupType = IndexLookupType.get( jobj.getString( Members.LOOKUP_TYPE.name() ) );
        m_endType = IndexLookupType.get( jobj.getString( Members.END_TYPE.name() ) );
        m_targetIndexName = jobj.getString(Members.TARGET_INDEX_NAME.name());
        m_catalogIndex = db.getTables().get(super.m_targetTableName).getIndexes().get(m_targetIndexName);
        //load end_expression
        AbstractExpression.loadFromJSONArrayChild(m_endkeyExpressions, jobj,
                Members.ENDKEY_EXPRESSIONS.name(), m_tableScan);
        AbstractExpression.loadFromJSONArrayChild(m_searchkeyExpressions, jobj,
                Members.SEARCHKEY_EXPRESSIONS.name(), m_tableScan);

        m_skip_null_predicate = AbstractExpression.fromJSONChild(jobj, Members.SKIP_NULL_PREDICATE.name(), m_tableScan);
    }

    @Override
    protected String explainPlanForNode(String indent) {
        assert(m_catalogIndex != null);

        int indexSize = CatalogUtil.getCatalogIndexSize(m_catalogIndex);
        int searchkeySize = m_searchkeyExpressions.size();
        int endkeySize = m_endkeyExpressions.size();

        int keySize = Math.max(searchkeySize, endkeySize);
        String scanType = "tree-counter";

        String cover = "covering";
        if (indexSize > keySize)
            cover = String.format("%d/%d cols", keySize, indexSize);

        String usageInfo = String.format("(%s %s)", scanType, cover);

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

        // Explain the search keys that describe the boundaries of the index count, like
        // "(event_type = 1 AND event_start > x.start_time)"
        if (searchkeySize > 0) {
            String start = explainKeys(asIndexed, m_searchkeyExpressions, m_targetTableName, m_lookupType);
            usageInfo += "\n" + indent + " count matches from " + start;
        }
        if (endkeySize > 0) {
            String end = explainKeys(asIndexed, m_endkeyExpressions, m_targetTableName, m_endType);
            usageInfo += "\n" + indent + " count matches to " + end;
        }
        if (m_skip_null_predicate != null) {
            String predicate = m_skip_null_predicate.explain(m_targetTableName);
            usageInfo += "\n" + indent + " discounting rows where " + predicate;
        }
        // Describe the table name and either a user-provided name of the index or
        // its user-specified role ("primary key").
        String retval = "INDEX COUNT of \"" + m_targetTableName + "\"";
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
        retval += usageInfo;
        return retval;
    }

    private static String explainKeys(String[] asIndexed, List<AbstractExpression> keyExpressions,
            String targetTableName, IndexLookupType lookupType) {
        String conjunction = "";
        String result = "(";
        int prefixSize = keyExpressions.size() - 1;
        for (int ii = 0; ii < prefixSize; ++ii) {
            result += conjunction +
                asIndexed[ii] + " = " + keyExpressions.get(ii).explain(targetTableName);
            conjunction = ") AND (";
        }
        // last element
        result += conjunction +
            asIndexed[prefixSize] + " " + lookupType.getSymbol() + " " +
            keyExpressions.get(prefixSize).explain(targetTableName) + ")";
        return result;
    }

    public ArrayList<AbstractExpression> getBindings() {
        return m_bindings;
    }
}
