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
import java.util.Collection;
import java.util.List;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.utils.CatalogUtil;

public class IndexCountPlanNode extends AbstractScanPlanNode {

    public enum Members {
        TARGET_INDEX_NAME,
        KEY_ITERATE,
        SEARCHKEY_EXPRESSIONS,
        ENDKEY_EXPRESSIONS,
        LOOKUP_TYPE,
        END_TYPE;
    }

    /**
     * Attributes
     */

    // The index to use in the scan operation
    protected String m_targetIndexName;

    // ???
    protected Boolean m_keyIterate = false;

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

    public IndexCountPlanNode() {
        super();
    }

    private IndexCountPlanNode(IndexScanPlanNode isp, AggregatePlanNode apn,
                               IndexLookupType endType, List<AbstractExpression> endKeys)
    {
        super();

        m_catalogIndex = isp.m_catalogIndex;

        m_estimatedOutputTupleCount = 1;
        m_tableSchema = isp.m_tableSchema;
        m_tableScanSchema = isp.m_tableScanSchema.clone();

        m_targetTableAlias = isp.m_targetTableAlias;
        m_targetTableName = isp.m_targetTableName;
        m_targetIndexName = isp.m_targetIndexName;

        m_predicate = null;
        m_bindings = isp.getBindings();

        m_outputSchema = apn.getOutputSchema().clone();
        m_hasSignificantOutputSchema = true;

        if (!isp.isReverseScan()) {
            m_lookupType = isp.m_lookupType;
            m_searchkeyExpressions = isp.m_searchkeyExpressions;

            m_endType = endType;
            m_endkeyExpressions.addAll(endKeys);
        } else {
            // for reverse scan, swap everything of searchkey and endkey
            // because we added the last < / <= to searchkey but not endExpr
            m_lookupType = endType;     // must be EQ, but doesn't matter, since previous lookup type is not GT
            m_searchkeyExpressions.addAll(endKeys);
            m_endType = isp.m_lookupType;
            m_endkeyExpressions = isp.getSearchKeyExpressions();
        }
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
                indexedExprs = AbstractExpression.fromJSONArrayString(jsonstring, null);
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
    public void getTablesAndIndexes(Collection<String> tablesRead, Collection<String> tableUpdated,
                                    Collection<String> indexes)
    {
        super.getTablesAndIndexes(tablesRead, tableUpdated, indexes);
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
        stringer.key(Members.KEY_ITERATE.name()).value(m_keyIterate);
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
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        super.loadFromJSONObject(jobj, db);
        m_keyIterate = jobj.getBoolean( Members.KEY_ITERATE.name() );

        m_lookupType = IndexLookupType.get( jobj.getString( Members.LOOKUP_TYPE.name() ) );
        m_endType = IndexLookupType.get( jobj.getString( Members.END_TYPE.name() ) );
        m_targetIndexName = jobj.getString(Members.TARGET_INDEX_NAME.name());
        m_catalogIndex = db.getTables().get(super.m_targetTableName).getIndexes().get(m_targetIndexName);
        JSONObject tempjobj = null;
        //load end_expression
        if( !jobj.isNull( Members.ENDKEY_EXPRESSIONS.name() ) ) {
            JSONArray jarray = jobj.getJSONArray(Members.ENDKEY_EXPRESSIONS.name());
            int size = jarray.length();
            for( int i = 0; i < size; i++ ){
                tempjobj = jarray.getJSONObject(i);
                m_endkeyExpressions.add( AbstractExpression.fromJSONObject( tempjobj, db) );
            }
        }
        JSONArray jarray = jobj.getJSONArray( Members.SEARCHKEY_EXPRESSIONS.name() );
        int size = jarray.length();
        for( int i = 0 ; i < size; i++ ) {
            tempjobj = jarray.getJSONObject( i );
            m_searchkeyExpressions.add( AbstractExpression.fromJSONObject(tempjobj, db));
        }
    }

    @Override
    protected String explainPlanForNode(String indent) {
        assert(m_catalogIndex != null);

        int indexSize = CatalogUtil.getCatalogIndexSize(m_catalogIndex);
        int keySize = m_searchkeyExpressions.size();

        String scanType = "tree-counter";
        if (m_lookupType != IndexLookupType.EQ)
            scanType = "tree-counter";

        String cover = "covering";
        if (indexSize > keySize)
            cover = String.format("%d/%d cols", keySize, indexSize);

        String usageInfo = String.format("(%s %s)", scanType, cover);
        if (keySize == 0)
            usageInfo = "(for sort order only)";

        String retval = "INDEX COUNT of \"" + m_targetTableName + "\"";
        retval += " using \"" + m_targetIndexName + "\"";
        retval += " " + usageInfo;
        return retval;
    }

    public ArrayList<AbstractExpression> getBindings() {
        return m_bindings;
    }
}
