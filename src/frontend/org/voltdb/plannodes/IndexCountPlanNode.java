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

package org.voltdb.plannodes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hsqldb_voltpatches.HSQLInterface;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ExpressionUtil;
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
        COMPARE_NOTDISTINCT,
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
    protected List<AbstractExpression> m_endkeyExpressions = new ArrayList<>();

    // This list of expressions corresponds to the values that we will use
    // at runtime in the lookup on the index
    protected List<AbstractExpression> m_searchkeyExpressions = new ArrayList<>();

    // If the search key expression is actually a "not distinct" expression, we do not want the executor to skip null candidates.
    protected List<Boolean> m_compareNotDistinct = new ArrayList<>();

    // The overall index lookup operation type
    protected IndexLookupType m_lookupType = IndexLookupType.EQ;

    // The overall index lookup operation type
    protected IndexLookupType m_endType = IndexLookupType.EQ;

    // A reference to the Catalog index object which defined the index which
    // this index scan is going to use
    protected Index m_catalogIndex = null;

    private List<AbstractExpression> m_bindings;

    private AbstractExpression m_skip_null_predicate;

    public IndexCountPlanNode() {
        super();
    }

    public IndexCountPlanNode(String tableName, String tableAlias) {
        super(tableName, tableAlias);
        assert(tableName != null && tableAlias != null);
    }

    private IndexCountPlanNode(IndexScanPlanNode isp, AggregatePlanNode apn,
            IndexLookupType endType, List<AbstractExpression> endKeys) {
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

        if ( ! isp.isReverseScan()) {
            m_lookupType = isp.m_lookupType;
            m_searchkeyExpressions = isp.m_searchkeyExpressions;
            m_compareNotDistinct = isp.m_compareNotDistinct;

            m_endType = endType;
            m_endkeyExpressions.addAll(endKeys);

            setSkipNullPredicate(false);
        } else {
            // for reverse scan, swap everything of searchkey and endkey
            // because we added the last < / <= to searchkey but not endExpr
            assert(endType == IndexLookupType.EQ);
            m_lookupType = endType;     // must be EQ, but doesn't matter, since previous lookup type is not GT
            m_searchkeyExpressions.addAll(endKeys);
            m_compareNotDistinct = isp.m_compareNotDistinct;
            // For this additional < / <= expression, we set CompareNotDistinctFlag = false.
            // This is because the endkey came from doubleBoundExpr (in getRelevantAccessPathForIndex()),
            // which has no way to be "IS NOT DISTINCT FROM". (ENG-11096)
            m_compareNotDistinct.add(false);
            m_endType = isp.m_lookupType;
            m_endkeyExpressions = isp.getSearchKeyExpressions();

            setSkipNullPredicate(true);
        }
    }

    public boolean hasTargetIndexName(String indexName) {
        return m_targetIndexName.equals(indexName);
    }

    public boolean hasSkipNullPredicate() {
        return m_skip_null_predicate != null;
    }

    public List<Boolean> getCompareNotDistinctFlags() {
        return m_compareNotDistinct;
    }

    private void setSkipNullPredicate(boolean isReverseScan) {
        int nullExprIndex;
        if (isReverseScan) {
            if (m_searchkeyExpressions.size() >= m_endkeyExpressions.size()) {
                return;
            }

            assert(m_endType == IndexLookupType.LT || m_endType == IndexLookupType.LTE);
            assert(m_endkeyExpressions.size() - m_searchkeyExpressions.size() == 1);
            nullExprIndex = m_searchkeyExpressions.size();
        } else {
            // useful for underflow case to eliminate nulls
            if (m_searchkeyExpressions.size() < m_endkeyExpressions.size() ||
                    (m_lookupType != IndexLookupType.GT && m_lookupType != IndexLookupType.GTE)) {
                return;
            }

            assert(m_searchkeyExpressions.size() > 0);
            nullExprIndex = m_searchkeyExpressions.size() - 1;
        }
        m_skip_null_predicate = IndexScanPlanNode.buildSkipNullPredicate(
                nullExprIndex, m_catalogIndex, m_tableScan, 0,
                m_searchkeyExpressions, m_compareNotDistinct);
        if (m_skip_null_predicate != null) {
            m_skip_null_predicate.resolveForTable((Table)m_catalogIndex.getParent());
        }
    }

    // Create an IndexCountPlanNode that replaces the parent aggregate and child
    // indexscan IF the indexscan's end expressions are a form that can be
    // modeled with an end key.
    // The supported forms for end expression are:
    //   - null
    //   - one filter expression per index key component (ANDed together)
    //     as "combined" for the IndexScan.
    //   - fewer filter expressions than index key components with one of the
    //     (the last) being a LT comparison.
    //   - 1 fewer filter expressions than index key components,
    //     but all ANDed equality filters
    // The LT restriction comes because when index key prefixes are identical
    // to the prefix-only end key, the entire index key sorts greater than the
    // prefix-only end-key, because it is always longer.
    // These prefix-equal cases would be missed in an EQ or LTE filter,
    // causing undercounts.
    // A prefix-only LT filter discards prefix-equal cases, so it is allowed.
    // @return the IndexCountPlanNode or null if one is not possible.
    public static IndexCountPlanNode createOrNull(
            IndexScanPlanNode isp, AggregatePlanNode apn) {
        // add support for reverse scan
        // for ASC scan, check endExpression;
        // for DESC scan (isReverseScan()), check the searchkeys
        List<AbstractExpression> endKeys = new ArrayList<>();

        // Translate the index scan's end condition into a list of end key
        // expressions and note the comparison operand of the last one.

        // Initially assume it to be an equality filter.
        IndexLookupType endType = IndexLookupType.EQ;
        List<AbstractExpression> endComparisons = ExpressionUtil.uncombinePredicate(isp.getEndExpression());
        for (AbstractExpression ae: endComparisons) {
            // There should be no more end expressions after the
            // LT or LTE expression that resets the end type.
            assert(endType == IndexLookupType.EQ);

            final ExpressionType exprType = ae.getExpressionType();
            if (exprType == ExpressionType.COMPARE_LESSTHAN) {
                endType = IndexLookupType.LT;
            } else if (exprType == ExpressionType.COMPARE_LESSTHANOREQUALTO) {
                endType = IndexLookupType.LTE;
            } else {
                assert(exprType == ExpressionType.COMPARE_EQUAL || exprType == ExpressionType.COMPARE_NOTDISTINCT);
            }

            // PlanNodes all need private deep copies of expressions
            // so that the resolveColumnIndexes results
            // don't get bashed by other nodes or subsequent planner runs
            endKeys.add(ae.getRight().clone());
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

        int searchKeySize = isp.getSearchKeyExpressions().size();
        int endKeySize = endKeys.size();

        if (! isp.isReverseScan() &&
                endType != IndexLookupType.LT &&
                endKeySize > 0 &&
                endKeySize < indexSize) {

            // Decide whether to pad last endKey to solve
            // SELECT COUNT(*) FROM T WHERE C1 = ? AND C2 >[=] ?;
            // Avoid the cases that would cause undercounts for prefix matches.
            // That is, when a prefix-only key exists and does not use LT.
            if (endType != IndexLookupType.EQ || searchKeySize != indexSize || endKeySize < indexSize - 1) {
                return null;
            }

            // To use an index count for an equality search of da compound key,
            // both the search key and end key must have a component for each
            // index component.
            // If the search key is long enough but the end key is one component
            // short, it can be patched with a type-appropriate max key value
            // (if one exists for the type), but the end key comparison needs to
            // change from EQ to LTE to compensate.
            VoltType missingEndKeyType;

            // Check that the missing filter is on the last key component
            // and get the missing key component's indexed expression.
            if (jsonstring.isEmpty()) {
                int lastIndex = indexedColRefs.get(endKeySize).getColumn().getIndex();
                for (AbstractExpression expr : endComparisons) {
                    if (((TupleValueExpression)(expr.getLeft())).getColumnIndex() == lastIndex) {
                        return null;
                    }
                }
                int catalogTypeCode = indexedColRefs.get(endKeySize).getColumn().getType();
                missingEndKeyType = VoltType.get((byte)catalogTypeCode);
            } else {
                AbstractExpression lastIndexedExpr = indexedExprs.get(endKeySize);
                for (AbstractExpression expr : endComparisons) {
                    if (expr.getLeft().bindingToIndexedExpression(lastIndexedExpr) != null) {
                        return null;
                    }
                }
                missingEndKeyType = lastIndexedExpr.getValueType();
            }

            String maxValueForType = missingEndKeyType.getMaxValueForKeyPadding();

            // The last end key's type must have a canonical maximum value
            // for which all legal values are less than or equal to it.
            if (maxValueForType == null) {
                return null;
            }

            ConstantValueExpression maxKey = new ConstantValueExpression();
            maxKey.setValueType(missingEndKeyType);
            maxKey.setValue(maxValueForType);
            maxKey.setValueSize(missingEndKeyType.getLengthInBytesForFixedTypes());
            endType = IndexLookupType.LTE;
            endKeys.add(maxKey);
        }

        // DESC case
        if (searchKeySize > 0 && searchKeySize < indexSize) {
            return null;
        } else {
            return new IndexCountPlanNode(isp, apn, endType, endKeys);
        }
    }

    @Override
    public void getTablesAndIndexes(Map<String, StmtTargetTableScan> tablesRead,
            Collection<String> indexes) {
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
    public void validate() {
        super.validate();

        // There needs to be at least one search key expression
        // TODO: see for example, TestExplainCommandSuite
        /*if (m_searchkeyExpressions.isEmpty()) {
            throw new RuntimeException("ERROR: There were no search key expressions defined for " + this);
        }*/
        m_searchkeyExpressions.forEach(AbstractExpression::validate);
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
    public void computeCostEstimates(long childOutputTupleCountEstimate,
                                     DatabaseEstimates estimates, ScalarValueHints[] paramHints) {
        // Cost counting index scans as constant, almost negligible work.
        // This might be unfair, as the tree has O(logn) complexity, but we
        // really want to pick this kind of search over others.
        m_estimatedProcessedTupleCount = 1;
        m_estimatedOutputTupleCount = 1;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.keySymbolValuePair(Members.LOOKUP_TYPE.name(), m_lookupType.toString());
        stringer.keySymbolValuePair(Members.END_TYPE.name(), m_endType.toString());
        stringer.keySymbolValuePair(Members.TARGET_INDEX_NAME.name(), m_targetIndexName);

        stringer.key(Members.ENDKEY_EXPRESSIONS.name());
        if ( m_endkeyExpressions.isEmpty()) {
            stringer.valueNull();
        } else {
            stringer.array(m_endkeyExpressions);
        }

        stringer.key(Members.SEARCHKEY_EXPRESSIONS.name()).array(m_searchkeyExpressions);
        booleanArrayToJSONString(stringer, Members.COMPARE_NOTDISTINCT.name(), m_compareNotDistinct);

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
        // load end_expression
        AbstractExpression.loadFromJSONArrayChild(m_endkeyExpressions, jobj,
                Members.ENDKEY_EXPRESSIONS.name(), m_tableScan);
        // load searchkey_expressions
        AbstractExpression.loadFromJSONArrayChild(m_searchkeyExpressions, jobj,
                Members.SEARCHKEY_EXPRESSIONS.name(), m_tableScan);
        // load COMPARE_NOTDISTINCT flag vector
        loadBooleanArrayFromJSONObject(jobj, Members.COMPARE_NOTDISTINCT.name(), m_compareNotDistinct);
        // load skip_null_predicate
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
        if (indexSize > keySize) {
            cover = String.format("%d/%d cols", keySize, indexSize);
        }

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
        } else {
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
            String start = explainKeys(asIndexed, m_searchkeyExpressions, m_targetTableName, m_lookupType, m_compareNotDistinct);
            usageInfo += "\n" + indent + " count matches from " + start;
        }
        if (endkeySize > 0) {
            String end = explainKeys(asIndexed, m_endkeyExpressions, m_targetTableName, m_endType, m_compareNotDistinct);
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
                m_targetIndexName.startsWith(HSQLInterface.AUTO_GEN_NAMED_CONSTRAINT_IDX) ||
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
            String targetTableName, IndexLookupType lookupType, List<Boolean> compareNotDistinct) {
        String conjunction = "";
        StringBuilder result = new StringBuilder("(");
        int prefixSize = keyExpressions.size() - 1;
        for (int ii = 0; ii < prefixSize; ++ii) {
            result.append(conjunction)
                    .append(asIndexed[ii])
                    .append(compareNotDistinct.get(ii) ? " NOT DISTINCT " : " = ")
                    .append(keyExpressions.get(ii).explain(targetTableName));
            conjunction = ") AND (";
        }
        // last element
        result.append(conjunction).append(asIndexed[prefixSize]).append(" ");
        if (lookupType == IndexLookupType.EQ && compareNotDistinct.get(prefixSize)) {
            result.append("NOT DISTINCT");
        } else {
            result.append(lookupType.getSymbol());
        }
        result.append(" ").append(keyExpressions.get(prefixSize).explain(targetTableName));
        if (lookupType != IndexLookupType.EQ && compareNotDistinct.get(prefixSize)) {
            result.append(", including NULLs");
        }
        result.append(")");
        return result.toString();
    }

    public List<AbstractExpression> getBindings() {
        return m_bindings;
    }

    @Override
    public void findAllExpressionsOfClass(Class< ? extends AbstractExpression> aeClass, Set<AbstractExpression> collected) {
        super.findAllExpressionsOfClass(aeClass, collected);
        if (m_skip_null_predicate != null) {
            collected.addAll(m_skip_null_predicate.findAllSubexpressionsOfClass(aeClass));
        }
        for (AbstractExpression ae : m_searchkeyExpressions) {
            collected.addAll(ae.findAllSubexpressionsOfClass(aeClass));
        }
        if (m_bindings != null) {
            for (AbstractExpression ae : m_bindings) {
                collected.addAll(ae.findAllSubexpressionsOfClass(aeClass));
            }
        }
    }
}
