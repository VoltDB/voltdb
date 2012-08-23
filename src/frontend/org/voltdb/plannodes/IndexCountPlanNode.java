/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.PlanStatistics;
import org.voltdb.planner.StatsField;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.PlanNodeType;

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
    final protected List<AbstractExpression> m_endkeyExpressions = new ArrayList<AbstractExpression>();

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

    public IndexCountPlanNode() {
        super();
    }

    public IndexCountPlanNode(IndexScanPlanNode isp, AggregatePlanNode apn) {
        super();

        m_catalogIndex = isp.m_catalogIndex;

        m_estimatedOutputTupleCount = 1;
        m_tableSchema = isp.m_tableSchema;
        m_tableScanSchema = isp.m_tableScanSchema.clone();

        m_targetTableAlias = isp.m_targetTableAlias;
        m_targetTableName = isp.m_targetTableName;
        m_targetIndexName = isp.m_targetIndexName;

        m_lookupType = isp.m_lookupType;
        m_searchkeyExpressions = isp.m_searchkeyExpressions;
        m_predicate = null;

        m_outputSchema = apn.getOutputSchema().clone();
        this.setEndKeyExpression(isp.getEndExpression());
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
     *
     * @param lookupType
     */
    public void setLookupType(IndexLookupType lookupType) {
        m_lookupType = lookupType;
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

    public void addEndKeyExpression(AbstractExpression expr)
    {
        if (expr != null)
        {
            // PlanNodes all need private deep copies of expressions
            // so that the resolveColumnIndexes results
            // don't get bashed by other nodes or subsequent planner runs
            try
            {
                m_endkeyExpressions.add(0,(AbstractExpression) expr.clone());
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
     * When call this function, we can assume there is not post expression
     * when I want to set endKey. And the endKey can not be null.
     * @param endExpr
     */
    public void setEndKeyExpression(AbstractExpression endExpr) {
        if (endExpr != null) {
            ArrayList <AbstractExpression> subEndExpr = endExpr.findAllSubexpressionsOfClass(ComparisonExpression.class);
            for (AbstractExpression ae: subEndExpr) {
                assert (ae.getLeft() instanceof TupleValueExpression);
                if (ae.getExpressionType() == ExpressionType.COMPARE_LESSTHAN)
                    m_endType = IndexLookupType.LT;
                else if (ae.getExpressionType() == ExpressionType.COMPARE_LESSTHANOREQUALTO)
                    m_endType = IndexLookupType.LTE;
                this.addEndKeyExpression(ae.getRight());
            }
        }
    }

    @Override
    public void generateOutputSchema(Database db){}

    @Override
    public void resolveColumnIndexes(){}

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

        stats.incrementStatistic(0, StatsField.TUPLES_READ, 1);
        m_estimatedOutputTupleCount = 1;

        return true;
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

        int indexSize = m_catalogIndex.getColumns().size();
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
}
