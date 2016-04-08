package org.voltdb.plannodes;

import java.util.ArrayList;
import java.util.List;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AggregateExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

public class PartitionPlanNode extends AggregatePlanNode {
    public enum Members {
        SORT_COLUMNS,
        SORT_EXPRESSION,
        SORT_DIRECTION,
        GROUPBY_EXPRESSIONS
    };
    
    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.PARTITION;
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        
        for (AbstractExpression expr : m_sortExpressions) {
            expr.validate();
        }
        for (AbstractExpression expr : m_partitionExpressions) {
            expr.validate();
        }
    }
    
    @Override
    public void generateOutputSchema(Database db) {
        super.generateOutputSchema(db);
        //
        // Add a column for the RANK operator.
        //
        // This is kind of convoluted.  The type is always BIGINT.  But this
        // is the way the projection node does it, and this is the way we will
        // need to do it when we support more than just RANK.  We create
        // an aggregate expression, but only so that we can get the value
        // type and size.
        AbstractExpression rankExpression = new AggregateExpression(ExpressionType.AGGREGATE_RANK);
        rankExpression.finalizeValueTypes();
        TupleValueExpression tve = new TupleValueExpression();
        tve.setValueType(rankExpression.getValueType());
        tve.setValueSize(rankExpression.getValueSize());
        SchemaColumn rankColumn = new SchemaColumn(null, null, null, null, tve);
        m_outputSchema.addColumn(rankColumn);
    }
    
    @Override
    public void resolveColumnIndexes() {
        assert(m_children.size() == 1);
        m_children.get(0).resolveColumnIndexes();
        NodeSchema input_schema = m_children.get(0).getOutputSchema();
        // get all the TVEs in the output columns
        List<TupleValueExpression> output_tves =
            new ArrayList<TupleValueExpression>();
        for (SchemaColumn col : m_outputSchema.getColumns())
        {
            output_tves.addAll(ExpressionUtil.getTupleValueExpressions(col.getExpression()));
        }
        for (AbstractExpression expr : m_partitionExpressions) {
            output_tves.addAll(ExpressionUtil.getTupleValueExpressions(expr));
        }
        for (AbstractExpression expr : m_sortExpressions) {
            output_tves.addAll(ExpressionUtil.getTupleValueExpressions(expr));
        }
        // and update their indexes against the table schema
        for (TupleValueExpression tve : output_tves)
        {
            int index = tve.resolveColumnIndexesUsingSchema(input_schema);
            tve.setColumnIndex(index);
        }
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return " PARTITION PLAN";
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        assert (m_sortExpressions.size() == m_sortDirections.size());
        /*
         * Serialize the sort columns.
         */
        stringer.key(Members.SORT_COLUMNS.name()).array();
        for (int ii = 0; ii < m_sortExpressions.size(); ii++) {
            stringer.object();
            stringer.key(Members.SORT_EXPRESSION.name());
            stringer.object();
            m_sortExpressions.get(ii).toJSONString(stringer);
            stringer.endObject();
            stringer.key(Members.SORT_DIRECTION.name()).value(m_sortDirections.get(ii).toString());
            stringer.endObject();
        }
        
        /*
         * Serialize the partition expressions.
         */
        stringer.key(Members.GROUPBY_EXPRESSIONS.name()).array();
        for (int idx = 0; idx < m_partitionExpressions.size(); idx += 1) {
            m_partitionExpressions.get(idx).toJSONString(stringer);
        }
        stringer.endArray();
    }

    @Override
    public void loadFromJSONObject(JSONObject jobj, Database db) throws JSONException {
        super.loadFromJSONObject(jobj, db);
        m_partitionExpressions.clear();
        m_sortExpressions.clear();
        m_sortDirections.clear();
        
        /*
         * Load the array of partition expressions.  The AbstractExpression class
         * has a useful routine to do just this for us.
         */
        AbstractExpression.loadFromJSONArrayChild(m_partitionExpressions, jobj, Members.GROUPBY_EXPRESSIONS.name(), null);
        
        /*
         * Unfortunately we cannot use AbstractExpression.loadFromJSONArrayChild here,
         * as we need to get a sort expression and a sort order for each column.
         */
        if (jobj.has(Members.SORT_COLUMNS.name())) {
            JSONArray jarray = jobj.getJSONArray(Members.SORT_COLUMNS.name());
            int size = jarray.length();
            for (int ii = size; ii < size; ii += 1) {
                JSONObject tempObj = jarray.getJSONObject(ii);
                m_sortDirections.add( SortDirectionType.get(tempObj.getString( Members.SORT_DIRECTION.name())) );
                m_sortExpressions.add( AbstractExpression.fromJSONChild(tempObj, Members.SORT_EXPRESSION.name()) );
            }
        }
    }

    private List<AbstractExpression> m_partitionExpressions;
    private List<AbstractExpression> m_sortExpressions;
    private List<SortDirectionType>  m_sortDirections;
}
