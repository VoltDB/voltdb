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
import java.util.Set;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.ValidationError;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AbstractSubqueryExpression;
import org.voltdb.expressions.AggregateExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

public class AggregatePlanNode extends AbstractPlanNode {

    public enum Members {
        PRE_PREDICATE,   // ENG-1565: to accelerate min() / max() using index purpose only
        POST_PREDICATE,
        AGGREGATE_COLUMNS,
        AGGREGATE_TYPE,
        AGGREGATE_DISTINCT,
        AGGREGATE_OUTPUT_COLUMN,
        AGGREGATE_EXPRESSION,
        GROUPBY_EXPRESSIONS,
        PARTIAL_GROUPBY_COLUMNS,
        USER_AGGREGATE_ID,
        IS_WORKER,
        IS_PARTITION
        ;
    }

    protected List<ExpressionType> m_aggregateTypes = new ArrayList<>();
    // a list of whether the aggregate is over distinct elements
    // 0 is not distinct, 1 is distinct
    protected List<Integer> m_aggregateDistinct = new ArrayList<>();
    // a list of column offsets/indexes not plan column guids.
    protected List<Integer> m_aggregateOutputColumns = new ArrayList<>();
    // a list of IDs for user define aggregate functions
    protected List<Integer> m_userAggregateId = new ArrayList<>();
    // a list of booleans that represent whether it is a worker or a coordinator
    protected List<Boolean> m_isWorker = new ArrayList<>();
    // a list of booleans that represent whether it is a partioned table or a replicated table
    protected List<Boolean> m_isPartition = new ArrayList<>();
    // List of the input TVEs into the aggregates.  Maybe should become
    // a list of SchemaColumns someday
    protected List<AbstractExpression> mAggregateExpressions =
        new ArrayList<>();

    // At the moment these are guaranteed to be TVES.  This might always be true
    protected List<AbstractExpression> m_groupByExpressions
        = new ArrayList<>();

    // This list is only used for the special case of instances of PartialAggregatePlanNode.
    protected List<Integer> m_partialGroupByColumns = null;

    // True if this aggregate node is the coordinator summary aggregator
    // for an aggregator that was pushed down. Must know to correctly
    // decide if other nodes can be pushed down / past this node.
    public boolean m_isCoordinatingAggregator = false;

    protected AbstractExpression m_prePredicate;
    protected AbstractExpression m_postPredicate;

    public AggregatePlanNode() {
        super();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.AGGREGATE;
    }

    public List<ExpressionType> getAggregateTypes() {
        return m_aggregateTypes;
    }

    @Override
    public void validate() {
        super.validate();
        //
        // We need to have an aggregate type and column
        // We're not checking that it's a valid ExpressionType because this plannode is a temporary hack
        //
        final int numAggTypes = m_aggregateTypes.size(),
                numAggDist = m_aggregateDistinct.size(),
                numAggExpr = mAggregateExpressions.size(),
                numAggOut = m_aggregateOutputColumns.size();
        final boolean eq = numAggTypes == numAggDist &&
                numAggDist == numAggExpr &&
                numAggExpr == numAggOut;
        if (! eq) {
            throw new ValidationError(
                    "Mismatched number of aggregate expression column attributes for PlanNode '%s'",
                    toString());
        } else if (m_aggregateTypes.contains(ExpressionType.INVALID)) {
            throw new ValidationError("Invalid Aggregate ExpressionType for PlanNode '%s'", toString());
        }
        // NOTE: m_aggregateTypes can be empty, in queries such as from ENG-12105.
    }

    public boolean isTableCountStar() {
        return m_groupByExpressions.isEmpty() &&
                m_aggregateTypes.size() == 1 &&
                m_aggregateTypes.get(0).equals(ExpressionType.AGGREGATE_COUNT_STAR);
    }

    public boolean isTableNonDistinctCount() {
        return m_groupByExpressions.isEmpty() &&
                m_aggregateTypes.size() == 1 &&
                m_aggregateTypes.get(0).equals(ExpressionType.AGGREGATE_COUNT) &&
                m_aggregateDistinct.get(0) != 1;
    }

    public boolean isTableNonDistinctCountConstant() {
        if (!isTableNonDistinctCount()) {
            return false;
        }
        AbstractExpression aggArgument = mAggregateExpressions.get(0);
        ExpressionType argumentType = aggArgument.getExpressionType();
        // Is the expression a constant?
        return argumentType.equals(ExpressionType.VALUE_PARAMETER) ||
                argumentType.equals(ExpressionType.VALUE_CONSTANT);
    }

    public boolean isTableCountNonDistinctNullableColumn() {
        if (!isTableNonDistinctCount()) {
            return false;
        }
        // Is the expression a column?
        AbstractExpression aggArgument = mAggregateExpressions.get(0);
        if (! aggArgument.getExpressionType().equals(ExpressionType.VALUE_TUPLE)) {
            return false;
        }
        // Need to go to its child node to see the table schema.
        // Normally it has to be a ScanPlanNode.
        // If the query is a join query then the child will be something like nested loop.
        assert (m_children.size() == 1);
        if (! (m_children.get(0) instanceof AbstractScanPlanNode) ) {
            return false;
        }
        AbstractScanPlanNode asp = (AbstractScanPlanNode)m_children.get(0);
        if ( ! (asp.getTableScan() instanceof StmtTargetTableScan)) {
            return false;
        }
        StmtTargetTableScan sttscan = (StmtTargetTableScan)asp.getTableScan();
        Table tbl = sttscan.getTargetTable();
        TupleValueExpression tve = (TupleValueExpression)aggArgument;
        String columnName = tve.getColumnName();
        Column col = tbl.getColumns().get(columnName);
        // Is the column nullable?
        return !col.getNullable();
    }

    // single min() without GROUP BY?
    public boolean isTableMin() {
        // do not support GROUP BY for now
        return m_groupByExpressions.isEmpty() &&
                m_aggregateTypes.size() == 1 &&
                m_aggregateTypes.get(0).equals(ExpressionType.AGGREGATE_MIN);
    }

    // single max() without GROUP BY?
    public boolean isTableMax() {
        // do not support GROUP BY for now
        return m_groupByExpressions.isEmpty() &&
                m_aggregateTypes.size() == 1 &&
                m_aggregateTypes.get(0).equals(ExpressionType.AGGREGATE_MAX);
    }

    // set predicate for SELECT MAX(X) FROM T WHERE X > / >= ? case
    public void setPrePredicate(AbstractExpression predicate) {
        m_prePredicate = predicate;
    }

    public void setPostPredicate(AbstractExpression predicate) {
        m_postPredicate = predicate;
    }

    public AbstractExpression getPostPredicate() {
        return m_postPredicate;
    }

    // for single min() / max(), return the single aggregate expression
    public AbstractExpression getFirstAggregateExpression() {
        return mAggregateExpressions.get(0);
    }

    public int getAggregateTypesSize () {
        return m_aggregateTypes.size();
    }

    public List<AbstractExpression> getGroupByExpressions() {
        return m_groupByExpressions;
    }

    public int getGroupByExpressionsSize () {
        return m_groupByExpressions.size();
    }

    public int getUserAggregateId(int index) {
        return m_userAggregateId.get(index);
    }

    public void setOutputSchema(NodeSchema schema) {
        // aggregates currently have their output schema specified
        m_outputSchema = schema.clone();
        m_hasSignificantOutputSchema = true;
    }

    @Override
    public void generateOutputSchema(Database db) {
        // aggregate's output schema is pre-determined
        if (m_children.size() == 1) {
            m_children.get(0).generateOutputSchema(db);

            assert(m_hasSignificantOutputSchema);
        }

        // Generate the output schema for subqueries
        Collection<AbstractExpression> subqueryExpressions = findAllSubquerySubexpressions();
        for (AbstractExpression subqueryExpression : subqueryExpressions) {
            assert(subqueryExpression instanceof AbstractSubqueryExpression);
            ((AbstractSubqueryExpression) subqueryExpression).generateOutputSchema(db);
        }
    }

    @Override
    public void resolveColumnIndexes() {
        // Aggregates need to resolve indexes for the output schema but don't need
        // to reorder it.  Some of the outputs may be local aggregate columns and
        // won't have a TVE to resolve.
        assert (m_children.size() == 1);
        m_children.get(0).resolveColumnIndexes();
        NodeSchema inputSchema = m_children.get(0).getOutputSchema();

        resolveColumnIndexesUsingSchema(inputSchema);
    }

    void resolveColumnIndexesUsingSchema(NodeSchema inputSchema) {
        Collection<TupleValueExpression> allTves;

        // get all the TVEs in the output columns
        for (SchemaColumn col : m_outputSchema) {
            AbstractExpression colExpr = col.getExpression();
            allTves = ExpressionUtil.getTupleValueExpressions(colExpr);
            for (TupleValueExpression tve : allTves) {
                int index = tve.setColumnIndexUsingSchema(inputSchema);
                if (index == -1) {
                    // check to see if this TVE is the aggregate output
                    if ( ! tve.getTableName().equals(AbstractParsedStmt.TEMP_TABLE_NAME)) {
                        throw new RuntimeException("Unable to find index for column: " +
                                tve.getColumnName());
                    }
                }
            }
        }

        // Aggregates also need to resolve indexes for aggregate inputs
        // Find the proper index for the sort columns.  Not quite
        // sure these should be TVEs in the long term.

        for (AbstractExpression agg_exp : mAggregateExpressions) {
            allTves = ExpressionUtil.getTupleValueExpressions(agg_exp);
            for (TupleValueExpression tve : allTves) {
                tve.setColumnIndexUsingSchema(inputSchema);
            }
        }

        // Aggregates also need to resolve indexes for group_by inputs
        for (AbstractExpression group_exp : m_groupByExpressions) {
            allTves = ExpressionUtil.getTupleValueExpressions(group_exp);
            for (TupleValueExpression tve : allTves) {
                tve.setColumnIndexUsingSchema(inputSchema);
            }
        }

        // Post filter also needs to resolve indexes, but a little
        // differently since it applies to the OUTPUT tuple.
        allTves = ExpressionUtil.getTupleValueExpressions(m_postPredicate);
        for (TupleValueExpression tve : allTves) {
            int index = m_outputSchema.getIndexOfTve(tve);
            tve.setColumnIndex(index);
        }

        resolveSubqueryColumnIndexes();
    }

    @Override
    protected void resolveSubqueryColumnIndexes() {
        // Possible subquery expressions
        Collection<AbstractExpression> exprs = findAllSubquerySubexpressions();
        for (AbstractExpression expr: exprs) {
            ((AbstractSubqueryExpression) expr).resolveColumnIndexes();
        }
    }

    public void addUserDefineAggregateId(int id) {
        m_userAggregateId.add(id);
    }

    /**
     * Add an aggregate to this plan node.
     * @param aggType
     * @param isDistinct  Is distinct being applied to the argument of this aggregate?
     * @param aggOutputColumn  Which output column in the output schema this
     *        aggregate should occupy
     * @param aggInputExpr  The input expression which should get aggregated
     */
    public void addAggregate(ExpressionType aggType,
                             boolean isDistinct,
                             Integer aggOutputColumn,
                             AbstractExpression aggInputExpr) {
        m_aggregateTypes.add(aggType);
        if (isDistinct) {
            m_aggregateDistinct.add(1);
        } else {
            m_aggregateDistinct.add(0);
        }
        m_aggregateOutputColumns.add(aggOutputColumn);
        if (aggType.isNullary()) {
            assert(aggInputExpr == null);
            mAggregateExpressions.add(null);
        } else {
            assert(aggInputExpr != null);
            mAggregateExpressions.add(aggInputExpr.clone());
        }
        m_isWorker.add(true);
        m_isPartition.add(true);
    }

    public void updateAggregate(
            int index,
            ExpressionType aggType) {

        // Create a new aggregate expression which we'll use to update the
        // output schema (whose exprs are TVEs).
        AggregateExpression aggExpr = new AggregateExpression(aggType);
        aggExpr.finalizeValueTypes();

        int outputSchemaIndex = m_aggregateOutputColumns.get(index);
        SchemaColumn schemaCol = m_outputSchema.getColumn(outputSchemaIndex);
        AbstractExpression schemaExpr = schemaCol.getExpression();
        schemaExpr.setValueType(aggExpr.getValueType());
        schemaExpr.setValueSize(aggExpr.getValueSize());

        m_aggregateTypes.set(index, aggType);
    }

    // This method updates the return type of the distNode to be varbinary
    // and size to be 1048576 (which is the maximum)
    public void updateUserDefinedAggregate(int index) {
        int outputSchemaIndex = m_aggregateOutputColumns.get(index);
        SchemaColumn schemaCol = m_outputSchema.getColumn(outputSchemaIndex);
        AbstractExpression schemaExpr = schemaCol.getExpression();
        schemaExpr.setValueType(VoltType.VARBINARY);
        schemaExpr.setValueSize(1048576);
    }

    // This method updates m_isWorker at a given index
    // to be false which means it is a coordinator, not a worker
    public void updateWorkerOrCoordinator(int index) {
        m_isWorker.set(index, false);
    }

    // This method updates m_isPartition at a given index
    // to be false which means it is a replicated table, not a partitioned table
    public void updatePartitionOrReplicate(int index) {
        m_isPartition.set(index, false);
    }

    public void addGroupByExpression(AbstractExpression expr) {
        if (expr != null) {
            m_groupByExpressions.add(expr.clone());
        }
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);

        stringer.key("AGGREGATE_COLUMNS");
        stringer.array();
        for (int ii = 0; ii < m_aggregateTypes.size(); ii++) {
            stringer.object();
            // UDFTODO: You only put in the id, is_worker, is_partition when the id is not -1
            /*
            if (m_userAggregateId.get(ii) != -1) {
                stringer.keySymbolValuePair(Members.USER_AGGREGATE_ID.name(), m_userAggregateId.get(ii));
                stringer.keySymbolValuePair(Members.IS_WORKER.name(), m_isWorker.get(ii));
                stringer.keySymbolValuePair(Members.IS_PARTITION.name(), m_isPartition.get(ii));
            }
            */
            if (m_userAggregateId.size() > ii) {
                stringer.keySymbolValuePair(Members.USER_AGGREGATE_ID.name(), m_userAggregateId.get(ii));
            }
            stringer.keySymbolValuePair(Members.IS_WORKER.name(), m_isWorker.get(ii));
            stringer.keySymbolValuePair(Members.IS_PARTITION.name(), m_isPartition.get(ii));
            stringer.keySymbolValuePair(Members.AGGREGATE_TYPE.name(), m_aggregateTypes.get(ii).name());
            stringer.keySymbolValuePair(Members.AGGREGATE_DISTINCT.name(), m_aggregateDistinct.get(ii));
            stringer.keySymbolValuePair(Members.AGGREGATE_OUTPUT_COLUMN.name(), m_aggregateOutputColumns.get(ii));
            AbstractExpression ae = mAggregateExpressions.get(ii);
            if (ae != null) {
                stringer.key(Members.AGGREGATE_EXPRESSION.name());
                stringer.object();
                ae.toJSONString(stringer);
                stringer.endObject();
            }
            stringer.endObject();
        }
        stringer.endArray();

        if (! m_groupByExpressions.isEmpty()) {
            stringer.key(Members.GROUPBY_EXPRESSIONS.name()).array();
            for (AbstractExpression m_groupByExpression : m_groupByExpressions) {
                stringer.object();
                m_groupByExpression.toJSONString(stringer);
                stringer.endObject();
            }
            stringer.endArray();

            if (m_partialGroupByColumns != null) {
                assert(! m_partialGroupByColumns.isEmpty());
                stringer.key(Members.PARTIAL_GROUPBY_COLUMNS.name()).array();
                for (Integer ith: m_partialGroupByColumns) {
                    stringer.value(ith.longValue());
                }
                stringer.endArray();
            }
        }

        if (m_prePredicate != null) {
            stringer.key(Members.PRE_PREDICATE.name()).value(m_prePredicate);
        }
        if (m_postPredicate != null) {
            stringer.key(Members.POST_PREDICATE.name()).value(m_postPredicate);
        }
    }

    private static String planNodeTypeToAggDescString(PlanNodeType nodeType) {
        switch (nodeType) {
        case AGGREGATE:
            return "Serial";
        case PARTIALAGGREGATE:
            return "Partial";
        default:
            assert(nodeType == PlanNodeType.HASHAGGREGATE);
            return "Hash";
        }
    }

    @Override
    protected String explainPlanForNode(String indent) {
        StringBuilder sb = new StringBuilder();
        String optionalTableName = "*NO MATCH -- USE ALL TABLE NAMES*";
        String aggType = planNodeTypeToAggDescString(getPlanNodeType());

        sb.append(aggType).append(" AGGREGATION ops: ");
        String sep = "";
        int ii = 0;
        for (ExpressionType e : m_aggregateTypes) {
            sb.append(sep).append(e.symbol());
            sep = ", ";
            if (e != ExpressionType.AGGREGATE_COUNT_STAR) {
                if (m_aggregateDistinct.get(ii) == 1) {
                    sb.append(" DISTINCT");
                }
                AbstractExpression ae = mAggregateExpressions.get(ii);
                assert(ae != null);
                sb.append("(");
                sb.append(ae.explain(optionalTableName));
                sb.append(")");
            }
            ++ii;
        }
        if (m_prePredicate != null) {
            sb.append(" ONLY IF ").append(m_prePredicate.explain(optionalTableName));
        }
        if (m_postPredicate != null) {
            // HAVING is always defined WRT to the current outputSchema (NOT inputschema).
            // This might be a little surprising to the user
            // -- maybe we can find some better way to describe the TVEs, here.
            sb.append(" HAVING ").append(m_postPredicate.explain(AbstractParsedStmt.TEMP_TABLE_NAME));
        }

        return sb.toString();
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        helpLoadFromJSONObject(jobj, db);
        JSONArray jarray = jobj.getJSONArray( Members.AGGREGATE_COLUMNS.name() );
        int size = jarray.length();
        for (int i = 0; i < size; i++) {
            JSONObject tempObj = jarray.getJSONObject( i );
            m_aggregateTypes.add( ExpressionType.get( tempObj.getString( Members.AGGREGATE_TYPE.name() )));
            m_aggregateDistinct.add( tempObj.getInt( Members.AGGREGATE_DISTINCT.name() ) );
            m_aggregateOutputColumns.add( tempObj.getInt( Members.AGGREGATE_OUTPUT_COLUMN.name() ));

            if (tempObj.isNull(Members.AGGREGATE_EXPRESSION.name())) {
                mAggregateExpressions.add(null);
            } else {
                mAggregateExpressions.add(
                    AbstractExpression.fromJSONChild(tempObj, Members.AGGREGATE_EXPRESSION.name()));
            }
        }
        AbstractExpression.loadFromJSONArrayChild(m_groupByExpressions, jobj,
                                                  Members.GROUPBY_EXPRESSIONS.name(), null);

        if ( ! jobj.isNull(Members.PARTIAL_GROUPBY_COLUMNS.name())) {
            JSONArray jarray2 = jobj.getJSONArray(Members.PARTIAL_GROUPBY_COLUMNS.name());
            int numCols = jarray2.length();
            m_partialGroupByColumns = new ArrayList<>(numCols);
            for (int ii = 0; ii < numCols; ++ii) {
                m_partialGroupByColumns.add(jarray2.getInt(ii));
            }
        }

        m_prePredicate = AbstractExpression.fromJSONChild(jobj, Members.PRE_PREDICATE.name());
        m_postPredicate = AbstractExpression.fromJSONChild(jobj, Members.POST_PREDICATE.name());
    }

    public static AggregatePlanNode getInlineAggregationNode(AbstractPlanNode node) {
        AggregatePlanNode aggNode =
                (AggregatePlanNode) (node.getInlinePlanNode(PlanNodeType.AGGREGATE));
        if (aggNode == null) {
            aggNode = (HashAggregatePlanNode) (node.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));
        }
        if (aggNode == null) {
            aggNode = (PartialAggregatePlanNode) (node.getInlinePlanNode(PlanNodeType.PARTIALAGGREGATE));
        }

        return aggNode;
    }

    @Override
    public void findAllExpressionsOfClass(Class< ? extends AbstractExpression> aeClass, Set<AbstractExpression> collected) {
        super.findAllExpressionsOfClass(aeClass, collected);
        if (m_prePredicate != null) {
            collected.addAll(m_prePredicate.findAllSubexpressionsOfClass(aeClass));
        }
        if (m_postPredicate != null) {
            collected.addAll(m_postPredicate.findAllSubexpressionsOfClass(aeClass));
        }
        for (AbstractExpression ae : mAggregateExpressions) {
            if (ae == null) {
                // This is a place-holder for the "*" in "COUNT(*)".
                // There are no subexpressions to find here.
                continue;
            }
            collected.addAll(ae.findAllSubexpressionsOfClass(aeClass));
        }
        for (AbstractExpression ae : m_groupByExpressions) {
            collected.addAll(ae.findAllSubexpressionsOfClass(aeClass));
        }
    }

    @Override
    public boolean isOutputOrdered (List<AbstractExpression> sortExpressions, List<SortDirectionType> sortDirections) {
        if (getPlanNodeType() == PlanNodeType.HASHAGGREGATE) {
            return false;
        } else {
            // the order for Serial and Partial aggregates is determined by the order
            // of the keys from the child node
            assert(getChildCount() == 1);
            AbstractPlanNode child = getChild(0);
            return child.isOutputOrdered(sortExpressions, sortDirections);
        }
    }

    /**
     * Convert HashAggregate into a Serialized Aggregate
     *
     * @param hashAggregateNode HashAggregatePlanNode
     * @return AggregatePlanNode
     */
    public static AggregatePlanNode convertToSerialAggregatePlanNode(HashAggregatePlanNode hashAggregateNode) {
        AggregatePlanNode serialAggr = new AggregatePlanNode();
        return setAggregatePlanNode(hashAggregateNode, serialAggr);
    }

    /**
     * Convert HashAggregate into a Partial Aggregate
     *
     * @param hashAggregateNode HashAggregatePlanNode
     * @param aggrColumnIdxs partial aggregate column indexes
     * @return AggregatePlanNode
     */
    public static AggregatePlanNode convertToPartialAggregatePlanNode(HashAggregatePlanNode hashAggregateNode,
            List<Integer> aggrColumnIdxs) {
        final AggregatePlanNode partialAggr = setAggregatePlanNode(hashAggregateNode, new PartialAggregatePlanNode());
        partialAggr.m_partialGroupByColumns = aggrColumnIdxs;
        return partialAggr;
    }

    private static AggregatePlanNode setAggregatePlanNode(AggregatePlanNode origin, AggregatePlanNode destination) {
        destination.m_isCoordinatingAggregator = origin.m_isCoordinatingAggregator;
        destination.m_prePredicate = origin.m_prePredicate;
        destination.m_postPredicate = origin.m_postPredicate;
        for (AbstractExpression expr : origin.m_groupByExpressions) {
            destination.addGroupByExpression(expr);
        }

        List<ExpressionType> aggregateTypes = origin.m_aggregateTypes;
        List<Integer> aggregateDistinct = origin.m_aggregateDistinct;
        List<Integer> aggregateOutputColumns = origin.m_aggregateOutputColumns;
        List<AbstractExpression> aggregateExpressions = origin.mAggregateExpressions;
        for (int i = 0; i < origin.getAggregateTypesSize(); i++) {
            destination.addAggregate(aggregateTypes.get(i),
                    aggregateDistinct.get(i) == 1,
                    aggregateOutputColumns.get(i),
                    aggregateExpressions.get(i));
        }
        destination.setOutputSchema(origin.getOutputSchema());
        return destination;
    }

    @Override
    /**
     * AggregatePlanNodes don't need projection nodes.
     */
    public boolean planNodeClassNeedsProjectionNode() {
        return false;
    }
}
