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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.exceptions.ValidationError;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AbstractSubqueryExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.parseinfo.StmtCommonTableScan;
import org.voltdb.planner.parseinfo.StmtSubqueryScan;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.types.PlanNodeType;
import org.voltdb.utils.CatalogUtil;

public abstract class AbstractScanPlanNode extends AbstractPlanNode {
    public enum Members {
        PREDICATE,
        TARGET_TABLE_NAME,
        TARGET_TABLE_ALIAS,
        SUBQUERY_INDICATOR,
        PREDICATE_FALSE;
    }

    // Store the columns from the table as an internal NodeSchema
    // for consistency of interface
    protected NodeSchema m_tableSchema = null;
    private NodeSchema m_preAggOutputSchema;
    // Store the columns we use from this table as an internal schema
    protected NodeSchema m_tableScanSchema = new NodeSchema();
    protected Map<Integer, Integer> m_differentiatorMap = new HashMap<>();
    protected AbstractExpression m_predicate;

    // The target table is the table that the plannode wants to perform some operation on.
    protected String m_targetTableName = "";
    protected String m_targetTableAlias = null;

    // Flag marking the sub-query plan
    protected boolean m_isSubQuery = false;
    protected StmtTableScan m_tableScan = null;

    protected AbstractScanPlanNode() {
        super();
    }


    protected AbstractScanPlanNode(String tableName, String tableAlias) {
        super();
        m_targetTableName = tableName;
        m_targetTableAlias = tableAlias;
    }

    @Override
    public void getTablesAndIndexes(Map<String, StmtTargetTableScan> tablesRead,
            Collection<String> indexes) {
        if (m_tableScan != null) {
            if (m_tableScan instanceof StmtTargetTableScan) {
                tablesRead.put(m_targetTableName, (StmtTargetTableScan)m_tableScan);
                getTablesAndIndexesFromSubqueries(tablesRead, indexes);
            } else if (m_tableScan instanceof StmtSubqueryScan) {
                getChild(0).getTablesAndIndexes(tablesRead, indexes);
            } else {
                // This is the only other choice.
                assert(m_tableScan instanceof StmtCommonTableScan);
                getTablesAndIndexesFromCommonTableQueries(tablesRead, indexes);
            }
        }
    }

    protected void getTablesAndIndexesFromCommonTableQueries(Map<String, StmtTargetTableScan> tablesRead,
                                                             Collection<String> indexes) {
        // Search the base and recursive plans.
        StmtCommonTableScan ctScan = (StmtCommonTableScan)m_tableScan;
        ctScan.getTablesAndIndexesFromCommonTableQueries(tablesRead, indexes);
    }


    @Override
    public void validate() {
        super.validate();
        //
        // TargetTableId
        //
        if (m_targetTableName == null) {
            throw new ValidationError("TargetTableName is null for PlanNode '%s'", toString());
        } else if (m_targetTableAlias == null) {
            throw new ValidationError("TargetTableAlias is null for PlanNode '%s'", toString());
        }
        //
        // Filter Expression
        // It is allowed to be null, but we need to check that it's valid
        //
        if (m_predicate != null) {
            m_predicate.validate();
        }
        // All the schema columns better reference this table
        for (SchemaColumn col : m_tableScanSchema) {
            if (!m_targetTableName.equals(col.getTableName())) {
                throw new ValidationError("The scan column: %s in table: %s refers to table: %s",
                        col.getColumnName(), m_targetTableName, col.getTableName());
            }
        }
    }

    /**
     * @return the target_table_name
     */
    public String getTargetTableName() {
        assert(m_targetTableName != null);
        return m_targetTableName;
    }

    /**
     * @param name
     */
    public void setTargetTableName(String name) {
        assert(m_isSubQuery || name != null);
        m_targetTableName = name;
    }

    /**
     * @return the target_table_alias
     */
    public String getTargetTableAlias() {
        assert(m_targetTableAlias != null);
        return m_targetTableAlias;
    }

    /**
     * @param alias
     */
    public void setTargetTableAlias(String alias) {
        assert(alias != null);
        m_targetTableAlias = alias;
    }

    public void setTableScan(StmtTableScan tableScan) {
        m_tableScan = tableScan;
        setSubQuery(tableScan instanceof StmtSubqueryScan);
        setTargetTableAlias(tableScan.getTableAlias());
        setTargetTableName(tableScan.getTableName());
        List<SchemaColumn> scanColumns = tableScan.getScanColumns();
        if (scanColumns != null && ! scanColumns.isEmpty()) {
            setScanColumns(scanColumns);
        }
    }

    public StmtTableScan getTableScan() {
        return m_tableScan;
    }

    /**
     * @return the predicate
     */
    public AbstractExpression getPredicate() {
        return m_predicate;
    }

    /**
     * @param exps the predicates to clone and combine into one predicate
     */
    @SafeVarargs
    public final void setPredicate(Collection<AbstractExpression>... colExps) {
        assert(colExps != null);
        // PlanNodes all need private deep copies of expressions
        // so that the resolveColumnIndexes results
        // don't get bashed by other nodes or subsequent planner runs
        m_predicate = ExpressionUtil.cloneAndCombinePredicates(colExps);
        if (m_predicate != null) {
            m_predicate.finalizeValueTypes();
        }
    }

    protected void setScanColumns(List<SchemaColumn> scanColumns) {
        assert(scanColumns != null);
        int i = 0;
        for (SchemaColumn col : scanColumns) {
            TupleValueExpression tve = (TupleValueExpression)col.getExpression();
            int difftor = tve.getDifferentiator();
            m_differentiatorMap.put(difftor, i);
            SchemaColumn clonedCol = col.clone();
            clonedCol.setDifferentiator(i);
            m_tableScanSchema.addColumn(clonedCol);
            ++i;
        }
    }

    /**
     * When a project node is added to the top of the plan, we need to adjust
     * the differentiator field of TVEs to reflect differences in the scan
     * schema vs the storage schema of a table, so that fields with duplicate names
     * produced by expanding "SELECT *" can resolve correctly.
     *
     * We recurse until we find either a join node or a scan node.
     *
     * For scan nodes, we need to reflect the difference between the
     * storage order of columns produced by a subquery, and the columns
     * that are actually projected (via an inlined project) from the scan,
     * since unused columns are typically omitted from the output schema
     * of the scan.
     *
     * @param  tve
     */
    @Override
    public void adjustDifferentiatorField(TupleValueExpression tve) {
        int storageIndex = tve.getColumnIndex();
        Integer scanIndex = m_differentiatorMap.get(storageIndex);
        assert(scanIndex != null);
        tve.setDifferentiator(storageIndex);
    }

    NodeSchema getTableSchema() {
        return m_tableSchema;
    }

    /**
     * Set the sub-query flag
     * @param isSubQuery
     */
    public void setSubQuery(boolean isSubQuery) {
        m_isSubQuery = isSubQuery;
    }

    /**
     * Accessor to return the sub-query flag
     * @return m_isSubQuery
     */
    @Override
    public boolean isSubQuery() {
        return m_isSubQuery;
    }

    /**
     * Is this a scan of a common table?
     * @return a boolean value indicating whether this is a common table scan.
     */
    public boolean isCommonTableScan() {
        // This function is only used to determine the output schema at planning time.
        // The StmtCommonTableScan, like all other types of TableScan class, is not
        // serialized to JSON format.
        // Therefore, when coming back to the scan plan nodes after the planning is done,
        // we cannot use this function to determine whether a scan is CTE scan or not.
        // SeqScanPlanNode has a separate boolean flag which is serialized with the JSON string.
        // Use isCommonTableScan() method in SeqScanPlanNode to test if it is a CTE scan.
        return (m_tableScan instanceof StmtCommonTableScan);
    }

    public boolean isPersistentTableScan() {
        return (! isCommonTableScan()) && (! isSubQuery());
    }

    @Override
    public void generateOutputSchema(Database db) {
        // fill in the table schema if we haven't already
        if (m_tableSchema == null) {
            initTableSchema(db);
        }
        InsertPlanNode ins = (InsertPlanNode)getInlinePlanNode(PlanNodeType.INSERT);
        if (ins != null) {
            ins.generateOutputSchema(db);
        }
        initPreAggOutputSchema();

        // Generate the output schema for subqueries
        Collection<AbstractExpression> exprs = findAllSubquerySubexpressions();
        for (AbstractExpression expr: exprs) {
            ((AbstractSubqueryExpression) expr).generateOutputSchema(db);
        }

        AggregatePlanNode aggNode = AggregatePlanNode.getInlineAggregationNode(this);
        if (aggNode != null) {
            // generate its subquery output schema
            aggNode.generateOutputSchema(db);

            m_outputSchema = aggNode.getOutputSchema().copyAndReplaceWithTVE();
            m_hasSignificantOutputSchema = true;
        }
    }


    // Until the scan has an implicit projection rather than an explicitly
    // inlined one, the output schema generation is going to be a bit odd.
    // It will depend on two bits of state: whether any scan columns were
    // specified for this table and whether or not there is an inlined
    // projection.
    //
    // If there is an inline projection, then we'll just steal that
    // output schema as our own.
    // If there is no existing projection, then, if there are no scan columns
    // specified, use the entire table's schema as the output schema.
    // Otherwise add an inline projection that projects the scan columns
    // and then take that output schema as our own.
    // These have the effect of repeatably generating the correct output
    // schema if called again and again, but also allowing the planner
    // to overwrite the inline projection and still have the right thing
    // happen.
    //
    // Note that when an index scan is inlined into a join node (as with
    // nested loop index joins), then there will be a project node inlined into
    // the index scan node that determines which columns from the inner table
    // are used as an output of the join, but that predicates evaluated against
    // this table should use the complete schema of the table being scanned.
    // See also the comments in NestLoopIndexPlanNode.resolveColumnIndexes.
    // Related tickets: ENG-9389, ENG-9533.
    private void initPreAggOutputSchema() {
        ProjectionPlanNode proj =
            (ProjectionPlanNode)getInlinePlanNode(PlanNodeType.PROJECTION);
        if (proj != null) {
            // Does this operation needs to change complex expressions
            // into tuple value expressions with an column alias?
            // Is this always true for clone?  Or do we need a new method?
            m_outputSchema = proj.getOutputSchema().copyAndReplaceWithTVE();
            // It's just a cheap knock-off of the projection's
            m_hasSignificantOutputSchema = false;
        } else if (m_tableScanSchema.size() != 0) {
            // Order the scan columns according to the table schema
            // before we stick them in the projection output
            int difftor = 0;
            for (SchemaColumn col : m_tableScanSchema) {
                col.setDifferentiator(difftor);
                ++difftor;
                AbstractExpression colExpr = col.getExpression();
                assert(colExpr instanceof TupleValueExpression);
                TupleValueExpression tve = (TupleValueExpression) colExpr;
                tve.setColumnIndexUsingSchema(m_tableSchema);
            }
            // and update their indexes against the table schema
            m_tableScanSchema.sortByTveIndex();

            // Create inline projection to map table outputs to scan outputs
            ProjectionPlanNode projectionNode =
                    new ProjectionPlanNode(m_tableScanSchema);
            addInlinePlanNode(projectionNode);
            // a bit redundant but logically consistent
            m_outputSchema = projectionNode.getOutputSchema().copyAndReplaceWithTVE();
            m_hasSignificantOutputSchema = false; // It's just a cheap knock-off of the projection's
        } else {
            // We come here if m_tableScanSchema is empty.
            //
            // m_tableScanSchema might be empty for cases like
            //   select now from table;
            // where there are no columns in the table that are accessed.
            //
            // Just fill m_outputSchema with the table's columns.
            m_outputSchema = m_tableSchema.clone();
            m_hasSignificantOutputSchema = true;
        }
        m_preAggOutputSchema = m_outputSchema;
    }

    private void initTableSchema(Database db) {
        if (isSubQuery()) {
            assert(m_children.size() == 1);
            AbstractPlanNode childNode = m_children.get(0);
            childNode.generateOutputSchema(db);
            m_tableSchema = childNode.getOutputSchema();
            // step to transfer derived table schema to upper level
            m_tableSchema = m_tableSchema.replaceTableClone(getTargetTableAlias());
        } else if (isCommonTableScan()) {
            m_tableSchema = new NodeSchema();
            StmtCommonTableScan ctScan = (StmtCommonTableScan)m_tableScan;
            for (SchemaColumn col : ctScan.getOutputSchema()) {
                m_tableSchema.addColumn(col.clone());
            }
        } else {
            m_tableSchema = new NodeSchema();
            CatalogMap<Column> cols =
                    db.getTables().getExact(m_targetTableName).getColumns();
            // you don't strictly need to sort this,
            // but it makes diff-ing easier
            List<Column> sortedCols =
                    CatalogUtil.getSortedCatalogItems(cols, "index");
            for (Column col : sortedCols) {
                // must produce a tuple value expression for this column.
                TupleValueExpression tve = new TupleValueExpression(
                        m_targetTableName, m_targetTableAlias,
                        col, col.getIndex());
                m_tableSchema.addColumn(m_targetTableName, m_targetTableAlias,
                        col.getTypeName(), col.getTypeName(),
                        tve, col.getIndex());
            }
        }
    }

    @Override
    public void resolveColumnIndexes() {
        // The following applies to both seq and index scan.  Index scan has
        // some additional expressions that need to be handled as well

        // predicate expression
        List<TupleValueExpression> predicate_tves =
            ExpressionUtil.getTupleValueExpressions(m_predicate);
        for (TupleValueExpression tve : predicate_tves) {
            tve.setColumnIndexUsingSchema(m_tableSchema);
        }

        // inline projection and insert
        InsertPlanNode ins =
                (InsertPlanNode)getInlinePlanNode(PlanNodeType.INSERT);
        ProjectionPlanNode proj =
            (ProjectionPlanNode)getInlinePlanNode(PlanNodeType.PROJECTION);
        // Resolve the inline projection and insert if there are any.
        if (proj != null) {
            proj.resolveColumnIndexesUsingSchema(m_tableSchema);
        }
        if (ins != null) {
            ins.resolveColumnIndexes();
        }
        // Snag the insert or projection node's output schema
        // if there are any, in that order.
        if (ins != null) {
            m_outputSchema = ins.getOutputSchema().clone();
        } else if (proj != null) {
            m_outputSchema = proj.getOutputSchema().clone();
        } else {
            m_outputSchema = m_preAggOutputSchema;
            // With no inline projection to define the output columns,
            // iterate through the output schema TVEs
            // and sort them by table schema index order.
            for (SchemaColumn col : m_outputSchema) {
                AbstractExpression colExpr = col.getExpression();
                // At this point, they'd better all be TVEs.
                assert(colExpr instanceof TupleValueExpression);
                TupleValueExpression tve = (TupleValueExpression) colExpr;
                tve.setColumnIndexUsingSchema(m_tableSchema);
            }
            m_outputSchema.sortByTveIndex();
        }

        // The outputschema of an inline limit node is completely irrelevant to the EE except that
        // serialization will complain if it contains expressions of unresolved columns.
        // Logically, the limited scan output has the same schema as the pre-limit scan.
        // It's at least as easy to just re-use the known-good output schema of the scan
        // than it would be to carefully resolve the limit node's current output schema.
        // And this simply works regardless of whether the limit was originally applied or inlined
        // before or after the (possibly inline) projection.
        // There's no need to be concerned about re-adjusting the irrelevant outputschema
        // based on the different schema of the original raw scan and the projection.
        LimitPlanNode limit = (LimitPlanNode)getInlinePlanNode(PlanNodeType.LIMIT);
        if (limit != null) {
            limit.m_outputSchema = m_outputSchema.clone();
            limit.m_hasSignificantOutputSchema = false; // It's just another cheap knock-off
        }

        // Resolve subquery expression indexes
        resolveSubqueryColumnIndexes();

        AggregatePlanNode aggNode = AggregatePlanNode.getInlineAggregationNode(this);

        if (aggNode != null) {
            aggNode.resolveColumnIndexesUsingSchema(m_outputSchema);
            m_outputSchema = aggNode.getOutputSchema().copyAndReplaceWithTVE();
            // Aggregate plan node change its output schema, and
            // EE does not have special code to get output schema from inlined aggregate node.
            m_hasSignificantOutputSchema = true;
        }
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);

        if (m_predicate != null) {
            if (ConstantValueExpression.isBooleanFalse(m_predicate)) {
                stringer.keySymbolValuePair(Members.PREDICATE_FALSE.name(), "TRUE");
            }
            stringer.key(Members.PREDICATE.name());
            stringer.value(m_predicate);
        }
        stringer.keySymbolValuePair(Members.TARGET_TABLE_NAME.name(), m_targetTableName);
        stringer.keySymbolValuePair(Members.TARGET_TABLE_ALIAS.name(), m_targetTableAlias);
        if (m_isSubQuery) {
            stringer.keySymbolValuePair(Members.SUBQUERY_INDICATOR.name(), "TRUE");
        }
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        helpLoadFromJSONObject(jobj, db);
        m_predicate = AbstractExpression.fromJSONChild(jobj, Members.PREDICATE.name(), m_tableScan);
        m_targetTableName = jobj.getString( Members.TARGET_TABLE_NAME.name() );
        m_targetTableAlias = jobj.getString( Members.TARGET_TABLE_ALIAS.name() );
        if (jobj.has("SUBQUERY_INDICATOR")) {
            m_isSubQuery = "TRUE".equals(jobj.getString( Members.SUBQUERY_INDICATOR.name() ));
        }
    }

    @Override
    protected void getScanNodeList_recurse(List<AbstractScanPlanNode> collected, Set<AbstractPlanNode> visited) {
        if (visited.contains(this)) {
            assert(false): "do not expect loops in plangraph.";
            return;
        }
        visited.add(this);
        collected.add(this);
    }

    protected String explainPredicate(String prefix) {
        if (m_predicate != null) {
            return prefix + m_predicate.explain(getTableNameForExplain());
        }
        return "";
    }

    protected String getTableNameForExplain() {
        return (m_targetTableAlias != null) ? m_targetTableAlias : m_targetTableName;
    }

    @Override
    public void findAllExpressionsOfClass(Class< ? extends AbstractExpression> aeClass, Set<AbstractExpression> collected) {
        super.findAllExpressionsOfClass(aeClass, collected);
        if (m_predicate != null) {
            collected.addAll(m_predicate.findAllSubexpressionsOfClass(aeClass));
        }
    }

    protected void copyDifferentiatorMap(
            Map<Integer, Integer> diffMap) {
        m_differentiatorMap = new HashMap<>(diffMap);
    }

    @Override
    public String getUpdatedTable() {
        InsertPlanNode ipn = (InsertPlanNode)getInlinePlanNode(PlanNodeType.INSERT);
        if (ipn == null) {
            return null;
        }
        return ipn.getUpdatedTable();
    }

}
