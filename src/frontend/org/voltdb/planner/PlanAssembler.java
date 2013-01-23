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

package org.voltdb.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.json_voltpatches.JSONException;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.ConnectorTableInfo;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AggregateExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.expressions.TupleAddressExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.ParsedSelectStmt.ParsedColInfo;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.DeletePlanNode;
import org.voltdb.plannodes.DistinctPlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.InsertPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.MaterializePlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.plannodes.UpdatePlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;
import org.voltdb.utils.CatalogUtil;

/**
 * The query planner accepts catalog data, SQL statements from the catalog, then
 * outputs a set of complete and correct query plans. It will output MANY plans
 * and some of them will be stupid. The best plan will be selected by computing
 * resource usage statistics for the plans, then using those statistics to
 * compute the cost of a specific plan. The plan with the lowest cost wins.
 *
 */
public class PlanAssembler {

    /** convenience pointer to the cluster object in the catalog */
    final Cluster m_catalogCluster;
    /** convenience pointer to the database object in the catalog */
    final Database m_catalogDb;

    /** parsed statement for an insert */
    ParsedInsertStmt m_parsedInsert = null;
    /** parsed statement for an update */
    ParsedUpdateStmt m_parsedUpdate = null;
    /** parsed statement for an delete */
    ParsedDeleteStmt m_parsedDelete = null;
    /** parsed statement for an select */
    ParsedSelectStmt m_parsedSelect = null;
    /** parsed statement for an union */
    ParsedUnionStmt m_parsedUnion = null;

    /** plan selector */
    PlanSelector m_planSelector;

    /** Describes the specified and inferred partition context. */
    private final PartitioningForStatement m_partitioning;

    /** Error message */
    String m_recentErrorMsg;

    /**
     * Used to generate the table-touching parts of a plan. All join-order and
     * access path selection stuff is done by the SelectSubPlanAssember.
     */
    SubPlanAssembler subAssembler = null;

    /**
     * Counter for the number of plans generated to date for a single statement.
     */
    boolean m_insertPlanWasGenerated = false;

    /**
     * Whenever a parameter has its type changed during compilation, the new type is stored
     * here, indexed by parameter index.
     */
    Map<Integer, VoltType> m_paramTypeOverrideMap = new HashMap<Integer, VoltType>();

    /**
     *
     * @param catalogCluster
     *            Catalog info about the physical layout of the cluster.
     * @param catalogDb
     *            Catalog info about schema, metadata and procedures.
     * @param partitioning
     *            Describes the specified and inferred partition context.
     */
    PlanAssembler(Cluster catalogCluster, Database catalogDb, PartitioningForStatement partitioning, PlanSelector planSelector) {
        m_catalogCluster = catalogCluster;
        m_catalogDb = catalogDb;
        m_partitioning = partitioning;
        m_planSelector = planSelector;
    }

    String getSQLText() {
        if (m_parsedDelete != null) {
            return m_parsedDelete.sql;
        }
        else if (m_parsedInsert != null) {
            return m_parsedInsert.sql;
        }
        else if (m_parsedUpdate != null) {
            return m_parsedUpdate.sql;
        }
        else if (m_parsedSelect != null) {
            return m_parsedSelect.sql;
        }
        assert(false);
        return null;
    }

    /**
     * Return true if tableList includes at least one matview.
     */
    private boolean tableListIncludesView(List<Table> tableList) {
        for (Table table : tableList) {
            if (table.getMaterializer() != null) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return true if tableList includes at least one export table.
     */
    private boolean tableListIncludesExportOnly(List<Table> tableList) {
        // the single well-known connector
        Connector connector = m_catalogDb.getConnectors().get("0");

        // no export tables with out a connector
        if (connector == null) {
            return false;
        }

        CatalogMap<ConnectorTableInfo> tableinfo = connector.getTableinfo();

        // this loop is O(number-of-joins * number-of-export-tables)
        // which seems acceptable if not great. Probably faster than
        // re-hashing the export only tables for faster lookup.
        for (Table table : tableList) {
            for (ConnectorTableInfo ti : tableinfo) {
                if (ti.getAppendonly() &&
                    ti.getTable().getTypeName().equalsIgnoreCase(table.getTypeName()))
                {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Clear any old state and get ready to plan a new plan. The next call to
     * getNextPlan() will return the first candidate plan for these parameters.
     *
     */
    void setupForNewPlans(AbstractParsedStmt parsedStmt) {
        m_insertPlanWasGenerated = false;
        int countOfPartitionedTables = 0;
        Map<String, String> partitionColumnByTable = new HashMap<String, String>();
        // Do we have a need for a distributed scan at all?
        // Iterate over the tables to collect partition columns.
        for (Table table : parsedStmt.tableList) {
            if (table.getIsreplicated()) {
                continue;
            }
            ++countOfPartitionedTables;
            String colName = null;
            Column partitionCol = table.getPartitioncolumn();
            // "(partitionCol != null)" tests around an obscure edge case.
            // The table is declared non-replicated yet specifies no partitioning column.
            // This can occur legitimately when views based on partitioned tables neglect to group by the partition column.
            // The interpretation of this edge case is that the table has "randomly distributed data".
            // In such a case, the table is valid for use by MP queries only and can only be joined with replicated tables
            // because it has no recognized partitioning join key.
            if (partitionCol != null) {
                colName = partitionCol.getTypeName(); // Note getTypeName gets the column name -- go figure.
            }

            //TODO: This map really wants to be indexed by table "alias" (the in-query table scan identifier)
            // so self-joins can be supported without ambiguity.
            String partitionedTable = table.getTypeName();
            partitionColumnByTable.put(partitionedTable, colName);
        }
        m_partitioning.setPartitionedTables(partitionColumnByTable, countOfPartitionedTables);
        if ((m_partitioning.wasSpecifiedAsSingle() == false) && m_partitioning.getCountOfPartitionedTables() > 0) {
            m_partitioning.analyzeForMultiPartitionAccess(parsedStmt.tableList, parsedStmt.valueEquivalence);
            int multiPartitionScanCount = m_partitioning.getCountOfIndependentlyPartitionedTables();
            if (multiPartitionScanCount > 1) {
                // The case of more than one independent partitioned table would result in an illegal plan with more than two fragments.
                String msg = "Join or union of multiple partitioned tables has insufficient join criteria.";
                throw new PlanningErrorException(msg);
            }
        }

        if (parsedStmt instanceof ParsedUnionStmt) {
            m_parsedUnion = (ParsedUnionStmt) parsedStmt;
            subAssembler = new UnionSubPlanAssembler(m_catalogDb, parsedStmt, m_partitioning);
        } else if (parsedStmt instanceof ParsedSelectStmt) {
            if (tableListIncludesExportOnly(parsedStmt.tableList)) {
                throw new RuntimeException(
                "Illegal to read an export table.");
            }
            m_parsedSelect = (ParsedSelectStmt) parsedStmt;
            subAssembler = new SelectSubPlanAssembler(m_catalogDb, parsedStmt, m_partitioning);
        } else {
            // check that no modification happens to views
            if (tableListIncludesView(parsedStmt.tableList)) {
                throw new RuntimeException(
                "Illegal to modify a materialized view.");
            }

            // Check that only multi-partition writes are made to replicated tables.
            // figure out which table we're updating/deleting
            assert (parsedStmt.tableList.size() == 1);
            Table targetTable = parsedStmt.tableList.get(0);
            if (targetTable.getIsreplicated()) {
                if (m_partitioning.wasSpecifiedAsSingle()) {
                    String msg = "Trying to write to replicated table '" + targetTable.getTypeName()
                                 + "' in a single-partition procedure.";
                    throw new PlanningErrorException(msg);
                }
            } else if (m_partitioning.wasSpecifiedAsSingle() == false) {
                m_partitioning.setPartitioningColumn(targetTable.getPartitioncolumn());
            }

            if (parsedStmt instanceof ParsedInsertStmt) {
                m_parsedInsert = (ParsedInsertStmt) parsedStmt;
                // The currently handled inserts are too simple to even require a subplan assembler. So, done.
                return;
            }

            if (parsedStmt instanceof ParsedUpdateStmt) {
                if (tableListIncludesExportOnly(parsedStmt.tableList)) {
                    throw new RuntimeException(
                    "Illegal to update an export table.");
                }
                m_parsedUpdate = (ParsedUpdateStmt) parsedStmt;
            } else if (parsedStmt instanceof ParsedDeleteStmt) {
                if (tableListIncludesExportOnly(parsedStmt.tableList)) {
                    throw new RuntimeException(
                    "Illegal to delete from an export table.");
                }
                m_parsedDelete = (ParsedDeleteStmt) parsedStmt;
            } else {
                throw new RuntimeException(
                        "Unknown subclass of AbstractParsedStmt.");
            }
            subAssembler = new WriterSubPlanAssembler(m_catalogDb, parsedStmt, m_partitioning);
        }
    }

    /**
     * Generate the best cost plan for the current SQL statement context.
     *
     * @param parsedStmt Current SQL statement to generate plan for
     * @return The best cost plan or null.
     */
    public CompiledPlan getBestCostPlan(AbstractParsedStmt parsedStmt) {

        // set up the plan assembler for this statement
        setupForNewPlans(parsedStmt);

        // get ready to find the plan with minimal cost
        CompiledPlan rawplan = null;

        // loop over all possible plans
        while (true) {

            try {
                rawplan = getNextPlan();
            }
            // on exception, set the error message and bail...
            catch (PlanningErrorException e) {
                m_recentErrorMsg = e.getMessage();
                return null;
            }

            // stop this while loop when no more plans are generated
            if (rawplan == null)
                break;
            // Update the best cost plan so far
            m_planSelector.considerCandidatePlan(rawplan);
        }
        return m_planSelector.m_bestPlan;
    }

    /**
     * Output the best cost plan.
     *
     */
    public void finalizeBestCostPlan() {
        m_planSelector.finalizeOutput();
    }

    /**
     * Generate a unique and correct plan for the current SQL statement context.
     * This method gets called repeatedly until it returns null, meaning there
     * are no more plans.
     *
     * @return A not-previously returned query plan or null if no more
     *         computable plans.
     */
    CompiledPlan getNextPlan() {
        // reset the plan column guids and pool
        //PlanColumn.resetAll();

        CompiledPlan retval = new CompiledPlan();
        AbstractParsedStmt nextStmt = null;
        if (m_parsedUnion != null) {
            nextStmt = m_parsedUnion;
            retval = getNextUnionPlan();
            if (retval != null) {
                retval.readOnly = true;
            }
        } else if (m_parsedSelect != null) {
            nextStmt = m_parsedSelect;
            retval.rootPlanGraph = getNextSelectPlan();
            retval.readOnly = true;
            if (retval.rootPlanGraph != null)
            {
                // only add the output columns if we actually have a plan
                // avoid PlanColumn resource leakage
                addColumns(retval, m_parsedSelect);
                boolean orderIsDeterministic = m_parsedSelect.isOrderDeterministic();
                boolean contentIsDeterministic = (m_parsedSelect.hasLimitOrOffset() == false) || orderIsDeterministic;
                retval.statementGuaranteesDeterminism(contentIsDeterministic, orderIsDeterministic);
            }
        } else {
            retval.readOnly = false;
            if (m_parsedInsert != null) {
                nextStmt = m_parsedInsert;
                retval.rootPlanGraph = getNextInsertPlan();
            } else if (m_parsedUpdate != null) {
                nextStmt = m_parsedUpdate;
                retval.rootPlanGraph = getNextUpdatePlan();
                // note that for replicated tables, multi-fragment plans
                // need to divide the result by the number of partitions
            } else if (m_parsedDelete != null) {
                nextStmt = m_parsedDelete;
                retval.rootPlanGraph = getNextDeletePlan();
                // note that for replicated tables, multi-fragment plans
                // need to divide the result by the number of partitions
            } else {
                throw new RuntimeException(
                        "setupForNewPlans not called or not successfull.");
            }
            assert (nextStmt.tableList.size() == 1);
            if (nextStmt.tableList.get(0).getIsreplicated())
                retval.replicatedTableDML = true;
            retval.statementGuaranteesDeterminism(true, true); // Until we support DML w/ subqueries/limits
        }

        if (retval == null || retval.rootPlanGraph == null) {
            return null;
        }

        assert (nextStmt != null);
        addParameters(retval, nextStmt);
        retval.fullWhereClause = nextStmt.where;
        retval.fullWinnerPlan = retval.rootPlanGraph;
        // Do a final generateOutputSchema pass.
        retval.rootPlanGraph.generateOutputSchema(m_catalogDb);
        retval.setPartitioningKey(m_partitioning.effectivePartitioningValue());
        return retval;
    }

    /**
     * This is a UNION specific method. Generate a unique and correct plan
     * for the current SQL UNION statement by building the best plans for each individual statements
     * within the UNION.
     *
     * @return A union plan or null.
     */
    private CompiledPlan getNextUnionPlan() {
        AbstractPlanNode subUnionRoot = subAssembler.nextPlan();
        if (subUnionRoot == null) {
            return null;
        }
        m_recentErrorMsg = null;

        ArrayList<CompiledPlan> childrenPlans = new ArrayList<CompiledPlan>();
        boolean orderIsDeterministic = true;
        boolean contentIsDeterministic = true;

        // The children plans are never final - don't need send/receive pair on top
        ArrayList<PartitioningForStatement> partitioningList = new ArrayList<PartitioningForStatement>();

        // Build best plans for the children first
        int planId = 0;
        for (AbstractParsedStmt parsedChildStmt : m_parsedUnion.m_children) {
            PartitioningForStatement partitioning = (PartitioningForStatement)m_partitioning.clone();
            PlanSelector processor = (PlanSelector) m_planSelector.clone();
            processor.m_planId = planId;
            PlanAssembler assembler = new PlanAssembler(
                    m_catalogCluster, m_catalogDb, partitioning, processor);
            CompiledPlan bestChildPlan = assembler.getBestCostPlan(parsedChildStmt);
            // make sure we got a winner
            if (bestChildPlan == null) {
                if (m_recentErrorMsg == null) {
                    m_recentErrorMsg = "Unable to plan for statement. Error unknown.";
                }
                return null;
            }
            childrenPlans.add(bestChildPlan);
            orderIsDeterministic = orderIsDeterministic && bestChildPlan.isOrderDeterministic();
            contentIsDeterministic = contentIsDeterministic && bestChildPlan.isContentDeterministic();
            partitioningList.add(partitioning);
            // Make sure that next child's plans won't override current ones.
            planId = processor.m_planId;
        }
        // need to reset plan id for the entire UNION
        m_planSelector.m_planId = planId;

        // Add and link children plans
        for (CompiledPlan selectPlan : childrenPlans) {
            subUnionRoot.addAndLinkChild(selectPlan.rootPlanGraph);
        }

        CompiledPlan retval = new CompiledPlan();
            retval.rootPlanGraph = subUnionRoot;
        retval.readOnly = true;
        retval.sql = m_planSelector.m_sql;
        retval.statementGuaranteesDeterminism(contentIsDeterministic, orderIsDeterministic);

        // compute the cost - total of all children
        retval.cost = 0.0;
        for (CompiledPlan bestChildPlan : childrenPlans) {
            retval.cost += bestChildPlan.cost;
        }
        return retval;
    }

    private void addColumns(CompiledPlan plan, ParsedSelectStmt stmt) {
        NodeSchema output_schema = plan.rootPlanGraph.getOutputSchema();
        // Sanity-check the output NodeSchema columns against the display columns
        if (stmt.displayColumns.size() != output_schema.size())
        {
            throw new PlanningErrorException("Mismatched plan output cols " +
            "to parsed display columns");
        }
        for (ParsedColInfo display_col : stmt.displayColumns)
        {
            SchemaColumn col = output_schema.find(display_col.tableName,
                                                  display_col.columnName,
                                                  display_col.alias);
            if (col == null)
            {
                throw new PlanningErrorException("Mismatched plan output cols " +
                                                 "to parsed display columns");
            }
        }
        plan.columns = output_schema;
    }

    private void addParameters(CompiledPlan plan, AbstractParsedStmt stmt) {
        plan.parameters = new VoltType[stmt.paramList.length];

        for (int i = 0; i < stmt.paramList.length; ++i) {
            VoltType override = m_paramTypeOverrideMap.get(i);
            if (override != null) {
                plan.parameters[i] = override;
            }
            else {
                plan.parameters[i] = stmt.paramList[i];
            }
        }
    }

    private AbstractPlanNode getNextSelectPlan() {
        assert (subAssembler != null);

        AbstractPlanNode subSelectRoot = subAssembler.nextPlan();
        if (subSelectRoot == null)
            return null;

        AbstractPlanNode root = subSelectRoot;

        /*
         * Establish the output columns for the sub select plan.
         */
        root.generateOutputSchema(m_catalogDb);
        root = handleAggregationOperators(root);

        root = handleOrderBy(root);

        if ((root.getPlanNodeType() != PlanNodeType.AGGREGATE) &&
            (root.getPlanNodeType() != PlanNodeType.HASHAGGREGATE) &&
            (root.getPlanNodeType() != PlanNodeType.DISTINCT) &&
            (root.getPlanNodeType() != PlanNodeType.PROJECTION)) {
            root = addProjection(root);
        }

        if (m_parsedSelect.hasLimitOrOffset())
        {
            root = handleLimitOperator(root);
        }

        root.generateOutputSchema(m_catalogDb);

        return root;
    }

    private AbstractPlanNode getNextDeletePlan() {
        assert (subAssembler != null);

        // figure out which table we're deleting from
        assert (m_parsedDelete.tableList.size() == 1);
        Table targetTable = m_parsedDelete.tableList.get(0);

        AbstractPlanNode subSelectRoot = subAssembler.nextPlan();
        if (subSelectRoot == null)
            return null;

        // generate the delete node with the right target table
        DeletePlanNode deleteNode = new DeletePlanNode();
        deleteNode.setTargetTableName(targetTable.getTypeName());

        ProjectionPlanNode projectionNode = new ProjectionPlanNode();
        AbstractExpression addressExpr = new TupleAddressExpression();
        NodeSchema proj_schema = new NodeSchema();
        // This planner-created column is magic.
        proj_schema.addColumn(new SchemaColumn("VOLT_TEMP_TABLE",
                                               "tuple_address",
                                               "tuple_address",
                                               addressExpr));
        projectionNode.setOutputSchema(proj_schema);

        assert(subSelectRoot instanceof AbstractScanPlanNode);

        // If the scan matches all rows, we can throw away the scan
        // nodes and use a truncate delete node.
        // Assume all index scans have filters in this context, so only consider seq scans.
        if (m_partitioning.wasSpecifiedAsSingle() &&
                (subSelectRoot instanceof SeqScanPlanNode) &&
                (((SeqScanPlanNode) subSelectRoot).getPredicate() == null)) {
            deleteNode.setTruncate(true);
            return deleteNode;
        }

        // OPTIMIZATION: Projection Inline
        // If the root node we got back from createSelectTree() is an
        // AbstractScanNode, then
        // we put the Projection node we just created inside of it
        // When we inline this projection into the scan, we're going
        // to overwrite any original projection that we might have inlined
        // in order to simply cull the columns from the persistent table.
        // The call here to generateOutputSchema() will recurse down to
        // the scan node and cause it to update appropriately.
        subSelectRoot.addInlinePlanNode(projectionNode);
        // connect the nodes to build the graph
        deleteNode.addAndLinkChild(subSelectRoot);

        if (m_partitioning.wasSpecifiedAsSingle() || m_partitioning.hasPartitioningConstantLockedIn()) {
            deleteNode.generateOutputSchema(m_catalogDb);
            return deleteNode;
        }

        // Send the local result counts to the coordinator.
        AbstractPlanNode recvNode = subAssembler.addSendReceivePair(deleteNode);
        // add a sum and send on top of the union
        return addSumAndSendToDMLNode(recvNode);
    }

    private AbstractPlanNode getNextUpdatePlan() {
        assert (subAssembler != null);

        AbstractPlanNode subSelectRoot = subAssembler.nextPlan();
        if (subSelectRoot == null)
            return null;

        UpdatePlanNode updateNode = new UpdatePlanNode();
        Table targetTable = m_parsedUpdate.tableList.get(0);
        updateNode.setTargetTableName(targetTable.getTypeName());
        // set this to false until proven otherwise
        updateNode.setUpdateIndexes(false);

        ProjectionPlanNode projectionNode = new ProjectionPlanNode();
        TupleAddressExpression tae = new TupleAddressExpression();
        NodeSchema proj_schema = new NodeSchema();
        // This planner-generated column is magic.
        proj_schema.addColumn(new SchemaColumn("VOLT_TEMP_TABLE",
                                               "tuple_address",
                                               "tuple_address",
                                               tae));

        // get the set of columns affected by indexes
        Set<String> affectedColumns = getIndexedColumnSetForTable(targetTable);

        // add the output columns we need to the projection
        //
        // Right now, the EE is going to use the original column names
        // and compare these to the persistent table column names in the
        // update executor in order to figure out which table columns get
        // updated.  We'll associate the actual values with VOLT_TEMP_TABLE
        // to avoid any false schema/column matches with the actual table.
        for (Entry<Column, AbstractExpression> col : m_parsedUpdate.columns.entrySet()) {

            // make the literal type we're going to insert match the column type
            AbstractExpression castedExpr = null;
            try {
                castedExpr = (AbstractExpression) col.getValue().clone();
                ExpressionUtil.setOutputTypeForInsertExpression(
                        castedExpr, VoltType.get((byte) col.getKey().getType()), col.getKey().getSize(), m_paramTypeOverrideMap);
            } catch (Exception e) {
                throw new PlanningErrorException(e.getMessage());
            }

            proj_schema.addColumn(new SchemaColumn("VOLT_TEMP_TABLE",
                                                   col.getKey().getTypeName(),
                                                   col.getKey().getTypeName(),
                                                   castedExpr));

            // check if this column is an indexed column
            if (affectedColumns.contains(col.getKey().getTypeName()))
            {
                updateNode.setUpdateIndexes(true);
            }
        }
        projectionNode.setOutputSchema(proj_schema);


        // add the projection inline (TODO: this will break if more than one
        // layer is below this)
        //
        // When we inline this projection into the scan, we're going
        // to overwrite any original projection that we might have inlined
        // in order to simply cull the columns from the persistent table.
        // The call here to generateOutputSchema() will recurse down to
        // the scan node and cause it to update appropriately.
        assert(subSelectRoot instanceof AbstractScanPlanNode);
        subSelectRoot.addInlinePlanNode(projectionNode);

        // connect the nodes to build the graph
        updateNode.addAndLinkChild(subSelectRoot);

        if (m_partitioning.wasSpecifiedAsSingle() || m_partitioning.hasPartitioningConstantLockedIn()) {
            updateNode.generateOutputSchema(m_catalogDb);

            return updateNode;
        }

        // Send the local result counts to the coordinator.
        AbstractPlanNode recvNode = subAssembler.addSendReceivePair(updateNode);
        // add a sum and send on top of the union
        return addSumAndSendToDMLNode(recvNode);
    }

    /**
     * Get the next (only) plan for a SQL insertion. Inserts are pretty simple
     * and this will only generate a single plan.
     *
     * @return The next plan for a given insert statement.
     */
    private AbstractPlanNode getNextInsertPlan() {
        // there's really only one way to do an insert, so just
        // do it the right way once, then return null after that
        if (m_insertPlanWasGenerated)
            return null;
        m_insertPlanWasGenerated = true;

        // figure out which table we're inserting into
        assert (m_parsedInsert.tableList.size() == 1);
        Table targetTable = m_parsedInsert.tableList.get(0);

        // the root of the insert plan is always an InsertPlanNode
        InsertPlanNode insertNode = new InsertPlanNode();
        insertNode.setTargetTableName(targetTable.getTypeName());
        insertNode.setMultiPartition(m_partitioning.wasSpecifiedAsSingle() == false);

        // the materialize node creates a tuple to insert (which is frankly not
        // always optimal)
        MaterializePlanNode materializeNode = new MaterializePlanNode();
        NodeSchema mat_schema = new NodeSchema();

        // get the ordered list of columns for the targettable using a helper
        // function they're not guaranteed to be in order in the catalog
        List<Column> columns =
            CatalogUtil.getSortedCatalogItems(targetTable.getColumns(), "index");

        // for each column in the table in order...
        for (Column column : columns) {

            // get the expression for the column
            AbstractExpression expr = m_parsedInsert.columns.get(column);

            // if there's no expression, make sure the column has
            // some supported default value
            if (expr == null) {
                // if it's not nullable or defaulted we have a problem
                if (column.getNullable() == false && column.getDefaulttype() == 0)
                {
                    throw new PlanningErrorException("Column " + column.getName()
                            + " has no default and is not nullable.");
                }
                ConstantValueExpression const_expr =
                    new ConstantValueExpression();
                expr = const_expr;
                if (column.getDefaulttype() != 0)
                {
                    const_expr.setValue(column.getDefaultvalue());
                    const_expr.setValueType(VoltType.get((byte) column.getDefaulttype()));
                }
                else
                {
                    const_expr.setValue(null);
                }
            }

            if (expr.getValueType() == VoltType.NULL) {
                ConstantValueExpression const_expr =
                    new ConstantValueExpression();
                const_expr.setValue("NULL");
            }

            // set the expression type to match the corresponding Column.
            try {
                ExpressionUtil.setOutputTypeForInsertExpression(expr, VoltType.get((byte)column.getType()), column.getSize(), m_paramTypeOverrideMap);
            } catch (Exception e) {
                throw new PlanningErrorException(e.getMessage());
            }

            // Hint that this statement can be executed SP.
            if (column.equals(m_partitioning.getColumn())) {
                String fullColumnName = targetTable.getTypeName() + "." + column.getTypeName();
                m_partitioning.addPartitioningExpression(fullColumnName, expr);
                m_partitioning.setInferredValue(ConstantValueExpression.extractPartitioningValue(expr.getValueType(), expr));
            }

            // add column to the materialize node.
            // This table name is magic.
            mat_schema.addColumn(new SchemaColumn("VOLT_TEMP_TABLE",
                                                  column.getTypeName(),
                                                  column.getTypeName(),
                                                  expr));
        }

        materializeNode.setOutputSchema(mat_schema);
        // connect the insert and the materialize nodes together
        insertNode.addAndLinkChild(materializeNode);
        insertNode.generateOutputSchema(m_catalogDb);

        if (m_partitioning.wasSpecifiedAsSingle() || m_partitioning.hasPartitioningConstantLockedIn()) {
            return insertNode;
        }

        SendPlanNode sendNode = new SendPlanNode();
        // this will make the child plan fragment be sent to all partitions
        sendNode.isMultiPartition = true;
        sendNode.addAndLinkChild(insertNode);
        // sendNode.generateOutputSchema(m_catalogDb);

        AbstractPlanNode recvNode = new ReceivePlanNode();
        recvNode.addAndLinkChild(sendNode);
        recvNode.generateOutputSchema(m_catalogDb);

        // add a count and send on top of the union
        return addSumAndSendToDMLNode(recvNode);
    }

    AbstractPlanNode addSumAndSendToDMLNode(AbstractPlanNode dmlRoot)
    {
        // create the nodes being pushed on top of dmlRoot.
        AggregatePlanNode countNode = new AggregatePlanNode();
        SendPlanNode sendNode = new SendPlanNode();

        // configure the count aggregate (sum) node to produce a single
        // output column containing the result of the sum.
        // Create a TVE that should match the tuple count input column
        // This TVE is magic.
        // really really need to make this less hard-wired
        TupleValueExpression count_tve = new TupleValueExpression();
        count_tve.setValueType(VoltType.BIGINT);
        count_tve.setValueSize(VoltType.BIGINT.getLengthInBytesForFixedTypes());
        count_tve.setColumnIndex(0);
        count_tve.setColumnName("modified_tuples");
        count_tve.setColumnAlias("modified_tuples");
        count_tve.setTableName("VOLT_TEMP_TABLE");
        countNode.addAggregate(ExpressionType.AGGREGATE_SUM, false, 0, count_tve);

        // The output column. Not really based on a TVE (it is really the
        // count expression represented by the count configured above). But
        // this is sufficient for now.  This looks identical to the above
        // TVE but it's logically different so we'll create a fresh one.
        // And yes, oh, oh, it's magic</elo>
        TupleValueExpression tve = new TupleValueExpression();
        tve.setValueType(VoltType.BIGINT);
        tve.setValueSize(VoltType.BIGINT.getLengthInBytesForFixedTypes());
        tve.setColumnIndex(0);
        tve.setColumnName("modified_tuples");
        tve.setColumnAlias("modified_tuples");
        tve.setTableName("VOLT_TEMP_TABLE");
        NodeSchema count_schema = new NodeSchema();
        SchemaColumn col = new SchemaColumn("VOLT_TEMP_TABLE",
                                            "modified_tuples",
                                            "modified_tuples",
                                            tve);
        count_schema.addColumn(col);
        countNode.setOutputSchema(count_schema);

        // connect the nodes to build the graph
        countNode.addAndLinkChild(dmlRoot);
        countNode.generateOutputSchema(m_catalogDb);
        sendNode.addAndLinkChild(countNode);
        sendNode.generateOutputSchema(m_catalogDb);

        return sendNode;
    }

    /**
     * Given a relatively complete plan-sub-graph, apply a trivial projection
     * (filter) to it. If the root node can embed the projection do so. If not,
     * add a new projection node.
     *
     * @param rootNode
     *            The root of the plan-sub-graph to add the projection to.
     * @return The new root of the plan-sub-graph (might be the same as the
     *         input).
     */
    AbstractPlanNode addProjection(AbstractPlanNode rootNode) {
        assert (m_parsedSelect != null);
        assert (m_parsedSelect.displayColumns != null);

        ProjectionPlanNode projectionNode =
            new ProjectionPlanNode();
        NodeSchema proj_schema = new NodeSchema();

        // Build the output schema for the projection based on the display columns
        for (ParsedSelectStmt.ParsedColInfo outputCol : m_parsedSelect.displayColumns)
        {
            assert(outputCol.expression != null);
            SchemaColumn col = new SchemaColumn(outputCol.tableName,
                                                outputCol.columnName,
                                                outputCol.alias,
                                                outputCol.expression);
            proj_schema.addColumn(col);
        }
        projectionNode.setOutputSchema(proj_schema);

        // if the projection can be done inline...
        if (rootNode instanceof AbstractScanPlanNode) {
            rootNode.addInlinePlanNode(projectionNode);
            return rootNode;
        } else {
            projectionNode.addAndLinkChild(rootNode);
            projectionNode.generateOutputSchema(m_catalogDb);
            return projectionNode;
        }
    }

    /**
     * Create an order by node as required by the statement and make it a parent of root.
     * @param root
     * @return new orderByNode (the new root) or the original root if no orderByNode was required.
     */
    AbstractPlanNode handleOrderBy(AbstractPlanNode root) {
        assert (m_parsedSelect != null);

        // Only sort when the statement has an ORDER BY.
        if ( ! m_parsedSelect.hasOrderByColumns()) {
            return root;
        }

        // Ignore ORDER BY in cases where there can be at most one row.
        if (m_parsedSelect.guaranteesUniqueRow()) {
            return root;
        }

        // Skip the explicit ORDER BY plan step if an IndexScan is already providing the equivalent ordering.
        // Note that even tree index scans that produce values in their own "key order" only report
        // their sort direction != SortDirectionType.INVALID
        // when they enforce an ordering equivalent to the one requested in the ORDER BY clause.
        if (root.getPlanNodeType() == PlanNodeType.INDEXSCAN) {
            if (((IndexScanPlanNode) root).getSortDirection() != SortDirectionType.INVALID) {
                return root;
            }
        }

        OrderByPlanNode orderByNode = new OrderByPlanNode();
        for (ParsedSelectStmt.ParsedColInfo col : m_parsedSelect.orderColumns) {
            orderByNode.addSort(col.expression,
                                col.ascending ? SortDirectionType.ASC
                                              : SortDirectionType.DESC);
        }
        orderByNode.addAndLinkChild(root);
        orderByNode.generateOutputSchema(m_catalogDb);

        // In theory, for a single-table query, there just needs to exist a uniqueness constraint (primary key or other unique index)
        // on some of the ORDER BY values regardless of whether the associated index is used in the selected plan.
        // Strictly speaking, if it was used at the top of the plan, this function would have already returned without adding an orderByNode.
        // The interesting case here, addressing issue ENG-3335, is when the index scan is in the distributed part of the plan.
        // Then, the orderByNode is required to re-order the results at the coordinator.

        // Start by eliminating joins since, in general, a join (one-to-many) may produce multiple joined rows for each unique input row.
        // TODO: In theory, it is possible to analyze the join criteria and/or projected columns
        // to determine whether the particular join preserves the uniqueness of its index-scanned input.
        if (m_parsedSelect.tableList.size() == 1) {

            Table table = m_parsedSelect.tableList.get(0);

            // get all of the columns in the sort
            List<AbstractExpression> orderExpressions = orderByNode.getSortExpressions();

            // search indexes for one that makes the order by deterministic
            for (Index index : table.getIndexes()) {
                // skip non-unique indexes
                if (!index.getUnique()) {
                    continue;
                }

                // get the list of expressions for the index
                List<AbstractExpression> indexExpressions = new ArrayList<AbstractExpression>();

                String jsonExpr = index.getExpressionsjson();
                // if this is a pure-column index...
                if (jsonExpr.isEmpty()) {
                    for (ColumnRef cref : index.getColumns()) {
                        Column col = cref.getColumn();
                        TupleValueExpression tve = new TupleValueExpression();
                        tve.setColumnIndex(col.getIndex());
                        tve.setColumnName(col.getName());
                        tve.setExpressionType(ExpressionType.VALUE_TUPLE);
                        tve.setHasAggregate(false);
                        tve.setTableName(table.getTypeName());
                        tve.setValueSize(col.getSize());
                        tve.setValueType(VoltType.get((byte) col.getType()));
                        indexExpressions.add(tve);
                    }
                }
                // if this is a fancy expression-based index...
                else {
                    try {
                        indexExpressions = AbstractExpression.fromJSONArrayString(jsonExpr, null);
                    } catch (JSONException e) {
                        e.printStackTrace(); // danger will robinson
                        assert(false);
                        return null;
                    }
                }

                // if the sort covers the index, then it's a unique sort
                if (orderExpressions.containsAll(indexExpressions)) {
                    orderByNode.setOrderingByUniqueColumns();
                }
            }
        }
        return orderByNode;
    }

    /**
     * Add a limit, pushed-down if possible, and return the new root.
     * @param root top of the original plan
     * @return new plan's root node
     */
    AbstractPlanNode handleLimitOperator(AbstractPlanNode root) {
        int limitParamIndex = m_parsedSelect.getLimitParameterIndex();
        int offsetParamIndex = m_parsedSelect.getOffsetParameterIndex();

        // The coordinator's top limit graph fragment for a MP plan.
        // If planning "order by ... limit", getNextSelectPlan()
        // will have already added an order by to the coordinator frag.
        // This is the only limit node in a SP plan
        LimitPlanNode topLimit = new LimitPlanNode();
        topLimit.setLimit((int)m_parsedSelect.limit);
        topLimit.setOffset((int) m_parsedSelect.offset);
        topLimit.setLimitParameterIndex(limitParamIndex);
        topLimit.setOffsetParameterIndex(offsetParamIndex);

        /*
         * TODO: allow push down limit with distinct (select distinct C from T limit 5)
         * or distinct in aggregates.
         */
        AbstractPlanNode sendNode = null;
        // Whether or not we can push the limit node down
        boolean canPushDown = ! m_parsedSelect.distinct;
        if (canPushDown) {
            sendNode = checkPushDownViability(root);
            if (sendNode == null) {
                canPushDown = false;
            } else {
                for (ParsedSelectStmt.ParsedColInfo col : m_parsedSelect.displayColumns) {
                    AbstractExpression rootExpr = col.expression;
                    if (rootExpr instanceof AggregateExpression) {
                        if (((AggregateExpression)rootExpr).m_distinct) {
                            canPushDown = false;
                            break;
                        }
                    }
                }
            }
        }

        /*
         * Push down the limit plan node when possible even if offset is set. If
         * the plan is for a partitioned table, do the push down. Otherwise,
         * there is no need to do the push down work, the limit plan node will
         * be run in the partition.
         */
        if (canPushDown) {
            /*
             * For partitioned table, the pushed-down limit plan node has a limit based
             * on the combined limit and offset, which may require an expression if either of these was not a hard-coded constant.
             * The top level limit plan node remains the same, with the original limit and offset values.
             */
            LimitPlanNode distLimit = new LimitPlanNode();
            // Offset on a pushed-down limit node makes no sense, just defaults to 0
            // -- the original offset must be factored into the pushed-down limit as a pad on the limit.
            if (m_parsedSelect.limit != -1) {
                distLimit.setLimit((int) (m_parsedSelect.limit + m_parsedSelect.offset));
            }

            if (m_parsedSelect.hasLimitOrOffsetParameters()) {

                AbstractExpression left = m_parsedSelect.getOffsetExpression();
                assert (left != null);
                AbstractExpression right = m_parsedSelect.getLimitExpression();
                assert (right != null);
                OperatorExpression expr = new OperatorExpression(ExpressionType.OPERATOR_PLUS, left, right);
                expr.setValueType(VoltType.INTEGER);
                expr.setValueSize(VoltType.INTEGER.getLengthInBytesForFixedTypes());
                distLimit.setLimitExpression(expr);
            }
            // else let the parameterized forms of offset/limit default to unused/invalid.

            // Disconnect the distributed parts of the plan below the SEND node
            AbstractPlanNode distributedPlan = sendNode.getChild(0);
            distributedPlan.clearParents();
            sendNode.clearChildren();

            // If the distributed limit must be performed on ordered input,
            // ensure the order of the data on each partition.
            distributedPlan = handleOrderBy(distributedPlan);

            // Apply the distributed limit.
            distLimit.addAndLinkChild(distributedPlan);

            // Add the distributed work back to the plan
            sendNode.addAndLinkChild(distLimit);
        }

        topLimit.addAndLinkChild(root);
        topLimit.generateOutputSchema(m_catalogDb);
        return topLimit;
    }

    AbstractPlanNode handleAggregationOperators(AbstractPlanNode root) {
        boolean containsAggregateExpression = false;
        AggregatePlanNode aggNode = null;

        /* Check if any aggregate expressions are present */
        for (ParsedSelectStmt.ParsedColInfo col : m_parsedSelect.displayColumns) {
            if (col.expression.hasAnySubexpressionOfClass(AggregateExpression.class)) {
                containsAggregateExpression = true;
                break;
            }
        }

        /*
         * "Select A from T group by A" is grouped but has no aggregate operator
         * expressions. Catch that case by checking the grouped flag
         */
        if (containsAggregateExpression || m_parsedSelect.isGrouped()) {
            AggregatePlanNode topAggNode;
            //TODO: add "m_parsedSelect.grouped &&" to the preconditions for HashAggregate.
            // Otherwise, a runtime hash is built for nothing -- just to hold a single entry.
            if (root.getPlanNodeType() != PlanNodeType.INDEXSCAN ||
                ((IndexScanPlanNode) root).getSortDirection() == SortDirectionType.INVALID) {
                aggNode = new HashAggregatePlanNode();
                topAggNode = new HashAggregatePlanNode();
            } else {
                aggNode = new AggregatePlanNode();
                topAggNode = new AggregatePlanNode();
            }

            int outputColumnIndex = 0;
            int topOutputColumnIndex = 0;
            NodeSchema agg_schema = new NodeSchema();
            NodeSchema topAggSchema = new NodeSchema();
            boolean hasAggregates = false;
            boolean isPushDownAgg = true;
            // TODO: Aggregates could theoretically ONLY appear in the ORDER BY clause but not the display columns, but we don't support that yet.
            for (ParsedSelectStmt.ParsedColInfo col : m_parsedSelect.displayColumns)
            {
                AbstractExpression rootExpr = col.expression;
                AbstractExpression agg_input_expr = null;
                SchemaColumn schema_col = null;
                SchemaColumn topSchemaCol = null;
                ExpressionType agg_expression_type = rootExpr.getExpressionType();
                if (rootExpr.hasAnySubexpressionOfClass(AggregateExpression.class)) {
                    // If the rootExpr is not itself an AggregateExpression but simply contains one (or more)
                    // like "MAX(counter)+1" or "MAX(col)/MIN(col)",
                    // it gets classified as a non-push-down-able aggregate.
                    // That beats getting it confused with a pass-through column.
                    // TODO: support expressions of aggregates by greater differentiation of display columns between the top-level
                    // aggregate (potentially containing aggregate functions and expressions of aggregate functions) and the pushed-down
                    // aggregate (potentially containing aggregate functions and aggregate functions of expressions).
                    agg_input_expr = rootExpr.getLeft();
                    hasAggregates = true;

                    // count(*) hack.  we're not getting AGGREGATE_COUNT_STAR
                    // expression types from the parsing, so we have
                    // to detect the null inner expression case and do the
                    // switcharoo ourselves.
                    if (rootExpr.getExpressionType() == ExpressionType.AGGREGATE_COUNT &&
                             rootExpr.getLeft() == null)
                    {
                        agg_expression_type = ExpressionType.AGGREGATE_COUNT_STAR;

                        // Just need a random input column for now.
                        // The EE won't actually evaluate this, so we
                        // just pick something innocuous
                        // At some point we should special-case count-star so
                        // we don't go digging for TVEs
                        // XXX: Danger: according to standard SQL, if first_col has nulls, COUNT(first_col) < COUNT(*)
                        // -- consider using something non-nullable like TupleAddressExpression?
                        SchemaColumn first_col = root.getOutputSchema().getColumns().get(0);
                        TupleValueExpression tve = new TupleValueExpression();
                        tve.setValueType(first_col.getType());
                        tve.setValueSize(first_col.getSize());
                        tve.setColumnIndex(0);
                        tve.setColumnName(first_col.getColumnName());
                        tve.setColumnAlias(first_col.getColumnName());
                        tve.setTableName(first_col.getTableName());
                        agg_input_expr = tve;
                    }

                    // A bit of a hack: ProjectionNodes after the
                    // aggregate node need the output columns here to
                    // contain TupleValueExpressions (effectively on a temp table).
                    // So we construct one based on the output of the
                    // aggregate expression, the column alias provided by HSQL,
                    // and the offset into the output table schema for the
                    // aggregate node that we're computing.
                    // Oh, oh, it's magic, you know..
                    TupleValueExpression tve = new TupleValueExpression();
                    tve.setValueType(rootExpr.getValueType());
                    tve.setValueSize(rootExpr.getValueSize());
                    tve.setColumnIndex(outputColumnIndex);
                    tve.setColumnName("");
                    tve.setColumnAlias(col.alias);
                    tve.setTableName("VOLT_TEMP_TABLE");
                    boolean is_distinct = ((AggregateExpression)rootExpr).m_distinct;
                    aggNode.addAggregate(agg_expression_type, is_distinct,
                                         outputColumnIndex, agg_input_expr);
                    schema_col = new SchemaColumn("VOLT_TEMP_TABLE",
                                                  "",
                                                  col.alias,
                                                  tve);

                    /*
                     * Special case count(*), count(), sum(), min() and max() to
                     * push them down to each partition. It will do the
                     * push-down if the select columns only contains the listed
                     * aggregate operators and other group-by columns. If the
                     * select columns includes any other aggregates, it will not
                     * do the push-down. - nshi
                     */
                    if (!is_distinct &&
                        (agg_expression_type == ExpressionType.AGGREGATE_COUNT_STAR ||
                         agg_expression_type == ExpressionType.AGGREGATE_COUNT ||
                         agg_expression_type == ExpressionType.AGGREGATE_SUM))
                    {
                        /*
                         * For count(*), count() and sum(), the pushed-down
                         * aggregate node doesn't change. An extra sum()
                         * aggregate node is added to the coordinator to sum up
                         * the numbers from all the partitions. The input schema
                         * and the output schema of the sum() aggregate node is
                         * the same as the output schema of the push-down
                         * aggregate node.
                         *
                         * If DISTINCT is specified, don't do push-down for
                         * count() and sum()
                         */

                        // Output column for the sum() aggregate node
                        TupleValueExpression topOutputExpr = new TupleValueExpression();
                        topOutputExpr.setValueType(rootExpr.getValueType());
                        topOutputExpr.setValueSize(rootExpr.getValueSize());
                        topOutputExpr.setColumnIndex(topOutputColumnIndex);
                        topOutputExpr.setColumnName("");
                        topOutputExpr.setColumnAlias(col.alias);
                        topOutputExpr.setTableName("VOLT_TEMP_TABLE");

                        /*
                         * Input column of the sum() aggregate node is the
                         * output column of the push-down aggregate node
                         */
                        topAggNode.addAggregate(ExpressionType.AGGREGATE_SUM,
                                                false,
                                                outputColumnIndex,
                                                tve);
                        topSchemaCol = new SchemaColumn("VOLT_TEMP_TABLE",
                                                        "",
                                                        col.alias,
                                                        topOutputExpr);
                    }
                    else if (agg_expression_type == ExpressionType.AGGREGATE_MIN ||
                             agg_expression_type == ExpressionType.AGGREGATE_MAX)
                    {
                        /*
                         * For min() and max(), the pushed-down aggregate node
                         * doesn't change. An extra aggregate node of the same
                         * type is added to the coordinator. The input schema
                         * and the output schema of the top aggregate node is
                         * the same as the output schema of the pushed-down
                         * aggregate node.
                         */
                        topAggNode.addAggregate(agg_expression_type,
                                                is_distinct,
                                                outputColumnIndex,
                                                tve);
                        topSchemaCol = schema_col;
                    }
                    else
                    {
                        /*
                         * Unsupported aggregate (AVG for example)
                         * or some expression of aggregates.
                         */
                        isPushDownAgg = false;
                    }
                }
                else
                {
                    /*
                     * These columns are the pass through columns that are not being
                     * aggregated on. These are the ones from the SELECT list. They
                     * MUST already exist in the child node's output. Find them and
                     * add them to the aggregate's output.
                     */
                    schema_col = new SchemaColumn(col.tableName,
                                                  col.columnName,
                                                  col.alias,
                                                  col.expression);
                }

                if (topSchemaCol == null)
                {
                    /*
                     * If we didn't set the column schema for the top node, it
                     * means either it's not a count(*) aggregate or it's a
                     * pass-through column. So just copy it.
                     */
                    topSchemaCol = new SchemaColumn(schema_col.getTableName(),
                                                    schema_col.getColumnName(),
                                                    schema_col.getColumnAlias(),
                                                    schema_col.getExpression());
                }

                agg_schema.addColumn(schema_col);
                topAggSchema.addColumn(topSchemaCol);
                outputColumnIndex++;
                topOutputColumnIndex++;
            }

            for (ParsedSelectStmt.ParsedColInfo col : m_parsedSelect.groupByColumns)
            {
                if (agg_schema.find(col.tableName, col.columnName, col.alias) == null)
                {
                    throw new PlanningErrorException("GROUP BY column " + col.alias +
                                                     " is not in the display columns." +
                                                     " Please specify " + col.alias +
                                                     " as a display column.");
                }

                aggNode.addGroupByExpression(col.expression);
                topAggNode.addGroupByExpression(col.expression);
            }

            aggNode.setOutputSchema(agg_schema);
            topAggNode.setOutputSchema(agg_schema);
            /*
             * Is there a necessary coordinator-aggregate node...
             */
            if (!hasAggregates || !isPushDownAgg)
            {
                topAggNode = null;
            }
            root = pushDownAggregate(root, aggNode, topAggNode);
        }
        else
        {
            /*
             * Handle DISTINCT only when there is no aggregate operator
             * expression
             */
            root = handleDistinct(root);
        }

        return root;
    }

    /**
     * Push the given aggregate if the plan is distributed, then add the
     * coordinator node on top of the send/receive pair. If the plan
     * is not distributed, or coordNode is not provided, the distNode
     * is added at the top of the plan.
     *
     * Note: this works in part because the push-down node is also an acceptable
     * top level node if the plan is not distributed. This wouldn't be true
     * if we started pushing down something like (sum, count) to calculate
     * a distributed average.
     *
     * @param root
     *            The root node
     * @param distNode
     *            The node to push down
     * @param coordNode
     *            The top node to put on top of the send/receive pair after
     *            push-down. If this is null, no push-down will be performed.
     * @return The new root node.
     */
    AbstractPlanNode pushDownAggregate(AbstractPlanNode root,
                                       AggregatePlanNode distNode,
                                       AggregatePlanNode coordNode) {

        // remember that coordinating aggregation has a pushed-down
        // counterpart deeper in the plan. this allows other operators
        // to be pushed down past the receive as well.
        if (coordNode != null) {
            coordNode.m_isCoordinatingAggregator = true;
        }

        /*
         * Push this node down to partition if it's distributed. First remove
         * the send/receive pair, add the node, then put the send/receive pair
         * back on top of the node, followed by another top node at the
         * coordinator.
         */
        AbstractPlanNode accessPlanTemp = root;
        if (coordNode != null && root instanceof ReceivePlanNode) {
            root = root.getChild(0).getChild(0);
            root.clearParents();
        } else {
            accessPlanTemp = null;
        }

        distNode.addAndLinkChild(root);
        distNode.generateOutputSchema(m_catalogDb);
        root = distNode;

        // Put the send/receive pair back into place
        if (accessPlanTemp != null) {
            accessPlanTemp.getChild(0).clearChildren();
            accessPlanTemp.getChild(0).addAndLinkChild(root);
            root = accessPlanTemp;

            // Add the top node
            coordNode.addAndLinkChild(root);
            coordNode.generateOutputSchema(m_catalogDb);
            root = coordNode;
        }
        return root;
    }

    /**
     * Check if we can push the limit node down.
     *
     * @param root
     * @return If we can push it down, the receive node is returned. Otherwise,
     *         it returns null.
     */
    protected AbstractPlanNode checkPushDownViability(AbstractPlanNode root) {
        AbstractPlanNode receiveNode = root;

        // Return a mid-plan send node, if one exists and can host a distributed limit node.
        // There is guaranteed to be at most a single receive/send pair.
        // Abort the search if a node that a "limit" can't be pushed past is found before its receive node.
        //
        // Can only push past:
        //   * coordinatingAggregator: a distributed aggregator a copy of which  has already been pushed down.
        //     Distributing a LIMIT to just above that aggregator is correct. (I've got some doubts that this is correct??? --paul)
        //
        //   * order by: if the plan requires a sort, getNextSelectPlan()  will have already added an ORDER BY.
        //     A distributed LIMIT will be added above a copy of that ORDER BY node.
        //
        //   * projection: these have no effect on the application of limits.
        //
        // Return null if the plan is single-partition or if its "coordinator" part contains a push-blocking node type.

        while (!(receiveNode instanceof ReceivePlanNode)) {

            // Limitation: can only push past some nodes (see above comment)
            if (!(receiveNode instanceof AggregatePlanNode) &&
                !(receiveNode instanceof OrderByPlanNode) &&
                !(receiveNode instanceof ProjectionPlanNode)) {
                return null;
            }

            // Limitation: can only push past coordinating aggregation nodes
            if (receiveNode instanceof AggregatePlanNode &&
                !((AggregatePlanNode)receiveNode).m_isCoordinatingAggregator) {
                return null;
            }

            if (receiveNode instanceof OrderByPlanNode) {
                for (ParsedSelectStmt.ParsedColInfo col : m_parsedSelect.orderByColumns()) {
                    AbstractExpression rootExpr = col.expression;
                    // Fix ENG-3487: can't push down limits when results are ordered by aggregate values.
                    if (rootExpr instanceof TupleValueExpression) {
                        if  (((TupleValueExpression) rootExpr).hasAggregate()) {
                            return null;
                        }
                    }
                }
            }

            // Traverse...
            if (receiveNode.getChildCount() == 0) {
                return null;
            }

            // nothing that allows pushing past has multiple inputs
            assert(receiveNode.getChildCount() == 1);
            receiveNode = receiveNode.getChild(0);
        }
        return receiveNode.getChild(0);
    }

    /**
     * Handle select distinct a from t
     *
     * @param root
     * @return
     */
    AbstractPlanNode handleDistinct(AbstractPlanNode root) {
        if (m_parsedSelect.distinct) {
            // We currently can't handle DISTINCT of multiple columns.
            // Throw a planner error if this is attempted.
            //if (m_parsedSelect.displayColumns.size() > 1)
            //{
            //    throw new PlanningErrorException("Multiple DISTINCT columns currently unsupported");
            //}
            AbstractExpression distinctExpr = null;
            AbstractExpression nextExpr = null;
            for (ParsedSelectStmt.ParsedColInfo col : m_parsedSelect.displayColumns) {
                // Distinct can in theory handle any expression now, but it's
                // untested so we'll balk on anything other than a TVE here
                // --izzy
                if (col.expression instanceof TupleValueExpression)
                {
                    // Add distinct node(s) to the plan
                    if (distinctExpr == null) {
                        distinctExpr = col.expression;
                        nextExpr = distinctExpr;
                    } else {
                        nextExpr.setRight(col.expression);
                        nextExpr = nextExpr.getRight();
                    }
                 }
                else
                {
                    throw new PlanningErrorException("DISTINCT of an expression currently unsupported");
                }
            }
            // Add distinct node(s) to the plan
            root = addDistinctNodes(root, distinctExpr);
            // aggregate handlers are expected to produce the required projection.
            // the other aggregates do this inherently but distinct may need a
            // projection node.
            root = addProjection(root);

        }

        return root;
    }

    /**
     * If plan is distributed than add distinct nodes to each partition and the coordinator.
     * Otherwise simply add the distinct node on top of the current root
     *
     * @param root The root node
     * @param expr The distinct expression
     * @return The new root node.
     */
    AbstractPlanNode addDistinctNodes(AbstractPlanNode root, AbstractExpression expr)
    {
        assert(root != null);
        AbstractPlanNode accessPlanTemp = root;
        if (root instanceof ReceivePlanNode) {
            // Temporarily strip send/receive pair
            accessPlanTemp = root.getChild(0).getChild(0);
            accessPlanTemp.clearParents();
            root.getChild(0).unlinkChild(accessPlanTemp);

            // Add new distinct node to each partition
            AbstractPlanNode distinctNode = addDistinctNode(accessPlanTemp, expr);
            // Add send/receive pair back
            root.getChild(0).addAndLinkChild(distinctNode);
        }

        // Add new distinct node to the coordinator
        root = addDistinctNode(root, expr);
        return root;
    }

    /**
     * Build new distinct node and put it on top of the current root
     *
     * @param root The root node
     * @param expr The distinct expression
     * @return The new root node.
     */
    AbstractPlanNode addDistinctNode(AbstractPlanNode root, AbstractExpression expr)
    {
        DistinctPlanNode distinctNode = new DistinctPlanNode();
        distinctNode.setDistinctExpression(expr);
        distinctNode.addAndLinkChild(root);
        distinctNode.generateOutputSchema(m_catalogDb);
        return distinctNode;
    }

    /**
     * Get the unique set of names of all columns that are part of an index on
     * the given table.
     *
     * @param table
     *            The table to build the list of index-affected columns with.
     * @return The set of column names affected by indexes with duplicates
     *         removed.
     */
    public Set<String> getIndexedColumnSetForTable(Table table) {
        HashSet<String> columns = new HashSet<String>();

        for (Index index : table.getIndexes()) {
            for (ColumnRef colRef : index.getColumns()) {
                columns.add(colRef.getColumn().getTypeName());
            }
        }

        return columns;
    }

    public String getErrorMessage() {
        return m_recentErrorMsg;
    }

}
