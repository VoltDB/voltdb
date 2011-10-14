/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.planner;

import java.util.*;
import java.util.Map.Entry;

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
import org.voltdb.expressions.ParameterValueExpression;
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

    /** does the statement touch more than one partition? */
    boolean m_singlePartition;

    /** The number of partitions (fetched from the cluster info) */
    final int m_partitionCount;

    /**
     * Used to generate the table-touching parts of a plan. All join-order and
     * access path selection stuff is done by the SelectSubPlanAssember.
     */
    SubPlanAssembler subAssembler = null;

    /**
     * Counter for the number of plans generated to date for a single statement.
     */
    int plansGenerated = 0;

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
     */
    PlanAssembler(Cluster catalogCluster, Database catalogDb) {
        m_catalogCluster = catalogCluster;
        m_catalogDb = catalogDb;
        m_partitionCount = m_catalogCluster.getPartitions().size();
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
     * @param xmlSQL
     *            The parsed/analyzed SQL in XML form from HSQLDB to be planned.
     * @param readOnly
     *            Is the SQL statement read only.
     * @param singlePartition
     *            Does the SQL statement use only a single partition?
     */
    void setupForNewPlans(AbstractParsedStmt parsedStmt, boolean singlePartition)
    {
        m_singlePartition = singlePartition;

        if (parsedStmt instanceof ParsedSelectStmt) {
            if (tableListIncludesExportOnly(parsedStmt.tableList)) {
                throw new RuntimeException(
                "Illegal to read an export table.");
            }
            m_parsedSelect = (ParsedSelectStmt) parsedStmt;
            subAssembler =
                new SelectSubPlanAssembler(m_catalogDb,
                                           parsedStmt, singlePartition,
                                           m_partitionCount);
        } else {
            // check that no modification happens to views
            if (tableListIncludesView(parsedStmt.tableList)) {
                throw new RuntimeException(
                "Illegal to modify a materialized view.");
            }

            if (parsedStmt instanceof ParsedInsertStmt) {
                m_parsedInsert = (ParsedInsertStmt) parsedStmt;
            } else if (parsedStmt instanceof ParsedUpdateStmt) {
                if (tableListIncludesExportOnly(parsedStmt.tableList)) {
                    throw new RuntimeException(
                    "Illegal to update an export table.");
                }
                m_parsedUpdate = (ParsedUpdateStmt) parsedStmt;
                subAssembler =
                    new WriterSubPlanAssembler(m_catalogDb, parsedStmt, singlePartition, m_partitionCount);
            } else if (parsedStmt instanceof ParsedDeleteStmt) {
                if (tableListIncludesExportOnly(parsedStmt.tableList)) {
                    throw new RuntimeException(
                    "Illegal to delete from an export table.");
                }
                m_parsedDelete = (ParsedDeleteStmt) parsedStmt;
                subAssembler =
                    new WriterSubPlanAssembler(m_catalogDb, parsedStmt, singlePartition, m_partitionCount);
            } else
                throw new RuntimeException(
                        "Unknown subclass of AbstractParsedStmt.");
        }
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
        CompiledPlan.Fragment fragment = new CompiledPlan.Fragment();
        retval.fragments.add(fragment);
        if (m_parsedInsert != null) {
            retval.fullWhereClause = m_parsedInsert.where;
            fragment.planGraph = getNextInsertPlan();
            retval.fullWinnerPlan = fragment.planGraph;
            addParameters(retval, m_parsedInsert);
            assert (m_parsedInsert.tableList.size() == 1);
            if (m_parsedInsert.tableList.get(0).getIsreplicated())
                retval.replicatedTableDML = true;
        } else if (m_parsedUpdate != null) {
            retval.fullWhereClause = m_parsedUpdate.where;
            fragment.planGraph = getNextUpdatePlan();
            retval.fullWinnerPlan = fragment.planGraph;
            addParameters(retval, m_parsedUpdate);
            // note that for replicated tables, multi-fragment plans
            // need to divide the result by the number of partitions
            assert (m_parsedUpdate.tableList.size() == 1);
            if (m_parsedUpdate.tableList.get(0).getIsreplicated())
                retval.replicatedTableDML = true;
        } else if (m_parsedDelete != null) {
            retval.fullWhereClause = m_parsedDelete.where;
            fragment.planGraph = getNextDeletePlan();
            retval.fullWinnerPlan = fragment.planGraph;
            addParameters(retval, m_parsedDelete);
            // note that for replicated tables, multi-fragment plans
            // need to divide the result by the number of partitions
            assert (m_parsedDelete.tableList.size() == 1);
            if (m_parsedDelete.tableList.get(0).getIsreplicated())
                retval.replicatedTableDML = true;
        } else if (m_parsedSelect != null) {
            retval.fullWhereClause = m_parsedSelect.where;
            fragment.planGraph = getNextSelectPlan();
            retval.fullWinnerPlan = fragment.planGraph;
            addParameters(retval, m_parsedSelect);
            if (fragment.planGraph != null)
            {
                // only add the output columns if we actually have a plan
                // avoid PlanColumn resource leakage
                addColumns(retval, m_parsedSelect);
            }
        } else
            throw new RuntimeException(
                    "setupForNewPlans not called or not successfull.");

        plansGenerated++;
        if (fragment.planGraph == null)
        {
            return null;
        }
        else
        {
            // Do a final generateOutputSchema pass.
            fragment.planGraph.generateOutputSchema(m_catalogDb);
        }

        return retval;
    }

    private void addColumns(CompiledPlan plan, ParsedSelectStmt stmt) {
        NodeSchema output_schema = plan.fragments.get(0).planGraph.getOutputSchema();
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
        ParameterInfo outParam = null;
        for (ParameterInfo param : stmt.paramList) {
            outParam = new ParameterInfo();
            outParam.index = param.index;

            VoltType override = m_paramTypeOverrideMap.get(param.index);
            if (override != null) {
                outParam.type = override;
            }
            else {
                outParam.type = param.type;
            }
            plan.parameters.add(outParam);
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

        if ((subSelectRoot.getPlanNodeType() != PlanNodeType.INDEXSCAN ||
            ((IndexScanPlanNode) subSelectRoot).getSortDirection() == SortDirectionType.INVALID) &&
            m_parsedSelect.orderColumns.size() > 0) {
            root = addOrderBy(root);
        }

        if ((root.getPlanNodeType() != PlanNodeType.AGGREGATE) &&
            (root.getPlanNodeType() != PlanNodeType.HASHAGGREGATE) &&
            (root.getPlanNodeType() != PlanNodeType.DISTINCT) &&
            (root.getPlanNodeType() != PlanNodeType.PROJECTION)) {
            root = addProjection(root);
        }

        if ((m_parsedSelect.limit != -1) || (m_parsedSelect.limitParameterId != -1) ||
            (m_parsedSelect.offset > 0) || (m_parsedSelect.offsetParameterId != -1))
        {
            root = handleLimitOperator(root);
        }


        SendPlanNode sendNode = new SendPlanNode();

        // connect the nodes to build the graph
        sendNode.addAndLinkChild(root);
        sendNode.generateOutputSchema(m_catalogDb);

        return sendNode;
    }

    private AbstractPlanNode getNextDeletePlan() {
        assert (subAssembler != null);

        // figure out which table we're deleting from
        assert (m_parsedDelete.tableList.size() == 1);
        Table targetTable = m_parsedDelete.tableList.get(0);

        // this is never going to go well
        if (m_singlePartition && (targetTable.getIsreplicated())) {
            String msg =
                "Trying to delete from replicated table '"
                        + targetTable.getTypeName() + "'";
            msg += " in a single-partition procedure.";
            throw new PlanningErrorException(msg);
        }

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

        if (m_singlePartition == true) {

            assert(subSelectRoot instanceof AbstractScanPlanNode);

            // if the scan below matches all nodes, we can throw away the scan
            // nodes and use a truncate delete node
            if ((subSelectRoot instanceof SeqScanPlanNode)
                    && (((AbstractScanPlanNode) subSelectRoot).getPredicate() == null)) {
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
            deleteNode.generateOutputSchema(m_catalogDb);
            return deleteNode;

        } else {
            // make sure the thing we have is a receive node which
            // indicates it's a multi-site plan
            assert (subSelectRoot instanceof ReceivePlanNode);

            // put the delete node in the right place
            // get the recv node
            ReceivePlanNode recvNode = (ReceivePlanNode) subSelectRoot;
            // get the send node
            assert (recvNode.getChildCount() == 1);
            AbstractPlanNode sendNode = recvNode.getChild(0);

            // get the scan node and unlink
            assert (sendNode.getChildCount() == 1);
            AbstractPlanNode scanNode = sendNode.getChild(0);
            sendNode.unlinkChild(scanNode);

            // link in the delete node
            assert (scanNode instanceof AbstractScanPlanNode);
            scanNode.addInlinePlanNode(projectionNode);
            deleteNode.addAndLinkChild(scanNode);
            deleteNode.generateOutputSchema(m_catalogDb);
            sendNode.addAndLinkChild(deleteNode);
            sendNode.generateOutputSchema(m_catalogDb);
            // fix the receive node's output columns
            recvNode.generateOutputSchema(m_catalogDb);
            // add a sum and send on top of the union
            return addSumAndSendToDMLNode(subSelectRoot);
        }
    }

    private AbstractPlanNode getNextUpdatePlan() {
        assert (subAssembler != null);

        // figure out which table we're updating
        assert (m_parsedUpdate.tableList.size() == 1);
        Table targetTable = m_parsedUpdate.tableList.get(0);

        // this is never going to go well
        if (m_singlePartition && (targetTable.getIsreplicated())) {
            String msg =
                "Trying to update replicated table '" + targetTable.getTypeName()
                        + "'";
            msg += " in a single-partition procedure.";
            throw new PlanningErrorException(msg);
        }

        AbstractPlanNode subSelectRoot = subAssembler.nextPlan();
        if (subSelectRoot == null)
            return null;

        UpdatePlanNode updateNode = new UpdatePlanNode();
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

        if (m_singlePartition == true) {

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
            updateNode.generateOutputSchema(m_catalogDb);

            return updateNode;
        } else {
            // make sure the thing we have is a receive node which
            // indicates it's a multi-site plan
            assert (subSelectRoot instanceof ReceivePlanNode);

            // put the update node in the right place
            // get the recv node
            ReceivePlanNode recvNode = (ReceivePlanNode) subSelectRoot;
            // get the send node
            assert (recvNode.getChildCount() == 1);
            AbstractPlanNode sendNode = recvNode.getChild(0);

            // get the scan node and unlink
            assert (sendNode.getChildCount() == 1);
            AbstractPlanNode scanNode = sendNode.getChild(0);
            sendNode.unlinkChild(scanNode);

            // link in the update node
            assert (scanNode instanceof AbstractScanPlanNode);
            scanNode.addInlinePlanNode(projectionNode);
            updateNode.addAndLinkChild(scanNode);
            updateNode.generateOutputSchema(m_catalogDb);
            sendNode.addAndLinkChild(updateNode);
            sendNode.generateOutputSchema(m_catalogDb);
            // fix the receive node's output columns
            recvNode.generateOutputSchema(m_catalogDb);
            // add a count and send on top of the union
            return addSumAndSendToDMLNode(subSelectRoot);
        }
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
        if (plansGenerated > 0)
            return null;

        // figure out which table we're inserting into
        assert (m_parsedInsert.tableList.size() == 1);
        Table targetTable = m_parsedInsert.tableList.get(0);

        // this is never going to go well
        if (m_singlePartition && targetTable.getIsreplicated()) {
            String msg =
                "Trying to insert into replicated table '"
                        + targetTable.getTypeName() + "'";
            msg += " in a single-partition procedure.";
            throw new PlanningErrorException(msg);
        }

        // the root of the insert plan is always an InsertPlanNode
        InsertPlanNode insertNode = new InsertPlanNode();
        insertNode.setTargetTableName(targetTable.getTypeName());
        insertNode.setMultiPartition(m_singlePartition == false);

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
        AbstractPlanNode rootNode = insertNode;

        if (m_singlePartition == false) {
            // all sites to a scan -> send
            // root site has many recvs feeding into a union

            SendPlanNode sendNode = new SendPlanNode();
            // this will make the child planfragment be sent to all partitions
            sendNode.isMultiPartition = true;
            sendNode.addAndLinkChild(rootNode);
            sendNode.generateOutputSchema(m_catalogDb);

            ReceivePlanNode recvNode = new ReceivePlanNode();
            recvNode.addAndLinkChild(sendNode);
            recvNode.generateOutputSchema(m_catalogDb);
            rootNode = recvNode;

            // add a count and send on top of the union
            rootNode = addSumAndSendToDMLNode(rootNode);
        }

        return rootNode;
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
     * Configure the sort columns for a new OrderByPlanNode
     * @return new OrderByPlanNode
     */
    OrderByPlanNode createOrderBy() {
        assert (m_parsedSelect != null);

        OrderByPlanNode orderByNode = new OrderByPlanNode();
        for (ParsedSelectStmt.ParsedColInfo col : m_parsedSelect.orderColumns) {
            orderByNode.addSort(col.expression,
                                col.ascending ? SortDirectionType.ASC
                                              : SortDirectionType.DESC);
        }
        return orderByNode;
    }

    /**
     * Create an order by node and add make it a parent of root.
     * @param root
     * @return new orderByNode (the new root)
     */
    AbstractPlanNode addOrderBy(AbstractPlanNode root) {
        OrderByPlanNode orderByNode = createOrderBy();
        orderByNode.addAndLinkChild(root);
        orderByNode.generateOutputSchema(m_catalogDb);
        return orderByNode;
    }

    AbstractPlanNode addOffsetAndLimit(AbstractPlanNode root) {
        return null;
    }

    /**
     * Add a limit, pushed-down if possible, and return the new root.
     * @param root top of the original plan
     * @return new plan's root node
     */
    AbstractPlanNode handleLimitOperator(AbstractPlanNode root) {
        // Whether or not we can push the limit node down
        boolean canPushDown = true;

        // The nodes that need to be applied at the coordinator
        Stack<AbstractPlanNode> coordGraph = new Stack<AbstractPlanNode>();

        // The nodes that need to be applied at the distributed plan.
        Stack<AbstractPlanNode> distGraph = new Stack<AbstractPlanNode>();

        // The coordinator's top limit graph fragment for a MP plan.
        // If planning "order by ... limit", getNextSelectPlan()
        // will have already added an order by to the coordinator frag.
        LimitPlanNode coordLimit = new LimitPlanNode();
        coordLimit.setLimit((int)m_parsedSelect.limit);
        coordLimit.setOffset((int) m_parsedSelect.offset);
        if (m_parsedSelect.offsetParameterId != -1) {
            ParameterInfo parameterInfo =
                m_parsedSelect.paramsById.get(m_parsedSelect.offsetParameterId);
            coordLimit.setOffsetParameterIndex(parameterInfo.index);
        }
        if (m_parsedSelect.limitParameterId != -1) {
            ParameterInfo parameterInfo =
                m_parsedSelect.paramsById.get(m_parsedSelect.limitParameterId);
            coordLimit.setLimitParameterIndex(parameterInfo.index);
        }

        coordGraph.push(coordLimit);

        /*
         * TODO: allow push down limit with distinct (select distinct C from T limit 5)
         * or distinct in aggregates.
         */
        if (m_parsedSelect.distinct || checkPushDownViability(root) == null) {
            canPushDown = false;
        }
        for (ParsedSelectStmt.ParsedColInfo col : m_parsedSelect.displayColumns) {
            AbstractExpression rootExpr = col.expression;
            if (rootExpr instanceof AggregateExpression) {
                if (((AggregateExpression)rootExpr).m_distinct) {
                    canPushDown = false;
                    break;
                }
            }
        }

        // The distributed limit and the only limit node in a SP plan
        LimitPlanNode distLimit = new LimitPlanNode();
        distLimit.setLimit((int) m_parsedSelect.limit);
        distLimit.setOffset((int) m_parsedSelect.offset);
        if (m_parsedSelect.offsetParameterId != -1) {
            ParameterInfo parameterInfo =
                m_parsedSelect.paramsById.get(m_parsedSelect.offsetParameterId);
            distLimit.setOffsetParameterIndex(parameterInfo.index);
        }
        if (m_parsedSelect.limitParameterId != -1) {
            ParameterInfo parameterInfo =
                m_parsedSelect.paramsById.get(m_parsedSelect.limitParameterId);
            distLimit.setLimitParameterIndex(parameterInfo.index);
        }
        distGraph.push(distLimit);

        /*
         * Push down the limit plan node when possible even if offset is set. If
         * the plan is for a partitioned table, do the push down. Otherwise,
         * there is no need to do the push down work, the limit plan node will
         * be run in the partition.
         */
        if (root.findAllNodesOfType(PlanNodeType.RECEIVE).isEmpty() || !canPushDown) {
            // not for partitioned table or cannot push down
            coordGraph.clear();
        } else {
            /*
             * For partitioned table, the pushed-down limit plan node contains
             * an expression for the limit, and the offset is always 0. The
             * expression for the limit is the original (limit + offset). The
             * top level limit plan node remains the same, with the original
             * limit and offset values.
             */
            distGraph.clear();

            distLimit = new LimitPlanNode();
            distLimit.setLimit((int) (m_parsedSelect.limit + m_parsedSelect.offset));
            distLimit.setOffset(0);

            AbstractExpression left = new ConstantValueExpression();
            ((ConstantValueExpression) left).setValue(Long.toString(m_parsedSelect.offset));
            left.setValueType(VoltType.INTEGER);

            AbstractExpression right = new ConstantValueExpression();
            ((ConstantValueExpression) right).setValue(Long.toString(m_parsedSelect.limit));
            right.setValueType(VoltType.INTEGER);

            if (m_parsedSelect.offsetParameterId != -1 ||
                    m_parsedSelect.limitParameterId != -1) {
                if (m_parsedSelect.offsetParameterId != -1) {
                    left = new ParameterValueExpression();
                    ParameterInfo paramInfo =
                        m_parsedSelect.paramsById.get(m_parsedSelect.offsetParameterId);
                    ((ParameterValueExpression) left).setParameterId(paramInfo.index);
                    left.setValueType(paramInfo.type);
                    left.setValueSize(paramInfo.type.getLengthInBytesForFixedTypes());
                }

                if (m_parsedSelect.limitParameterId != -1) {
                    right = new ParameterValueExpression();
                    ParameterInfo paramInfo =
                        m_parsedSelect.paramsById.get(m_parsedSelect.limitParameterId);
                    ((ParameterValueExpression) right).setParameterId(paramInfo.index);
                    right.setValueType(paramInfo.type);
                    right.setValueSize(paramInfo.type.getLengthInBytesForFixedTypes());
                }
            }

            OperatorExpression expr = new OperatorExpression(ExpressionType.OPERATOR_PLUS,
                                                             left, right);
            expr.setValueType(VoltType.INTEGER);
            expr.setValueSize(VoltType.INTEGER.getLengthInBytesForFixedTypes());
            distLimit.setLimitExpression(expr);
            distGraph.push(distLimit);
        }

        return pushDownLimit(root, distGraph, coordGraph);
    }

    AbstractPlanNode handleAggregationOperators(AbstractPlanNode root) {
        boolean containsAggregateExpression = false;
        AggregatePlanNode aggNode = null;

        /* Check if any aggregate expressions are present */
        for (ParsedSelectStmt.ParsedColInfo col : m_parsedSelect.displayColumns) {
            if (col.expression.getExpressionType() == ExpressionType.AGGREGATE_SUM ||
                col.expression.getExpressionType() == ExpressionType.AGGREGATE_COUNT ||
                col.expression.getExpressionType() == ExpressionType.AGGREGATE_COUNT_STAR ||
                col.expression.getExpressionType() == ExpressionType.AGGREGATE_MIN ||
                col.expression.getExpressionType() == ExpressionType.AGGREGATE_MAX ||
                col.expression.getExpressionType() == ExpressionType.AGGREGATE_AVG) {
                containsAggregateExpression = true;
            }
        }

        /*
         * "Select A from T group by A" is grouped but has no aggregate operator
         * expressions. Catch that case by checking the grouped flag
         */
        if (m_parsedSelect.grouped)
        {
            containsAggregateExpression = true;
        }

        if (containsAggregateExpression) {
            AggregatePlanNode topAggNode;
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
            for (ParsedSelectStmt.ParsedColInfo col : m_parsedSelect.displayColumns)
            {
                AbstractExpression rootExpr = col.expression;
                AbstractExpression agg_input_expr = null;
                SchemaColumn schema_col = null;
                SchemaColumn topSchemaCol = null;
                ExpressionType agg_expression_type = rootExpr.getExpressionType();
                if (rootExpr.getExpressionType() == ExpressionType.AGGREGATE_SUM ||
                    rootExpr.getExpressionType() == ExpressionType.AGGREGATE_MIN ||
                    rootExpr.getExpressionType() == ExpressionType.AGGREGATE_MAX ||
                    rootExpr.getExpressionType() == ExpressionType.AGGREGATE_AVG ||
                    rootExpr.getExpressionType() == ExpressionType.AGGREGATE_COUNT ||
                    rootExpr.getExpressionType() == ExpressionType.AGGREGATE_COUNT_STAR)
                {
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
     * Push the distributed node down if the plan is distributed, then add the
     * coord. nodes at the top of the root plan. If the coord node is not given,
     * nothing is pushed down - the distributed node is added on top of the
     * send/receive pair directly.
     *
     * Note: this works in part because the push-down node is also an acceptable
     * top level node if the plan is not distributed. This wouldn't be true
     * if we started pushing down something like (sum, count) to calculate
     * a distributed average.
     *
     * @param root
     *            The root node
     * @param distributedNode
     *            The node to push down
     * @param coordNodes
     *            New coordinator node(s) to put on top of the plan.
     *            If this is null, no push-down will be performed.
     * @return The new root node.
     */
    AbstractPlanNode pushDownLimit(AbstractPlanNode root,
                                  Stack<AbstractPlanNode> distNodes,
                                  Stack<AbstractPlanNode> coordNodes) {

        AbstractPlanNode receiveNode = checkPushDownViability(root);

        // If there is work to distribute and a receive node was found,
        // disconnect the coordinator and distributed parts of the plan
        // below the SEND node
        AbstractPlanNode distributedPlan = root;
        if (!coordNodes.isEmpty() && receiveNode != null) {
            distributedPlan = receiveNode.getChild(0).getChild(0);
            distributedPlan.clearParents();
            receiveNode.getChild(0).clearChildren();
        }

        // If there is work to distribute, determine if the distributed
        // limit must be performed on ordered input. If so, produce that
        // order if an explicit sort is necessary
        if (!coordNodes.isEmpty() && receiveNode != null) {
            if ((distributedPlan.getPlanNodeType() != PlanNodeType.INDEXSCAN ||
                ((IndexScanPlanNode) distributedPlan).getSortDirection() == SortDirectionType.INVALID) &&
                m_parsedSelect.orderColumns.size() > 0) {
                distNodes.push(createOrderBy());
            }
        }

        // Add the distributed work to the plan
        while (!distNodes.isEmpty()) {
            AbstractPlanNode distributedNode = distNodes.pop();
            distributedNode.addAndLinkChild(distributedPlan);
            distributedPlan = distributedNode;
        }

        // Reconnect the plans and add the coordinator's work
        if (!coordNodes.isEmpty() && receiveNode != null) {
            receiveNode.getChild(0).addAndLinkChild(distributedPlan);

            while (!coordNodes.isEmpty()) {
                AbstractPlanNode coordNode = coordNodes.pop();
                coordNode.addAndLinkChild(root);
                root = coordNode;
            }
        }
        else {
            root = distributedPlan;
        }

        root.generateOutputSchema(m_catalogDb);
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

        // Find a receive node, if one exists. There is guaranteed to be at
        // most a single receive. Abort the search if between root and receive
        // a node that can't be pushed down past is found.
        //
        // Can only push past:
        //   * coordinatingAggregator: a distributed aggregator has
        //     has already been pushed down. Distributed LIMIT of that
        //     aggregation is correct.
        //
        //   * order by: if the plan requires a sort, getNextSelectPlan()
        //     will have already added an ORDER BY. LIMIT will be added
        //     above that sort. However, if LIMIT can be successfully
        //     pushed down, it may be necessary to create and push down
        //     a distributed sort as well. That work is done here.
        //
        //   * projection: we only LIMIT on constant value expressions.
        //     whether the LIMIT happens pre-or-post projection is
        //     is irrelevant.
        //
        // Set receiveNode to null if the plan is not distributed or if
        // the distributed plan does not allow push-down of a limit.

        while (!(receiveNode instanceof ReceivePlanNode)) {

            // Limitation: can only push past some nodes (see above comment)
            if (!(receiveNode instanceof AggregatePlanNode) &&
                !(receiveNode instanceof OrderByPlanNode) &&
                !(receiveNode instanceof ProjectionPlanNode)) {
                receiveNode = null;
                break;
            }

            // Limitation: can only push past coordinating aggregation nodes
            if (receiveNode instanceof AggregatePlanNode &&
                !((AggregatePlanNode)receiveNode).m_isCoordinatingAggregator) {
                receiveNode = null;
                break;
            }

            // Traverse...
            if (receiveNode.getChildCount() == 0) {
                receiveNode = null;
                break;
            }

            // nothing that allows pushing past has multiple inputs
            assert(receiveNode.getChildCount() == 1);
            receiveNode = receiveNode.getChild(0);
        }
        return receiveNode;
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
            if (m_parsedSelect.displayColumns.size() > 1)
            {
                throw new PlanningErrorException("Multiple DISTINCT columns currently unsupported");
            }
            for (ParsedSelectStmt.ParsedColInfo col : m_parsedSelect.displayColumns) {
                // Distinct can in theory handle any expression now, but it's
                // untested so we'll balk on anything other than a TVE here
                // --izzy
                if (col.expression instanceof TupleValueExpression)
                {
                    root = addDistinctNode(root, col.expression);
                    // aggregate handlers are expected to produce the required projection.
                    // the other aggregates do this inherently but distinct may need a
                    // projection node.
                    root = addProjection(root);
                }
                else
                {
                    throw new PlanningErrorException("DISTINCT of an expression currently unsupported");
                }
            }
        }

        return root;
    }

    AbstractPlanNode addDistinctNode(AbstractPlanNode root,
                                     AbstractExpression expr)
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
}
