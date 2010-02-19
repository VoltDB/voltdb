/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;
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
import org.voltdb.expressions.TupleAddressExpression;
import org.voltdb.expressions.TupleValueExpression;
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
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
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

    private static final int MAX_LOCAL_ID = 1000000;
    private static boolean m_useGlobalIds = true;

    /**
     * Internal PlanNodeId counter. Note that this member is static, which means
     * all PlanNodes will have a unique id
     */
    private static int NEXT_PLAN_NODE_ID = 1;
    private static int NEXT_LOCAL_PLAN_NODE_ID = 1;

    /**
     * Dependency id counter. This is only used for connection send and receive
     * nodes in plan fragments
     */

    /** convenience pointer to the cluster object in the catalog */
    final Cluster m_catalogCluster;
    /** convenience pointer to the database object in the catalog */
    final Database m_catalogDb;

    /** Context object with planner-local information. */
    final PlannerContext m_context;

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
     *
     * @param catalogCluster
     *            Catalog info about the physical layout of the cluster.
     * @param catalogDb
     *            Catalog info about schema, metadata and procedures.
     */
    PlanAssembler(PlannerContext context, Cluster catalogCluster, Database catalogDb) {
        m_context = context;
        m_catalogCluster = catalogCluster;
        m_catalogDb = catalogDb;
        m_partitionCount = m_catalogCluster.getPartitions().size();
    }

    static void setUseGlobalIds(boolean useGlobalIds) {
        if (useGlobalIds) {
            m_useGlobalIds = true;
            NEXT_LOCAL_PLAN_NODE_ID = 1;
        } else {
            m_useGlobalIds = false;
        }
    }

    public static int getNextPlanNodeId() {
        assert ((NEXT_LOCAL_PLAN_NODE_ID + 1) <= MAX_LOCAL_ID);
        if (m_useGlobalIds)
            return NEXT_PLAN_NODE_ID++;
        else
            return NEXT_LOCAL_PLAN_NODE_ID++;
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
     * Return true if tableList includes at least one export-only table.
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
                    ti.getTable().getTypeName().equals(table.getTypeName()))
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
                "Illegal to read an export-only table.");
            }
            m_parsedSelect = (ParsedSelectStmt) parsedStmt;
            subAssembler =
                new SelectSubPlanAssembler(m_context, m_catalogDb,
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
                    "Illegal to update an export-only table.");
                }
                m_parsedUpdate = (ParsedUpdateStmt) parsedStmt;
                subAssembler =
                    new WriterSubPlanAssembler(m_context, m_catalogDb, parsedStmt, singlePartition, m_partitionCount);
            } else if (parsedStmt instanceof ParsedDeleteStmt) {
                if (tableListIncludesExportOnly(parsedStmt.tableList)) {
                    throw new RuntimeException(
                    "Illegal to delete from an export-only table.");
                }
                m_parsedDelete = (ParsedDeleteStmt) parsedStmt;
                subAssembler =
                    new WriterSubPlanAssembler(m_context, m_catalogDb, parsedStmt, singlePartition, m_partitionCount);
            } else
                throw new RuntimeException(
                        "Unknown subclass of AbstractParsedStmt.");
        }
    }

    ParsedSelectStmt.ParsedColInfo
    removeAggregation(ParsedSelectStmt.ParsedColInfo column)
    {
        ParsedSelectStmt.ParsedColInfo retval =
            new ParsedSelectStmt.ParsedColInfo();
        retval.alias = column.alias;
        retval.columnName = column.columnName;
        retval.tableName = column.tableName;
        retval.finalOutput = column.finalOutput;
        retval.ascending = column.ascending;
        retval.index = column.index;
        retval.orderBy = column.orderBy;
        retval.ascending = column.ascending;
        retval.groupBy = column.groupBy;

        if ((column.expression.getExpressionType() ==
                ExpressionType.AGGREGATE_AVG) ||
            (column.expression.getExpressionType() ==
                ExpressionType.AGGREGATE_COUNT) ||
            (column.expression.getExpressionType() ==
                ExpressionType.AGGREGATE_COUNT_STAR) ||
            (column.expression.getExpressionType() ==
                ExpressionType.AGGREGATE_MAX) ||
            (column.expression.getExpressionType() ==
                ExpressionType.AGGREGATE_MIN) ||
            (column.expression.getExpressionType() ==
                ExpressionType.AGGREGATE_SUM))
        {
            retval.expression = column.expression.getLeft();
        }
        else
        {
            retval.expression = column.expression;
        }

        return retval;
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

        return retval;
    }

    private void addColumns(CompiledPlan plan, ParsedSelectStmt stmt) {
        int index = 0;
        for (ParsedSelectStmt.ParsedColInfo col : stmt.displayColumns) {
            PlanColumn outcol = m_context.getPlanColumn(col.expression, col.alias);
            plan.columns.add(outcol.guid());
            index++;
        }
    }

    private void addParameters(CompiledPlan plan, AbstractParsedStmt stmt) {
        ParameterInfo outParam = null;
        for (ParameterInfo param : stmt.paramList) {
            outParam = new ParameterInfo();
            outParam.index = param.index;
            outParam.type = param.type;
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
         * The order, aggregation and expression operations placed
         * "on top" of this work should calculate correct output
         * column state as nodes are added. (That is,
         * the recursive updateOutputColumns() ideally wouldn't
         * have other callers.)
         */
        root.updateOutputColumns(m_catalogDb);

        root = handleAggregationOperators(root);
        root.updateOutputColumns(m_catalogDb);

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
            LimitPlanNode limit = new LimitPlanNode(m_context, getNextPlanNodeId());
            limit.setLimit((int) m_parsedSelect.limit);
            limit.setOffset((int) m_parsedSelect.offset);

            if (m_parsedSelect.offsetParameterId != -1) {
                ParameterInfo parameterInfo =
                    m_parsedSelect.paramsById.get(m_parsedSelect.offsetParameterId);
                limit.setOffsetParameterIndex(parameterInfo.index);
            }
            if (m_parsedSelect.limitParameterId != -1) {
                ParameterInfo parameterInfo =
                    m_parsedSelect.paramsById.get(m_parsedSelect.limitParameterId);
                limit.setLimitParameterIndex(parameterInfo.index);
            }
            limit.addAndLinkChild(root);
            root = limit;
        }

        SendPlanNode sendNode = new SendPlanNode(m_context, getNextPlanNodeId());

        // connect the nodes to build the graph
        sendNode.addAndLinkChild(root);

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
        DeletePlanNode deleteNode = new DeletePlanNode(m_context, getNextPlanNodeId());
        deleteNode.setTargetTableName(targetTable.getTypeName());

        ProjectionPlanNode projectionNode = new ProjectionPlanNode(m_context, getNextPlanNodeId());
        AbstractExpression addressExpr = new TupleAddressExpression();
        PlanColumn colInfo = m_context.getPlanColumn(addressExpr, "tuple_address");
        projectionNode.appendOutputColumn(colInfo);

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
            subSelectRoot.addInlinePlanNode(projectionNode);
            // connect the nodes to build the graph
            deleteNode.addAndLinkChild(subSelectRoot);

            return deleteNode;

        } else {
            // make sure the thing we have is a receive node which
            // indicates it's a multi-site plan
            assert (subSelectRoot instanceof ReceivePlanNode);

            //
            // put the delete node in the right place
            //

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

            AbstractPlanNode countNode = insertCountInDMLPlan(deleteNode);

            sendNode.addAndLinkChild(countNode);

            // fix the receive node's output columns
            recvNode.updateOutputColumns(m_catalogDb);
            /*
             * recvNode.getOutputColumnNames().clear();
             * recvNode.getOutputColumnSizes().clear();
             * recvNode.getOutputColumnTypes().clear(); for (OutputColumnInfo
             * oci : recvNode.m_outputColumns) {
             * recvNode.getOutputColumnNames().add(oci.name);
             * recvNode.getOutputColumnSizes().add(oci.size);
             * recvNode.getOutputColumnTypes().add(oci.type); }
             */

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

        UpdatePlanNode updateNode = new UpdatePlanNode(m_context, getNextPlanNodeId());
        updateNode.setTargetTableName(targetTable.getTypeName());
        // set this to false until proven otherwise
        updateNode.setUpdateIndexes(false);

        ProjectionPlanNode projectionNode = new ProjectionPlanNode(m_context, getNextPlanNodeId());
        TupleAddressExpression tae = new TupleAddressExpression();
        PlanColumn colInfo = m_context.getPlanColumn(tae, "tuple_address");
        projectionNode.appendOutputColumn(colInfo);

        // get the set of columns affected by indexes
        Set<String> affectedColumns = getIndexedColumnSetForTable(targetTable);

        // add the output columns we need
        int index = 1;
        for (Entry<Column, AbstractExpression> col : m_parsedUpdate.columns.entrySet()) {
            colInfo = m_context.getPlanColumn(col.getValue(), col.getKey().getTypeName());
            projectionNode.appendOutputColumn(colInfo);
            index++;

            // check if this column is an indexed column
            if (affectedColumns.contains(colInfo.displayName()))
                updateNode.setUpdateIndexes(true);
        }

        if (m_singlePartition == true) {

            // add the projection inline (TODO: this will break if more than one
            // layer is below this)
            assert(subSelectRoot instanceof AbstractScanPlanNode);
            subSelectRoot.addInlinePlanNode(projectionNode);

            // connect the nodes to build the graph
            updateNode.addAndLinkChild(subSelectRoot);

            return updateNode;
        } else {
            // make sure the thing we have is a receive node which
            // indicates it's a multi-site plan
            assert (subSelectRoot instanceof ReceivePlanNode);

            //
            // put the update node in the right place
            //

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

            AbstractPlanNode countNode = insertCountInDMLPlan(updateNode);

            sendNode.addAndLinkChild(countNode);

            // fix the receive node's output columns
            recvNode.updateOutputColumns(m_catalogDb);
            /*
             * recvNode.getOutputColumnNames().clear();
             * recvNode.getOutputColumnSizes().clear();
             * recvNode.getOutputColumnTypes().clear(); for (OutputColumnInfo
             * oci : recvNode.m_outputColumns) {
             * recvNode.getOutputColumnNames().add(oci.name);
             * recvNode.getOutputColumnSizes().add(oci.size);
             * recvNode.getOutputColumnTypes().add(oci.type); }
             */

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
        if (m_singlePartition && (targetTable.getIsreplicated())) {
            String msg =
                "Trying to insert into replicated table '"
                        + targetTable.getTypeName() + "'";
            msg += " in a single-partition procedure.";
            throw new PlanningErrorException(msg);
        }

        // the root of the insert plan is always an InsertPlanNode
        InsertPlanNode insertNode = new InsertPlanNode(m_context, getNextPlanNodeId());
        insertNode.setTargetTableName(targetTable.getTypeName());
        insertNode.setMultiPartition(m_singlePartition == false);

        // the materialize node creates a tuple to insert (which is frankly not
        // always optimal)
        MaterializePlanNode materializeNode =
            new MaterializePlanNode(m_context, getNextPlanNodeId());

        // get the ordered list of columns for the targettable using a helper
        // function they're not guaranteed to be in order in the catalog
        List<Column> columns =
            CatalogUtil
                    .getSortedCatalogItems(targetTable.getColumns(), "index");

        // for each column in the table in order...
        for (Column column : columns) {

            // get the expression for the column
            AbstractExpression expr = m_parsedInsert.columns.get(column);

            // if there's no expression, make sure the column is nullable
            if (expr == null) {
                // XXX HACK TEMPORARY WORKAROUND FOR TICKET #169
                // Rather than checking to see if this column is nullable we'll
                // just always throw the runtime exception if we get here.
                // I'm leaving the original code commented out here
                // --izzy 8-27-2009
                //// if it's not nullable we have a problem
                //if (column.getNullable() == false)
                //    throw new RuntimeException("Column " + column.getName()
                //            + " is not nullable.");
                //expr = new NullValueExpression();
                throw new PlanningErrorException("INSERT statements which only " +
                                                 "insert a subset of the columns " +
                                                 "are currently unsupported.");

                // rtb: if there is no expression, isn't it as simple as
                // making a constant value expression where
                // (constant == (default ? default : null).
            }
            // set the expression type to match the corresponding Column.
            // in reality, the expression will cast its resulting NValue to
            // the intermediate table's tuple; but, that tuple takes its
            // type from these expression types (for now). The tempTuple is
            // eventually tableTuple::copy()'d into the persistent table
            // and must match the persistent table's column type and size.
            // A little round-about, I know. Will get better.
            expr.setValueSize(column.getSize());
            expr.setValueType(VoltType.get((byte)column.getType()));

            // add column to the materialize node.
            PlanColumn colInfo = m_context.getPlanColumn(expr, column.getTypeName());
            materializeNode.appendOutputColumn(colInfo);
        }

        // connect the insert and the materialize nodes together
        insertNode.addAndLinkChild(materializeNode);
        AbstractPlanNode rootNode = insertNode;

        if (m_singlePartition == false) {
            // all sites to a scan -> send
            // root site has many recvs feeding into a union

            rootNode = insertCountInDMLPlan(rootNode);


            SendPlanNode sendNode = new SendPlanNode(m_context, getNextPlanNodeId());
            // this will make the child planfragment be sent to all partitions
            sendNode.isMultiPartition = true;
            sendNode.addAndLinkChild(rootNode);

            ReceivePlanNode recvNode = new ReceivePlanNode(m_context, getNextPlanNodeId());
            recvNode.addAndLinkChild(sendNode);
            rootNode = recvNode;

            // receive node requires the schema of its output table
            recvNode.updateOutputColumns(m_catalogDb);

            // add a count and send on top of the union
            rootNode = addSumAndSendToDMLNode(rootNode);
        }

        return rootNode;
    }

    // Add the result row count above the DML node in the plan fragment
    // that executes on all the sites in a multi-partition plan.  This
    // is the result that will be summed at the coordinator by the node
    // added by addSumAndSendToDMLNode()
    AbstractPlanNode insertCountInDMLPlan(AbstractPlanNode dmlRoot)
    {
        // update the output columns in case our caller hasn't
        dmlRoot.updateOutputColumns(m_catalogDb);
        // Add an aggregate count.
        AggregatePlanNode countNode = new AggregatePlanNode(m_context, getNextPlanNodeId());
        List<String> countColumnNames = new ArrayList<String>();
        List<Integer> countColumnGuids = new ArrayList<Integer>();
        List<ExpressionType> countColumnTypes = new ArrayList<ExpressionType>();
        List<Integer> countOutputColumns = countNode.getAggregateOutputColumns();

        // aggregate column name same as original dmlRoot name.
        int colGuid = dmlRoot.m_outputColumns.get(0); // offset 0.
        countColumnNames.add(m_context.get(colGuid).displayName());
        countColumnGuids.add(colGuid);
        countOutputColumns.add(0);
        countColumnTypes.add(ExpressionType.AGGREGATE_COUNT_STAR);
        countNode.setAggregateColumnNames(countColumnNames);
        countNode.setAggregateColumnGuids(countColumnGuids);
        countNode.setAggregateTypes(countColumnTypes);

        // The output column. Not really based on a TVE (it is really the
        // count expression represented by the count configured above). But
        // this is sufficient for now.
        TupleValueExpression tve = new TupleValueExpression();
        tve.setValueType(VoltType.BIGINT);
        tve.setValueSize(VoltType.BIGINT.getLengthInBytesForFixedTypes());
        tve.setColumnIndex(0);
        tve.setColumnName(m_context.get(colGuid).displayName());
        tve.setColumnAlias(m_context.get(colGuid).displayName());
        tve.setTableName("");
        PlanColumn countColInfo = m_context.getPlanColumn(tve, "modified_tuples");
        countNode.appendOutputColumn(countColInfo);
        countNode.addAndLinkChild(dmlRoot);
        return countNode;
    }

    AbstractPlanNode addSumAndSendToDMLNode(AbstractPlanNode dmlRoot) {
        // do some output column organizing...
        dmlRoot.updateOutputColumns(m_catalogDb);

        // create the nodes being pushed on top of dmlRoot.
        AggregatePlanNode countNode = new AggregatePlanNode(m_context, getNextPlanNodeId());
        SendPlanNode sendNode = new SendPlanNode(m_context, getNextPlanNodeId());

        // configure the count aggregate (sum) node to produce a single
        // output column containing the result of the sum.

        List<String> countColumnNames = new ArrayList<String>();
        List<Integer> countColumnGuids = new ArrayList<Integer>();
        List<ExpressionType> countColumnTypes = new ArrayList<ExpressionType>();
        List<Integer> countOutputColumns = countNode.getAggregateOutputColumns();

        // aggregate column name same as original dmlRoot name.
        int colGuid = dmlRoot.m_outputColumns.get(0); // offset 0.
        countColumnNames.add(m_context.get(colGuid).displayName());
        countColumnGuids.add(colGuid);
        countOutputColumns.add(0);
        countColumnTypes.add(ExpressionType.AGGREGATE_SUM);
        countNode.setAggregateColumnNames(countColumnNames);
        countNode.setAggregateColumnGuids(countColumnGuids);
        countNode.setAggregateTypes(countColumnTypes);

        // The output column. Not really based on a TVE (it is really the
        // count expression represented by the count configured above). But
        // this is sufficient for now.
        TupleValueExpression tve = new TupleValueExpression();
        tve.setValueType(VoltType.BIGINT);
        tve.setValueSize(VoltType.BIGINT.getLengthInBytesForFixedTypes());
        tve.setColumnIndex(0);
        tve.setColumnName(m_context.get(colGuid).displayName());
        tve.setColumnAlias(m_context.get(colGuid).displayName());
        tve.setTableName("");
        PlanColumn colInfo = m_context.getPlanColumn(tve, "modified_tuples");
        countNode.appendOutputColumn(colInfo);

        // connect the nodes to build the graph
        countNode.addAndLinkChild(dmlRoot);
        sendNode.addAndLinkChild(countNode);

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
        PlanColumn colInfo = null;

        // The rootNode must have a correct output column set.
        rootNode.updateOutputColumns(m_catalogDb);

        ProjectionPlanNode projectionNode =
            new ProjectionPlanNode(m_context, PlanAssembler.getNextPlanNodeId());

        // The input to this projection MUST include all the columns needed
        // to satisfy any TupleValueExpression in the parsed select statement's
        // output expressions.
        //
        // For each parsed select statement output column, create a new PlanColumn
        // cloning the expression. Walk the clone and configure each TVE with
        // the offset into the input column array.
        for (ParsedSelectStmt.ParsedColInfo outputCol : m_parsedSelect.displayColumns) {
            assert(outputCol.expression != null);
            outputCol = removeAggregation(outputCol);
            try {
                AbstractExpression expressionWithRealOffsets =
                    (AbstractExpression) outputCol.expression.clone();
                calculateTupleValueColumnIndexes(expressionWithRealOffsets, rootNode.m_outputColumns);
                colInfo = m_context.getPlanColumn(expressionWithRealOffsets, outputCol.alias);
                projectionNode.appendOutputColumn(colInfo);
            } catch (CloneNotSupportedException ex) {
                throw new PlanningErrorException(ex.getMessage());
            }
        }

        // if the projection can be done inline...
        if (rootNode instanceof AbstractScanPlanNode) {
            rootNode.addInlinePlanNode(projectionNode);
            return rootNode;
        } else {
            projectionNode.addAndLinkChild(rootNode);
            return projectionNode;
        }
    }

    /**
     * Walk expression and calculate the right columnIndex (offset)
     * into sourceColumns for each tupleValueExpression.
     * @param expression
     * @param sourceColumns
     */
    private void calculateTupleValueColumnIndexes(
            AbstractExpression expression,
            ArrayList<Integer> sourceColumns)
    {
        Stack<AbstractExpression> stack = new Stack<AbstractExpression>();
        AbstractExpression currExp = expression;
        while (currExp != null) {

            // found a TVE - calculate its offset into the sourceColumns
            if (currExp instanceof TupleValueExpression) {
                TupleValueExpression tve = (TupleValueExpression)currExp;
                boolean found = false;
                int offset = 0;
                for (Integer colguid : sourceColumns) {
                    PlanColumn plancol = m_context.get(colguid);
                    /* System.out.printf("Expression: %s/%s. Candidate column: %s/%s\n",
                            tve.getColumnAlias(), tve.getTableName(),
                            plancol.originColumnName(), plancol.originTableName()); */
                    if (plancol.originColumnName().equals(tve.getColumnName()) &&
                        plancol.originTableName().equals(tve.getTableName()))
                    {
                        tve.setColumnIndex(offset);
                        found = true;
                        break;
                    }
                    ++offset;
                }
                if (!found) {
                    // rtb: would like to throw here but doing so breaks sqlcoverage suite.
                    // for now - make this error obvious at least.
                    System.out.println("PLANNER ERROR: could not match tve column alias");
                    System.out.println(getSQLText());
                    // throw new RuntimeException("Could not match TVE column alias.");
                }
            }

            // save rhs. process lhs. when lhs is leaf, process a rhs.
            if (currExp.getRight() != null) {
                stack.push(currExp.getRight());
            }
            currExp = currExp.getLeft();
            if (currExp == null) {
                if (!stack.empty())
                    currExp = stack.pop();
            }
        }
    }

    AbstractPlanNode addOrderBy(AbstractPlanNode root) {
        assert (m_parsedSelect != null);

        OrderByPlanNode orderByNode = new OrderByPlanNode(m_context, getNextPlanNodeId());
        for (ParsedSelectStmt.ParsedColInfo col : m_parsedSelect.orderColumns) {
            orderByNode.getSortColumnNames().add(col.alias);
            orderByNode.getSortColumns().add(col.index);
            orderByNode.getSortDirections()
                    .add(
                         col.ascending ? SortDirectionType.ASC
                                      : SortDirectionType.DESC);
            PlanColumn orderByCol =
                root.findMatchingOutputColumn(col.tableName, col.columnName,
                                              col.alias);
            orderByNode.getSortColumnGuids().add(orderByCol.guid());
        }
        // connect the nodes to build the graph
        orderByNode.addAndLinkChild(root);
        orderByNode.updateOutputColumns(m_catalogDb);
        return orderByNode;
    }

    AbstractPlanNode addOffsetAndLimit(AbstractPlanNode root) {
        return null;
    }

    AbstractPlanNode handleAggregationOperators(AbstractPlanNode root) {
        boolean containsAggregateExpression = false;
        HashAggregatePlanNode aggNode = null;

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

        // "Select A from T group by A" is grouped but has no aggregate operator expressions
        // Catch that case by checking the grouped flag. Probably the OutputColumn iteration
        // above is unnecessary?

        if (m_parsedSelect.grouped)
            containsAggregateExpression = true;

        if (containsAggregateExpression) {
            aggNode = new HashAggregatePlanNode(m_context, getNextPlanNodeId());

            for (ParsedSelectStmt.ParsedColInfo col : m_parsedSelect.groupByColumns) {
                aggNode.getGroupByColumns().add(col.index);
                aggNode.getGroupByColumnNames().add(col.alias);
                PlanColumn groupByColumn =
                    root.findMatchingOutputColumn(col.tableName, col.columnName,
                                                  col.alias);
                aggNode.appendGroupByColumn(groupByColumn);
            }

            int outputColumnIndex = 0;
            for (ParsedSelectStmt.ParsedColInfo col : m_parsedSelect.displayColumns) {

                AbstractExpression rootExpr = col.expression;
                ExpressionType agg_expression_type = rootExpr.getExpressionType();
                if (rootExpr.getExpressionType() == ExpressionType.AGGREGATE_SUM ||
                    rootExpr.getExpressionType() == ExpressionType.AGGREGATE_MIN ||
                    rootExpr.getExpressionType() == ExpressionType.AGGREGATE_MAX ||
                    rootExpr.getExpressionType() == ExpressionType.AGGREGATE_AVG ||
                    rootExpr.getExpressionType() == ExpressionType.AGGREGATE_COUNT ||
                    rootExpr.getExpressionType() == ExpressionType.AGGREGATE_COUNT_STAR)
                {
                    PlanColumn aggregateColumn = null;
                    if (rootExpr.getLeft() instanceof TupleValueExpression)
                    {
                        TupleValueExpression nested =
                            (TupleValueExpression) rootExpr.getLeft();

                        if (((AggregateExpression)rootExpr).m_distinct) {
                            root = addDistinctNode(root, nested);
                        }

                        aggregateColumn =
                            root.findMatchingOutputColumn(nested.getTableName(),
                                                          nested.getColumnName(),
                                                          nested.getColumnAlias());
                    }
                    // count(*) hack.  we're not getting AGGREGATE_COUNT_STAR
                    // expression types from the parsing, so we have
                    // to detect the null inner expression case and do the
                    // switcharoo ourselves.
                    else if (rootExpr.getExpressionType() == ExpressionType.AGGREGATE_COUNT &&
                             rootExpr.getLeft() == null)
                    {
                        aggregateColumn =
                            m_context.get(root.m_outputColumns.get(0));
                        agg_expression_type = ExpressionType.AGGREGATE_COUNT_STAR;
                    }
                    else
                    {
                        throw new PlanningErrorException("Expressions in aggregates currently unsupported");
                    }

                    aggNode.getAggregateColumnGuids().add(aggregateColumn.guid());
                    aggNode.getAggregateColumnNames().add(aggregateColumn.displayName());
                    aggNode.getAggregateTypes().add(agg_expression_type);
                    PlanColumn colInfo = m_context.getPlanColumn(rootExpr, col.alias);
                    aggNode.appendOutputColumn(colInfo);
                    aggNode.getAggregateOutputColumns().add(outputColumnIndex);
                }
                else
                {
                    /*
                     * These columns are the pass through columns that are not being
                     * aggregated on. These are the ones from the SELECT list. They
                     * MUST already exist in the child node's output. Find them and
                     * add them to the aggregate's output.
                     */
                    PlanColumn passThruColumn =
                        root.findMatchingOutputColumn(col.tableName,
                                                      col.columnName,
                                                      col.alias);
                    aggNode.appendOutputColumn(passThruColumn);
                }

                outputColumnIndex++;
            }

            aggNode.addAndLinkChild(root);
            root = aggNode;
        }

        // handle select distinct a from t - which is planned as an aggregate but
        // doesn't trigger the above aggregate conditions as it is neither grouped
        // nor does it have aggregate expressions
        if (aggNode == null && m_parsedSelect.distinct) {
            // We currently can't handle DISTINCT of multiple columns.
            // Throw a planner error if this is attempted.
            if (m_parsedSelect.displayColumns.size() > 1)
            {
                throw new PlanningErrorException("Multiple DISTINCT columns currently unsupported");
            }
            for (ParsedSelectStmt.ParsedColInfo col : m_parsedSelect.displayColumns) {
                if (col.expression instanceof TupleValueExpression)
                {
                    TupleValueExpression colexpr = (TupleValueExpression)(col.expression);
                    root = addDistinctNode(root, colexpr);

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
                                     TupleValueExpression expr)
    {
        DistinctPlanNode distinctNode = new DistinctPlanNode(m_context, getNextPlanNodeId());
        distinctNode.setDistinctColumnName(expr.getColumnAlias());

        PlanColumn distinctColumn =
            root.findMatchingOutputColumn(expr.getTableName(),
                                          expr.getColumnName(),
                                          expr.getColumnAlias());
        distinctNode.setDistinctColumnGuid(distinctColumn.guid());

        distinctNode.addAndLinkChild(root);
        distinctNode.updateOutputColumns(m_catalogDb);
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
