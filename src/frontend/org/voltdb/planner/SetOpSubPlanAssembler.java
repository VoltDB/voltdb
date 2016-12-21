/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.MergeReceivePlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.plannodes.SetOpPlanNode;
import org.voltdb.plannodes.SetOpReceivePlanNode;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SetOpType;

/**
 * For a Set Op plan, this class builds the plans for the children queries
 * and adds them to a final plan for the whole Set Op plan
 */
class SetOpSubPlanAssembler extends SubPlanAssembler {

    private final Cluster m_catalogCluster;
    private final PlanSelector m_planSelector;
    // Common partitioning across all the children
    private StatementPartitioning m_setOpPrtitioning = null;
    private String m_isContentDeterministic = null;

    /**
     *
     * @param catalogCluster Catalog info about the physical layout of the cluster.
     * @param db The catalog's Database object.
     * @param parsedStmt The Set Op parsed statement object describing the sql to execute.
     * @param partitioning Initial partitioning context.
     * @param planSelector
     */
    SetOpSubPlanAssembler(Cluster catalogCluster, Database db, AbstractParsedStmt parsedStmt, StatementPartitioning partitioning, PlanSelector planSelector)
    {
        super(db, parsedStmt, partitioning);
        m_catalogCluster = catalogCluster;
        m_planSelector = planSelector;
    }

    /**
     * Build best plans for the children queries and add them to a final Set Op plan
     */
    @Override
    AbstractPlanNode nextPlan() {
        assert(m_parsedStmt instanceof ParsedSetOpStmt);
        ParsedSetOpStmt parsedSetOpStmt = (ParsedSetOpStmt) m_parsedStmt;
        SetOpPlanNode setOpPlanNode = new SetOpPlanNode(parsedSetOpStmt.m_unionType);
        m_recentErrorMsg = null;

        ArrayList<CompiledPlan> childrenPlans = new ArrayList<>();

        Set<Integer> commonPartitionColumns = null;
        // Indicator that the SetOp node can be pushed down to the partition fragment
        boolean canPushSetOpDown = true;
        // Indicator that there must be two SetOp nodes - at the coordinator and partition fragments
        boolean canMPSetOp = true;

        // Build best plans for the children first
        int planId = 0;

        m_setOpPrtitioning = null;
        for (AbstractParsedStmt parsedChildStmt : parsedSetOpStmt.m_children) {
            StatementPartitioning partitioning = (StatementPartitioning) m_partitioning.clone();
            PlanSelector planSelector = (PlanSelector) m_planSelector.clone();
            planSelector.m_planId = planId;
            PlanAssembler assembler = new PlanAssembler(
                    m_catalogCluster, m_db, partitioning, planSelector);
            CompiledPlan bestChildPlan = assembler.getBestCostPlan(parsedChildStmt);
            // Reset the partitioning because it may change if the child itself a Set Op plan
            partitioning = assembler.getPartitioning();

            // make sure we got a winner
            if (bestChildPlan == null) {
                m_recentErrorMsg = assembler.getErrorMessage();
                if (m_recentErrorMsg == null) {
                    m_recentErrorMsg = "Unable to plan for statement. Error unknown.";
                }
                return null;
            }
            childrenPlans.add(bestChildPlan);
            // Remember the content non-determinism message for the
            // first non-deterministic children we find.
            if (m_isContentDeterministic != null) {
                m_isContentDeterministic = bestChildPlan.nondeterminismDetail();
            }

            // Make sure that next child's plans won't override current ones.
            planId = planSelector.m_planId;

            // Evaluate whether the SetOP node can be safely pushed down below the Send/Receive pair
            // in case of multi partitioned query. For this to happen all children  must
            // satisfy the following conditions:
            //  - each statement must be a MP statement with a trivial coordinator fragment
            //  - each statement output must contain at least one partitioning column from its distributed tables
            //  - all statements must agree on the position of at least one partitioning column
            //    in their respective output schemas
            // The above requirements guarantee that the given set op can be run disjointly on each
            // individual partition and the results can be simply aggregated at the coordinator
            // If the statements fail to find a consensus on the partitioning column position but do have
            // trivial coordinator fragments the plan must have two Set Op nodes for each fragment.
            if (canMPSetOp) {
                // Is statement MP and has a trivial  coordinator?
                canMPSetOp = partitioning.requiresTwoFragments() && hasTrivialCoordinator(bestChildPlan.rootPlanGraph);
            }
            // Can we push down the SetOp?
            if (canPushSetOpDown && canMPSetOp) {
                // Extract partition column(s) from the child
                Set<Integer> partitionColumns = extractPrationColumn(bestChildPlan.rootPlanGraph);
                if (commonPartitionColumns == null) {
                    commonPartitionColumns = partitionColumns;
                } else {
                    // Compare child and the common sets. Only retain entries that are present in both sets
                    commonPartitionColumns.retainAll(partitionColumns);
                }
                if (commonPartitionColumns.isEmpty()) {
                    // Partitions couldn't agree on a common position of their partitioning columns
                    // in the output schema
                    canPushSetOpDown = false;
                }
            } else {
                canPushSetOpDown = false;
            }

            // Decide whether child statements' partitioning is compatible.
            if (m_setOpPrtitioning == null) {
                m_setOpPrtitioning = partitioning;
                continue;
            }

            AbstractExpression statementPartitionExpression = partitioning.singlePartitioningExpression();
            if (m_setOpPrtitioning.requiresTwoFragments()) {
                if ((partitioning.requiresTwoFragments() && !(canPushSetOpDown || canMPSetOp))
                        || statementPartitionExpression != null) {
                    // If two child statements need to use a second fragment,
                    // the set op node must be pushed down to the partition fragments.
                    // Otherwise, the coordinator expects a single-table result from each partition.
                    // Also, currently the coordinator of a two-fragment plan is not allowed to
                    // target a particular partition, so neither can the union of the coordinator
                    // and a statement that wants to run single-partition.
                    throw new PlanningErrorException(
                            "Statements are too complex in set operation using multiple partitioned tables.");
                }
                // the new statement is apparently a replicated read and has no effect on partitioning
                continue;
            }
            AbstractExpression commonPartitionExpression = m_setOpPrtitioning.singlePartitioningExpression();
            if (commonPartitionExpression == null) {
                // the prior statement(s) were apparently replicated reads
                // and have no effect on partitioning
                m_setOpPrtitioning = partitioning;
                continue;
            }
            if (partitioning.requiresTwoFragments()) {
                // Again, currently the coordinator of a two-fragment plan is not allowed to
                // target a particular partition, so neither can the union of the coordinator
                // and a statement that wants to run single-partition.
                assert(!(canPushSetOpDown || canMPSetOp));
                throw new PlanningErrorException(
                        "Statements are too complex in set operation using multiple partitioned tables.");
            }
            if (statementPartitionExpression == null) {
                // the new statement is apparently a replicated read and has no effect on partitioning
                continue;
            }
            if ( ! commonPartitionExpression.equals(statementPartitionExpression)) {
                throw new PlanningErrorException(
                        "Statements use conflicting partitioned table filters in set operation or sub-query.");
            }
        }

        // need to reset plan id for the entire UNION
        m_planSelector.m_planId = planId;

        // Add and link children plans. Push down the SetOP if needed
        return buildSetOpPlan(setOpPlanNode, childrenPlans,  canPushSetOpDown, canMPSetOp);
    }

    /**
     * Return positions for all partitioning columns for a given plan from its output schema.
     *
     * @param rootNode A root node for a set op child query
     * @return Set<Integer>
     */
    private Set<Integer> extractPrationColumn(AbstractPlanNode rootNode) {
        Set<Integer> partitioningColumnSet = new HashSet<Integer>();

        NodeSchema outputSchema = null;
        AbstractPlanNode nodeWithOutputSchema = rootNode;
        // Find the first node that has not null output schema. It may be an inline projection or aggregation node
        // like in the case of "SELECT PART_COLUMN, COUNT(*) FROM T GROUP BY PART_COLUMN";
        while (outputSchema == null) {
            outputSchema = nodeWithOutputSchema.getOutputSchema();
            if (outputSchema == null) {
                // Try inline nodes if any
                for (AbstractPlanNode inlineNodeWithOutputSchema : nodeWithOutputSchema.getInlinePlanNodes().values()) {
                    outputSchema = inlineNodeWithOutputSchema.getOutputSchema();
                    if (outputSchema != null) {
                        break;
                    }
                }
                if (outputSchema == null) {
                    // If a node and its inline nodes doen't have an output schema there must be a child that provide the schema
                    assert (nodeWithOutputSchema.getChildCount() != 0);
                    nodeWithOutputSchema = nodeWithOutputSchema.getChild(0);
                }
            }
        }
        // There must be an output schema, right?
        assert (outputSchema != null);

        ArrayList<SchemaColumn> outputColumns = outputSchema.getColumns();

        ArrayList<AbstractScanPlanNode> scanNodes = rootNode.getScanNodeList();
        for (AbstractScanPlanNode scanNode : scanNodes) {
            StmtTableScan stmtScan = scanNode.getTableScan();
            List<SchemaColumn> scanPartitioningColumns = stmtScan.getPartitioningColumns();
            if (scanPartitioningColumns == null || scanPartitioningColumns.isEmpty()) {
                // Replicated scan
                continue;
            }
            // A table scan can have only one partitioning column
            // A subquery scan could have multiple partitioning columns but in this case
            // the hasTrivialCoordinator would have returned FALSE (not a trivial coordinator fragment
            // containing a scan node) and we shouldn't get there
            assert(scanPartitioningColumns.size() == 1);
            SchemaColumn scanPartitioningColumn = scanPartitioningColumns.get(0);
            // Ideally, the column differentiator should be used to identify the partitioning column
            // position. But it not always set if the schema is taken from an inline aggregate
            int outputColumnIdx = 0;
            for (SchemaColumn outputColumn : outputColumns) {
                if (outputColumn.compareNames(scanPartitioningColumn) == 0) {
                    partitioningColumnSet.add(outputColumnIdx);
                    // Keep going there since the partitioning column may appear multiple times in the output schema
                }
                ++outputColumnIdx;
            }
        }
        return partitioningColumnSet;
    }

    /**
     * Returns true if this plan tree is a MP tree and its coordinator fragment is trivial:
     *      <ProjectionPlanNode>
     *          ReceivePlanNode or MergeReceivePlanNode
     * @return
     */
    private boolean hasTrivialCoordinator(AbstractPlanNode planNode) {
        PlanNodeType planNodeType = planNode.getPlanNodeType();
        if (PlanNodeType.RECEIVE == planNodeType || PlanNodeType.MERGERECEIVE == planNodeType) {
            return true;
        } else if (PlanNodeType.PROJECTION == planNodeType) {
            assert(planNode.getChildCount() == 1);
            return hasTrivialCoordinator(planNode.getChild(0));
        }
        return false;
    }

    /**
     * Connect Set Op plan node with its children. Since the coordinator fragment will be trivial
     * (no schema changes), the Projection node above the Set Op one is not required
     *
     * @param setOpPlanNode Set Op Plan Node
     * @param childrenPlans individual children plans
     * @param needPushSetOpDown TRUE if the Set Op plan node needs to be pushed down
     * @param needTwoSetOps TRUE if two Set Op plan nodes are required
     *
     * @return The final combined plan
     */
    private AbstractPlanNode buildSetOpPlan(SetOpPlanNode setOpPlanNode, List<CompiledPlan> childrenPlans, boolean needPushSetOpDown, boolean needTwoSetOps) {
        AbstractPlanNode rootNode = setOpPlanNode;
        if (needPushSetOpDown) {
            // Add a Send/Receive pair on top of the current root node
            rootNode = SubPlanAssembler.addSendReceivePair(rootNode);
            setOpPlanNode = connectSetOpChildren(setOpPlanNode, childrenPlans);
        } else if (needTwoSetOps) {
            // Prepare partition fragment
            setOpPlanNode = connectSetOpChildren(setOpPlanNode, childrenPlans);

            if (SetOpType.UNION_ALL == setOpPlanNode.getSetOpType()) {
                // UNION ALL doesn't require any additional cross partition post processing
                // Simply add send/receive pair on top
                rootNode = SubPlanAssembler.addSendReceivePair(setOpPlanNode);
            } else {
                AbstractPlanNode sendNode = new SendPlanNode();
                sendNode.addAndLinkChild(setOpPlanNode);

                rootNode = new SetOpReceivePlanNode(setOpPlanNode.getSetOpType(), childrenPlans.size());
                rootNode.addAndLinkChild(sendNode);
                // EXCEPT (ALL) /INTERCEPT (ALL) Set ops require children results to be propagated
                // to the coordinator to do a cross partition set op. Since all children have a
                // compatible schema the same output table can be used to hold all the rows but
                // we need to add a tag column to be able to differentiate between original sources.
                //
                // The UNION at the coordinator level only needs the partition set op result.
                if (SetOpType.UNION != setOpPlanNode.getSetOpType()) {
                    // Set a flag to the partition SetOp node that it needs an additional tag column
                    // to track the origin of each row contributing to the final result se
                    setOpPlanNode.setNeedTagColumn(true);

                    // Need to add a projection node on top of the coordinator SetOp node to weed out the tag column.
                    assert(!childrenPlans.isEmpty());
                    AbstractPlanNode projection = buildProjection(childrenPlans.get(0).rootPlanGraph);
                    assert(projection != null);
                    ProjectionPlanNode coordinatorProjection = new ProjectionPlanNode(projection.getOutputSchema());
                    coordinatorProjection.addAndLinkChild(rootNode);
                    rootNode = coordinatorProjection;
                }
            }
        } else {
            // Single partition plan or Multi partition plan with a single distributed table
            // that does not require the Set Op node to be pushed down
            for (CompiledPlan selectPlan : childrenPlans) {
                rootNode.addAndLinkChild(selectPlan.rootPlanGraph);
            }
        }
        return rootNode;
    }

    StatementPartitioning getSetOpPartitioning() {
        return m_setOpPrtitioning;
    }

    String getIsContentDeterministic() {
        return m_isContentDeterministic;
    }

    /**
     * Connect the SetOP node with its children
     * @param setOpPlanNode
     * @param childrenPlans
     * @return
     */
    private SetOpPlanNode connectSetOpChildren(SetOpPlanNode setOpPlanNode, List<CompiledPlan> childrenPlans) {
        for (CompiledPlan selectPlan : childrenPlans) {
            AbstractPlanNode childPlan = selectPlan.rootPlanGraph;
            AbstractPlanNode childParent = setOpPlanNode;
            if (selectPlan.rootPlanGraph instanceof ProjectionPlanNode) {
                // Keep the child projection by adding it directly under the Set Op node
                setOpPlanNode.addAndLinkChild(selectPlan.rootPlanGraph);
                // Detach the rest of the child plan
                assert(selectPlan.rootPlanGraph.getChildCount() == 1);
                childPlan = selectPlan.rootPlanGraph.getChild(0);
                selectPlan.rootPlanGraph.clearChildren();
                // Reset child parent to be its own projection node
                childParent = selectPlan.rootPlanGraph;
            }
            // Remove child Send/Receive nodes
            assert(childPlan instanceof MergeReceivePlanNode || childPlan instanceof ReceivePlanNode);
            childPlan = childPlan.getChild(0).getChild(0);
            childPlan.clearParents();
            // Add simplified child plan to its parent
            childParent.addAndLinkChild(childPlan);
        }
        return setOpPlanNode;
    }

    /**
     * Build a projection node for the coordinator fragment in case of an extra TAG column was added
     * to the partition's schema for the EXCEPT/INTERSECT set ops to aid the cross partition set op
     * that has to be removed from the final output.
     * We can't simply copy the projection from the first (or any) child statement even it's guaranteed
     * to have the same column count and types because the child projection may have extra expression(s)
     * that is unique to this child only. For example,
     * SELECT ABS(F) FROM P1 EXCEPT SELECT F FROM P2;
     *
     * @param childRoot
     * @return Coordinator's projection node
     */
    AbstractPlanNode buildProjection(AbstractPlanNode childRoot) {
        AbstractPlanNode childProjecton = findTopProjection(childRoot);
        assert(childProjecton != null);
        assert(childProjecton.getOutputSchema() != null);

        ProjectionPlanNode setOpProjection = new ProjectionPlanNode();
        NodeSchema schema = new NodeSchema();
        for (SchemaColumn column : childProjecton.getOutputSchema().getColumns()) {
            TupleValueExpression tve = new TupleValueExpression(column.getTableName(), column.getTableAlias(),
                    column.getColumnName(), column.getColumnAlias());
            assert(column.getExpression() != null);
            tve.setValueSize(column.getExpression().getValueSize());
            tve.setValueType(column.getExpression().getValueType());
            SchemaColumn setOpColumn = new SchemaColumn(tve.getTableName(), tve.getTableAlias(),
                    tve.getColumnName(), tve.getColumnAlias(), tve);
            schema.addColumn(setOpColumn);
        }
        setOpProjection.setOutputSchemaWithoutClone(schema);
        return setOpProjection;
    }

    /**
     * Return the final projection for a given root node
     * @param node
     * @return
     */
    private AbstractPlanNode findTopProjection(AbstractPlanNode rootNode) {
        if (rootNode instanceof ProjectionPlanNode) {
            return rootNode;
        }
        AbstractPlanNode inlineProjection = rootNode.getInlinePlanNode(PlanNodeType.PROJECTION);
        if (inlineProjection != null) {
            return inlineProjection;
        }
        for (int i = 0; i < rootNode.getChildCount(); i++) {
            AbstractPlanNode childProjection = findTopProjection(rootNode.getChild(i));
            if (childProjection != null) {
                return childProjection;
            }
        }
        return null;
    }
}
