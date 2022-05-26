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

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltcore.utils.Pair;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.exceptions.ValidationError;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AbstractSubqueryExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannerv2.utils.AbstractPlanNodeVisitor;
import org.voltdb.planner.PlanStatistics;
import org.voltdb.planner.StatsField;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

public abstract class AbstractPlanNode implements JSONString, Comparable<AbstractPlanNode> {

    /**
     * Internal PlanNodeId counter. Note that this member is static, which means
     * all PlanNodes will have a unique id
     */
    private static int NEXT_PLAN_NODE_ID = 1;

    // Keep this flag turned off in production or when testing user-accessible EXPLAIN output or when
    // using EXPLAIN output to validate plans.
    protected static boolean m_verboseExplainForDebugging = false; // CODE REVIEWER! KEEP false on master!
    public static boolean enableVerboseExplainForDebugging() {
        boolean was = m_verboseExplainForDebugging;
        m_verboseExplainForDebugging = true;
        return was;
    }
    public static boolean disableVerboseExplainForDebugging() {
        boolean was = m_verboseExplainForDebugging;
        m_verboseExplainForDebugging = false;
        return was;
    }
    public static void restoreVerboseExplainForDebugging(boolean was) {
        m_verboseExplainForDebugging = was;
    }

    /*
     * IDs only need to be unique for a single plan.
     * Reset between plans
     */
    public static void resetPlanNodeIds() {
        NEXT_PLAN_NODE_ID = 1;
    }

    public enum Members {
        ID,
        PLAN_NODE_TYPE,
        INLINE_NODES,
        CHILDREN_IDS,
        PARENT_IDS,
        OUTPUT_SCHEMA;
    }

    protected int m_id = -1;
    protected List<AbstractPlanNode> m_children = new ArrayList<>();
    protected List<AbstractPlanNode> m_parents = new ArrayList<>();
    protected Set<AbstractPlanNode> m_dominators = new HashSet<>();

    // TODO: planner accesses this data directly. Should be protected.
    protected List<ScalarValueHints> m_outputColumnHints = new ArrayList<>();
    protected long m_estimatedOutputTupleCount = 0;
    protected long m_estimatedProcessedTupleCount = 0;
    protected boolean m_hasComputedEstimates = false;

    // The output schema for this node
    protected boolean m_hasSignificantOutputSchema;
    protected NodeSchema m_outputSchema;

    /**
     * Some PlanNodes can take advantage of inline PlanNodes to perform
     * certain additional tasks while performing their main operation, rather than
     * having to re-read tuples from intermediate results
     */
    protected Map<PlanNodeType, AbstractPlanNode> m_inlineNodes = new LinkedHashMap<>();
    protected boolean m_isInline = false;

    /**
     * The textual explanation of why the plan may fail to have a deterministic result or effect when replayed.
     */
    protected String  m_nondeterminismDetail = "the query result does not guarantee a consistent ordering";

    /**
     * Instantiates a new plan node.
     */
    protected AbstractPlanNode() {
        m_id = NEXT_PLAN_NODE_ID++;
    }

    /**
     * Test if current node, or any children node, has the given property recursively
     * @param pred predicate on a plan node
     * @return if current node or any children node has the given property
     */
    public boolean anyChild(Predicate<AbstractPlanNode> pred) {
        return pred.test(this) || m_children.stream().anyMatch(child -> child.anyChild(pred));
    }

    /**
     * Test if current node as well as all children nodes, have the given property recursively
     * @param pred predicate on a plan node
     * @return if current node and all children nodes has the given property
     */
    public boolean allChild(Predicate<AbstractPlanNode> pred) {
        return anyChild(pred.negate());
    }

    public int resetPlanNodeIds(int nextId) {
        nextId = overrideId(nextId);
        for (AbstractPlanNode inNode : getInlinePlanNodes().values()) {
            // Inline nodes also need their ids to be overridden to make sure
            // the subquery node ids are also globally unique
            nextId = inNode.resetPlanNodeIds(nextId);
        }

        for (int i = 0; i < getChildCount(); i++) {
            AbstractPlanNode child = getChild(i);
            assert(child != null);
            nextId = child.resetPlanNodeIds(nextId);
        }

        return nextId;
    }

    public int overrideId(int newId) {
        m_id = newId++;
        // Override subqueries ids
        Collection<AbstractExpression> subqueries = findAllSubquerySubexpressions();
        for (AbstractExpression expr : subqueries) {
            assert(expr instanceof AbstractSubqueryExpression);
            AbstractSubqueryExpression subquery = (AbstractSubqueryExpression) expr;
            // overrideSubqueryNodeIds(newId) will get an NPE if the subquery
            // has not been planned, presumably the effect of hitting a bug
            // earlier in the planner. If that happens again, it MAY be useful
            // to preempt those cases here and single-step through a replay of
            // findAllSubquerySubexpressions. Determining where in the parent
            // plan this subquery expression was found MAY provide a clue
            // as to why the subquery was not planned. It has helped before.
            //REDO to debug*/ if (subquery instanceof SelectSubqueryExpression) {
            //REDO to debug*/     CompiledPlan subqueryPlan = ((SelectSubqueryExpression)subquery)
            //REDO to debug*/             .getSubqueryScan().getBestCostPlan();
            //REDO to debug*/     if (subqueryPlan == null) {
            //REDO to debug*/         findAllSubquerySubexpressions();
            //REDO to debug*/     }
            //REDO to debug*/ }
            newId = subquery.overrideSubqueryNodeIds(newId);
        }
        return newId;
    }

    /**
     * Create a PlanNode that clones the configuration information but
     * is not inserted in the plan graph and has a unique plan node id.
     */
    protected void produceCopyForTransformation(AbstractPlanNode copy) {
        copy.m_outputSchema = m_outputSchema;
        copy.m_hasSignificantOutputSchema = m_hasSignificantOutputSchema;
        copy.m_outputColumnHints = m_outputColumnHints;
        copy.m_estimatedOutputTupleCount = m_estimatedOutputTupleCount;
        copy.m_estimatedProcessedTupleCount = m_estimatedProcessedTupleCount;

        // clone is not yet implemented for every node.
        assert(m_inlineNodes.size() == 0);
        assert(! m_isInline);

        // the api requires the copy is not (yet) connected
        assert (copy.m_parents.size() == 0);
        assert (copy.m_children.size() == 0);
    }

    public abstract PlanNodeType getPlanNodeType();

    /**
     * Generate the output schema for this node based on the
     * output schemas of its children.  The generated schema consists of
     * the complete set of columns but is not yet ordered.
     *
     * Right now it's best to call this on every node after it gets added
     * and linked to the top of the current plan graph.
     * FIXME: "it's best to call this" means "to be on the paranoid safe side".
     * It used to be that there was a hacky dependency in some non-critical aggregate code,
     * so it would crash if generateOutputSchema had not been run earlier on its subtree.
     * Historically, there may have been other dependencies like this, too,
     * but they are mostly gone or otherwise completely avoidable.
     * This means that one definitive depth-first recursive call that combines the effects
     * of resolveColumnIndexes and then generateOutputSchema should suffice,
     * if applied to a complete plan tree just before it gets fragmentized.
     * The newest twist is that most of this repeated effort goes into generating
     * redundant pass-thorugh structures that get ignored by the serializer.
     *
     * Many nodes will need to override this method in order to take whatever
     * action is appropriate (so, joins will combine two schemas, projections
     * will already have schemas defined and do nothing, etc).
     * They should set m_hasSignificantOutputSchema to true so that the serialization knows
     * not to ignore their work.
     *
     * @param db  A reference to the Database object from the catalog.
     */
    public void generateOutputSchema(Database db) {
        // default behavior: just copy the input schema
        // to the output schema
        assert(m_children.size() == 1);
        AbstractPlanNode childNode = m_children.get(0);
        childNode.generateOutputSchema(db);
        // Replace the expressions in our children's columns with TVEs.  When
        // we resolve the indexes in these TVEs they will point back at the
        // correct input column, which we are assuming that the child node
        // has filled in with whatever expression was here before the replacement.
        // Output schemas defined using this standard algorithm
        // are just cached "fillers" that satisfy the legacy
        // resolveColumnIndexes/generateOutputSchema/getOutputSchema protocol
        // until it can be fixed up  -- see the FIXME comment on generateOutputSchema.
        m_hasSignificantOutputSchema = false;
        m_outputSchema = childNode.getOutputSchema().copyAndReplaceWithTVE();
    }

    /**
     * Recursively iterate through the plan and resolve the column_idx value for
     * every TupleValueExpression in every AbstractExpression in every PlanNode.
     * Few enough common cases so we force every AbstractPlanNode subclass to
     * implement this.  After index resolution, this method also sorts
     * the columns in the output schema appropriately, depending upon what
     * sort of node it is, so that its parent will be able to resolve
     * its indexes successfully.
     *
     * Should get called on the plan graph after any optimizations but before
     * the plan gets fragmented.
     * FIXME: This needs to be reworked with generateOutputSchema to eliminate redundancies.
     */
    public abstract void resolveColumnIndexes();

    protected void resolveSubqueryColumnIndexes() {
        // Possible subquery expressions
        Collection<AbstractExpression> exprs = findAllSubquerySubexpressions();
        for (AbstractExpression expr: exprs) {
            ((AbstractSubqueryExpression) expr).resolveColumnIndexes();
        }
    }

    public void validate() {
        /*if (m_outputSchema.isEmpty()) {
            throw new RuntimeException("AbstractPlanNode's output schema is empty: " + toString());
        }*/
        //
        // Make sure our children have us listed as their parents
        //
        for (AbstractPlanNode child : m_children) {
            if (!child.m_parents.contains(this)) {
                throw new ValidationError(
                        "The child PlanNode '%s' does not have its parent PlanNode '%s' in its parents list",
                        child.toString(), toString());
            }
            child.validate();
        }
        //
        // Inline PlanNodes
        //
        for (AbstractPlanNode node : m_inlineNodes.values()) {
            //
            // Make sure that we're not attached to some kind of tree somewhere...
            //
            if (!node.m_children.isEmpty()) {
                throw new ValidationError("The inline PlanNode '%s' has children inside of PlanNode '%s'",
                        node, this);
            } else if (!node.m_parents.isEmpty()) {
                throw new ValidationError("The inline PlanNode '%s' has parents inside of PlanNode '%s'",
                        node, this);
            } else if (!node.isInline()) {
                throw new ValidationError("The inline PlanNode '%s' was not marked as inline for PlanNode '%s'",
                        node, this);
            } else if (!node.getInlinePlanNodes().isEmpty()) {
                // NOTE: we support recursive inline nodes
                //throw new RuntimeException("ERROR: The inline PlanNode '" + node + "' has its own inline PlanNodes inside of PlanNode '" + this + "'");
            }
            node.validate();
        }
    }

    public boolean hasReplicatedResult() {
        Map<String, StmtTargetTableScan> tablesRead = new TreeMap<>();
        getTablesAndIndexes(tablesRead, null);
        return tablesRead.values().stream().allMatch(StmtTableScan::getIsReplicated);
    }

    /**
     * Recursively build sets of read tables read and index names used.
     *
     * @param tablesRead Set of table aliases read potentially added to at each recursive level.
     * @param indexes Set of index names used in the plan tree
     * Only the current fragment is of interest.
     */
    public void getTablesAndIndexes(Map<String, StmtTargetTableScan> tablesRead, Collection<String> indexes) {
        m_inlineNodes.values().forEach(node -> node.getTablesAndIndexes(tablesRead, indexes));
        m_children.forEach(node -> node.getTablesAndIndexes(tablesRead, indexes));
        getTablesAndIndexesFromSubqueries(tablesRead, indexes);
    }

    /**
     * Collect read tables read and index names used in the current node subquery expressions.
     *
     * @param tablesRead Set of table aliases read potentially added to at each recursive level.
     * @param indexes Set of index names used in the plan tree
     * Only the current node is of interest.
     */
    protected void getTablesAndIndexesFromSubqueries(Map<String, StmtTargetTableScan> tablesRead,
            Collection<String> indexes) {
        for(AbstractExpression expr : findAllSubquerySubexpressions()) {
            assert(expr instanceof AbstractSubqueryExpression);
            AbstractSubqueryExpression subquery = (AbstractSubqueryExpression) expr;
            AbstractPlanNode subqueryNode = subquery.getSubqueryNode();
            assert(subqueryNode != null);
            subqueryNode.getTablesAndIndexes(tablesRead, indexes);
        }
    }

    /**
     * Recursively find the target table name for a DML statement.
     * The name will be attached to the AbstractOperationNode child
     * of a Send Node, in all cases, so the "recursion" can be very limited.
     * Most plan nodes can quickly stub out this recursion and return null.
     * @return
     */
    @SuppressWarnings("static-method")
    public String getUpdatedTable() {
        return null;
    }

    /**
     * Does the (sub)plan guarantee an identical result/effect when "replayed"
     * against the same database state, such as during replication or CL recovery.
     * @return
     */
    public boolean isOrderDeterministic() {
        // Leaf nodes need to re-implement this test.
        assert(m_children != null);
        for (AbstractPlanNode child : m_children) {
            if (! child.isOrderDeterministic()) {
                m_nondeterminismDetail = child.m_nondeterminismDetail;
                return false;
            }
        }
        return true;
    }

    /**
     * Does the plan guarantee a result sorted according to the required sort order.
     * The default implementation delegates the question to its child if there is only one child.
     *
     *@param sortExpressions list of ordering columns expressions
     *@param sortDirections list of corresponding sort orders
     *
     * @return TRUE if the node's output table is ordered. FALSE otherwise
     */
    public boolean isOutputOrdered (List<AbstractExpression> sortExpressions, List<SortDirectionType> sortDirections) {
        assert(sortExpressions.size() == sortDirections.size());
        return m_children.size() == 1 && m_children.get(0).isOutputOrdered(sortExpressions, sortDirections);
    }

    /**
     * Accessor for description of plan non-determinism.
     * @return the field
     */
    public String nondeterminismDetail() {
        return m_nondeterminismDetail;
    }

    @Override
    public final String toString() {
        return getPlanNodeType() + "[" + m_id + "]";
    }

    /**
     * Called to compute cost estimates and statistics on a plan graph. Computing of the costs
     * should be idempotent, but updating the PlanStatistics instance isn't, so this should
     * be called once per finished graph, and once per PlanStatistics instance.
     * TODO(XIN): It takes at least 14% planner CPU. Optimize it.
     */
    public final void computeEstimatesRecursively(PlanStatistics stats,
                                                  DatabaseEstimates estimates,
                                                  ScalarValueHints[] paramHints) {
        assert(stats != null);

        m_outputColumnHints.clear();
        m_estimatedOutputTupleCount = 0;

        // recursively compute and collect stats from children
        long childOutputTupleCountEstimate = 0;
        for (AbstractPlanNode child : m_children) {
            child.computeEstimatesRecursively(stats, estimates, paramHints);
            m_outputColumnHints.addAll(child.m_outputColumnHints);
            childOutputTupleCountEstimate += child.m_estimatedOutputTupleCount;
        }

        // make sure any inlined scans (for NLIJ mostly) are costed as well
        for (Entry<PlanNodeType, AbstractPlanNode> entry : m_inlineNodes.entrySet()) {
            AbstractPlanNode inlineNode = entry.getValue();
            if (inlineNode instanceof AbstractScanPlanNode) {
                inlineNode.computeCostEstimates(0, estimates, paramHints);
            }
        }

        computeCostEstimates(childOutputTupleCountEstimate, estimates, paramHints);
        stats.incrementStatistic(0, StatsField.TUPLES_READ, m_estimatedProcessedTupleCount);
    }

    /**
     * Given the number of tuples expected as input to this node, compute an estimate
     * of the number of tuples read/processed and the number of tuples output.
     * This will be called by
     * {@see AbstractPlanNode#computeEstimatesRecursively(PlanStatistics, Cluster, Database, DatabaseEstimates, ScalarValueHints[])}.
     */
    protected void computeCostEstimates(long childOutputTupleCountEstimate,
                                        DatabaseEstimates estimates,
                                        ScalarValueHints[] paramHints) {
        m_estimatedOutputTupleCount = childOutputTupleCountEstimate;
        m_estimatedProcessedTupleCount = childOutputTupleCountEstimate;
    }

    public long getEstimatedOutputTupleCount() {
        return m_estimatedOutputTupleCount;
    }

    public long getEstimatedProcessedTupleCount() {
        return m_estimatedProcessedTupleCount;
    }

    /**
     * Gets the id.
     *
     * @return the id
     */
    public Integer getPlanNodeId() {
        return m_id;
    }

    /**
     * Get this PlanNode's output schema
     * FIXME: This needs to be reworked with generateOutputSchema to eliminate redundancies.
     * In short, if generateOutputSchema was called definitively ONCE and returned the child's
     * effective outputSchema to its parent -- possibly without even caching it as m_outputSchema,
     * m_outputSchema could be used to cache only significant non-redundant output schemas.
     * For now, the m_hasSignificantOutputSchema flag is checked separately to determine whether
     * m_outputSchema is worth looking at.
     * @return the NodeSchema which represents this node's output schema
     */
    public NodeSchema getOutputSchema()
    {
        return m_outputSchema;
    }

    /**
     * If the output schema of this node is a cheap copy of
     * some other node's schema, and we decide to change the
     * output true output schema, we can change this to the
     * updated schema.
     *
     * @param childSchema
     */
    public void setOutputSchema(NodeSchema childSchema) {
        assert( ! m_hasSignificantOutputSchema);
        m_outputSchema = childSchema;
    }

    /**
     * Find the true output schema.  This may be in some child
     * node.  This seems to be the search order when constructing
     * a plan node in the EE.
     *
     * There are several cases.
     * 1.) If the child has an output schema, and if it is
     *     not a copy of one of its children's schemas,
     *     that's the one we want.  We know it's a copy
     *     if m_hasSignificantOutputSchema is false.
     * 2.) If the child has no significant output schema but it
     *     has an inline projection node, then
     *     a.) If it does <em>not</em> have an inline insert
     *         node then the output schema of the child is
     *         the output schema of the inline projection node.
     *     b.) If the output schema has an inline insert node
     *         then the output schema is the usual DML output
     *         schema, which will be the schema of the inline
     *         insert node.  I don't think we will ever see.
     *         this case in this function.  This function is
     *         only called from the microoptimizer to remove
     *         projection nodes.  So we don't see a projection
     *         node on top of a node with an inlined insert node.
     *  3.) Otherwise, the output schema is the output schema
     *      of the child's first child.  We should be able to
     *      follow the first children until we get something
     *      usable.
     *
     * Just for the record, if the child node has an inline
     * insert and a projection node, the projection node's
     * output schema is the schema of the tuples we will be
     * inserting into the target table.  The output schema of
     * the child node will be the output schema of the insert
     * node, which will be the usual DML schema.  This has one
     * long integer column counting the number of rows inserted.
     *
     * @param resetBack If this is true, we walk back the
     *                  chain of parent plan nodes, updating
     *                  the output schema in ancestor nodes
     *                  with the true output schema.  If we
     *                  somehow change the true output schema
     *                  we want to be able to change all the
     *                  ones which are copies of the true
     *                  output schema.
     * @return The true output schema.  This will never return null.
     */
    public final NodeSchema getTrueOutputSchema(boolean resetBack) throws PlanningErrorException {
        AbstractPlanNode child;
        NodeSchema answer = null;
        //
        // Note: This code is translated from the C++ code in
        //       AbstractPlanNode::getOutputSchema.  It's considerably
        //       different there, but I think this has the corner
        //       cases covered correctly.
        for (child = this;
                child != null;
                child = (child.getChildCount() == 0) ? null : child.getChild(0)) {
            NodeSchema childSchema;
            if (child.m_hasSignificantOutputSchema) {
                childSchema = child.getOutputSchema();
                assert(childSchema != null);
                answer = childSchema;
                break;
            }
            AbstractPlanNode childProj = child.getInlinePlanNode(PlanNodeType.PROJECTION);
            if (childProj != null) {
                AbstractPlanNode schemaSrc;
                AbstractPlanNode inlineInsertNode = childProj.getInlinePlanNode(PlanNodeType.INSERT);
                if (inlineInsertNode != null) {
                    schemaSrc = inlineInsertNode;
                } else {
                    schemaSrc = childProj;
                }
                childSchema = schemaSrc.getOutputSchema();
                if (childSchema != null) {
                    answer = childSchema;
                    break;
                }
            }
        }
        if (child == null) {
            // We've gone to the end of the plan.  This is a
            // failure in the EE.
            assert(false);
            throw new PlanningErrorException("AbstractPlanNode with no true output schema.  Please notify VoltDB Support.");
        }
        // Trace back the chain of parents and reset the
        // output schemas of the parent.  These will all be
        // exactly the same.  Note that the source of the
        // schema may be an inline plan node.  So we need
        // to set the child's output schema to be the answer.
        // If the schema source is the child node itself, this will
        // set the the output schema to itself, so no harm
        // will be done.
        if (resetBack) {
            do {
                if (child instanceof AbstractJoinPlanNode) {
                    // In joins with inlined aggregation, the inlined
                    // aggregate node is the one that determines the schema.
                    // (However, the enclosing join node still has its
                    // "m_hasSignificantOutputSchema" bit set.)
                    //
                    // The method resolveColumnIndexes will overwrite
                    // a join node's schema if there is aggregation.  In order
                    // to avoid undoing the work we've done here, we must
                    // also update the inlined aggregate node.
                    AggregatePlanNode aggNode = AggregatePlanNode.getInlineAggregationNode(child);
                    if (aggNode != null) {
                        aggNode.setOutputSchema(answer);
                    }
                }

                if (! child.m_hasSignificantOutputSchema) {
                    child.setOutputSchema(answer);
                }

                child = (child.getParentCount() == 0) ? null : child.getParent(0);
            } while (child != null);
        }
        return answer;
    }

    /**
     * Add a child and link this node child's parent.
     * @param child The node to add.
     */
    public void addAndLinkChild(AbstractPlanNode child) {
        assert(child != null);
        m_children.add(child);
        child.m_parents.add(this);
    }

    /**
     * Used to re-link the child without changing the order.
     *
     * This is called by PushDownLimit and RemoveUnnecessaryProjectNodes.
     * @param index
     * @param child
     */
    public void setAndLinkChild(int index, AbstractPlanNode child) {
        assert(child != null);
        m_children.set(index, child);
        child.m_parents.add(this);
    }

    /** Remove child from this node.
     * @param child to remove.
     */
    public void unlinkChild(AbstractPlanNode child) {
        assert(child != null);
        m_children.remove(child);
        child.m_parents.remove(this);
    }

    /**
     * Replace an existing child with a new one preserving the child's position.
     * @param oldChild The node to replace.
     * @param newChild The new node.
     * @return true if the child was replaced
     */
    public boolean replaceChild(AbstractPlanNode oldChild, AbstractPlanNode newChild) {
        assert(oldChild != null);
        assert(newChild != null);
        int idx = 0;
        for (AbstractPlanNode child : m_children) {
            if (child.equals(oldChild)) {
                oldChild.m_parents.clear();
                setAndLinkChild(idx, newChild);
                return true;
            }
            ++idx;
        }
        return false;
    }

    public void replaceChild(int oldChildIdx, AbstractPlanNode newChild) {
        assert(oldChildIdx >= 0 && oldChildIdx < getChildCount());
        assert(newChild != null);
        AbstractPlanNode oldChild = m_children.get(oldChildIdx);
        assert(oldChild != null);
        oldChild.m_parents.clear();
        setAndLinkChild(oldChildIdx, newChild);
    }


    /**
     * Gets the children.
     * @return the children
     */
    public int getChildCount() {
        return m_children.size();
    }

    /**
     * @param index
     * @return The child node of this node at a given index or null if none exists.
     */
    public AbstractPlanNode getChild(int index) {
        return m_children.get(index);
    }

    public List<AbstractPlanNode> getChildren() {
        return m_children;
    }

    public void clearChildren() {
        m_children.clear();
    }

    public boolean hasChild(AbstractPlanNode receive) {
        return m_children.contains(receive);
    }

    /**
     * Gets the parents.
     * @return the parents
     */
    public int getParentCount() {
        return m_parents.size();
    }

    public AbstractPlanNode getParent(int index) {
        return m_parents.get(index);
    }

    public void clearParents() {
        m_parents.clear();
    }

    public void removeFromGraph() {
        disconnectParents();
        disconnectChildren();
    }

    public void disconnectParents() {
        m_parents.forEach(parent -> parent.m_children.remove(this));
        m_parents.clear();
     }

    public void disconnectChildren() {
        m_children.forEach(child -> child.m_parents.remove(this));
        m_children.clear();
     }

    /** Interject the provided node between this node and this node's current children */
    public void addIntermediary(AbstractPlanNode node) {

        // transfer this node's children to node
        Iterator<AbstractPlanNode> it = m_children.iterator();
        while (it.hasNext()) {
            AbstractPlanNode child = it.next();
            it.remove();                          // remove this.child from m_children
            assert child.getParentCount() == 1;
            child.clearParents();                 // and reset child's parents list
            node.addAndLinkChild(child);          // set node.child and child.parent
        }

        // and add node to this node's children
        assert(m_children.size() == 0);
        addAndLinkChild(node);
    }

    /**
     * @return The map of inlined nodes.
     */
    public Map<PlanNodeType, AbstractPlanNode> getInlinePlanNodes() {
        return m_inlineNodes;
    }

    /**
     * @param node
     */
    public void addInlinePlanNode(AbstractPlanNode node) {
        node.m_isInline = true;
        m_inlineNodes.put(node.getPlanNodeType(), node);
        node.m_children.clear();
        node.m_parents.clear();
    }

    /**
     *
     * @param type
     */
    public void removeInlinePlanNode(PlanNodeType type) {
        m_inlineNodes.remove(type);
    }

    /**
     *
     * @param type
     * @return An inlined node of the given type or null if none.
     */
    public AbstractPlanNode getInlinePlanNode(PlanNodeType type) {
        return m_inlineNodes.get(type);
    }

    /**
     *
     * @return Is this node inlined in another node.
     */
    public Boolean isInline() {
        return m_isInline;
    }

    public boolean isSubQuery() {
        return false;
    }

    public boolean hasSubquery() {
        return isSubQuery() ||
                m_children.stream().anyMatch(AbstractPlanNode::hasSubquery) ||
                m_inlineNodes.values().stream().anyMatch(AbstractPlanNode::hasSubquery);
    }

    /**
     * Refer to the override implementation on NestLoopIndexJoin node.
     * @param tableName
     * @return whether this node has an inlined index scan node or not.
     */
    public boolean hasInlinedIndexScanOfTable(String tableName) {
        return IntStream.range(0, getChildCount()).anyMatch(i ->
                getChild(i).hasInlinedIndexScanOfTable(tableName));
    }


    /**
     * @return the dominator list for a node
     */
    public Set<AbstractPlanNode> getDominators() {
        return m_dominators;
    }

    /**
    *   Initialize a hashset for each node containing that node's dominators
    *   (the set of predecessors that *always* precede this node in a traversal
    *   of the plan-graph in reverse-execution order (from root to leaves)).
    */
    public void calculateDominators() {
        HashSet<AbstractPlanNode> visited = new HashSet<>();
        calculateDominators_recurse(visited);
    }

    private void calculateDominators_recurse(HashSet<AbstractPlanNode> visited) {
        if (visited.contains(this)) {
            assert(false): "do not expect loops in plangraph.";
            return;
        }

        visited.add(this);
        m_dominators.clear();
        m_dominators.add(this);

        // find nodes that are in every parent's dominator set.

        HashMap<AbstractPlanNode, Integer> union = new HashMap<>();
        for (AbstractPlanNode n : m_parents) {
            for (AbstractPlanNode d : n.getDominators()) {
                if (union.containsKey(d))
                    union.put(d, union.get(d) + 1);
                else
                    union.put(d, 1);
            }
        }

        for (AbstractPlanNode pd : union.keySet() ) {
            if (union.get(pd) == m_parents.size())
                m_dominators.add(pd);
        }

        for (AbstractPlanNode n : m_children)
            n.calculateDominators_recurse(visited);
    }

    /**
     * @param type plan node type to search for
     * @return a list of nodes that are eventual successors of this node of the desired type
     */
    public List<AbstractPlanNode> findAllNodesOfType(PlanNodeType type) {
        Set<AbstractPlanNode> visited = new HashSet<>();
        List<AbstractPlanNode> collected = new ArrayList<>();
        findAllNodesOfType_recurse(type, null, collected, visited);
        return collected;
    }

    /**
     * @param pnClass plan node class to search for
     * @return a list of nodes that are eventual successors of this node of the desired class
     */
    public List<AbstractPlanNode> findAllNodesOfClass(Class< ? extends AbstractPlanNode> pnClass) {
        Set<AbstractPlanNode> visited = new HashSet<>();
        List<AbstractPlanNode> collected = new ArrayList<>();
        findAllNodesOfType_recurse(null, pnClass, collected, visited);
        return collected;
    }

    private void findAllNodesOfType_recurse(PlanNodeType type, Class< ? extends AbstractPlanNode> pnClass,
                                            List<AbstractPlanNode> collected, Set<AbstractPlanNode> visited) {
        if (visited.contains(this)) {
            assert(false): "do not expect loops in plangraph.";
            return;
        }
        visited.add(this);
        if (getPlanNodeType() == type) {
                collected.add(this);
        } else if (pnClass != null && pnClass.isAssignableFrom(getClass())) {
            collected.add(this);
        }

        for (AbstractPlanNode child : m_children)
            child.findAllNodesOfType_recurse(type, pnClass, collected, visited);

        for (AbstractPlanNode inlined : m_inlineNodes.values())
            inlined.findAllNodesOfType_recurse(type, pnClass, collected, visited);
    }

    final public Collection<AbstractExpression> findAllSubquerySubexpressions() {
        Set<AbstractExpression> collected = new HashSet<>();
        findAllExpressionsOfClass(AbstractSubqueryExpression.class, collected);
        return collected;
    }

    /**
     * Collect a unique list of expressions of a given type that this node has including its inlined nodes
     * @param aeClass AbstractExpression class to search for
     * @param collection set to populate with expressions that this node has
     */
    public void findAllExpressionsOfClass(Class< ? extends AbstractExpression> aeClass,
            Set<AbstractExpression> collected) {
        // Check the inlined plan nodes
        for (AbstractPlanNode inlineNode: getInlinePlanNodes().values()) {
            // For inline node we MUST go recursive to its children!!!!!
            inlineNode.findAllExpressionsOfClass(aeClass, collected);
        }

        // add the output column expressions if there were no projection
        NodeSchema schema = getOutputSchema();
        if (schema != null) {
            schema.addAllSubexpressionsOfClassFromNodeSchema(collected, aeClass);
        }
    }

    /**
     * @param type plan node type to search for
     * @return whether a node of that type is contained in the plan tree
     */
    public boolean hasAnyNodeOfType(PlanNodeType type) {
        return getPlanNodeType() == type ||
                m_children.stream().anyMatch(n -> n.hasAnyNodeOfType(type)) ||
                m_inlineNodes.values().stream().anyMatch(n -> n.hasAnyNodeOfType(type));
    }

    /**
     * @param pnClass plan node class to search for
     * @return whether a node of that type is contained in the plan tree
     */
    public boolean hasAnyNodeOfClass(Class< ? extends AbstractPlanNode> pnClass) {
        return pnClass.isAssignableFrom(getClass()) ||
                m_children.stream().anyMatch(n -> n.hasAnyNodeOfClass(pnClass)) ||
                m_inlineNodes.values().stream().anyMatch(n -> n.hasAnyNodeOfClass(pnClass));
    }

    @Override
    public int compareTo(AbstractPlanNode other) {
        // compare child nodes
        final Map<Integer, AbstractPlanNode> nodesById = new HashMap<>();
        for (AbstractPlanNode node : m_children)
            nodesById.put(node.getPlanNodeId(), node);
        for (AbstractPlanNode node : other.m_children) {
            AbstractPlanNode myNode = nodesById.get(node.getPlanNodeId());
            int diff = myNode.compareTo(node);
            if (diff != 0) return diff;
        }

        // compare inline nodes
        final Map<Integer, Entry<PlanNodeType, AbstractPlanNode>> inlineNodesById =
                m_inlineNodes.entrySet().stream()
                        .map(e -> Pair.of(e.getValue().getPlanNodeId(), e))
                        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond, (a, b) -> b));
        for (Entry<PlanNodeType, AbstractPlanNode> e : other.m_inlineNodes.entrySet()) {
            Entry<PlanNodeType, AbstractPlanNode> myE = inlineNodesById.get(e.getValue().getPlanNodeId());
            if (myE.getKey() != e.getKey()) return -1;

            int diff = myE.getValue().compareTo(e.getValue());
            if (diff != 0) return diff;
        }
        return m_id - other.m_id;
    }

    // produce a file that can imported into graphviz for easier visualization
    public String toDOTString() {
        StringBuilder sb = new StringBuilder();

        // id [label=id: value-type <value-type-attributes>];
        // id -> child_id;
        // id -> child_id;

        sb.append(m_id).append(" [label=\"").append(m_id).append(": ").append(getPlanNodeType()).append("\" ");
        sb.append(getValueTypeDotString());
        sb.append("];\n");
        for (AbstractPlanNode node : m_inlineNodes.values()) {
            sb.append(m_id).append(" -> ").append(node.getPlanNodeId().intValue()).append(";\n");
            sb.append(node.toDOTString());
        }
        for (AbstractPlanNode node : m_children) {
           sb.append(m_id).append(" -> ").append(node.getPlanNodeId().intValue()).append(";\n");
        }

        return sb.toString();
    }

    // maybe not worth polluting
    private String getValueTypeDotString() {
        if (isInline()) {
            return "fontcolor=\"white\" style=\"filled\" fillcolor=\"red\"";
        } else {
            switch (getPlanNodeType()) {
                case SEND:
                case RECEIVE:
                case MERGERECEIVE:
                    return "fontcolor=\"white\" style=\"filled\" fillcolor=\"black\"";
                default:
                    return "";
            }
        }
    }

    @Override
    public String toJSONString() {
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object();
            toJSONString(stringer);
            stringer.endObject();
        } catch (JSONException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return stringer.toString();
    }

    public void toJSONString(JSONStringer stringer) throws JSONException {
        stringer.keySymbolValuePair(Members.ID.name(), m_id);
        String planNodeType = getPlanNodeType().toString();
        stringer.keySymbolValuePair(Members.PLAN_NODE_TYPE.name(), planNodeType);
        if (! m_inlineNodes.isEmpty()) {
            stringer.key(Members.INLINE_NODES.name()).array();
            final List<AbstractPlanNode> strm = m_inlineNodes.entrySet().stream()
                    .sorted(Entry.comparingByKey())
                    .map(Entry::getValue)
                    .collect(Collectors.toList());
            for(AbstractPlanNode node : strm) {
                    stringer.value(node);
            }
            stringer.endArray();
        }

        if (! m_children.isEmpty()) {
            stringer.key(Members.CHILDREN_IDS.name()).array();
            for (AbstractPlanNode node : m_children) {
                stringer.value(node.getPlanNodeId().intValue());
            }
            stringer.endArray();
        }

        outputSchemaToJSON(stringer);
    }

    private void outputSchemaToJSON(JSONStringer stringer) throws JSONException {
        if (m_hasSignificantOutputSchema) {
            stringer.key(Members.OUTPUT_SCHEMA.name());
            stringer.array();
            for (int colNo = 0; colNo < m_outputSchema.size(); colNo += 1) {
                SchemaColumn column = m_outputSchema.getColumn(colNo);
                column.toJSONString(stringer, true, colNo);
            }
            stringer.endArray();
        }
    }

    public String toExplainPlanString() {
        StringBuilder sb = new StringBuilder();
        explainPlan_recurse(sb, "");
        String fullExpalinString = sb.toString();
        // Extract subqueries into a map to explain them separately. Each subquery is
        // surrounded by the 'Subquery_[SubqueryId]' tags. Example:
        // Subquery_1SEQUENTIAL SCAN of "R1"Subquery_1
        Pattern subqueryPattern = Pattern.compile(
                String.format("(%s)([0-9]+)(.*)(\\s*)%s(\\2)",
                        AbstractSubqueryExpression.SUBQUERY_TAG, AbstractSubqueryExpression.SUBQUERY_TAG),
                Pattern.DOTALL);
        Map<String, String> subqueries = new TreeMap<>();
        String fullSb = extractExplainedSubquries(fullExpalinString, subqueryPattern, subqueries);
        return subqueries.entrySet().stream().flatMap(q -> Stream.of(q.getKey(), q.getValue()))
                .collect(Collectors.joining("\n", fullSb, ""));
    }

    private String extractExplainedSubquries(String explainedSubquery, Pattern pattern, Map<String, String> subqueries) {
        Matcher matcher = pattern.matcher(explainedSubquery);
        int pos = 0;
        StringBuilder sb = new StringBuilder();
        // Find all the subqueries from the input string
        while(matcher.find()) {
            sb.append(explainedSubquery, pos, matcher.end(2));
            pos = matcher.end();
            // Recurse into the subquery string to extract its own subqueries if any
            String nextExplainedStmt = extractExplainedSubquries(matcher.group(3), pattern, subqueries);
            subqueries.put(AbstractSubqueryExpression.SUBQUERY_TAG + matcher.group(2), nextExplainedStmt);
        }
        // Append the rest of the input string
        if (pos < explainedSubquery.length()) {
            sb.append(explainedSubquery.substring(pos));
        }
        return sb.toString();
    }

    public void explainPlan_recurse(StringBuilder sb, String indent) {
        String extraIndent = " ";
        // Except when verbosely debugging,
        // skip projection nodes basically (they're boring as all get out)
        boolean skipCurrentNode = ! m_verboseExplainForDebugging
                                 && getPlanNodeType() == PlanNodeType.PROJECTION;
        if (skipCurrentNode) {
            extraIndent = "";
        } else {
            if (! m_skipInitalIndentationForExplain) {
                sb.append(indent);
            }
            String nodePlan = explainPlanForNode(indent);
            sb.append(nodePlan);

            if (m_verboseExplainForDebugging && m_outputSchema != null) {
                sb.append(indent).append(" ").append(m_outputSchema.toExplainPlanString());
            }

            sb.append("\n");
        }

        // Agg < Proj < Limit < Scan
        // Order the inline nodes with integer in ascending order
        TreeMap<Integer, AbstractPlanNode> sort_inlineNodes = new TreeMap<>();

        // every inline plan node is unique
        int ii = 4;
        for (AbstractPlanNode inlineNode : m_inlineNodes.values()) {
            if (inlineNode instanceof AggregatePlanNode) {
                sort_inlineNodes.put(0, inlineNode);
            } else if (inlineNode instanceof ProjectionPlanNode) {
                sort_inlineNodes.put(1, inlineNode);
            } else if (inlineNode instanceof LimitPlanNode) {
                sort_inlineNodes.put(2, inlineNode);
            } else if (inlineNode instanceof AbstractScanPlanNode) {
                sort_inlineNodes.put(3, inlineNode);
            } else {
                // any other inline nodes currently ?  --xin
                sort_inlineNodes.put(ii++, inlineNode);
            }
        }
        // inline nodes with ascending order as their integer keys
        for (AbstractPlanNode inlineNode : sort_inlineNodes.values()) {
            // don't bother with inlined projections
            if (! m_verboseExplainForDebugging &&
                    inlineNode.getPlanNodeType() == PlanNodeType.PROJECTION) {
                continue;
            }
            inlineNode.setSkipInitalIndentationForExplain(true);

            sb.append(indent).append(extraIndent).append("inline ");
            inlineNode.explainPlan_recurse(sb, indent + extraIndent);
        }

        for (AbstractPlanNode node : m_children) {
            // inline nodes shouldn't have children I hope
            assert(! m_isInline);
            if (skipCurrentNode) {
                // If the current node is skipped, I would like to pass the skip indentation
                // flag on to the next level.
                node.setSkipInitalIndentationForExplain(m_skipInitalIndentationForExplain);
            }
            node.explainPlan_recurse(sb, indent + extraIndent);
        }
    }

    private boolean m_skipInitalIndentationForExplain = false;
    public void setSkipInitalIndentationForExplain(boolean skip) {
        m_skipInitalIndentationForExplain = skip;
    }

    protected abstract String explainPlanForNode(String indent);

    public List<AbstractScanPlanNode> getScanNodeList () {
        Set<AbstractPlanNode> visited = new HashSet<>();
        List<AbstractScanPlanNode> collected = new ArrayList<>();
        getScanNodeList_recurse(collected, visited);
        return collected;
    }

    //postorder adding scan nodes
    protected void getScanNodeList_recurse(List<AbstractScanPlanNode> collected,
            Set<AbstractPlanNode> visited) {
        if (visited.contains(this)) {
            assert(false): "do not expect loops in plangraph.";
            return;
        }
        visited.add(this);
        for (AbstractPlanNode n : m_children) {
            n.getScanNodeList_recurse(collected, visited);
        }

        for (AbstractPlanNode node : m_inlineNodes.values()) {
            node.getScanNodeList_recurse(collected, visited);
        }
    }

    public List<AbstractPlanNode> getPlanNodeList () {
        Set<AbstractPlanNode> visited = new HashSet<>();
        List<AbstractPlanNode> collected = new ArrayList<>();
        getPlanNodeList_recurse(collected, visited);
        return collected;
    }

    //postorder add nodes
    public void getPlanNodeList_recurse(List<AbstractPlanNode> collected,
            Set<AbstractPlanNode> visited) {
        if (visited.contains(this)) {
            assert(false): "do not expect loops in plangraph.";
            return;
        }
        visited.add(this);

        for (AbstractPlanNode n : m_children) {
            n.getPlanNodeList_recurse(collected, visited);
        }
        collected.add(this);
    }

    abstract protected void loadFromJSONObject(JSONObject obj, Database db) throws JSONException;

    protected static void loadBooleanArrayFromJSONObject(JSONObject jobj, String key, List<Boolean> target)
            throws JSONException {
        if (! jobj.isNull(key)) {
            JSONArray jarray = jobj.getJSONArray(key);
            int numCols = jarray.length();
            for (int ii = 0; ii < numCols; ++ii) {
                target.add(jarray.getBoolean(ii));
            }
        }
    }

    protected static void booleanArrayToJSONString(JSONStringer stringer, String key, List<Boolean> array)
            throws JSONException {
        stringer.key(key).array();
        for (Boolean arrayElement : array) {
            stringer.value(arrayElement);
        }
        stringer.endArray();
    }

    protected static NodeSchema loadSchemaFromJSONObject(JSONObject jobj,
            String jsonKey) throws JSONException {
        NodeSchema nodeSchema = new NodeSchema();
        JSONArray jarray = jobj.getJSONArray(jsonKey);
        int size = jarray.length();
        for (int i = 0; i < size; ++i) {
            nodeSchema.addColumn(SchemaColumn.fromJSONObject(jarray.getJSONObject(i)));
        }
        return nodeSchema;
    }

    protected final void helpLoadFromJSONObject(JSONObject jobj, Database db) throws JSONException {
        assert(jobj != null);
        m_id = jobj.getInt(Members.ID.name());

        JSONArray jarray;
        //load inline nodes
        if (! jobj.isNull(Members.INLINE_NODES.name())) {
            jarray = jobj.getJSONArray( Members.INLINE_NODES.name() );
            PlanNodeTree pnt = new PlanNodeTree();
            pnt.loadPlanNodesFromJSONArrays(jarray, db);
            pnt.getNodeList().forEach(pn -> m_inlineNodes.put(pn.getPlanNodeType(), pn));
        }
        //children and parents list loading implemented in planNodeTree.loadFromJsonArray

        // load the output schema if it was marked significant.
        if ( ! jobj.isNull(Members.OUTPUT_SCHEMA.name())) {
            m_hasSignificantOutputSchema = true;
            m_outputSchema = loadSchemaFromJSONObject(jobj,
                    Members.OUTPUT_SCHEMA.name());
        }
    }

    /**
     * @param jobj
     * @param key
     * @return
     * @throws JSONException
     */
    List<String> loadStringListMemberFromJSON(JSONObject jobj, String key) throws JSONException {
        if (jobj.isNull(key)) {
            return null;
        }
        JSONArray jarray = jobj.getJSONArray(key);
        int numElems = jarray.length();
        List<String> result = new ArrayList<>(numElems);
        for (int ii = 0; ii < numElems; ++ii) {
            result.add(jarray.getString(ii));
        }
        return result;
    }

    /**
     * @param stringer
     * @param key
     * @param stringList
     * @throws JSONException
     */
    void toJSONStringArrayString(JSONStringer stringer, String key,
            List<String> stringList) throws JSONException {
        stringer.key(key).array();
        for (String elem : stringList) {
            stringer.value(elem);
        }
        stringer.endArray();
    }

    /**
     * @param jobj
     * @param key
     * @return
     * @throws JSONException
     */
    int[] loadIntArrayMemberFromJSON(JSONObject jobj, String key) throws JSONException {
        if (jobj.isNull(key)) {
            return null;
        }
        JSONArray jarray = jobj.getJSONArray(key);
        int numElems = jarray.length();
        int[] result = new int[numElems];
        for (int ii = 0; ii < numElems; ++ii) {
            result[ii] = jarray.getInt(ii);
        }
        return result;
    }

    /**
     * @param stringer
     * @param key
     * @param intArray
     * @throws JSONException
     */
    void toJSONIntArrayString(JSONStringer stringer, String key, int[] intArray) throws JSONException {
        stringer.key(key).array();
        for (int i : intArray) {
            stringer.value(i);
        }
        stringer.endArray();
    }

    public boolean reattachFragment(AbstractPlanNode child) {
        return m_children.stream().anyMatch(node -> node.reattachFragment(child));
    }

    public boolean planNodeClassNeedsProjectionNode() {
        return true;
    }

    /**
     * When a project node is added to the top of the plan, we need to adjust
     * the differentiator field of TVEs to reflect differences in the scan
     * schema vs the storage schema of a table, so that fields with duplicate names
     * produced by expanding "SELECT *" can resolve correctly.
     *
     * We recurse until we find either a join node or a scan node.
     *
     * @param  existing differentiator field of a TVE
     * @return new differentiator value
     */
    public void adjustDifferentiatorField(TupleValueExpression tve) {
        assert (m_children.size() == 1);
        m_children.get(0).adjustDifferentiatorField(tve);
    }

    public void setHaveSignificantOutputSchema(boolean hasSignificantOutputSchema) {
        m_hasSignificantOutputSchema = hasSignificantOutputSchema;
    }

    /**
     * Traverse the plan node tree to allow a visitor interact with each node.
     */
    public void acceptVisitor(AbstractPlanNodeVisitor visitor) {
        visitor.visitNode(this);
    }
}
