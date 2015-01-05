/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.planner.PlanStatistics;
import org.voltdb.planner.StatsField;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.types.PlanNodeType;

public abstract class AbstractPlanNode implements JSONString, Comparable<AbstractPlanNode> {

    /**
     * Internal PlanNodeId counter. Note that this member is static, which means
     * all PlanNodes will have a unique id
     */
    private static int NEXT_PLAN_NODE_ID = 1;

    // Keep this flag turned off in production or when testing user-accessible EXPLAIN output or when
    // using EXPLAIN output to validate plans.
    protected static boolean m_verboseExplainForDebugging = false; // CODE REVIEWER! this SHOULD be false!
    public static void enableVerboseExplainForDebugging() { m_verboseExplainForDebugging = true; }
    public static boolean disableVerboseExplainForDebugging()
    {
        boolean was = m_verboseExplainForDebugging;
        m_verboseExplainForDebugging = false;
        return was;
    }
    public static void restoreVerboseExplainForDebugging(boolean was) { m_verboseExplainForDebugging = was; }

    /*
     * IDs only need to be unique for a single plan.
     * Reset between plans
     */
    public static final void resetPlanNodeIds() {
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
    protected List<AbstractPlanNode> m_children = new ArrayList<AbstractPlanNode>();
    protected List<AbstractPlanNode> m_parents = new ArrayList<AbstractPlanNode>();
    protected Set<AbstractPlanNode> m_dominators = new HashSet<AbstractPlanNode>();

    // TODO: planner accesses this data directly. Should be protected.
    protected List<ScalarValueHints> m_outputColumnHints = new ArrayList<ScalarValueHints>();
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
    protected Map<PlanNodeType, AbstractPlanNode> m_inlineNodes =
        new HashMap<PlanNodeType, AbstractPlanNode>();
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

    public void overrideId(int newId) {
        m_id = newId;
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
        assert(m_isInline == false);

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
    public void generateOutputSchema(Database db)
    {
        // default behavior: just copy the input schema
        // to the output schema
        assert(m_children.size() == 1);
        m_children.get(0).generateOutputSchema(db);
        // Replace the expressions in our children's columns with TVEs.  When
        // we resolve the indexes in these TVEs they will point back at the
        // correct input column, which we are assuming that the child node
        // has filled in with whatever expression was here before the replacement.
        // Output schemas defined using this standard algorithm
        // are just cached "fillers" that satisfy the legacy
        // resolveColumnIndexes/generateOutputSchema/getOutputSchema protocol
        // until it can be fixed up  -- see the FIXME comment on generateOutputSchema.
        m_hasSignificantOutputSchema = false;
        m_outputSchema =
            m_children.get(0).getOutputSchema().copyAndReplaceWithTVE();
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

    public void validate() throws Exception {
        //
        // Make sure our children have us listed as their parents
        //
        for (AbstractPlanNode child : m_children) {
            if (!child.m_parents.contains(this)) {
                throw new Exception("ERROR: The child PlanNode '" + child.toString() + "' does not " +
                                    "have its parent PlanNode '" + toString() + "' in its parents list");
            }
            child.validate();
        }
        //
        // Inline PlanNodes
        //
        if (!m_inlineNodes.isEmpty()) {
            for (AbstractPlanNode node : m_inlineNodes.values()) {
                //
                // Make sure that we're not attached to some kind of tree somewhere...
                //
                if (!node.m_children.isEmpty()) {
                    throw new Exception("ERROR: The inline PlanNode '" + node + "' has children inside of PlanNode '" + this + "'");
                } else if (!node.m_parents.isEmpty()) {
                    throw new Exception("ERROR: The inline PlanNode '" + node + "' has parents inside of PlanNode '" + this + "'");
                } else if (!node.isInline()) {
                    throw new Exception("ERROR: The inline PlanNode '" + node + "' was not marked as inline for PlanNode '" + this + "'");
                } else if (!node.getInlinePlanNodes().isEmpty()) {
                    throw new Exception("ERROR: The inline PlanNode '" + node + "' has its own inline PlanNodes inside of PlanNode '" + this + "'");
                }
                node.validate();
            }
        }
    }

    public boolean hasReplicatedResult()
    {
        Map<String, StmtTargetTableScan> tablesRead = new TreeMap<String, StmtTargetTableScan>();
        getTablesAndIndexes(tablesRead, null);
        for (StmtTableScan tableScan : tablesRead.values()) {
            if ( ! tableScan.getIsReplicated()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Recursively build sets of read tables read and index names used.
     *
     * @param tablesRead Set of table aliases read potentially added to at each recursive level.
     * @param indexes Set of index names used in the plan tree
     * Only the current fragment is of interest.
     */
    public void getTablesAndIndexes(Map<String, StmtTargetTableScan> tablesRead,
            Collection<String> indexes)
    {
        for (AbstractPlanNode node : m_inlineNodes.values()) {
            node.getTablesAndIndexes(tablesRead, indexes);
        }
        for (AbstractPlanNode node : m_children) {
            node.getTablesAndIndexes(tablesRead, indexes);
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
                                                  Cluster cluster,
                                                  Database db,
                                                  DatabaseEstimates estimates,
                                                  ScalarValueHints[] paramHints)
    {
        assert(stats != null);

        m_outputColumnHints.clear();
        m_estimatedOutputTupleCount = 0;

        // recursively compute and collect stats from children
        long childOutputTupleCountEstimate = 0;
        for (AbstractPlanNode child : m_children) {
            child.computeEstimatesRecursively(stats, cluster, db, estimates, paramHints);
            m_outputColumnHints.addAll(child.m_outputColumnHints);
            childOutputTupleCountEstimate += child.m_estimatedOutputTupleCount;
        }

        // make sure any inlined scans (for NLIJ mostly) are costed as well
        for (Entry<PlanNodeType, AbstractPlanNode> entry : m_inlineNodes.entrySet()) {
            AbstractPlanNode inlineNode = entry.getValue();
            if (inlineNode instanceof AbstractScanPlanNode) {
                inlineNode.computeCostEstimates(0, cluster, db, estimates, paramHints);
            }
        }

        computeCostEstimates(childOutputTupleCountEstimate, cluster, db, estimates, paramHints);
        stats.incrementStatistic(0, StatsField.TUPLES_READ, m_estimatedProcessedTupleCount);
    }

    /**
     * Given the number of tuples expected as input to this node, compute an estimate
     * of the number of tuples read/processed and the number of tuples output.
     * This will be called by
     * {@see AbstractPlanNode#computeEstimatesRecursively(PlanStatistics, Cluster, Database, DatabaseEstimates, ScalarValueHints[])}.
     */
    protected void computeCostEstimates(long childOutputTupleCountEstimate,
                                        Cluster cluster,
                                        Database db,
                                        DatabaseEstimates estimates,
                                        ScalarValueHints[] paramHints)
    {
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
     * Add a child and link this node child's parent.
     * @param child The node to add.
     */
    public void addAndLinkChild(AbstractPlanNode child) {
        m_children.add(child);
        child.m_parents.add(this);
    }

    // called by PushDownLimit, re-link the child without changing the order
    public void setAndLinkChild(int index, AbstractPlanNode child) {
        m_children.set(index, child);
        child.m_parents.add(this);
    }

    /** Remove child from this node.
     * @param child to remove.
     */
    public void unlinkChild(AbstractPlanNode child) {
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

    public boolean replaceChild(int oldChildIdx, AbstractPlanNode newChild) {
        if (oldChildIdx < 0 || oldChildIdx >= getChildCount()) {
            return false;
        }

        AbstractPlanNode oldChild = m_children.get(oldChildIdx);
        oldChild.m_parents.clear();
        setAndLinkChild(oldChildIdx, newChild);
        return true;
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
        for (AbstractPlanNode parent : m_parents)
            parent.m_children.remove(this);
        m_parents.clear();
     }

    public void disconnectChildren() {
        for (AbstractPlanNode child : m_children)
            child.m_parents.remove(this);
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
        if (m_inlineNodes.containsKey(type)) {
            m_inlineNodes.remove(type);
        }
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
        if (isSubQuery()) {
            return true;
        }
        for (AbstractPlanNode n : m_children) {
            if (n.hasSubquery()) {
                return true;
            }
        }
        for (AbstractPlanNode inlined : m_inlineNodes.values()) {
            if (inlined.hasSubquery()) {
                return true;
            }
        }

        return false;
    }

    /**
     * Refer to the override implementation on NestLoopIndexJoin node.
     * @param tableName
     * @return whether this node has an inlined index scan node or not.
     */
    public boolean hasInlinedIndexScanOfTable(String tableName) {
        for (int i = 0; i < getChildCount(); i++) {
            AbstractPlanNode child = getChild(i);
            if (child.hasInlinedIndexScanOfTable(tableName) == true) {
                return true;
            }
        }

        return false;
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
        HashSet<AbstractPlanNode> visited = new HashSet<AbstractPlanNode>();
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

        HashMap<AbstractPlanNode, Integer> union = new HashMap<AbstractPlanNode, Integer>();
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
    public ArrayList<AbstractPlanNode> findAllNodesOfType(PlanNodeType type) {
        HashSet<AbstractPlanNode> visited = new HashSet<AbstractPlanNode>();
        ArrayList<AbstractPlanNode> collected = new ArrayList<AbstractPlanNode>();
        findAllNodesOfType_recurse(type, collected, visited);
        return collected;
    }

    private void findAllNodesOfType_recurse(PlanNodeType type,ArrayList<AbstractPlanNode> collected,
        HashSet<AbstractPlanNode> visited)
    {
        if (visited.contains(this)) {
            assert(false): "do not expect loops in plangraph.";
            return;
        }
        visited.add(this);
        if (getPlanNodeType() == type)
            collected.add(this);

        for (AbstractPlanNode child : m_children)
            child.findAllNodesOfType_recurse(type, collected, visited);

        for (AbstractPlanNode inlined : m_inlineNodes.values())
            inlined.findAllNodesOfType_recurse(type, collected, visited);
    }

    /**
     * @param type plan node type to search for
     * @return whether a node of that type is contained in the plan tree
     */
    public boolean hasAnyNodeOfType(PlanNodeType type) {
        if (getPlanNodeType() == type)
            return true;

        for (AbstractPlanNode n : m_children) {
            if (n.hasAnyNodeOfType(type)) {
                return true;
            }
        }

        for (AbstractPlanNode inlined : m_inlineNodes.values()) {
            if (inlined.hasAnyNodeOfType(type)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public int compareTo(AbstractPlanNode other) {
        int diff = 0;

        // compare child nodes
        HashMap<Integer, AbstractPlanNode> nodesById = new HashMap<Integer, AbstractPlanNode>();
        for (AbstractPlanNode node : m_children)
            nodesById.put(node.getPlanNodeId(), node);
        for (AbstractPlanNode node : other.m_children) {
            AbstractPlanNode myNode = nodesById.get(node.getPlanNodeId());
            diff = myNode.compareTo(node);
            if (diff != 0) return diff;
        }

        // compare inline nodes
        HashMap<Integer, Entry<PlanNodeType, AbstractPlanNode>> inlineNodesById =
               new HashMap<Integer, Entry<PlanNodeType, AbstractPlanNode>>();
        for (Entry<PlanNodeType, AbstractPlanNode> e : m_inlineNodes.entrySet())
            inlineNodesById.put(e.getValue().getPlanNodeId(), e);
        for (Entry<PlanNodeType, AbstractPlanNode> e : other.m_inlineNodes.entrySet()) {
            Entry<PlanNodeType, AbstractPlanNode> myE = inlineNodesById.get(e.getValue().getPlanNodeId());
            if (myE.getKey() != e.getKey()) return -1;

            diff = myE.getValue().compareTo(e.getValue());
            if (diff != 0) return diff;
        }

        diff = m_id - other.m_id;
        return diff;
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
        PlanNodeType pnt = getPlanNodeType();
        if (isInline()) {
            return "fontcolor=\"white\" style=\"filled\" fillcolor=\"red\"";
        }
        if (pnt == PlanNodeType.SEND || pnt == PlanNodeType.RECEIVE) {
            return "fontcolor=\"white\" style=\"filled\" fillcolor=\"black\"";
        }
        return "";
    }

    @Override
    public String toJSONString() {
        JSONStringer stringer = new JSONStringer();
        try
        {
            stringer.object();
            toJSONString(stringer);
            stringer.endObject();
        }
        catch (JSONException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage(), e);
        }
        return stringer.toString();
    }

    public void toJSONString(JSONStringer stringer) throws JSONException {
        stringer.key(Members.ID.name()).value(m_id);
        stringer.key(Members.PLAN_NODE_TYPE.name()).value(getPlanNodeType().toString());

        if (m_inlineNodes.size() > 0) {
            stringer.key(Members.INLINE_NODES.name()).array();

            PlanNodeType types[] = new PlanNodeType[m_inlineNodes.size()];
            int i = 0;
            for (PlanNodeType type : m_inlineNodes.keySet()) {
                types[i++] = type;
            }
            Arrays.sort(types);
            for (PlanNodeType type : types) {
                AbstractPlanNode node = m_inlineNodes.get(type);
                assert(node != null);
                assert(node instanceof JSONString);
                stringer.value(node);
            }
            stringer.endArray();
        }

        if (m_children.size() > 0) {
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
            for (SchemaColumn column : m_outputSchema.getColumns()) {
                column.toJSONString(stringer, true);
            }
            stringer.endArray();
        }
    }

    public String toExplainPlanString() {
        StringBuilder sb = new StringBuilder();
        explainPlan_recurse(sb, "");
        return sb.toString();
    }

    public void explainPlan_recurse(StringBuilder sb, String indent) {
        if (m_verboseExplainForDebugging && m_hasSignificantOutputSchema) {
            sb.append(indent + "Detailed Output Schema: ");
            JSONStringer stringer = new JSONStringer();
            try
            {
                stringer.object();
                outputSchemaToJSON(stringer);
                stringer.endObject();
                sb.append(stringer.toString());
            }
            catch (Exception e)
            {
                sb.append(indent + "CORRUPTED beyond the ability to format? " + e);
                e.printStackTrace();
            }
            sb.append(indent + "from\n");
        }
        String extraIndent = " ";
        // Except when verbosely debugging,
        // skip projection nodes basically (they're boring as all get out)
        if (( ! m_verboseExplainForDebugging) && (getPlanNodeType() == PlanNodeType.PROJECTION)) {
            extraIndent = "";
        }
        else {
            if ( ! m_skipInitalIndentationForExplain) {
                sb.append(indent);
            }
            String nodePlan = explainPlanForNode(indent);
            sb.append(nodePlan + "\n");
        }

        // Agg < Proj < Limit < Scan
        // Order the inline nodes with integer in ascending order
        TreeMap<Integer, AbstractPlanNode> sort_inlineNodes =
                new TreeMap<Integer, AbstractPlanNode>();

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
            if (( ! m_verboseExplainForDebugging) &&
                (inlineNode.getPlanNodeType() == PlanNodeType.PROJECTION)) {
                continue;
            }
            inlineNode.setSkipInitalIndentationForExplain(true);

            sb.append(indent + extraIndent + "inline ");
            inlineNode.explainPlan_recurse(sb, indent + extraIndent);
        }

        for (AbstractPlanNode node : m_children) {
            // inline nodes shouldn't have children I hope
            assert(m_isInline == false);
            node.explainPlan_recurse(sb, indent + extraIndent);
        }
    }

    private boolean m_skipInitalIndentationForExplain = false;
    public void setSkipInitalIndentationForExplain(boolean skip) {
        m_skipInitalIndentationForExplain = skip;
    }

    protected abstract String explainPlanForNode(String indent);

    public ArrayList<AbstractScanPlanNode> getScanNodeList () {
        HashSet<AbstractPlanNode> visited = new HashSet<AbstractPlanNode>();
        ArrayList<AbstractScanPlanNode> collected = new ArrayList<AbstractScanPlanNode>();
        getScanNodeList_recurse( collected, visited);
        return collected;
    }

    //postorder adding scan nodes
    public void getScanNodeList_recurse(ArrayList<AbstractScanPlanNode> collected,
            HashSet<AbstractPlanNode> visited) {
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

    public ArrayList<AbstractPlanNode> getPlanNodeList () {
        HashSet<AbstractPlanNode> visited = new HashSet<AbstractPlanNode>();
        ArrayList<AbstractPlanNode> collected = new ArrayList<AbstractPlanNode>();
        getPlanNodeList_recurse( collected, visited);
        return collected;
    }

    //postorder add nodes
    public void getPlanNodeList_recurse(ArrayList<AbstractPlanNode> collected,
            HashSet<AbstractPlanNode> visited) {
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

    protected final void helpLoadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        assert( jobj != null );
        m_id = jobj.getInt( Members.ID.name() );

        JSONArray jarray = null;
        //load inline nodes
        if( !jobj.isNull( Members.INLINE_NODES.name() ) ){
            jarray = jobj.getJSONArray( Members.INLINE_NODES.name() );
            PlanNodeTree pnt = new PlanNodeTree();
            pnt.loadFromJSONArray(jarray, db);
            List<AbstractPlanNode> list = pnt.getNodeList();
            for( AbstractPlanNode pn : list ) {
                m_inlineNodes.put( pn.getPlanNodeType(), pn);
            }
        }
        //children and parents list loading implemented in planNodeTree.loadFromJsonArray

        // load the output schema if it was marked significant.
        if ( !jobj.isNull( Members.OUTPUT_SCHEMA.name() ) ) {
            m_outputSchema = new NodeSchema();
            m_hasSignificantOutputSchema = true;
            jarray = jobj.getJSONArray( Members.OUTPUT_SCHEMA.name() );
            int size = jarray.length();
            for( int i = 0; i < size; i++ ) {
                m_outputSchema.addColumn( SchemaColumn.fromJSONObject(jarray.getJSONObject(i)) );
            }
        }
    }

    public boolean reattachFragment( SendPlanNode child ) {
        for( AbstractPlanNode pn : m_inlineNodes.values() ) {
            if( pn.reattachFragment( child) )
                return true;
        }
        for( AbstractPlanNode pn : m_children ) {
            if( pn.reattachFragment( child) )
                return true;
        }
        return false;
    }
}
