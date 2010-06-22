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

package org.voltdb.plannodes;

import java.util.*;
import java.util.Map.Entry;
import org.json.JSONString;
import org.json.JSONStringer;
import org.json.JSONException;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.planner.PlanColumn;
import org.voltdb.planner.PlanStatistics;
import org.voltdb.planner.PlannerContext;
import org.voltdb.planner.StatsField;
import org.voltdb.types.*;

public abstract class AbstractPlanNode implements JSONString, Comparable<AbstractPlanNode> {

    /**
     * Internal PlanNodeId counter. Note that this member is static, which means
     * all PlanNodes will have a unique id
     */
    private static int NEXT_PLAN_NODE_ID = 1;
    private static int NEXT_LOCAL_PLAN_NODE_ID = 1;

    private static final int MAX_LOCAL_ID = 1000000;
    private static boolean m_useGlobalIds = true;

    public static void setUseGlobalIds(boolean useGlobalIds) {
        if (useGlobalIds) {
            m_useGlobalIds = true;
            NEXT_LOCAL_PLAN_NODE_ID = 1;
        } else {
            m_useGlobalIds = false;
        }
    }

    static int getNextPlanNodeId() {
        assert ((NEXT_LOCAL_PLAN_NODE_ID + 1) <= MAX_LOCAL_ID);
        if (m_useGlobalIds)
            return NEXT_PLAN_NODE_ID++;
        else
            return NEXT_LOCAL_PLAN_NODE_ID++;
    }

    public enum Members {
        ID,
        PLAN_NODE_TYPE,
        INLINE_NODES,
        CHILDREN_IDS,
        PARENT_IDS,
        OUTPUT_COLUMNS;
    }

    protected int m_id = -1;
    protected List<AbstractPlanNode> m_children = new Vector<AbstractPlanNode>();
    protected List<AbstractPlanNode> m_parents = new Vector<AbstractPlanNode>();
    protected HashSet<AbstractPlanNode> m_dominators = new HashSet<AbstractPlanNode>();

    // TODO: planner accesses this data directly. Should be protected.
    public ArrayList<Integer> m_outputColumns = new ArrayList<Integer>();
    protected List<ScalarValueHints> m_outputColumnHints = new ArrayList<ScalarValueHints>();
    protected long m_estimatedOutputTupleCount = 0;

    /**
     * Some PlanNodes can take advantage of inline PlanNodes to perform
     * certain additional tasks while performing their main operation, rather than
     * having to re-read tuples from intermediate results
     */
    protected Map<PlanNodeType, AbstractPlanNode> m_inlineNodes = new HashMap<PlanNodeType, AbstractPlanNode>();
    protected boolean m_isInline = false;

    protected final PlannerContext m_context;

    /**
     * Instantiates a new plan node.
     */
    protected AbstractPlanNode(PlannerContext context) {
        assert(context != null);
        m_context = context;
        m_id = getNextPlanNodeId();
    }

    public void overrideId(int newId) {
        m_id = newId;
    }

    /**
     * Create a PlanNode that clones the configuration information but
     * is not inserted in the plan graph and has a unique plan node id.
     */
    protected void produceCopyForTransformation(AbstractPlanNode copy) {
        for (Integer colGuid : m_outputColumns) {
            copy.m_outputColumns.add(colGuid);
        }
        copy.m_outputColumnHints = m_outputColumnHints;
        copy.m_estimatedOutputTupleCount = m_estimatedOutputTupleCount;

        // clone is not yet implemented for every node.
        assert(m_inlineNodes.size() == 0);
        assert(m_isInline == false);

        // the api requires the copy is not (yet) connected
        assert (copy.m_parents.size() == 0);
        assert (copy.m_children.size() == 0);
    }


    public abstract PlanNodeType getPlanNodeType();

    public boolean updateOutputColumns(Database db) {
        ArrayList<Integer> childCols = new ArrayList<Integer>();
        for (AbstractPlanNode child : m_children) {
            boolean result = child.updateOutputColumns(db);
            assert(result);
            childCols.addAll(child.m_outputColumns);
        }

        ArrayList<Integer> new_output_cols = new ArrayList<Integer>();
        new_output_cols = createOutputColumns(db, childCols);
        for (AbstractPlanNode child : m_inlineNodes.values()) {
            if (child instanceof IndexScanPlanNode)
                continue;
            new_output_cols = child.createOutputColumns(db, new_output_cols);
        }

        // Before we wipe out the old column list, free any PlanColumns that
        // aren't getting reused
        for (Integer col : m_outputColumns)
        {
            if (!new_output_cols.contains(col))
            {
                m_context.freeColumn(col);
            }
        }

        m_outputColumns = new_output_cols;

        return true;
    }

    /** By default, a plan node does not alter its input schema */
    @SuppressWarnings("unchecked")
    protected ArrayList<Integer> createOutputColumns(Database db, ArrayList<Integer> input) {
        return (ArrayList<Integer>)input.clone();
    }

    public PlanColumn findMatchingOutputColumn(String tableName,
                                               String columnName,
                                               String columnAlias)
    {
        boolean found = false;
        PlanColumn retval = null;
        for (Integer colguid : m_outputColumns) {
            PlanColumn plancol = m_context.get(colguid);
            if ((plancol.originTableName().equals(tableName)) &&
                ((plancol.originColumnName().equals(columnName)) ||
                 (plancol.originColumnName().equals(columnAlias))))
            {
                found = true;
                retval = plancol;
                break;
            }
        }
        if (!found) {
            assert(found) : "Found no candidate output column.";
            throw new RuntimeException("Found no candidate output column.");
        }
        return retval;
    }

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

    @Override
    public final String toString() {
        return getPlanNodeType() + "[" + m_id + "]";
    }

    public boolean computeEstimatesRecursively(PlanStatistics stats, Cluster cluster, Database db, DatabaseEstimates estimates, ScalarValueHints[] paramHints) {
        assert(estimates != null);

        m_outputColumnHints.clear();
        m_estimatedOutputTupleCount = 0;

        // recursively compute and collect stats from children
        for (AbstractPlanNode child : m_children) {
            boolean result = child.computeEstimatesRecursively(stats, cluster, db, estimates, paramHints);
            assert(result);
            m_outputColumnHints.addAll(child.m_outputColumnHints);
            m_estimatedOutputTupleCount += child.m_estimatedOutputTupleCount;

            stats.incrementStatistic(0, StatsField.TUPLES_READ, m_estimatedOutputTupleCount);
        }

        return true;
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
     * Add a plan node as a child of this node and link this node as it's parent.
     * @param child The node to add.
     */
    public void addAndLinkChild(AbstractPlanNode child) {
        m_children.add(child);
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
       for (AbstractPlanNode parent : m_parents)
           parent.m_children.remove(this);
       for (AbstractPlanNode child : m_children)
           child.m_parents.remove(this);

       m_parents.clear();
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


    /**
     * @return the dominator list for a node
     */
    public HashSet<AbstractPlanNode> getDominators() {
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

    public void findAllNodesOfType_recurse(PlanNodeType type,ArrayList<AbstractPlanNode> collected,
        HashSet<AbstractPlanNode> visited)
    {
        if (visited.contains(this)) {
            assert(false): "do not expect loops in plangraph.";
            return;
        }
        visited.add(this);
        if (getPlanNodeType() == type)
            collected.add(this);

        for (AbstractPlanNode n : m_children)
            n.findAllNodesOfType_recurse(type, collected, visited);
    }

    public void freeColumns()
    {
        for (Integer guid : m_outputColumns)
        {
            m_context.freeColumn(guid);
        }
        for (AbstractPlanNode n : m_children)
        {
            n.freeColumns();
        }
        for (PlanNodeType t : m_inlineNodes.keySet())
        {
            m_inlineNodes.get(t).freeColumns();
        }
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
        sb.append(getValueTypeDotString(this));
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
    private String getValueTypeDotString(AbstractPlanNode pn) {
        PlanNodeType pnt = pn.getPlanNodeType();
        if (pn.isInline()) {
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
            System.exit(-1);
        }
        return stringer.toString();
    }

    public void toJSONString(JSONStringer stringer) throws JSONException {
        stringer.key(Members.ID.name()).value(m_id);
        stringer.key(Members.PLAN_NODE_TYPE.name()).value(getPlanNodeType().toString());
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
        /*for (Map.Entry<PlanNodeType, AbstractPlanNode> entry : m_inlineNodes.entrySet()) {
            assert (entry.getValue() instanceof JSONString);
            stringer.value(entry.getValue());
        }*/
        stringer.endArray();
        stringer.key(Members.CHILDREN_IDS.name()).array();
        for (AbstractPlanNode node : m_children) {
            stringer.value(node.getPlanNodeId().intValue());
        }
        stringer.endArray().key(Members.PARENT_IDS.name()).array();
        for (AbstractPlanNode node : m_parents) {
            stringer.value(node.getPlanNodeId().intValue());
        }
        stringer.endArray(); //end inlineNodes

        stringer.key(Members.OUTPUT_COLUMNS.name());
        stringer.array();
        for (int col = 0; col < m_outputColumns.size(); col++) {
            PlanColumn column = m_context.get(m_outputColumns.get(col));
            column.toJSONString(stringer);
        }
        stringer.endArray();
    }
}
