package org.voltdb.planner;

import java.util.HashMap;
import java.util.Map.Entry;

import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.types.PlanNodeType;

public class plannerTester {
	private static void diffTwoPlans( AbstractPlanNode pn1, AbstractPlanNode pn2 ){
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
}
