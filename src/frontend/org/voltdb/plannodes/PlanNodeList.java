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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;

/**
 *
 */
public class PlanNodeList extends PlanNodeTree implements Comparable<PlanNodeList> {

    public enum Members {
        EXECUTE_LIST;
    }

    protected List<AbstractPlanNode> m_list = new ArrayList<AbstractPlanNode>();

    public PlanNodeList() {
        super();
    }

    public PlanNodeList(AbstractPlanNode root_node) {
        super(root_node);
        try {
            constructList();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<AbstractPlanNode> getExecutionList() {
        return m_list;
    }

    @Override
    public String toString() {
        String ret = "EXECUTE LIST: " + m_list.size() + " nodes\n";
        for (int ctr = 0, cnt = m_list.size(); ctr < cnt; ctr++) {
            ret += "   [" + ctr + "] " + m_list.get(ctr) + "\n";
        }
        ret += m_planNodes.get(0).toString();
        return ret;
    }

    public void constructList() throws Exception {
        //
        // Create a counter for each node based on the # of children that it has
        // If any node has no children, put it in the execute list
        //
        List<AbstractPlanNode> execute_list = Collections.synchronizedList(new ArrayList<AbstractPlanNode>());
        Map<AbstractPlanNode, Integer> child_cnts = new HashMap<AbstractPlanNode, Integer>();
        for (AbstractPlanNode node : m_planNodes) {
            int num_of_children = node.getChildCount();
            if (num_of_children == 0) {
                execute_list.add(node);
            } else {
                child_cnts.put(node, num_of_children);
            }
        }
        //
        // Now run through a simulation
        // Doing it this way maintains the nuances of the parent-child relationships
        //
        m_list.clear();
        while (!execute_list.isEmpty()) {
            AbstractPlanNode node = execute_list.remove(0);
            //
            // Add the node to our execution list
            //
            m_list.add(node);
            //
            // Then update all of this node's parents and reduce their wait counter by 1
            // If the counter is at zero, then we'll add it to end of our list
            //
            for (int i = 0; i < node.getParentCount(); i++) {
                AbstractPlanNode parent = node.getParent(i);
                int remaining = child_cnts.get(parent) - 1;
                child_cnts.put(parent, remaining);
                if (remaining == 0) {
                    execute_list.add(parent);
                }
            }
        }

        //
        // Important! Make sure that our list has the same number of entries in our tree
        //
        if (m_list.size() != m_planNodes.size()) {
            throw new Exception("ERROR: The execution list has '" + m_list.size() + "' PlanNodes but our original tree has '" + m_planNodes.size() + "' PlanNode entries");
        }
        return;
    }

    @Override
    public int compareTo(PlanNodeList o) {
        if (m_list.size() != o.m_list.size()) return -1;

        int diff = getRootPlanNode().compareTo(o.getRootPlanNode());
        if (diff != 0) return diff;

        for (int i = 0; i < m_list.size(); i++) {
            diff = m_list.get(i).m_id - o.m_list.get(i).m_id;
            if (diff != 0) return diff;
        }

        return 0;
    }

    @Override
    public String toJSONString() {
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object();
            super.toJSONString(stringer);

            stringer.key(Members.EXECUTE_LIST.name()).array();
            for (AbstractPlanNode node : m_list) {
                stringer.value(node.getPlanNodeId().intValue());
            }
            stringer.endArray(); //end execution list

            stringer.endObject(); //end PlanNodeList
        } catch (JSONException e) {
            // HACK ugly ugly to make the JSON handling
            // in QueryPlanner generate a JSONException for a plan we know
            // here that we can't serialize.  Making this method throw
            // JSONException pushes that exception deep into the bowels of
            // Volt with no good place to catch it and handle the error.
            // Consider this the coward's way out.
            return "This JSON error message is a lie";
        }
        return stringer.toString();
    }

    public String toDOTString(String name) {
        StringBuilder sb = new StringBuilder();
        sb.append("digraph ").append(name).append(" {\n");

        for (AbstractPlanNode node : m_list) {
                sb.append(node.toDOTString());
        }

        sb.append("\n}\n");
        return sb.toString();
    }
}
