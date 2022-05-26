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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;

/**
 *
 */
public class PlanNodeList implements JSONString, Comparable<PlanNodeList> {
    private static final String EXECUTE_LIST_MEMBER_NAME = "EXECUTE_LIST";
    private static final String EXECUTE_LISTS_MEMBER_NAME = "EXECUTE_LISTS";
    private static final String IS_LARGE_QUERY_MEMBER_NAME = "IS_LARGE_QUERY";

    protected PlanNodeTree m_tree;
    protected List<List<AbstractPlanNode>> m_executeLists = new ArrayList<>();
    protected boolean m_isLargeQuery = false;

    public PlanNodeList() {
        super();
    }

    public PlanNodeList(PlanNodeTree tree, boolean isLargeQuery) {
        m_tree = tree;
        try {
            // Construct execute lists for all sub statement
            for (List<AbstractPlanNode> nodeList : m_tree.m_planNodesListMap.values()) {
                List<AbstractPlanNode> list = constructList(nodeList);
                m_executeLists.add(list);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        m_isLargeQuery = isLargeQuery;
    }

    public PlanNodeList(AbstractPlanNode root_node, boolean isLargeQuery) {
        this(new PlanNodeTree(root_node), isLargeQuery);
    }

    public List<AbstractPlanNode> getExecutionList() {
        assert(!m_executeLists.isEmpty());
        return m_executeLists.get(0);
    }

    public List<AbstractPlanNode> getExecutionList(int idx) {
        assert(idx < m_executeLists.size());
        return m_executeLists.get(idx);
    }

    public AbstractPlanNode getRootPlanNode() {
        return m_tree.getRootPlanNode();
    }

    @Override
    public String toString() {
        StringBuilder ret = new StringBuilder("EXECUTE LISTS: " + m_tree.m_planNodesListMap.size() + " lists\n");
        for (Map.Entry<Integer, List<AbstractPlanNode>> entry : m_tree.m_planNodesListMap.entrySet()) {
            List<AbstractPlanNode> nodeList = entry.getValue();
            ret = new StringBuilder("\tEXECUTE LIST id:" + entry.getKey() + " ," + nodeList.size() + " nodes\n");
            for (int ctr = 0, cnt = nodeList.size(); ctr < cnt; ctr++) {
                ret.append("   [").append(ctr).append("] ").append(nodeList.get(ctr)).append("\n");
            }
            ret.append(nodeList.get(0).toString());
        }
        return ret.toString();
    }

    public List<AbstractPlanNode> constructList(List<AbstractPlanNode> planNodes) throws Exception {
        //
        // Create a counter for each node based on the # of children that it has
        // If any node has no children, put it in the execute list
        //
        List<AbstractPlanNode> execute_list = Collections.synchronizedList(new ArrayList<AbstractPlanNode>());
        Map<AbstractPlanNode, Integer> child_cnts = new HashMap<>();
        for (AbstractPlanNode node : planNodes) {
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
        List<AbstractPlanNode> list = new ArrayList<>();
        while (!execute_list.isEmpty()) {
            AbstractPlanNode node = execute_list.remove(0);
            //
            // Add the node to our execution list
            //
            list.add(node);
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
        if (list.size() != planNodes.size()) {
            throw new Exception("ERROR: The execution list has '" + list.size() +
                    "' PlanNodes but our original tree has '" + planNodes.size() + "' PlanNode entries");
        }
        return list;
    }

    @Override
    public int compareTo(PlanNodeList o) {
        if (m_executeLists.size() != o.m_executeLists.size()) return -1;
        int size = m_executeLists.size();
        for (int idx = 0; idx < size; ++idx) {
            List<AbstractPlanNode> list = m_executeLists.get(idx);
            List<AbstractPlanNode> olist = o.m_executeLists.get(idx);
            if (list.size() != olist.size()) return -1;

            int diff = getRootPlanNode().compareTo(o.getRootPlanNode());
            if (diff != 0) return diff;

            for (int i = 0; i < list.size(); i++) {
                diff = list.get(i).m_id - olist.get(i).m_id;
                if (diff != 0) return diff;
            }
        }
        return 0;
    }

    @Override
    public String toJSONString() {
        try {
            JSONStringer stringer = new JSONStringer();
            stringer.object();
            m_tree.toJSONString(stringer);

            if (m_executeLists.size() == 1) {
                stringer.key(EXECUTE_LIST_MEMBER_NAME).array();
                List<AbstractPlanNode> list = m_executeLists.get(0);
                for (AbstractPlanNode node : list) {
                    stringer.value(node.getPlanNodeId().intValue());
                }
                stringer.endArray(); //end execution list
            } else {
                stringer.key(EXECUTE_LISTS_MEMBER_NAME).array();
                for (List<AbstractPlanNode> list : m_executeLists) {
                    stringer.object().key(EXECUTE_LIST_MEMBER_NAME).array();
                    for (AbstractPlanNode node : list) {
                        stringer.value(node.getPlanNodeId().intValue());
                    }
                    stringer.endArray().endObject(); //end execution list
                }
                stringer.endArray(); //end execution list
            }

            stringer.keySymbolValuePair(IS_LARGE_QUERY_MEMBER_NAME, m_isLargeQuery);

            stringer.endObject(); //end PlanNodeList
            return stringer.toString();
        } catch (JSONException e) {
            // HACK ugly ugly to make the JSON handling
            // in QueryPlanner generate a JSONException for a plan we know
            // here that we can't serialize.  Making this method throw
            // JSONException pushes that exception deep into the bowels of
            // Volt with no good place to catch it and handle the error.
            // Consider this the coward's way out.
            return "This JSON error message is a lie";
        }
    }

    public String toDOTString(String name) {
        StringBuilder sb = new StringBuilder();
        sb.append("digraph ").append(name).append(" {\n");

        for (List<AbstractPlanNode> list : m_executeLists) {
            for (AbstractPlanNode node : list) {
                sb.append(node.toDOTString());
            }
            sb.append('\n');
        }
        //TODO ENG-451-exists add subqueries
        sb.append("\n}\n");
        return sb.toString();
    }
}
