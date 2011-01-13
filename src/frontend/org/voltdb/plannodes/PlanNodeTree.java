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

package org.voltdb.plannodes;

import java.util.*;
import org.voltdb.VoltType;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltdb.utils.Pair;

/**
 *
 */
public class PlanNodeTree implements JSONString {

    public enum Members {
        PLAN_NODES,
        PARAMETERS;
    }

    protected final List<AbstractPlanNode> m_planNodes;
    protected final Map<Integer, AbstractPlanNode> m_idToNodeMap = new HashMap<Integer, AbstractPlanNode>();
    protected final List< Pair< Integer, VoltType > > m_parameters = new ArrayList< Pair< Integer, VoltType > >();

    public PlanNodeTree() {
        m_planNodes = new ArrayList<AbstractPlanNode>();
    }

    public PlanNodeTree(AbstractPlanNode root_node) {
        this();
        try {
            constructTree(root_node);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Integer getRootPlanNodeId() {
        return m_planNodes.get(0).getPlanNodeId();
    }

    public AbstractPlanNode getRootPlanNode() {
        return m_planNodes.get(0);
    }

    public Boolean constructTree(AbstractPlanNode node) throws Exception {
        m_planNodes.add(node);
        m_idToNodeMap.put(node.getPlanNodeId(), node);
        for (int i = 0; i < node.getChildCount(); i++) {
            AbstractPlanNode child = node.getChild(i);
            if (!constructTree(child)) {
                return false;
            }
        }
        return true;
    }

    public List< Pair< Integer, VoltType > > getParameters() {
        return m_parameters;
    }

    public void setParameters(List< Pair< Integer, VoltType > > parameters) {
        m_parameters.clear();
        m_parameters.addAll(parameters);
    }

    @Override
    public String toJSONString() {
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object();
            toJSONString(stringer);
            stringer.endObject();
        } catch (JSONException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return stringer.toString();
    }

    public void toJSONString(JSONStringer stringer) throws JSONException {
        stringer.key(Members.PLAN_NODES.name()).array();
        for (AbstractPlanNode node : m_planNodes) {
            assert (node instanceof JSONString);
            stringer.value(node);
        }
        stringer.endArray(); //end entries

        stringer.key(Members.PARAMETERS.name()).array();
        for (Pair< Integer, VoltType > parameter : m_parameters) {
            stringer.array().value(parameter.getFirst()).value(parameter.getSecond().name()).endArray();
        }
        stringer.endArray();
    }
}
