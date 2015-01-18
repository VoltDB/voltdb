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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltcore.utils.Pair;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.types.PlanNodeType;

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

        if (m_parameters.size() > 0) {
            stringer.key(Members.PARAMETERS.name()).array();
            for (Pair< Integer, VoltType > parameter : m_parameters) {
                stringer.array().value(parameter.getFirst()).value(parameter.getSecond().name()).endArray();
            }
            stringer.endArray();
        }
    }

    public List<AbstractPlanNode> getNodeList() {
        return m_planNodes;
    }

    public void loadFromJSONArray( JSONArray jArray, Database db )  {
        int size = jArray.length();

        try {
            for( int i = 0; i < size; i++ ) {
                JSONObject jobj;
                jobj = jArray.getJSONObject(i);
                String nodeTypeStr = jobj.getString("PLAN_NODE_TYPE");
                PlanNodeType nodeType = PlanNodeType.get( nodeTypeStr );
                AbstractPlanNode apn = null;
                try {
                    apn = nodeType.getPlanNodeClass().newInstance();
                } catch (InstantiationException e) {
                    System.err.println( e.getMessage() );
                    e.printStackTrace();
                    return;
                } catch (IllegalAccessException e) {
                    System.err.println( e.getMessage() );
                    e.printStackTrace();
                    return;
                }
                apn.loadFromJSONObject(jobj, db);
                m_planNodes.add(apn);
            }
            //link children and parents
            for( int i = 0; i < size; i++ ) {
                JSONObject jobj;
                jobj = jArray.getJSONObject(i);
                if (jobj.has("CHILDREN_IDS")) {
                    JSONArray children = jobj.getJSONArray("CHILDREN_IDS");
                    for( int j = 0; j < children.length(); j++ ) {
                        m_planNodes.get(i).addAndLinkChild( getNodeofId( children.getInt(j) ) );
                    }
                }
            }
        }
        catch (JSONException e) {
            e.printStackTrace();
        }
    }

    public AbstractPlanNode getNodeofId ( int ID ) {
        int size = m_planNodes.size();
        for( int i = 0; i < size; i++ ) {
            if( m_planNodes.get(i).getPlanNodeId() == ID ) {
                return m_planNodes.get(i);
            }
        }
        return null;
    }
}
