/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
import org.voltdb.types.PlanNodeType;
import org.apache.tools.ant.types.resources.Union;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltcore.utils.Pair;

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
    
    public List<AbstractPlanNode> getNodeList() {
    	return m_planNodes;
    }
    
    public void loadFromJSONArray( JSONArray jArray ) {
    	int size = jArray.length();
    	
		try {
			for( int i = 0; i < size; i++ ) {
		    	JSONObject jobj;
				jobj = jArray.getJSONObject(i);
				String nodeType = jobj.getString("PLAN_NODE_TYPE");
				int nodeTypeInt = PlanNodeType.get( nodeType ).getValue();
	    		AbstractPlanNode apn = null;
	    		
	    		if( nodeTypeInt == PlanNodeType.AGGREGATE.getValue() ) {
	    			apn = new AggregatePlanNode();
	    		}
	    		else if( nodeTypeInt == PlanNodeType.DELETE.getValue() ) {
	    			apn = new DeletePlanNode();
	    		}
	    		else if( nodeTypeInt == PlanNodeType.DISTINCT.getValue() ) {
	    			apn = new DistinctPlanNode();
	    		}
	    		else if( nodeTypeInt == PlanNodeType.HASHAGGREGATE.getValue() ) {
	    			apn = new HashAggregatePlanNode();
	    		}
	    		else if( nodeTypeInt == PlanNodeType.INDEXSCAN.getValue() ) {
	    			apn = new IndexScanPlanNode();
	    		}
	    		else if( nodeTypeInt == PlanNodeType.SEQSCAN.getValue() ) {
	    			apn = new SeqScanPlanNode();
	    		}
	    		else if( nodeTypeInt == PlanNodeType.INSERT.getValue() ) {
	    			apn = new InsertPlanNode();
	    		}
	    		else if( nodeTypeInt == PlanNodeType.LIMIT.getValue() ) {
	    			apn = new LimitPlanNode();
	    		}
	    		else if( nodeTypeInt == PlanNodeType.MATERIALIZE.getValue() ) {
	    			apn = new MaterializePlanNode();
	    		}
	    		else if( nodeTypeInt == PlanNodeType.NESTLOOP.getValue() ) {
	    			apn = new NestLoopPlanNode();
	    		}
	    		else if( nodeTypeInt == PlanNodeType.NESTLOOPINDEX.getValue() ) {
	    			apn = new NestLoopIndexPlanNode();
	    		}
	    		else if( nodeTypeInt == PlanNodeType.ORDERBY.getValue() ) {
	    			apn = new OrderByPlanNode();
	    		}
	    		else if( nodeTypeInt == PlanNodeType.PROJECTION.getValue() ) {
	    			apn = new ProjectionPlanNode();
	    		}
	    		else if( nodeTypeInt == PlanNodeType.RECEIVE.getValue() ) {
	    			apn = new ReceivePlanNode();
	    		}
	    		else if( nodeTypeInt == PlanNodeType.SEND.getValue() ) {
	    			apn = new SendPlanNode();
	    		}
	    		else if( nodeTypeInt == PlanNodeType.UNION.getValue() ) {
	    			apn = new UnionPlanNode();
	    		}
	    		else if( nodeTypeInt == PlanNodeType.UPDATE.getValue() ) {
	    			apn = new UpdatePlanNode();
	    		}
	    		else {
	    			System.err.println("plan node type not support: "+nodeType);
	    		}
	    		apn.loadFromJSONObject(jobj);
	    		m_planNodes.add(apn);
			}
			//link children and parents
			for( int i = 0; i < size; i++ ) {
				JSONObject jobj;
				jobj = jArray.getJSONObject(i);
				JSONArray children = jobj.getJSONArray("CHILDREN_IDS");
				for( int j = 0; j < children.length(); j++ ) {
					m_planNodes.get(i).addAndLinkChild( getNodeofId( children.getInt(j) ) );
				}
			}
    	}
		catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
    }
    
    public AbstractPlanNode getNodeofId ( int ID ) {
    	int size = m_planNodes.size();
    	for( int i = 0; i < size; i++ ) {
    		if( m_planNodes.get(i).getPlanNodeId() == ID )
    			return m_planNodes.get(i);
    	}
    	return null;
    }
}
