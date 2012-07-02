package org.voltdb.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.eclipse.jetty.util.ajax.JSON;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.types.PlanNodeType;

public class plannerTester {
	public static void writeToFile( AbstractPlanNode pn, String path ) {
		if( pn == null ) {
			System.err.println("the plan node is null, nothing to write");
			System.exit(-1);
		}
	//	while( pn.getChild(index))
	}
	
	
	public static void diffLeaves( AbstractPlanNode oldpn1, AbstractPlanNode newpn2 ){
		ArrayList<AbstractPlanNode> list1 = oldpn1.getLeafLists();
		ArrayList<AbstractPlanNode> list2 = newpn2.getLeafLists();
		int size1 = list1.size();
		int size2 = list2.size();
		int max = Math.max(size1, size2);
		int min = Math.min(size1, size2);
		if( size1 != size2 ){
			System.out.println( "Different time of scan used, used to be: "+size1+", now is: "+size2 );
		}
		try {
		for( int i = 0; i < min; i++ ) {
			JSONObject j1 = new JSONObject( list1.get(i).toJSONString() );
			JSONObject j2 = new JSONObject( list2.get(i).toJSONString() );
			String table1 = j1.getString("TARGET_TABLE_NAME");
			String table2 = j2.getString("TARGET_TABLE_NAME");
			String nodeType1 = j1.getString("PLAN_NODE_TYPE");
			String nodeType2 = j2.getString("PLAN_NODE_TYPE");
			if( !table1.equalsIgnoreCase(table2) )
				System.out.println("Different table at "+i+" used to be: "+nodeType1+" at "+table1+", now is: "+nodeType2+" at "+table2);
			else if( !nodeType1.equalsIgnoreCase(nodeType2) ) {
				System.out.println("Different scan at "+i+" used to be: "+nodeType1+", now is: "+nodeType2);
			}
			else if ( nodeType1.equalsIgnoreCase("INDEXSCAN") ) {
				String index1 = j1.getString("TARGET_INDEX_NAME");
				String index2 = j2.getString("TARGET_INDEX_NAME");
				if( !index1.equalsIgnoreCase(index2) )
					System.out.println("Different index at "+i+" used to be: "+index1+", now is: "+index2);
			}
			else
				System.out.println("Same at "+i);
		}
		//lists size are different
		if( size2 < max ) {
			for( int i = min; i < max; i++ ) {
				JSONObject j = new JSONObject( list1.get(i).toJSONString() );
				String table = j.getString("TARGET_TABLE_NAME");
				String nodeType = j.getString("PLAN_NODE_TYPE");
				String index = null;
				if( nodeType.equalsIgnoreCase("INDEXSCAN") )
				  index = j.getString("TARGET_INDEX_NAME");
				System.out.println("Different at "+i+" used to be table: "+table+" type :"+nodeType+" " +
						" index: "+index+" now is empty");
 			}
		}
		else if( size1 < max ) {
			for( int i = min; i < max; i++ ) {
				JSONObject j = new JSONObject( list2.get(i).toJSONString() );
				String table = j.getString("TARGET_TABLE_NAME");
				String nodeType = j.getString("PLAN_NODE_TYPE");
				String index = null;
				if( nodeType.equalsIgnoreCase("INDEXSCAN") )
				  index = j.getString("TARGET_INDEX_NAME");
				System.out.println("Different at "+i+" used to be empty, now is table: "+table+" type :"+nodeType+" " +
						" index: "+index);
 			}
		}
		else 
			System.out.println("same size");
		
		}
		catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}
}
