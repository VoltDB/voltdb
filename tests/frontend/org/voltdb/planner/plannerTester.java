package org.voltdb.planner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.eclipse.jetty.util.ajax.JSON;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.PlanNodeTree;
import org.voltdb.types.PlanNodeType;

public class plannerTester {
	private static PlannerTestAideDeCamp aide;
	private static String m_ddlFile = "testplans-plannerTester-ddl.sql";
	private static String m_stmtFile = "testplans-plannerTester-ddl.stmt";
	private static ArrayList<String> m_stmts = new ArrayList<String>();
	private static int m_treeSizeDiff;
	private static boolean m_changedSQL;
	
	private static class Pair {
	    private Object first; //first member of pair
	    private Object second; //second member of pair

	    public Pair(Object first, Object second) {
	        this.first = first;
	        this.second = second;
	    }
	    
	    public void set( Object first, Object second ) {
	    	this.first = first;
	        this.second = second;
	    }
	    
	    public Object getFirst( ) {
	    	return first;
	    }
	    
	    public Object getSecond( ) {
	    	return second;
	    }
	    
	    public void setFirst( Object first ) {
	    	this.first = first;
	    }
	    
	    public void setSecond( Object second ) {
	    	this.second = second;
	    }
	    
	    public String toString() {
	    	if( first == null )
	    		first = "[]";
	    	if( second == null )
	    		second = "[]";
	    	return "("+first.toString()+","+second.toString()+")";
	    }
	    
	    public boolean equals() {
	    	return first.equals(second);
	    }
	}
    //private Pair m_intPair = new Pair(0,0);

    public static void setUpSchema( String ddl, String basename ) throws Exception {
        aide = new PlannerTestAideDeCamp(TestIndexSelection.class.getResource(ddl),
        		basename);

        // Set all tables to non-replicated.
        Cluster cluster = aide.getCatalog().getClusters().get("cluster");
        CatalogMap<Table> tmap = cluster.getDatabases().get("database").getTables();
        for (Table t : tmap) {
            t.setIsreplicated(false);
        }
    }
    
    public static void loadStmts ( String stmtFile ) throws IOException {
    	m_stmts.clear();
    	String line = null;
    	BufferedReader reader = new BufferedReader( new FileReader( TestIndexSelection.class.getResource(stmtFile).getPath() ) );
    	while( ( line = reader.readLine() ) != null ) {
    		m_stmts.add( line );
    	}
    }
    
   
    public static void batchCompileSave( String ddl, String basename, String stmtFilePath, String savePath ) throws Exception {
    	setUpSchema( ddl, basename );
    	loadStmts( stmtFilePath );
    	int size = m_stmts.size();
    	for( int i = 0; i < size; i++ ) {
    		 //assumes single partition
    		AbstractPlanNode pn = compile( m_stmts.get(i), 0, true);
    		writePlanToFile(pn, savePath+i );
    	}
    }

    protected void tearDown() throws Exception {
        aide.tearDown();
    }
    
	public static AbstractPlanNode compile( String sql, int paramCount,
            boolean singlePartition ) throws Exception {
		List<AbstractPlanNode> pn = null;
        pn =  aide.compile(sql, paramCount, singlePartition);
        return pn.get(0);
	}
	
	public static void writePlanToFile( AbstractPlanNode pn, String path ) {
		if( pn == null ) {
			System.err.println("the plan node is null, nothing to write");
			System.exit(-1);
		}
		PlanNodeTree pnt = new PlanNodeTree( pn );
		String prettyJson = pnt.toJSONString();
        try {
		    	BufferedWriter writer = new BufferedWriter( new FileWriter( path ) );
		    	writer.write( prettyJson );
		    	writer.flush();
		    	writer.close();
	   	} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static PlanNodeTree loadPlanFromFile( String path ) {
		PlanNodeTree pnt = new PlanNodeTree();
		String prettyJson = "";
		String line = null;
		 try {
				BufferedReader reader = new BufferedReader( new FileReader( path ));
				while( (line = reader.readLine() ) != null ){
					line = line.trim();
					prettyJson += line;
				}
			}
	        catch (IOException e) {
	    		e.printStackTrace();
	    	}
			JSONObject jobj;
			try {
				jobj = new JSONObject( prettyJson );
				JSONArray jarray = 	jobj.getJSONArray("PLAN_NODES");
				pnt.loadFromJSONArray(jarray);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		return pnt;
	}
	
	//parameters : path to baseline and the new plans 
	//size : number of total files in the baseline directory
	public static void batchDiff( String pathBaseline, String pathNew, int size ) {
		PlanNodeTree pnt1 = null;
		PlanNodeTree pnt2 = null;
		for( int i = 0; i < size; i++ ){
			System.out.println("Statement "+i+":");
			pnt1 = loadPlanFromFile( pathBaseline+i );
			pnt2  = loadPlanFromFile( pathNew+i );
			AbstractPlanNode pn1 = pnt1.getRootPlanNode();
			AbstractPlanNode pn2 = pnt2.getRootPlanNode();
			diffLeaves( pn1, pn2 );
			diffInlineNodes( pn1, pn2);
		}
	}
	
	public static void diffInlineNodes( AbstractPlanNode oldpn1, AbstractPlanNode newpn2 ) {
		m_treeSizeDiff = 0;
		ArrayList<AbstractPlanNode> list1 = oldpn1.getLists();
		ArrayList<AbstractPlanNode> list2 = newpn2.getLists();
		int size1 = list1.size();
		int size2 = list2.size();
		m_treeSizeDiff = size1 - size2;
		Pair intPair = new Pair(0,0);
		Pair stringPair = new Pair(null,null);
		if( size1 != size2 ) {
			intPair.set(size1, size2);
			System.out.println( "Plan tree size diff: " );
			System.out.println( intPair.toString() );
		}
		if( !m_changedSQL ){
			if( m_treeSizeDiff < 0 ){
				System.out.println( "Old plan might be better" );
			}
			else if( m_treeSizeDiff > 0 ) {
				System.out.println( "New plan might be better");
			}
		}
		Map<Integer, AbstractPlanNode> projNodes1 = new LinkedHashMap<Integer, AbstractPlanNode>();
		Map<Integer, AbstractPlanNode> projNodes2 = new LinkedHashMap<Integer, AbstractPlanNode>();
		Map<Integer, AbstractPlanNode> limitNodes1 = new LinkedHashMap<Integer, AbstractPlanNode>();
		Map<Integer, AbstractPlanNode> limitNodes2 = new LinkedHashMap<Integer, AbstractPlanNode>();
		Map<Integer, AbstractPlanNode> orderByNodes1 = new LinkedHashMap<Integer, AbstractPlanNode>();
		Map<Integer, AbstractPlanNode> orderByNodes2 = new LinkedHashMap<Integer, AbstractPlanNode>();
		Map<Integer, AbstractPlanNode> projInlineNodes1 = new LinkedHashMap<Integer, AbstractPlanNode>();
		Map<Integer, AbstractPlanNode> projInlineNodes2 = new LinkedHashMap<Integer, AbstractPlanNode>();
		Map<Integer, AbstractPlanNode> limitInlineNodes1 = new LinkedHashMap<Integer, AbstractPlanNode>();
		Map<Integer, AbstractPlanNode> limitInlineNodes2 = new LinkedHashMap<Integer, AbstractPlanNode>();
		Map<Integer, AbstractPlanNode> orderByInlineNodes1 = new LinkedHashMap<Integer, AbstractPlanNode>();
		Map<Integer, AbstractPlanNode> orderByInlineNodes2 = new LinkedHashMap<Integer, AbstractPlanNode>();
		for( int i = 0; i<size1; i++ ) {
			AbstractPlanNode pn = list1.get(i);
			int id = pn.getPlanNodeId();
			int pnTypeValue = pn.getPlanNodeType().getValue();
			if( pnTypeValue == PlanNodeType.PROJECTION.getValue() ){
				projNodes1.put( id, pn );
			}
			else if( pnTypeValue == PlanNodeType.LIMIT.getValue() ) {
				limitNodes1.put( id, pn );
			}
			else if( pnTypeValue == PlanNodeType.ORDERBY.getValue() ) {
				orderByNodes1.put( id, pn );
			}
			//get the inlinenodes
			if( pn.getInlinePlanNode(PlanNodeType.PROJECTION) != null ) {
				projInlineNodes1.put(id, pn.getInlinePlanNode(PlanNodeType.PROJECTION ));
			} 
			if( pn.getInlinePlanNode(PlanNodeType.LIMIT) != null) {
				limitInlineNodes1.put(id, pn.getInlinePlanNode(PlanNodeType.LIMIT));
			}
			if( pn.getInlinePlanNode(PlanNodeType.ORDERBY) != null) {
				orderByInlineNodes1.put(id, pn.getInlinePlanNode(PlanNodeType.ORDERBY));
			}
		}
		for( int i = 0; i<size2; i++ ) {
			AbstractPlanNode pn = list2.get(i);
			int id = pn.getPlanNodeId();
			int pnTypeValue = pn.getPlanNodeType().getValue();
			if( pnTypeValue == PlanNodeType.PROJECTION.getValue() ){
				projNodes2.put( id, pn );
			}
			else if( pnTypeValue == PlanNodeType.LIMIT.getValue() ) {
				limitNodes2.put( id, pn );
			}
			else if( pnTypeValue == PlanNodeType.ORDERBY.getValue() ) {
				orderByNodes2.put( id, pn );
			}
			
			//get the inlinenodes
			if( pn.getInlinePlanNode(PlanNodeType.PROJECTION) != null ) {
				projInlineNodes2.put(id, pn.getInlinePlanNode(PlanNodeType.PROJECTION ));
			} 
			if( pn.getInlinePlanNode(PlanNodeType.LIMIT) != null) {
				limitInlineNodes2.put(id, pn.getInlinePlanNode(PlanNodeType.LIMIT));
			}
			if( pn.getInlinePlanNode(PlanNodeType.ORDERBY) != null) {
				orderByInlineNodes2.put(id, pn.getInlinePlanNode(PlanNodeType.ORDERBY));
			}
		}
		//do the diff
		ArrayList<Integer> indexList = new ArrayList<Integer>();
		for( int index: projInlineNodes1.keySet() ) {
			indexList.add(index);
		}
		stringPair.setFirst(indexList.clone());
		indexList.clear();
		for( int index: projInlineNodes2.keySet() ) {
			indexList.add(index);
		}
		stringPair.setSecond(indexList.clone());
		if( !stringPair.equals() ){
			System.out.println( "Inline Projection Nodes diff: ");
			System.out.println( stringPair.toString() );
		}
		indexList.clear();
		
		for( int index: limitInlineNodes1.keySet() ) {
			indexList.add(index);
		}
		stringPair.setFirst(indexList.clone());
		indexList.clear();
		for( int index: limitInlineNodes2.keySet() ) {
			indexList.add(index);
		}
		stringPair.setSecond(indexList.clone());
		if( !stringPair.equals() ) {
			System.out.println( "Inline Limit Nodes diff: ");
			System.out.println( stringPair.toString() );
		}
		indexList.clear();
		
		for( int index: orderByInlineNodes1.keySet() ) {
			indexList.add(index);
		}
		stringPair.setFirst(indexList.clone());
		indexList.clear();
		for( int index: orderByInlineNodes2.keySet() ) {
			indexList.add(index);
		}
		stringPair.setSecond(indexList.clone());
		if( !stringPair.equals() ) {
			System.out.println( "Inline Order By Node diff: " );
			System.out.println( stringPair.toString() );
		}
		indexList.clear();
		
		//non-inline proj limit order by nodes
		for( int index: projNodes1.keySet() ) {
			indexList.add(index);
		}
		stringPair.setFirst(indexList.clone());
		indexList.clear();
		for( int index: projNodes2.keySet() ) {
			indexList.add(index);
		}
		stringPair.setSecond(indexList.clone());
		if( !stringPair.equals() ){
			System.out.println( "Projection Node diff: " );
			System.out.println( stringPair.toString() );
		}
		indexList.clear();
		
		for( int index: limitNodes1.keySet() ) {
			indexList.add(index);
		}
		stringPair.setFirst(indexList.clone());
		indexList.clear();
		for( int index: limitNodes2.keySet() ) {
			indexList.add(index);
		}
		stringPair.setSecond(indexList.clone());
		if( !stringPair.equals() ){
			System.out.println( "Limit Node diff: " );
			System.out.println( stringPair.toString() );
		}
		indexList.clear();
		
		for( int index: orderByNodes1.keySet() ) {
			indexList.add(index);
		}
		stringPair.setFirst(indexList.clone());
		indexList.clear();
		for( int index: orderByNodes2.keySet() ) {
			indexList.add(index);
		}
		stringPair.setSecond(indexList.clone());
		if( !stringPair.equals() ){
			System.out.println( "Order By Node diff:" );
			System.out.println( stringPair.toString() );
		}
		indexList.clear();
	}
	
	public static void diffLeaves( AbstractPlanNode oldpn1, AbstractPlanNode newpn2 ){
		m_changedSQL = false;
		ArrayList<AbstractPlanNode> list1 = oldpn1.getLeafLists();
		ArrayList<AbstractPlanNode> list2 = newpn2.getLeafLists();
		int size1 = list1.size();
		int size2 = list2.size();
		int max = Math.max(size1, size2);
		int min = Math.min(size1, size2);
		Pair intPair = new Pair(0,0);
		Pair stringPair = new Pair("", "");
		if( size1 != size2 ){
			intPair.set(size1, size2);
			System.out.println( "Leaf size diff : " );
			System.out.println( intPair );
			System.out.println( "SQLSTMT might be changed" );
			m_changedSQL = true;
			try {
				for( int i = 0; i < min; i++ ) {
					JSONObject j1 = new JSONObject( list1.get(i).toJSONString() );
					JSONObject j2 = new JSONObject( list2.get(i).toJSONString() );
					String table1 = j1.getString("TARGET_TABLE_NAME");
					String table2 = j2.getString("TARGET_TABLE_NAME");
					String nodeType1 = j1.getString("PLAN_NODE_TYPE");
					String nodeType2 = j2.getString("PLAN_NODE_TYPE");
					if( !table1.equalsIgnoreCase(table2) ) {
						stringPair.set( nodeType1+" at "+table1, nodeType2+" at "+table2 );
						System.out.println("Table diff at leaf "+i+":");
						System.out.println( stringPair );
					}
					else if( !nodeType1.equalsIgnoreCase(nodeType2) ) {
						stringPair.set(nodeType1+" at "+table1, nodeType2+" at "+table2);
						System.out.println("Scan diff at leaf "+i+" :");
						System.out.println( stringPair );
					}
					else if ( nodeType1.equalsIgnoreCase("INDEXSCAN") ) {
						String index1 = j1.getString("TARGET_INDEX_NAME");
						String index2 = j2.getString("TARGET_INDEX_NAME");
						stringPair.set( index1, index2);
						if( !index1.equalsIgnoreCase(index2) ) {
							System.out.println("Index diff at leaf "+i+" :");
							System.out.println(stringPair);
						}
					}
					else//either index scan using same index or seqscan on same table 
						System.out.println("Same at leaf "+i);
				}
				//lists size are different
				if( size2 < max ) {
					for( int i = min; i < max; i++ ) {
						JSONObject j = new JSONObject( list1.get(i).toJSONString() );
						String table = j.getString("TARGET_TABLE_NAME");
						String nodeType = j.getString("PLAN_NODE_TYPE");
						String index = null;
						if( nodeType.equalsIgnoreCase("INDEXSCAN") ) {
						  index = j.getString("TARGET_INDEX_NAME");
						}
						if( index != null ) {
							stringPair.set(nodeType+" on "+table+" using "+index, "Empty" );
							System.out.println("Diff at leaf "+i+" :");
							System.out.println(stringPair.toString());
						}
						else
						{
							stringPair.set(nodeType+" on "+table, "Empty" );
							System.out.println("Diff at leaf "+i+": ");
							System.out.println(stringPair.toString());
						}
		 			}
				}
				else if( size1 < max ) {
					for( int i = min; i < max; i++ ) {
						JSONObject j = new JSONObject( list2.get(i).toJSONString() );
						String table = j.getString("TARGET_TABLE_NAME");
						String nodeType = j.getString("PLAN_NODE_TYPE");
						String index = null;
						if( nodeType.equalsIgnoreCase("INDEXSCAN") ) {
						  index = j.getString("TARGET_INDEX_NAME");
						  if( index != null ) {
								stringPair.set(nodeType+" on "+table+" using "+index, "Empty" );
								System.out.println("Diff at leaf "+i+" :");
								System.out.println(stringPair.toString());
							}
							else
							{
								stringPair.set(nodeType+" on "+table, "Empty" );
								System.out.println("Diff at leaf "+i+": ");
								System.out.println(stringPair.toString());
							}
		 			}
				}
				}
			}
			catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		else {
			System.out.println("same leaf size");
			try{
				if( max == 1 ) {
					System.out.println("Single table query");
					JSONObject j1 = new JSONObject( list1.get(0).toJSONString() );
					JSONObject j2 = new JSONObject( list2.get(0).toJSONString() );
					String table1 = j1.getString("TARGET_TABLE_NAME");
					String table2 = j2.getString("TARGET_TABLE_NAME");
					String nodeType1 = j1.getString("PLAN_NODE_TYPE");
					String nodeType2 = j2.getString("PLAN_NODE_TYPE");
					if( !table1.equalsIgnoreCase(table2) ){
						stringPair.set(nodeType1+" on "+table1, nodeType2+" on "+table2 );
						System.out.println("Diff table at leaf"+0+" :");
						System.out.println(stringPair);	
					}
					else if( !nodeType1.equalsIgnoreCase(nodeType2) ) {
						stringPair.set(nodeType1+" on "+table1, nodeType2+" on "+table2 );
						System.out.println("Diff scan at leaf"+0+" :");
						System.out.println(stringPair);
					}
					else if ( nodeType1.equalsIgnoreCase("INDEXSCAN") ) {
						String index1 = j1.getString("TARGET_INDEX_NAME");
						String index2 = j2.getString("TARGET_INDEX_NAME");
						if( !index1.equalsIgnoreCase(index2) ){
							stringPair.set(nodeType1+" on "+table1+" using "+index1, nodeType2+" on "+table2+" using "+index2 );
							System.out.println("Diff index at leaf"+0+": ");
							System.out.println(stringPair);
						}
					}
					else
						System.out.println("Same at "+0);
				}
				else {
					System.out.println("Join query");
					for( int i = 0; i < max; i++ ) {
						JSONObject j1 = new JSONObject( list1.get(i).toJSONString() );
						JSONObject j2 = new JSONObject( list2.get(i).toJSONString() );
						String table1 = j1.getString("TARGET_TABLE_NAME");
						String table2 = j2.getString("TARGET_TABLE_NAME");
						String nodeType1 = j1.getString("PLAN_NODE_TYPE");
						String nodeType2 = j2.getString("PLAN_NODE_TYPE");
						if( !table1.equalsIgnoreCase(table2) ){
							stringPair.set(nodeType1+" on "+table1, nodeType2+" on "+table2 );
							System.out.println("Diff table at leaf"+i+" :");
							System.out.println(stringPair);	
						}
						else if( !nodeType1.equalsIgnoreCase(nodeType2) ) {
							stringPair.set(nodeType1+" on "+table1, nodeType2+" on "+table2 );
							System.out.println("Diff scan at leaf"+0+" :");
							System.out.println(stringPair);	
						}
						else if ( nodeType1.equalsIgnoreCase("INDEXSCAN") ) {
							String index1 = j1.getString("TARGET_INDEX_NAME");
							String index2 = j2.getString("TARGET_INDEX_NAME");
								if( !index1.equalsIgnoreCase(index2) ){
									stringPair.set(nodeType1+" on "+table1+" using "+index1, nodeType2+" on "+table2+" using "+index2 );
									System.out.println("Diff index at leaf"+0+": ");
									System.out.println(stringPair);
								}
						}
					else
						System.out.println("Same at leaf"+i);
				}
			}
			}
			catch ( JSONException e ) {
				e.printStackTrace();
			}
		}	
	}
}
