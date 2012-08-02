/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package org.voltdb.planner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.PlanNodeTree;
import org.voltdb.types.PlanNodeType;

public class plannerTester {
    private static PlannerTestAideDeCamp aide;
    private static ArrayList<String> m_config = new ArrayList<String>();
    private static String m_currentConfig;
    private static String m_testName;
    private static String m_pathRefPlan;
    private static String m_baseName;
    private static String m_pathDDL;
    private static int m_numSQL = 0;
    private static String m_savePlanPath;
    private static ArrayList<Pair> m_partitionColumns = new ArrayList<plannerTester.Pair>();
    private static ArrayList<String> m_stmts = new ArrayList<String>();
    private static ArrayList<String> m_stmtsBase = new ArrayList<String>();
    private static int m_treeSizeDiff;
    private static boolean m_changedSQL;
    private static boolean m_isCompileSave = false;
    private static boolean m_isDiff = false;
    private static boolean m_showExpainedPlan = false;
    private static boolean m_showSQLStatement = false;
    private static int m_numTest = 0;
    private static int m_numPass = 0;
    private static int m_numFail = 0;
    private static ArrayList<String> m_diffMessages = new ArrayList<String>();
    private static String m_reportDir = "/tmp/";
    private static BufferedWriter m_reportWriter;

    public static class Pair {
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

        @Override
        public String toString() {
            if( first == null ) {
                first = "[]";
            }
            if( second == null ) {
                second = "[]";
            }
            return "("+first.toString()+" => "+second.toString()+")";
        }

        public boolean equals() {
            return first.equals(second);
        }
    }

    public static void main( String[] args ) {
        int size = args.length;
        for( int i=0; i<size; i++ ) {
            String str = args[i];
            if( str.startsWith("-C=")) {
                m_config.add(str.split("=")[1]);
            }
            else if( str.startsWith("-cs") ) {
                m_isCompileSave = true;
            }
            else if( str.startsWith("-d") ) {
                m_isDiff = true;
            }
            else if( str.startsWith("-r") ){
                m_reportDir = str.split("=")[1];
            }
            else if( str.startsWith("-e") ){
                m_showExpainedPlan = true;
            }
            else if( str.startsWith("-s") ){
                m_showSQLStatement = true;
            }
        }
        size = m_config.size();
        if( m_isCompileSave ) {
            for( String config : m_config ) {
                try {
                    setUp( config );
                    batchCompileSave();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        if( m_isDiff ) {
            if( !new File(m_reportDir).exists() ) {
                new File(m_reportDir).mkdirs();
            }
            try {
                m_reportWriter = new BufferedWriter(new FileWriter( m_reportDir+"plannerTester.report" ));
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            for( String config : m_config ) {
                try {
                    setUp( config );
                    batchDiff( );
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            System.out.println("Test: "+m_numTest);
            System.out.println("Pass: "+m_numPass);
            System.out.println("Fail: "+m_numFail);
            try {
                m_reportWriter.write( "\nTest: "+m_numTest+"\n"
                        +"Pass: "+m_numPass+"\n"
                        +"Fail: "+m_numFail+"\n");
                m_reportWriter.flush();
                m_reportWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("Report file created at "+m_reportDir+"plannerTester.report");
        }
        if( m_numFail == 0 ) {
            System.exit(0);
        } else {
            System.exit(-1);
        }
    }

    public static void setUp( String pathConfigFile ) throws IOException {
        m_currentConfig = pathConfigFile;
        BufferedReader reader = new BufferedReader( new FileReader( pathConfigFile ) );
        String line = null;
        while( ( line = reader.readLine() ) != null ) {
            if( line.startsWith("#") ) {
                continue;
            }
            else if( line.equalsIgnoreCase("Name:") ) {
                line = reader.readLine();
                m_testName = line;
            }
            else if ( line.equalsIgnoreCase("Ref:") ) {
                line = reader.readLine();
                m_pathRefPlan = new File( line ).getCanonicalPath();
                m_pathRefPlan += "/";
            }
            else if( line.equalsIgnoreCase("DDL:")) {
                line = reader.readLine();
                m_pathDDL = new File( line ).getCanonicalPath();
            }
            else if( line.equalsIgnoreCase("Base Name:") ) {
                line = reader.readLine();
                m_baseName = line;
            }
            else if( line.equalsIgnoreCase("SQL:")) {
                m_stmts.clear();
                m_numSQL = 0;
                while( (line = reader.readLine()).length() > 6 ) {
                    if( line.startsWith("#") ) {
                        continue;
                    }
                    m_stmts.add( line );
                }
                m_numSQL = m_stmts.size();
            }
            else if( line.equalsIgnoreCase("Save Path:") ) {
                line = reader.readLine();
                m_savePlanPath = new File( line ).getCanonicalPath();
                m_savePlanPath += "/";
            }
            else if( line.equalsIgnoreCase("Partition Columns:") ) {
                line = reader.readLine();
                int index = line.indexOf(".");
                Pair p = new Pair( line.substring(0, index), line.substring(index+1) );
                m_partitionColumns.add( p );
            }
        }
        try {
            setUpSchema();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void setUp( String pathDDL, String baseName, String table, String column ) {
        Pair p = new Pair( table, column );
        m_partitionColumns.add(p);
        m_pathDDL = pathDDL;
        m_baseName = baseName;
        try {
            setUpSchema();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void setUpSchema() throws Exception {
        File ddlFile = new File(m_pathDDL);
        aide = new PlannerTestAideDeCamp(ddlFile.toURI().toURL(),
                m_baseName);
        // Set all tables to non-replicated.
        Cluster cluster = aide.getCatalog().getClusters().get("cluster");
        CatalogMap<Table> tmap = cluster.getDatabases().get("database").getTables();
        for (Table t : tmap) {
            String tableName = t.getTypeName();
            for( Pair p : m_partitionColumns ) {
                if( ((String)p.getFirst()).equalsIgnoreCase(tableName) ){
                    t.setIsreplicated(false);
                    Column column = t.getColumns().getIgnoreCase( (String)p.getSecond() );
                    t.setPartitioncolumn(column);
                    break;
                }
            }
        }
    }

    public static void setTestName ( String name ) {
        m_testName = name;
    }

    protected void tearDown() throws Exception {
        aide.tearDown();
    }

    public static List<AbstractPlanNode> compile( String sql, int paramCount,
            boolean singlePartition ) throws Exception {
        List<AbstractPlanNode> pnList = null;
        pnList =  aide.compile(sql, paramCount, singlePartition);
        return pnList;
    }

    public static AbstractPlanNode combinePlanNodes( List<AbstractPlanNode> pnList ) {
        int size = pnList.size();
        AbstractPlanNode pn = pnList.get(0);
        if( size == 1 ) {
            return pn;
        } else {
            PlanNodeTree pnt = new PlanNodeTree( pn );
            for( int i = 1; i < size; i++ ) {
                pnt.concatenate( pnList.get(i) );
            }
            return pnt.getRootPlanNode();
        }
    }

    public static void writePlanToFile( AbstractPlanNode pn, String pathToDir, String fileName, String sql) {
        if( pn == null ) {
            System.err.println("the plan node is null, nothing to write");
            System.exit(-1);
        }
        PlanNodeTree pnt = new PlanNodeTree( pn );
        String prettyJson = pnt.toJSONString();
        if( !new File(pathToDir).exists() ) {
            new File(pathToDir).mkdirs();
        }
        try {
            BufferedWriter writer = new BufferedWriter( new FileWriter( pathToDir+fileName ) );
            writer.write( sql );
            writer.write( "\n" );
            writer.write( prettyJson );
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static PlanNodeTree loadPlanFromFile( String path, ArrayList<String> getsql ) throws FileNotFoundException {
        PlanNodeTree pnt = new PlanNodeTree();
        String prettyJson = "";
        String line = null;
        BufferedReader reader = new BufferedReader( new FileReader( path ));
        try {
            getsql.add( reader.readLine() );
            while( (line = reader.readLine() ) != null ){
                line = line.trim();
                prettyJson += line;
            }
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        JSONObject jobj;
        try {
            jobj = new JSONObject( prettyJson );
            JSONArray jarray =  jobj.getJSONArray("PLAN_NODES");
            Database db = aide.getDatabase();
            pnt.loadFromJSONArray(jarray, db);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return pnt;
    }

    public static String getKeyInfo( ArrayList<AbstractPlanNode> pnList ) {
        String str="";
        for( AbstractPlanNode pn : pnList ) {
            str += "[";
            str += pn.getPlanNodeId();
            str += "-";
            str += pn.getPlanNodeType().toString();
            str += ", ";
        }
        if(str.length()>1)
        {
            str = str.substring(0, str.length()-2 );
            str += "]";
        }
        else{
            str += "[]";
        }
        return str;
    }

    public static ArrayList< AbstractPlanNode > getJoinNodes( ArrayList<AbstractPlanNode> pnlist ) {
        ArrayList< AbstractPlanNode > joinNodeList = new ArrayList<AbstractPlanNode>();

        for( AbstractPlanNode pn : pnlist ) {
            if( pn.getPlanNodeType().equals(PlanNodeType.NESTLOOP) ||
                    pn.getPlanNodeType().equals(PlanNodeType.NESTLOOPINDEX) ) {
                joinNodeList.add(pn);
            }
        }
        return joinNodeList;
    }

    public static void batchCompileSave( ) throws Exception {
        int size = m_stmts.size();
        for( int i = 0; i < size; i++ ) {
            //assume multi partition
            List<AbstractPlanNode> pnList = compile( m_stmts.get(i), 0, false);
            AbstractPlanNode pn = combinePlanNodes(pnList);
            writePlanToFile(pn, m_savePlanPath, m_testName+".plan"+i, m_stmts.get(i) );
        }
    }

    //parameters : path to baseline and the new plans
    //size : number of total files in the baseline directory
    public static void batchDiff( String pathBaseline, String pathNew, int size ) throws IOException {
        PlanNodeTree pnt1 = null;
        PlanNodeTree pnt2 = null;
        m_stmts.clear();
        m_stmtsBase.clear();
        for( int i = 0; i < size; i++ ){
            try {
                ArrayList<String> getsql = new ArrayList<String>();
                pnt1 = loadPlanFromFile( pathBaseline+m_testName+".plan"+i, getsql );
                m_stmtsBase.add( getsql.get(0) );
                getsql.clear();
                pnt2  = loadPlanFromFile( pathNew+m_testName+".plan"+i, getsql );
                m_stmts.add( getsql.get(0) );
            } catch (FileNotFoundException e) {
                System.err.println("Plan files in"+pathBaseline+" don't exist. Use -cs(batchCompileSave) to generate plans and copy base plans to baseline directory.");
            }
            AbstractPlanNode pn1 = pnt1.getRootPlanNode();
            AbstractPlanNode pn2 = pnt2.getRootPlanNode();

            if( diff( pn1, pn2, false ) ) {
                m_numPass++;
            } else {
                m_numFail++;
                //                              System.out.println("Statement "+i+" of "+m_testName+": ");
                m_reportWriter.write( "Statement "+i+" of "+m_testName+": \n" );

                for( String msg : m_diffMessages ) {
                    //                                  System.out.println(msg);
                    m_reportWriter.write( msg+"\n\n" );
                }
                //                              System.out.println();
                if( m_showSQLStatement ) {
                    //                                  System.out.println(m_stmtsBase.get(i));
                    //                                  System.out.println("==>");
                    //                                  System.out.println(m_stmts.get(i));
                    //                                  System.out.println();
                    m_reportWriter.write( "SQL statement:\n"+m_stmtsBase.get(i)+"\n==>\n"+m_stmts.get(i)+"\n");
                }

                if( m_showExpainedPlan ) {
                    //                                  System.out.print( pn1.toExplainPlanString() );
                    //                                  System.out.println( "==>");
                    //                                  System.out.println("");
                    //                                  System.out.println( pn2.toExplainPlanString() );
                    //                                  System.out.println();
                    m_reportWriter.write("\nExplained plan:\n"+pn1.toExplainPlanString()+"\n==>\n"+pn2.toExplainPlanString()+"\n");
                }

                //                              System.out.println("Path to the config file :"+m_currentConfig );
                //                              System.out.println("Path to the baseline file :"+pathBaseline+m_testName+".plan"+i );
                //                              System.out.println("Path to the baseline file :"+pathNew+m_testName+".plan"+i );
                m_reportWriter.write("Path to the config file :"+m_currentConfig+"\n"
                        +"Path to the baseline file :"+pathBaseline+m_testName+".plan"+i+"\n"
                        +"Path to the current plan file :"+pathNew+m_testName+".plan"+i+"\n");
            }
            m_numTest++;
        }
        m_reportWriter.flush();
    }

    public static void batchDiff( ) throws IOException {
        //              System.out.println( "====================================================Begin test: "+m_testName );
        m_reportWriter.write( "===================================================================Begin test "+m_testName+"\n" );
        batchDiff( m_pathRefPlan, m_savePlanPath, m_numSQL );
        //              System.out.println("==================================================================="+
        //                              "End of "+m_testName);
        m_reportWriter.write( "==================================================================="+
                "End of "+m_testName+"\n");
        m_reportWriter.flush();
    }

    public static boolean diffInlineAndJoin( AbstractPlanNode oldpn1, AbstractPlanNode newpn2 ) {
        m_treeSizeDiff = 0;
        boolean noDiff = true;
        ArrayList<String> messages = new ArrayList<String>();
        ArrayList<AbstractPlanNode> list1 = oldpn1.getLists();
        ArrayList<AbstractPlanNode> list2 = newpn2.getLists();
        int size1 = list1.size();
        int size2 = list2.size();
        m_treeSizeDiff = size1 - size2;
        Pair intPair = new Pair(0,0);
        Pair stringPair = new Pair(null,null);
        if( size1 != size2 ) {
            intPair.set(size1, size2);
            //                  System.out.println( "Plan tree size diff: " );
            //                  System.out.println( intPair.toString() );
            messages.add( "Plan tree size diff: "+intPair.toString() );

        }
        if( !m_changedSQL ){
            if( m_treeSizeDiff < 0 ){
                //                              System.out.println( "Old plan might be better" );
                messages.add( "Old plan might be better" );
            }
            else if( m_treeSizeDiff > 0 ) {
                //                              System.out.println( "New plan might be better");
                messages.add( "New plan might be better" );
            }
        }
        //inline nodes diff
        Map<Integer, AbstractPlanNode> projNodes1 = new LinkedHashMap<Integer, AbstractPlanNode>();
        Map<Integer, AbstractPlanNode> projNodes2 = new LinkedHashMap<Integer, AbstractPlanNode>();
        Map<Integer, AbstractPlanNode> limitNodes1 = new LinkedHashMap<Integer, AbstractPlanNode>();
        Map<Integer, AbstractPlanNode> limitNodes2 = new LinkedHashMap<Integer, AbstractPlanNode>();
        Map<Integer, AbstractPlanNode> orderByNodes1 = new LinkedHashMap<Integer, AbstractPlanNode>();
        Map<Integer, AbstractPlanNode> orderByNodes2 = new LinkedHashMap<Integer, AbstractPlanNode>();
        Map<AbstractPlanNode, AbstractPlanNode> projInlineNodes1 = new LinkedHashMap<AbstractPlanNode, AbstractPlanNode>();
        Map<AbstractPlanNode, AbstractPlanNode> projInlineNodes2 = new LinkedHashMap<AbstractPlanNode, AbstractPlanNode>();
        Map<AbstractPlanNode, AbstractPlanNode> limitInlineNodes1 = new LinkedHashMap<AbstractPlanNode, AbstractPlanNode>();
        Map<AbstractPlanNode, AbstractPlanNode> limitInlineNodes2 = new LinkedHashMap<AbstractPlanNode, AbstractPlanNode>();
        Map<AbstractPlanNode, AbstractPlanNode> orderByInlineNodes1 = new LinkedHashMap<AbstractPlanNode, AbstractPlanNode>();
        Map<AbstractPlanNode, AbstractPlanNode> orderByInlineNodes2 = new LinkedHashMap<AbstractPlanNode, AbstractPlanNode>();
        Map<AbstractPlanNode, AbstractPlanNode> indexScanInlineNodes1 = new LinkedHashMap<AbstractPlanNode, AbstractPlanNode>();
        Map<AbstractPlanNode, AbstractPlanNode> indexScanInlineNodes2 = new LinkedHashMap<AbstractPlanNode, AbstractPlanNode>();

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
                projInlineNodes1.put(pn, pn.getInlinePlanNode(PlanNodeType.PROJECTION ));
            }
            if( pn.getInlinePlanNode(PlanNodeType.LIMIT) != null) {
                limitInlineNodes1.put(pn, pn.getInlinePlanNode(PlanNodeType.LIMIT));
            }
            if( pn.getInlinePlanNode(PlanNodeType.ORDERBY) != null) {
                orderByInlineNodes1.put(pn, pn.getInlinePlanNode(PlanNodeType.ORDERBY));
            }
            if( pn.getInlinePlanNode(PlanNodeType.INDEXSCAN) != null ){
                indexScanInlineNodes1.put(pn, pn.getInlinePlanNode(PlanNodeType.INDEXSCAN) );
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
                projInlineNodes2.put(pn, pn.getInlinePlanNode(PlanNodeType.PROJECTION ));
            }
            if( pn.getInlinePlanNode(PlanNodeType.LIMIT) != null) {
                limitInlineNodes2.put(pn, pn.getInlinePlanNode(PlanNodeType.LIMIT));
            }
            if( pn.getInlinePlanNode(PlanNodeType.ORDERBY) != null) {
                orderByInlineNodes2.put(pn, pn.getInlinePlanNode(PlanNodeType.ORDERBY));
            }
            if( pn.getInlinePlanNode(PlanNodeType.INDEXSCAN) != null ){
                indexScanInlineNodes2.put(pn, pn.getInlinePlanNode(PlanNodeType.INDEXSCAN) );
            }
        }
        //do the diff
        ArrayList<Integer> indexList = new ArrayList<Integer>();
        ArrayList<AbstractPlanNode> parentNodeList = new ArrayList<AbstractPlanNode>();
        for( AbstractPlanNode index: projInlineNodes1.keySet() ) {
            parentNodeList.add(index);
        }
        String info = getKeyInfo( parentNodeList );
        stringPair.setFirst( info );
        parentNodeList.clear();
        for( AbstractPlanNode index: projInlineNodes2.keySet() ) {
            parentNodeList.add(index);
        }
        info = getKeyInfo( parentNodeList );
        stringPair.setSecond(info);
        if( !stringPair.equals() ){
            //                  System.out.println( "Inline Projection Nodes diff: ");
            //                  System.out.println( stringPair.toString() );
            messages.add( "Inline Projection Nodes diff: \n"+stringPair.toString() );
        }
        parentNodeList.clear();

        for( AbstractPlanNode index: limitInlineNodes1.keySet() ) {
            parentNodeList.add(index);
        }
        info = getKeyInfo( parentNodeList );
        stringPair.setFirst(info);
        parentNodeList.clear();
        for( AbstractPlanNode index: limitInlineNodes2.keySet() ) {
            parentNodeList.add(index);
        }
        info = getKeyInfo( parentNodeList );
        stringPair.setSecond(info);
        if( !stringPair.equals() ) {
            //                  System.out.println( "Inline Limit Nodes diff: ");
            //                  System.out.println( stringPair.toString() );
            messages.add( "Inline Limit Nodes diff: \n"+stringPair.toString() );
        }
        parentNodeList.clear();

        for( AbstractPlanNode index: limitInlineNodes1.keySet() ) {
            parentNodeList.add(index);
        }
        info = getKeyInfo( parentNodeList );
        stringPair.setFirst(info);
        parentNodeList.clear();
        for( AbstractPlanNode index: limitInlineNodes2.keySet() ) {
            parentNodeList.add(index);
        }
        info = getKeyInfo( parentNodeList );
        stringPair.setSecond(info);
        if( !stringPair.equals() ) {
            //                  System.out.println( "Inline OrderBy Nodes diff: ");
            //                  System.out.println( stringPair.toString() );
            messages.add( "Inline OrderBy Nodes diff: \n"+stringPair.toString() );
        }
        parentNodeList.clear();

        for( AbstractPlanNode index: limitInlineNodes1.keySet() ) {
            parentNodeList.add(index);
        }
        info = getKeyInfo( parentNodeList );
        stringPair.setFirst(info);
        parentNodeList.clear();
        for( AbstractPlanNode index: limitInlineNodes2.keySet() ) {
            parentNodeList.add(index);
        }
        info = getKeyInfo( parentNodeList );
        stringPair.setSecond(info);
        if( !stringPair.equals() ) {
            //                  System.out.println( "Inline IndexScan Nodes diff: ");
            //                  System.out.println( stringPair.toString() );
            messages.add( "Inline IndexScan Nodes diff: \n"+stringPair.toString() );
        }
        parentNodeList.clear();

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
            //                  System.out.println( "Projection Node diff: " );
            //                  System.out.println( stringPair.toString() );
            messages.add( "Projection Nodes diff: \n"+stringPair.toString() );
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
            //                  System.out.println( "Limit Node diff: " );
            //                  System.out.println( stringPair.toString() );
            messages.add( "Limit Nodes diff: \n"+stringPair.toString() );
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
            //                  System.out.println( "Order By Node diff:" );
            //                  System.out.println( stringPair.toString() );
            messages.add( "Order By Nodes diff: \n"+stringPair.toString() );
        }
        indexList.clear();

        //join nodes diff
        ArrayList<AbstractPlanNode> joinNodes1 = getJoinNodes( list1 );
        ArrayList<AbstractPlanNode> joinNodes2 = getJoinNodes( list2 );
        size1 = joinNodes1.size();
        size2 = joinNodes2.size();
        if( size1 != size2 ) {
            intPair.set( size1 , size2);
            //                  System.out.println("Join Nodes Number diff:");
            //                  System.out.println( intPair.toString() );
            //                  System.out.println("SQL statement might be changed.");
            messages.add( "Join Nodes Number diff:\n"+intPair.toString()+"\nSQL statement might be changed.");
            m_changedSQL = true;
            String str1 = "";
            String str2 = "";
            for( AbstractPlanNode pn : joinNodes1 ) {
                str1 = str1 + pn.getPlanNodeType() + ", ";
            }
            for( AbstractPlanNode pn : joinNodes2 ) {
                str2 = str2 + pn.getPlanNodeType() + ", ";
            }
            if( str1.length() > 1  ){
                str1.subSequence(0, str1.length()-2);
            }
            if( str2.length() > 1  ){
                str2.subSequence(0, str2.length()-2);
            }
            stringPair.set( str1, str2);
        }
        else {
            for( AbstractPlanNode pn1 : joinNodes1 ) {
                for( AbstractPlanNode pn2 : joinNodes2 ) {
                    PlanNodeType pnt1 = pn1.getPlanNodeType();
                    PlanNodeType pnt2 = pn2.getPlanNodeType();
                    if( !pnt1.equals(pnt2) ) {
                        stringPair.set( pnt1+" at "+pn1.getPlanNodeId(), pnt2+" at "+pn2.getPlanNodeId());
                        //                                              System.out.println( "Join Node Type diff:");
                        //                                              System.out.println( stringPair );
                        messages.add( "Join Node Type diff:\n"+stringPair.toString());
                    }
                }
            }
        }

        for( String msg: messages ) {
            if( msg.contains("diff") || msg.contains("Diff") ) {
                m_diffMessages.add( msg );
                noDiff = false;
            }
        }
        return noDiff;
    }

    public static boolean diffScans( AbstractPlanNode oldpn, AbstractPlanNode newpn ){
        m_changedSQL = false;
        boolean noDiff = true;
        ArrayList<AbstractPlanNode> list1 = oldpn.getScanNodeList();
        ArrayList<AbstractPlanNode> list2 = newpn.getScanNodeList();
        int size1 = list1.size();
        int size2 = list2.size();
        int max = Math.max(size1, size2);
        int min = Math.min(size1, size2);
        Pair intPair = new Pair(0,0);
        Pair stringPair = new Pair("", "");
        ArrayList<String> messages = new ArrayList<String>();
        if( max == 0 ) {
            //                  System.out.println("0 scan statement");
            messages.add("0 scan statement");
        }
        else {
            if( size1 != size2 ){
                intPair.set(size1, size2);
                messages.add("Scan time diff : "+"\n"+intPair.toString()+"\n"+"SQL statement might be changed");
                //                      System.out.println( "Scan time diff : " );
                //                      System.out.println( intPair );
                //                      System.out.println( "SQL statement might be changed" );
                m_changedSQL = true;
                try {
                    for( int i = 0; i < min; i++ ) {
                        JSONObject j1 = new JSONObject( list1.get(i).toJSONString() );
                        JSONObject j2 = new JSONObject( list2.get(i).toJSONString() );
                        String table1 = j1.getString(AbstractScanPlanNode.Members.TARGET_TABLE_NAME.name());
                        String table2 = j2.getString(AbstractScanPlanNode.Members.TARGET_TABLE_NAME.name());
                        String nodeType1 = j1.getString(AbstractPlanNode.Members.PLAN_NODE_TYPE.name());
                        String nodeType2 = j2.getString(AbstractPlanNode.Members.PLAN_NODE_TYPE.name());
                        if( !table1.equalsIgnoreCase(table2) ) {
                            stringPair.set( nodeType1+" on "+table1, nodeType2+" on "+table2 );
                            //                                          System.out.println("Table diff at leaf "+i+":");
                            //                                          System.out.println( stringPair.toString() );
                            messages.add( "Table diff at leaf "+i+":"+"\n"+stringPair.toString());
                        }
                        else if( !nodeType1.equalsIgnoreCase(nodeType2) ) {
                            stringPair.set(nodeType1+" on "+table1, nodeType2+" on "+table2);
                            //                                          System.out.println("Scan diff at leaf "+i+" :");
                            //                                          System.out.println( stringPair.toString() );
                            messages.add("Scan diff at leaf "+i+" :"+"\n"+stringPair.toString());
                        }
                        else if ( nodeType1.equalsIgnoreCase(PlanNodeType.INDEXSCAN.name()) ) {
                            String index1 = j1.getString(IndexScanPlanNode.Members.TARGET_INDEX_NAME.name());
                            String index2 = j2.getString(IndexScanPlanNode.Members.TARGET_INDEX_NAME.name());
                            stringPair.set( index1, index2);
                            if( !index1.equalsIgnoreCase(index2) ) {
                                //                                                      System.out.println("Index diff at leaf "+i+" :");
                                //                                                      System.out.println(stringPair.toString());
                                messages.add("Index diff at leaf "+i+" :"+"\n"+stringPair.toString());
                            } else {
                                //                                                      System.out.println("Same at leaf "+i);
                                messages.add("Same at leaf "+i);
                            }
                        }
                        //either index scan using same index or seqscan on same table
                        else{
                            //System.out.println("Same at leaf "+i);
                            messages.add("Same at leaf "+i);
                        }
                    }
                    //lists size are different
                    if( size2 < max ) {
                        for( int i = min; i < max; i++ ) {
                            JSONObject j = new JSONObject( list1.get(i).toJSONString() );
                            String table = j.getString(AbstractScanPlanNode.Members.TARGET_TABLE_NAME.name());
                            String nodeType = j.getString(AbstractPlanNode.Members.PLAN_NODE_TYPE.name());
                            String index = null;
                            if( nodeType.equalsIgnoreCase(PlanNodeType.INDEXSCAN.name()) ) {
                                index = j.getString(IndexScanPlanNode.Members.TARGET_INDEX_NAME.name());
                            }
                            if( index != null ) {
                                stringPair.set(nodeType+" on "+table+" using "+index, "Empty" );
                                //                                                      System.out.println("Diff at leaf "+i+" :");
                                //                                                      System.out.println(stringPair.toString());
                                messages.add( "Diff at leaf "+i+" :"+"\n"+stringPair.toString());
                            }
                            else
                            {
                                stringPair.set(nodeType+" on "+table, "Empty" );
                                //                                                      System.out.println("Diff at leaf "+i+": ");
                                //                                                      System.out.println(stringPair.toString());
                                messages.add("Diff at leaf "+i+": "+"\n"+stringPair.toString());
                            }
                        }
                    }
                    else if( size1 < max ) {
                        for( int i = min; i < max; i++ ) {
                            JSONObject j = new JSONObject( list2.get(i).toJSONString() );
                            String table = j.getString(AbstractScanPlanNode.Members.TARGET_TABLE_NAME.name());
                            String nodeType = j.getString(AbstractPlanNode.Members.PLAN_NODE_TYPE.name());
                            String index = null;
                            if( nodeType.equalsIgnoreCase(PlanNodeType.INDEXSCAN.name()) ) {
                                index = j.getString(IndexScanPlanNode.Members.TARGET_INDEX_NAME.name());
                            }
                            if( index != null ) {
                                stringPair.set("Empty", nodeType+" on "+table+" using "+index );
                                //                                                              System.out.println("Diff at leaf "+i+" :");
                                //                                                              System.out.println(stringPair.toString());
                                messages.add( "Diff at leaf "+i+" :"+"\n"+stringPair.toString());
                            }
                            else
                            {
                                stringPair.set("Empty", nodeType+" on "+table );
                                //                                                              System.out.println("Diff at leaf "+i+": ");
                                //                                                              System.out.println(stringPair.toString());
                                messages.add( "Diff at leaf "+i+": "+"\n"+stringPair.toString() );
                            }
                        }
                    }
                }
                catch (JSONException e) {
                    e.printStackTrace();
                }
            }
            else {
                //                      System.out.println("same leaf size");
                messages.add( "same leaf size" );
                try{
                    if( max == 1 ) {
                        //                                      System.out.println("Single scan plan");
                        messages.add("Single scan plan");
                        JSONObject j1 = new JSONObject( list1.get(0).toJSONString() );
                        JSONObject j2 = new JSONObject( list2.get(0).toJSONString() );
                        String table1 = j1.getString(AbstractScanPlanNode.Members.TARGET_TABLE_NAME.name());
                        String table2 = j2.getString(AbstractScanPlanNode.Members.TARGET_TABLE_NAME.name());
                        String nodeType1 = j1.getString(AbstractPlanNode.Members.PLAN_NODE_TYPE.name());
                        String nodeType2 = j2.getString(AbstractPlanNode.Members.PLAN_NODE_TYPE.name());
                        if( !table1.equalsIgnoreCase(table2) ){
                            stringPair.set(nodeType1+" on "+table1, nodeType2+" on "+table2 );
                            //                                          System.out.println("Diff table at leaf"+0+" :");
                            //                                          System.out.println(stringPair.toString());
                            messages.add( "Diff table at leaf "+0+" :"+"\n"+stringPair.toString());
                        }
                        else if( !nodeType1.equalsIgnoreCase(nodeType2) ) {
                            stringPair.set(nodeType1+" on "+table1, nodeType2+" on "+table2 );
                            //                                          System.out.println("Diff scan at leaf "+0+" :");
                            //                                          System.out.println(stringPair);
                            messages.add("Diff scan at leaf "+0+" :"+"\n"+stringPair.toString());
                        }
                        else if ( nodeType1.equalsIgnoreCase(PlanNodeType.INDEXSCAN.name()) ) {
                            String index1 = j1.getString(IndexScanPlanNode.Members.TARGET_INDEX_NAME.name());
                            String index2 = j2.getString(IndexScanPlanNode.Members.TARGET_INDEX_NAME.name());
                            if( !index1.equalsIgnoreCase(index2) ){
                                stringPair.set(nodeType1+" on "+table1+" using "+index1, nodeType2+" on "+table2+" using "+index2 );
                                //                                                      System.out.println("Diff index at leaf"+0+": ");
                                //                                                      System.out.println(stringPair.toString());
                                messages.add("Diff index at leaf "+0+": "+"\n"+stringPair.toString());
                            }
                            else{
                                //                                                      System.out.println("Same at "+0);
                                messages.add( "Same at "+0 );
                            }
                        }
                        else {
                            //                                          System.out.println("Same at "+0);
                            messages.add("Same at "+0);
                        }
                    }
                    else {
                        //                                      System.out.println("Join query");
                        messages.add("Join query");
                        for( int i = 0; i < max; i++ ) {
                            JSONObject j1 = new JSONObject( list1.get(i).toJSONString() );
                            JSONObject j2 = new JSONObject( list2.get(i).toJSONString() );
                            String table1 = j1.getString(AbstractScanPlanNode.Members.TARGET_TABLE_NAME.name());
                            String table2 = j2.getString(AbstractScanPlanNode.Members.TARGET_TABLE_NAME.name());
                            String nodeType1 = j1.getString(AbstractPlanNode.Members.PLAN_NODE_TYPE.name());
                            String nodeType2 = j2.getString(AbstractPlanNode.Members.PLAN_NODE_TYPE.name());
                            if( !table1.equalsIgnoreCase(table2) ){
                                stringPair.set(nodeType1+" on "+table1, nodeType2+" on "+table2 );
                                //                                                      System.out.println("Diff table at leaf "+i+" :");
                                //                                                      System.out.println(stringPair);
                                messages.add("Diff table at leaf "+i+" :"+"\n"+stringPair.toString());
                            }
                            else if( !nodeType1.equalsIgnoreCase(nodeType2) ) {
                                stringPair.set(nodeType1+" on "+table1, nodeType2+" on "+table2 );
                                //                                                      System.out.println("Diff scan at leaf "+i+" :");
                                //                                                      System.out.println(stringPair);
                                messages.add( "Diff scan at leaf "+i+" :"+"\n"+stringPair.toString());
                            }
                            else if ( nodeType1.equalsIgnoreCase(PlanNodeType.INDEXSCAN.name()) ) {
                                String index1 = j1.getString(IndexScanPlanNode.Members.TARGET_INDEX_NAME.name());
                                String index2 = j2.getString(IndexScanPlanNode.Members.TARGET_INDEX_NAME.name());
                                if( !index1.equalsIgnoreCase(index2) ){
                                    stringPair.set(nodeType1+" on "+table1+" using "+index1, nodeType2+" on "+table2+" using "+index2 );
                                    //                                                                  System.out.println("Diff index at leaf "+i+": ");
                                    //                                                                  System.out.println(stringPair.toString());
                                    messages.add( "Diff index at leaf "+i+" :"+"\n"+stringPair.toString());
                                }
                                else {
                                    //                                                                  System.out.println("Same at leaf "+i);
                                    messages.add("Same at leaf "+i);
                                }
                            }
                            else{
                                //                                              System.out.println("Same at leaf "+i);
                                messages.add( "Same at leaf "+i );
                            }
                        }
                    }
                }
                catch ( JSONException e ) {
                    e.printStackTrace();
                }
            }
        }
        for( String msg : messages ) {
            if( msg.contains("diff") || msg.contains("Diff") ) {
                noDiff = false;
                m_diffMessages.add( msg );
            }
        }
        //              for ( String message : messages ) {
        //                      System.out.println( message );
        //              }
        return noDiff;
    }

    //return true is there are no diff
    //false if there's any diff
    public static boolean diff( AbstractPlanNode oldpn, AbstractPlanNode newpn, boolean print ) {
        m_diffMessages.clear();
        boolean noDiff1 = diffScans(oldpn, newpn);
        boolean noDiff2 = diffInlineAndJoin(oldpn, newpn);
        noDiff1 = noDiff1 && noDiff2;
        if( noDiff1  ) {
            return true;
        }
        else {
            if( print  ) {
                for( String msg : m_diffMessages ) {
                    System.out.println(msg);
                }
            }
            return false;
        }
    }
}
