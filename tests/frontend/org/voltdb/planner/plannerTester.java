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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.types.PlanNodeType;

public class plannerTester {
    private static PlannerTestAideDeCamp aide;
    private static String m_currentConfig;
    private static String m_testName;
    private static String m_pathRefPlan;
    private static String m_baseName;
    private static String m_pathDDL;
    private static ArrayList<String> m_savePlanPaths = new ArrayList<String>();
    private static String m_savePlanPath;
    private static boolean m_isSavePathFromCML = false;
    private static Map<String,String> m_partitionColumns = new HashMap<String, String>();
    private static ArrayList<String> m_stmts = new ArrayList<String>();
    private static int m_treeSizeDiff;
    private static boolean m_changedSQL;

    private static boolean m_isCompileSave = false;
    private static boolean m_isDiff = false;
    private static boolean m_showExpainedPlan = false;
    private static boolean m_showSQLStatement = false;
    private static ArrayList<String> m_config = new ArrayList<String>();

    private static int m_numPass;
    private static int m_numFail;
    public static ArrayList<String> m_diffMessages = new ArrayList<String>();
    private static String m_reportDir = "/tmp/";
    private static BufferedWriter m_reportWriter;
    private static ArrayList<String>  m_filters = new ArrayList<String> ();

    public static class diffPair {
        private Object m_first;
        private Object m_second;

        public diffPair( Object first, Object second ) {
            m_first = first;
            m_second = second;
        }

        @Override
        public String toString() {
            String first = ( ( m_first == null ) || ( m_first == "" ) ) ?
                            "[]" : m_first.toString();
            String second = ( m_second == null || ( m_second == "" )) ?
                            "[]" : m_second.toString();
            return "("+first+" => "+second+")";
        }

        public boolean equals() {
            return m_first.equals(m_second);
        }

        public void setFirst( Object first ) {
            m_first = first;
        }

        public void setSecond( Object second ) {
            m_second = second;
        }

        public void set( Object first, Object second ) {
            m_first = first;
            m_second = second;
        }
    }

    //TODO maybe a more robust parser? Need to figure out how to handle config file array if using the CLIConfig below
//    private static class PlannerTesterConfig extends CLIConfig {
//        @Option(shortOpt = "d", desc = "Do the diff")
//        boolean isDiff = false;
//        @Option(shortOpt = "cs", desc = "Compile queris and save according to the config file")
//        boolean isCompileSave = false;
//        @Option(shortOpt = "C", desc = "Specify the path to the config file")
//        ArrayList<String> configFiles = new ArrayList<String> ();
//
//        @Override
//        public void validate() {
//            if (maxerrors < 0)
//                exitWithMessageAndUsage("abortfailurecount must be >=0");
//        }
//
//        @Override
//        public void printUsage() {
//            System.out
//                .println("Usage: csvloader [args] tablename");
//            System.out
//                .println("       csvloader [args] -p procedurename");
//            super.printUsage();
//        }
//    }

    public static void main( String[] args ) {
        int size = args.length;
        for( int i=0; i<size; i++ ) {
            String str = args[i];
            if( str.startsWith("-C=")) {
                String subStr = str.split("=")[1];
                String [] configs = subStr.split(",");
                for( String config : configs ) {
                    m_config.add( config.trim() );
                }
            }
            if( str.startsWith("-sp=")) {
                m_isSavePathFromCML = true;
                String subStr = str.split("=")[1];
                String [] savePaths = subStr.split(",");
                for( String savePath : savePaths ) {
                    savePath = savePath.trim();
                    if( !savePath.endsWith("/") ){
                        savePath += "/";
                    }
                    m_savePlanPaths.add( savePath );
                }
            }
            else if( str.startsWith("-cd") ) {
                m_isCompileSave = true;
                m_isDiff = true;
                m_showExpainedPlan = true;
                m_showSQLStatement = true;
            }
            else if( str.startsWith("-cs") ) {
                m_isCompileSave = true;
            }
            else if( str.startsWith("-d") ) {
                m_isDiff = true;
            }
            else if( str.startsWith("-r") ){
                m_reportDir = str.split("=")[1];
                m_reportDir = m_reportDir.trim();
                if( !m_reportDir.endsWith("/") ) {
                    m_reportDir += "/";
                }
            }
            else if( str.startsWith("-e") ){
                m_showExpainedPlan = true;
            }
            else if( str.startsWith("-s") ){
                m_showSQLStatement = true;
            }
            else if( str.startsWith("-i=") ) {
                m_filters.add(str.split("=")[1] );
            }
            else if( str.startsWith("-help") || str.startsWith("-h") ){
                printUsage();
                System.exit(0);
            }
        }
        size = m_config.size();
        if( m_isCompileSave ) {
            Iterator<String> it = m_savePlanPaths.iterator();
            for( String config : m_config ) {
                try {
                    setUp( config );
                    if( m_isSavePathFromCML && it.hasNext() ) {
                        m_savePlanPath = it.next();
                    }
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
                System.out.println(e1.getMessage());
                System.exit(-1);
            }
            Iterator<String> it = m_savePlanPaths.iterator();
            for( String config : m_config ) {
                try {
                    setUp( config );
                    if( m_isSavePathFromCML && it.hasNext() ) {
                        m_savePlanPath = it.next();
                    }
                    startDiff();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            int numTest = m_numPass + m_numFail;
            System.out.println("Test: "+numTest);
            System.out.println("Pass: "+m_numPass);
            System.out.println("Fail: "+m_numFail);
            try {
                m_reportWriter.write( "\nTest: "+numTest+"\n"
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
            System.exit(1);
        }
    }

    public static void printUsage() {
        System.out.println("-C='configFile1, configFile2, ...'" +
                        "\nSpecify the path to a config file.\n");
        System.out.println("-sp='savePath1, savePath2, ...'" +
                        "\nspecify save paths for newly generated plan files, should be in the same order of the config files.\n");
        System.out.println("-r=reportFileDir " +
                        "\nSpecify report file path, default will be tmp/, report file name is plannerTester.report.\n");
        System.out.println("-i=ignorePattern" +
                        "\nSpecify a pattern to ignore, the pattern will not be recorded in the report file.\n");
        System.out.println("-cd" +
                        "\nSame as putting -cs -d -e -s.\n");
        System.out.println("-cs" +
                        "\nCompile queries and save the plans according to the config files.\n");
        System.out.println("-d" +
                        "\nDo the diff between plan files in baseline and the current ones.\n");
        System.out.println("-e" +
                        "\nOutput explained plan along with diff.\n");
        System.out.println("-s" +
                        "\nOutput sql statement along with diff.\n");

    }

    public static void setUp( String pathConfigFile ) throws IOException {
        m_currentConfig = pathConfigFile;
        m_partitionColumns.clear();
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
                while( (line = reader.readLine()).length() > 6 ) {
                    if( line.startsWith("#") ) {
                        continue;
                    }
                    m_stmts.add( line );
                }
            }
            else if( line.equalsIgnoreCase("Save Path:") ) {
                    line = reader.readLine();
                    m_savePlanPath = ( new File( line ).getCanonicalPath() ) + "/";
            }
            else if( line.equalsIgnoreCase("Partition Columns:") ) {
                line = reader.readLine();
                int index = line.indexOf(".");
                if( index == -1 ) {
                    System.err.println("Config file syntax error : Partition Columns should be table.column");
                }
                m_partitionColumns.put( line.substring(0, index).toLowerCase(), line.substring(index+1).toLowerCase());
            }
        }
        try {
            setUpSchema();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void setUpForTest( String pathDDL, String baseName, String table, String column ) {
        m_partitionColumns.clear();
        m_partitionColumns.put( table.toLowerCase(), column.toLowerCase());
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
        for( String tableName : m_partitionColumns.keySet() ) {
            Table t = tmap.getIgnoreCase( tableName );
            t.setIsreplicated(false);
            Column column = t.getColumns().getIgnoreCase( m_partitionColumns.get(tableName) );
            t.setPartitioncolumn(column);
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

    public static void writePlanToFile( AbstractPlanNode pn, String pathToDir, String fileName, String sql) {
        if( pn == null ) {
            System.err.println("the plan node is null, nothing to write");
            return;
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
            List<AbstractPlanNode> pnList = compile( m_stmts.get(i), 0, false);
            AbstractPlanNode pn = pnList.get(0);
            if( pnList.size() == 2 ){//multi partition query plan
                assert( pnList.get(1) instanceof SendPlanNode );
                if( ! pn.reattachFragment( ( SendPlanNode) pnList.get(1) ) ) {
                    System.err.println( "Receive plan node not found while reattachFragment." );
                }
            }
            writePlanToFile(pn, m_savePlanPath, m_testName+".plan"+i, m_stmts.get(i) );
        }
        System.out.println("Plan files generated at: "+m_savePlanPath);
    }

    //parameters : path to baseline and the new plans
    //size : number of total files in the baseline directory
    public static void batchDiff( ) throws IOException {
        PlanNodeTree pnt1 = null;
        PlanNodeTree pnt2 = null;
        int size = m_stmts.size();
        String baseStmt = null;
        for( int i = 0; i < size; i++ ){
            ArrayList<String> getsql = new ArrayList<String>();
            try {
                pnt1 = loadPlanFromFile( m_pathRefPlan+m_testName+".plan"+i, getsql );
                baseStmt = getsql.get(0);
            } catch (FileNotFoundException e) {
                System.err.println("Plan file "+m_pathRefPlan+m_testName+".plan"+i+" don't exist. Use -cs(batchCompileSave) to generate plans and copy base plans to baseline directory.");
                System.exit(1);
            }

            //if sql stmts not consistent
            if( !baseStmt.equalsIgnoreCase( m_stmts.get(i)) ) {
                diffPair strPair = new diffPair( baseStmt, m_stmts.get(i) );
                m_reportWriter.write("Statement "+i+" of "+m_testName+":\n SQL statement is not consistent with the one in baseline :"+"\n"+
                        strPair.toString()+"\n");
                m_numFail++;
                continue;
            }

            try{
                pnt2  = loadPlanFromFile( m_savePlanPath+m_testName+".plan"+i, getsql );
            } catch (FileNotFoundException e) {
                System.err.println("Plan file "+m_savePlanPath+m_testName+".plan"+i+" don't exist. Use -cs(batchCompileSave) to generate and save plans.");
                System.exit(1);
            }
            AbstractPlanNode pn1 = pnt1.getRootPlanNode();
            AbstractPlanNode pn2 = pnt2.getRootPlanNode();


            if( diff( pn1, pn2, false ) ) {
                m_numPass++;
            } else {
                m_numFail++;
                m_reportWriter.write( "Statement "+i+" of "+m_testName+": \n" );
                //TODO add more logic to determine which plan is better
                if( !m_changedSQL ){
                    if( m_treeSizeDiff < 0 ){
                        m_reportWriter.write( "Old plan might be better\n" );
                    }
                    else if( m_treeSizeDiff > 0 ) {
                        m_reportWriter.write( "New plan might be better\n" );
                    }
                }

                for( String msg : m_diffMessages ) {
                    boolean isIgnore = false;
                    for( String filter : m_filters ) {
                        if( msg.contains( filter ) ) {
                            isIgnore = true;
                            break;
                        }
                    }
                    if( !isIgnore )
                        m_reportWriter.write( msg+"\n\n" );
                }
                if( m_showSQLStatement ) {
                    m_reportWriter.write( "SQL statement:\n"+baseStmt+"\n==>\n"+m_stmts.get(i)+"\n");
                }

                if( m_showExpainedPlan ) {
                    m_reportWriter.write("\nExplained plan:\n"+pn1.toExplainPlanString()+"\n==>\n"+pn2.toExplainPlanString()+"\n");
                }

                m_reportWriter.write("Path to the config file :"+m_currentConfig+"\n"
                        +"Path to the baseline file :"+m_pathRefPlan+m_testName+".plan"+i+"\n"
                        +"Path to the current plan file :"+m_savePlanPath+m_testName+".plan"+i+
                        "\n\n----------------------------------------------------------------------\n");
            }
        }
        m_reportWriter.flush();
    }

    public static void startDiff( ) throws IOException {
        m_reportWriter.write( "===================================================================Begin test "+m_testName+"\n" );
        batchDiff( );
        m_reportWriter.write( "==================================================================="+
                "End of "+m_testName+"\n");
        m_reportWriter.flush();
    }

    public static boolean diffInlineAndJoin( AbstractPlanNode oldpn1, AbstractPlanNode newpn2 ) {
        m_treeSizeDiff = 0;
        boolean noDiff = true;
        ArrayList<String> messages = new ArrayList<String>();
        ArrayList<AbstractPlanNode> list1 = oldpn1.getPlanNodeList();
        ArrayList<AbstractPlanNode> list2 = newpn2.getPlanNodeList();
        int size1 = list1.size();
        int size2 = list2.size();
        m_treeSizeDiff = size1 - size2;
        diffPair intdiffPair = new diffPair(0,0);
        diffPair stringdiffPair = new diffPair(null,null);
        if( size1 != size2 ) {
            intdiffPair.set(size1, size2);
            messages.add( "Plan tree size diff: "+intdiffPair.toString() );
        }
        Map<String,ArrayList<Integer>> planNodesPosMap1 = new LinkedHashMap<String,ArrayList<Integer>> ();
        Map<String,ArrayList<AbstractPlanNode>> inlineNodesPosMap1 = new LinkedHashMap<String,ArrayList<AbstractPlanNode>> ();

        Map<String,ArrayList<Integer>> planNodesPosMap2 = new LinkedHashMap<String,ArrayList<Integer>> ();
        Map<String,ArrayList<AbstractPlanNode>> inlineNodesPosMap2 = new LinkedHashMap<String,ArrayList<AbstractPlanNode>> ();

        fetchPositionInfoFromList(list1, planNodesPosMap1, inlineNodesPosMap1);
        fetchPositionInfoFromList(list2, planNodesPosMap2, inlineNodesPosMap2);

        planNodePositionDiff( planNodesPosMap1, planNodesPosMap2, messages );
        inlineNodePositionDiff( inlineNodesPosMap1, inlineNodesPosMap2, messages );

        //join nodes diff
        ArrayList<AbstractPlanNode> joinNodes1 = getJoinNodes( list1 );
        ArrayList<AbstractPlanNode> joinNodes2 = getJoinNodes( list2 );
        size1 = joinNodes1.size();
        size2 = joinNodes2.size();
        if( size1 != size2 ) {
            intdiffPair.set( size1 , size2);
            messages.add( "Join Nodes Number diff:\n"+intdiffPair.toString()+"\nSQL statement might be changed.");
            m_changedSQL = true;
            String str1 = "";
            String str2 = "";
            for( AbstractPlanNode pn : joinNodes1 ) {
                str1 = str1 + pn.toString() + ", ";
            }
            for( AbstractPlanNode pn : joinNodes2 ) {
                str2 = str2 + pn.toString() + ", ";
            }
            if( str1.length() > 1  ){
                str1 = ( str1.subSequence(0, str1.length()-2) ).toString();
            }
            if( str2.length() > 1  ){
                str2 = ( str2.subSequence(0, str2.length()-2) ).toString();
            }
            stringdiffPair.set( str1, str2 );
            messages.add( "Join Node List diff: "+"\n"+stringdiffPair.toString()+"\n");
        }
        else {
            for( int i = 0 ; i < size1 ; i++  ) {
                AbstractPlanNode pn1 = joinNodes1.get(i);
                AbstractPlanNode pn2 = joinNodes2.get(i);
                PlanNodeType pnt1 = pn1.getPlanNodeType();
                PlanNodeType pnt2 = pn2.getPlanNodeType();
                if( !pnt1.equals(pnt2) ) {
                    stringdiffPair.set( pn1.toString(), pn2.toString() );
                    messages.add( "Join Node Type diff:\n"+stringdiffPair.toString());
                }
            }
        }

        for( String msg : messages ) {
            if( msg.contains("diff") || msg.contains("Diff") ) {
                noDiff = false;
                break;
            }
        }
        m_diffMessages.addAll(messages);
        return noDiff;
    }

    private static void fetchPositionInfoFromList( Collection<AbstractPlanNode> list,
            Map<String,ArrayList<Integer>> planNodesPosMap,
            Map<String,ArrayList<AbstractPlanNode>> inlineNodesPosMap ) {
        for( AbstractPlanNode pn : list ) {
            String nodeTypeStr = pn.getPlanNodeType().name();
            if( !planNodesPosMap.containsKey(nodeTypeStr) ) {
                ArrayList<Integer> intList = new ArrayList<Integer>( );
                intList.add( pn.getPlanNodeId() );
                planNodesPosMap.put(nodeTypeStr, intList );
            }
            else{
                planNodesPosMap.get( nodeTypeStr ).add( pn.getPlanNodeId() );
            }
            //walk inline nodes
            for( AbstractPlanNode inlinepn : pn.getInlinePlanNodes().values() ) {
                String inlineNodeTypeStr = inlinepn.getPlanNodeType().name();
                if( !inlineNodesPosMap.containsKey( inlineNodeTypeStr ) ) {
                    ArrayList<AbstractPlanNode> nodeList = new ArrayList<AbstractPlanNode>( );
                    nodeList.add(pn);
                    inlineNodesPosMap.put( inlineNodeTypeStr, nodeList );
                }
                else{
                    inlineNodesPosMap.get( inlineNodeTypeStr ).add( pn );
                }
            }
        }
    }

    private static void planNodePositionDiff( Map<String,ArrayList<Integer>> planNodesPosMap1, Map<String,ArrayList<Integer>> planNodesPosMap2, ArrayList<String> messages ) {
        Set<String> typeWholeSet = new HashSet<String>();
        typeWholeSet.addAll( planNodesPosMap1.keySet() );
        typeWholeSet.addAll( planNodesPosMap2.keySet() );

        for( String planNodeTypeStr : typeWholeSet ) {
            if( ! planNodesPosMap1.containsKey( planNodeTypeStr ) &&  planNodesPosMap2.containsKey( planNodeTypeStr ) ){
                diffPair strPair = new diffPair( null, planNodesPosMap2.get(planNodeTypeStr).toString() );
                messages.add( planNodeTypeStr+" diff: \n"+strPair.toString() );
            }
            else if( planNodesPosMap1.containsKey( planNodeTypeStr ) &&  !planNodesPosMap2.containsKey( planNodeTypeStr ) ) {
                diffPair strPair = new diffPair( planNodesPosMap1.get(planNodeTypeStr).toString(), null );
                messages.add( planNodeTypeStr+" diff: \n"+strPair.toString() );
            }
            else{
                diffPair strPair = new diffPair( planNodesPosMap1.get(planNodeTypeStr).toString(), planNodesPosMap2.get(planNodeTypeStr).toString() );
                if( !strPair.equals() ) {
                    messages.add( planNodeTypeStr+" diff: \n"+strPair.toString() );
                }
            }
        }
    }

    private static void inlineNodePositionDiff( Map<String,ArrayList<AbstractPlanNode>> inlineNodesPosMap1, Map<String,ArrayList<AbstractPlanNode>> inlineNodesPosMap2, ArrayList<String> messages ) {
        Set<String> typeWholeSet = new HashSet<String>();
        typeWholeSet.addAll( inlineNodesPosMap1.keySet() );
        typeWholeSet.addAll( inlineNodesPosMap2.keySet() );

        for( String planNodeTypeStr : typeWholeSet ) {
            if( ! inlineNodesPosMap1.containsKey( planNodeTypeStr ) &&  inlineNodesPosMap2.containsKey( planNodeTypeStr ) ){
                diffPair strPair = new diffPair( null, inlineNodesPosMap2.get(planNodeTypeStr).toString() );
                messages.add( "Inline "+planNodeTypeStr+" diff: \n"+strPair.toString() );
            }
            else if( inlineNodesPosMap1.containsKey( planNodeTypeStr ) &&  !inlineNodesPosMap2.containsKey( planNodeTypeStr ) ) {
                diffPair strPair = new diffPair( inlineNodesPosMap1.get(planNodeTypeStr).toString(), null );
                messages.add( "Inline "+planNodeTypeStr+" diff: \n"+strPair.toString() );
            }
            else{
                diffPair strPair = new diffPair( inlineNodesPosMap1.get(planNodeTypeStr).toString(), inlineNodesPosMap2.get(planNodeTypeStr).toString() );
                if( !strPair.equals() ) {
                    messages.add( "Inline "+planNodeTypeStr+" diff: \n"+strPair.toString() );
                }
            }
        }
    }

    private static void scanNodeDiffModule( int leafID, AbstractScanPlanNode spn1, AbstractScanPlanNode spn2, ArrayList<String> messages ) {
        diffPair stringdiffPair = new diffPair("", "");
        String table1 = "";
        String table2 = "";
        String nodeType1 = "";
        String nodeType2 = "";
        String index1 = "";
        String index2 = "";
        if( spn1 == null && spn2 != null ) {
            table2 = spn2.getTargetTableName();
            nodeType2 = spn2.getPlanNodeType().toString();
            if( nodeType2.equalsIgnoreCase(PlanNodeType.INDEXSCAN.name() ) ) {
                index2 = ((IndexScanPlanNode)spn2).getTargetIndexName();
            }
        }
        else if( spn2 == null && spn1 != null ) {
            table1 = spn1.getTargetTableName();
            nodeType1 = spn1.getPlanNodeType().toString();
            if( nodeType1.equalsIgnoreCase(PlanNodeType.INDEXSCAN.name() ) ) {
                index1 = ((IndexScanPlanNode)spn1).getTargetIndexName();
            }
        }
        //both null is not possible
        else{
            table1 = spn1.getTargetTableName();
            table2 = spn2.getTargetTableName();
            nodeType1 = spn1.getPlanNodeType().toString();
            nodeType2 = spn2.getPlanNodeType().toString();
            if( nodeType1.equalsIgnoreCase(PlanNodeType.INDEXSCAN.name() ) ) {
                index1 = ((IndexScanPlanNode)spn1).getTargetIndexName();
            }
            if( nodeType2.equalsIgnoreCase(PlanNodeType.INDEXSCAN.name() ) ) {
                index2 = ((IndexScanPlanNode)spn2).getTargetIndexName();
            }
        }
        if( !table1.equals(table2) ) {
            stringdiffPair.set( table1.equals("") ? null : nodeType1+" on "+table1,
                                table2.equals("") ? null : nodeType2+" on "+table2 );
            messages.add( "Table diff at leaf "+leafID+":"+"\n"+stringdiffPair.toString());
        }
        else if( !nodeType1.equals(nodeType2) ) {
            stringdiffPair.set(nodeType1+" on "+table1, nodeType2+" on "+table2);
            messages.add("Scan diff at leaf "+leafID+" :"+"\n"+stringdiffPair.toString());
        }
        else if ( nodeType1.equalsIgnoreCase(PlanNodeType.INDEXSCAN.name()) ) {
            if( !index1.equals(index2) ) {
                stringdiffPair.set( index1, index2);
                messages.add("Index diff at leaf "+leafID+" :"+"\n"+stringdiffPair.toString());
            } else {
                messages.add("Same at leaf "+leafID);
            }
        }
        //either index scan using same index or seqscan on same table
        else{
            messages.add("Same at leaf "+leafID);
        }
    }

    public static boolean diffScans( AbstractPlanNode oldpn, AbstractPlanNode newpn ){
        m_changedSQL = false;
        boolean noDiff = true;
        ArrayList<AbstractScanPlanNode> list1 = oldpn.getScanNodeList();
        ArrayList<AbstractScanPlanNode> list2 = newpn.getScanNodeList();
        int size1 = list1.size();
        int size2 = list2.size();
        int max = Math.max(size1, size2);
        int min = Math.min(size1, size2);
        diffPair intdiffPair = new diffPair(0,0);
        ArrayList<String> messages = new ArrayList<String>();
        AbstractScanPlanNode spn1 = null;
        AbstractScanPlanNode spn2 = null;
        if( max == 0 ) {
            messages.add("0 scan statement");
        }
        else {
            if( size1 != size2 ){
                intdiffPair.set(size1, size2);
                messages.add("Scan time diff : "+"\n"+intdiffPair.toString()+"\n"+"SQL statement might be changed");
                m_changedSQL = true;
                for( int i = 0; i < min; i++ ) {
                    spn1 = list1.get(i);
                    spn2 = list2.get(i);
                    scanNodeDiffModule(i, spn1, spn2, messages);
                }
                //lists size are different
                if( size2 < max ) {
                    for( int i = min; i < max; i++ ) {
                        spn1 = list1.get(i);
                        spn2 = null;
                        scanNodeDiffModule(i, spn1, spn2, messages);
                    }
                }
                else if( size1 < max ) {
                    for( int i = min; i < max; i++ ) {
                        spn1 = null;
                        spn2 = list2.get(i);
                        scanNodeDiffModule(i, spn1, spn2, messages);
                    }
                }
            }
            else {
                messages.add( "same leaf size" );
                if( max == 1 ) {
                    messages.add("Single scan plan");
                    spn1 = list1.get(0);
                    spn2 = list2.get(0);
                    scanNodeDiffModule(0, spn1, spn2, messages);
                }
                else {
                    messages.add("Join query");
                    for( int i = 0; i < max; i++ ) {
                        spn1 = list1.get(i);
                        spn2 = list2.get(i);
                        scanNodeDiffModule(i, spn1, spn2, messages);
                    }
                }
            }
        }
        for( String msg : messages ) {
            if( msg.contains("diff") || msg.contains("Diff") ) {
                noDiff = false;
                break;
            }
        }
        m_diffMessages.addAll(messages);
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
