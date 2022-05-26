/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.catalog.Database;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.PlanNodeTree;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.types.PlanNodeType;

public class plannerTester {
    private static PlannerTestCase s_singleton = new PlannerTestCase();
    private static String m_workPath = "/tmp/plannertester/";
    private static String m_baselinePath;
    private static String m_fixedBaselinePath = null;
    private static ArrayList<String> m_stmts = new ArrayList<String>();
    private static int m_treeSizeDiff;
    private static boolean m_changedSQL;

    private static boolean m_isCompile = false;
    private static boolean m_isSave = false;
    private static boolean m_isDiff = false;
    private static boolean m_reportExplainedPlan = false;
    private static boolean m_reportDiffExplainedPlan = false;
    private static boolean m_reportSQLStatement = false;
    private static ArrayList<String> m_config = new ArrayList<String>();

    private static int m_numPass;
    private static int m_numFail;
    public static ArrayList<String> m_diffMessages = new ArrayList<String>();
    private static String m_reportPath = "/tmp/";
    private static BufferedWriter m_reportWriter;
    private static ArrayList<String> m_filters = new ArrayList<String>();

    public static class diffPair {
        private Object m_first;
        private Object m_second;

        public diffPair(Object first, Object second) {
            m_first = first;
            m_second = second;
        }

        @Override
        public String toString() {
            String first = ((m_first == null) || (m_first == "")) ?
                            "[]" : m_first.toString();
            String second = ((m_second == null) || (m_second == "")) ?
                            "[]" : m_second.toString();
            return "(" + first + " => " + second + ")";
        }

        public boolean equals() {
            return m_first.equals(m_second);
        }

        public void setFirst(Object first) {
            m_first = first;
        }

        public void setSecond(Object second) {
            m_second = second;
        }

        public void set(Object first, Object second) {
            m_first = first;
            m_second = second;
        }
    }

    public static void main(String[] args) {
        int numError = 0;
        for(String str : args) {
            if (str.startsWith("-C=")) {
                String subStr = str.split("=")[1];
                String [] configs = subStr.split(",");
                for (String config : configs) {
                    m_config.add(config.trim());
                }
            }
            else if (str.startsWith("-sp=")) {
                m_workPath = str.split("=")[1];
                m_workPath = m_workPath.trim();
                if ( ! m_workPath.endsWith("/")) {
                    m_workPath += "/";
                }
            }
            else if (str.startsWith("-b=")) {
                m_fixedBaselinePath = str.split("=")[1];
                m_fixedBaselinePath = m_fixedBaselinePath.trim();
                if (!m_fixedBaselinePath.endsWith("/")) {
                    m_fixedBaselinePath += "/";
                }
            }
            else if (str.equals("-s")) {
                m_isCompile = true;
                m_isSave = true;
            }
            else if (str.equals("-sv")) {
                m_isCompile = true;
                m_isSave = true;
                m_reportExplainedPlan = true;
                m_reportSQLStatement = true;
            }
            else if (str.equals("-d")) {
                m_isCompile = true;
                m_isDiff = true;
            }
            else if (str.equals("-dv")) {
                m_isCompile = true;
                m_isDiff = true;
                m_reportDiffExplainedPlan = true;
                m_reportSQLStatement = true;
            }
            else if (str.startsWith("-r=")) {
                m_reportPath = str.split("=")[1];
                m_reportPath = m_reportPath.trim();
                if (!m_reportPath.endsWith("/")) {
                    m_reportPath += "/";
                }
            }
            else if (str.equals("-re")) {
                m_reportExplainedPlan = true;
                m_reportDiffExplainedPlan = true;
            }
            else if (str.equals("-rs")) {
                m_reportSQLStatement = true;
            }
            else if (str.startsWith("-i=")) {
                m_filters.add(str.split("=")[1]);
            }
            else if (str.startsWith("-help") || str.startsWith("-h")) {
                printUsage();
                System.exit(0);
            }
            else {
                System.out.println("Illegal command line argument: " + str);
                printUsage();
                System.exit(0);
            }
        }

        if ( ! new File(m_workPath).exists()) {
            new File(m_workPath).mkdirs();
        }

        if (m_isCompile) {
            for (String config : m_config) {
                try {
                    configCompileSave(config, m_isSave);
                }
                catch (Exception e) {
                    e.printStackTrace();
                    ++numError;
                }
            }
        }
        if (m_isDiff) {
            if ( ! new File(m_reportPath).exists()) {
                new File(m_reportPath).mkdirs();
            }
            try {
                m_reportWriter = new BufferedWriter(new FileWriter(m_reportPath + "plannerTester.report"));
            }
            catch (IOException e1) {
                System.out.println(e1.getMessage());
                System.exit(-1);
            }
            for (String config : m_config) {
                try {
                    configDiff(config);
                }
                catch (Exception e) {
                    e.printStackTrace();
                    ++numError;
                }
            }
            int numTest = m_numPass + m_numFail;
            String summary = "\nTest: " + numTest + "\nPass: " + m_numPass + "\nFail: " +
                    m_numFail + "\nError: " + numError + "\n";
            System.out.print(summary);
            try {
                m_reportWriter.write(summary);
                m_reportWriter.flush();
                m_reportWriter.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("Report file created at " + m_reportPath + "plannerTester.report");
        }
        if (numError != 0) {
            System.exit(2);
        }
        if (m_numFail != 0) {
            System.exit(1);
        }
        System.exit(0);
    }

    public static void printUsage() {
        System.out.println("-C=configDir1[,configDir2,...]" +
                        "\nSpecify the path to each config file.\n");
        System.out.println("-sp=savePath" +
                        "\nspecify path for newly generated plan files.\n");
        System.out.println("-b=baselinePath" +
                        "\nspecify path for ALL baseline reference plan files. Omit for separate <configDir>/baseline dirs\n");
        System.out.println("-r=reportFilePath " +
                        "\nSpecify report file path, default will be ./reports, report file name is plannerTester.report.\n");
        System.out.println("-i=ignorePattern" +
                        "\nSpecify a pattern to ignore, the pattern will not be recorded in the report file.\n");
        System.out.println("-s" +
                        "\nSave compiled queries in the baseline path (<config>/baseline by default.\n");
        System.out.println("-d" +
                        "\nDo the diff between plan files in baseline and the current ones.\n");
        System.out.println("-re" +
                        "\nOutput explained plan along with diff.\n");
        System.out.println("-rs" +
                        "\nOutput sql statement along with diff.\n");
        System.out.println("-dv" +
                        "\nSame as -d -re -rs.\n");
        System.out.println("-sv" +
                        "\nSame as -s -re -rs.\n");

    }

    public static boolean setUp(String config) throws Exception {
        m_baselinePath = (m_fixedBaselinePath != null) ? m_fixedBaselinePath : (config + "/baseline/");
        String ddlFilePath = null;
        m_stmts.clear();
        BufferedReader reader = new BufferedReader(new FileReader(config + "/config"));
        String line = null;
        while((line = reader.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }
            else if (line.equalsIgnoreCase("DDL:")) {
                if ((line = reader.readLine()) == null) {
                    break;
                }
                ddlFilePath = new File(line).getCanonicalPath();
            }
            else if (line.equalsIgnoreCase("SQL:")) {
                boolean atEof = false;
                while (true) {
                    if ((line = reader.readLine()) == null) {
                        atEof = true;
                        break;
                    }
                    if (line.startsWith("#")) {
                        continue;
                    }
                    if (line.length() <= 6) {
                        break;
                    }
                    if (line.startsWith("JOIN:")) {
                        // These lines have three parts JOIN:<joinOrder>:<query>
                        if (line.split(":").length != 3) {
                            System.err.println("Config file syntax error : ignoring line: " + line);
                        }
                    }
                    m_stmts.add(line);
                }
                if (atEof) {
                    break;
                }
            }
            // This section of the config file is optional, deprecated, and ignored.
            else if (line.equalsIgnoreCase("Partition Columns:")) {
                if ((line = reader.readLine()) == null) {
                    break;
                }
            }
            else if ( ! line.trim().equals("")) {
                System.err.println("Config file syntax error : ignoring line: " + line);
            }
        }
        boolean success = true;
        if (ddlFilePath == null) {
            System.err.println("ERROR: syntax error : config file '" + config + "/config' has no 'DDL:' section");
            success = false;
        }
        if (m_stmts.isEmpty()) {
            System.err.println("ERROR: syntax error : config file '" + config + "/config' has no 'SQL:' section or SQL statements");
            success = false;
        }
        if (success) {
            File ddlFile = new File(ddlFilePath);
            URL ddlURL = ddlFile.toURI().toURL();
            s_singleton.setupSchema(ddlURL, config, false);
        }
        return success;
    }

    public static void setUpForTest(String pathDDL, String config)
    {
        try {
            File ddlFile = new File(pathDDL);
            URL ddlURL = ddlFile.toURI().toURL();
            s_singleton.setupSchema(ddlURL, config, false);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static List<AbstractPlanNode> testCompile(String sql) throws Exception
    {
        return s_singleton.compileToFragments(sql);
    }

    public static void writePlanToFile(AbstractPlanNode pn, String pathToDir, String fileName, String sql) {
        assert(pn != null);
        PlanNodeTree pnt = new PlanNodeTree(pn);
        String prettyJson = pnt.toJSONString();
        if (!new File(pathToDir).exists()) {
            new File(pathToDir).mkdirs();
        }
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(pathToDir + fileName));
            writer.write(sql);
            writer.write("\n");
            writer.write(prettyJson);
            writer.flush();
            writer.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static AbstractPlanNode loadPlanFromFile(String path, List<String> getsql) {
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(path));
        }
        catch (FileNotFoundException e1) {
            e1.printStackTrace();
            String message = "ERROR: Plan file " + path + " doesn't exist.\n" +
                    "Use -s (the Compile/Save option) or 'ant plannertestrefresh'" +
                    " ' to generate plans to the baseline directory.\n";
            System.err.print(message);
            try {
                m_reportWriter.write(message);
            }
            catch (IOException e2) {
                e2.printStackTrace();
            }
            return null;
        }

        try {
            String json = "";
            try {
                String line = reader.readLine();
                getsql.add(line);
                while((line = reader.readLine()) != null) {
                    json += line;
                }
            }
            catch (IOException e2) {
                e2.printStackTrace();
                return null;
            }
            try {
                PlanNodeTree pnt = new PlanNodeTree();
                JSONObject jobj = new JSONObject(json);
                Database db = s_singleton.getDatabase();
                pnt.loadFromJSONPlan(jobj, db);
                return pnt.getRootPlanNode();
            }
            catch (JSONException e3) {
                e3.printStackTrace();
                System.out.println("Failed on input from file: " + path +
                        " with JSON text: \n'" + json + "'");
                return null;
            }
        }
        finally {
            try {
                reader.close();
            }
            catch (IOException e2) {
                e2.printStackTrace();
            }
        }
    }

    public static List< AbstractPlanNode > getJoinNodes(List<AbstractPlanNode> pnlist) {
        List< AbstractPlanNode > joinNodeList = new ArrayList<AbstractPlanNode>();

        for (AbstractPlanNode pn : pnlist) {
            if (pn.getPlanNodeType().equals(PlanNodeType.NESTLOOP) ||
                    pn.getPlanNodeType().equals(PlanNodeType.NESTLOOPINDEX)) {
                joinNodeList.add(pn);
            }
        }
        return joinNodeList;
    }

    private static void configCompileSave(String config, boolean isSave) throws Exception {
        if ( ! setUp(config)) {
            return;
        }
        int size = m_stmts.size();
        for (int i = 0; i < size; i++) {
            String query = m_stmts.get(i);
            String joinOrder = null;
            if (query.startsWith("JOIN:")) {
                String[] splitLine = query.split(":");
                joinOrder = splitLine[1];
                query = splitLine[2];
            }
            // If one compilation fails, try subsequent ones.
            // This avoids cascading "file-not-found" errors.
            try {
                List<AbstractPlanNode> pnList = s_singleton.compileWithJoinOrderToFragments(query, joinOrder);
                AbstractPlanNode pn = pnList.get(0);
                if (pnList.size() == 2) {// multi partition query plan
                    assert(pnList.get(1) instanceof SendPlanNode);
                    if ( ! pn.reattachFragment(pnList.get(1))) {
                        System.err.println("Receive plan node not found in reattachFragment.");
                    }
                }
                writePlanToFile(pn, m_workPath, config + ".plan" + i, m_stmts.get(i));
                if (isSave) {
                    writePlanToFile(pn, m_baselinePath, config + ".plan" + i, m_stmts.get(i));
                }
            } catch (PlanningErrorException ex) {
                System.err.printf("Planning error, line %d: %s\n", i, ex.getMessage());
            }
        }
        if (isSave) {
            System.out.println("Baseline files generated at: " + m_baselinePath);
        }
    }

    // parameters : path to baseline and the new plans
    // size : number of total files in the baseline directory
    public static void configDiff(String config) throws Exception {
        if ( ! setUp(config)) {
            return;
        }
        m_reportWriter.write("===================================================================Begin " + config + "\n");
        AbstractPlanNode pn1 = null;
        AbstractPlanNode pn2 = null;
        int size = m_stmts.size();
        String baseStmt = null;
        for (int i = 0; i < size; i++) {
            // * Enable for debug:*/ System.out.println("DEBUG: comparing " + m_savePlanPath + config + ".plan" + i + " and " + m_baselinePath + config + ".plan" + i);
            ArrayList<String> getsql = new ArrayList<String>();
            pn1 = loadPlanFromFile(m_baselinePath + config + ".plan" + i, getsql);
            if (pn1 == null) {
                continue;
            }

            baseStmt = getsql.get(0);
            // if sql stmts not consistent
            if (!baseStmt.equalsIgnoreCase(m_stmts.get(i))) {
                diffPair strPair = new diffPair(baseStmt, m_stmts.get(i));
                m_reportWriter.write("Statement " + i + " of " + config + "/config:\n" +
                        "SQL statement is not consistent with the one in baseline :\n" +
                        strPair.toString() + "\n");
                m_numFail++;
                continue;
            }

            pn2  = loadPlanFromFile(m_workPath + config + ".plan" + i, getsql);
            if (pn2 == null) {
                continue;
            }

            if (diff(pn1, pn2, false)) {
                m_numPass++;
                if (m_reportExplainedPlan) {
                    m_reportWriter.write("SQL statement:\n" + m_stmts.get(i) + "\n");
                    m_reportWriter.write("\nExplained plan:\n" + pn2.toExplainPlanString() + "\n");
                }
            }
            else {
                m_numFail++;
                m_reportWriter.write("Statement " + i + " of " + config + ": \n");
                // TODO add more logic to determine which plan is better
                if (!m_changedSQL) {
                    if (m_treeSizeDiff < 0) {
                        m_reportWriter.write("Old plan might be better\n");
                    }
                    else if (m_treeSizeDiff > 0) {
                        m_reportWriter.write("New plan might be better\n");
                    }
                }

                for (String msg : m_diffMessages) {
                    boolean isIgnore = false;
                    for (String filter : m_filters) {
                        if (msg.contains(filter)) {
                            isIgnore = true;
                            break;
                        }
                    }
                    if (!isIgnore)
                        m_reportWriter.write(msg + "\n\n");
                }
                if (m_reportSQLStatement) {
                    m_reportWriter.write("SQL statement:\n" + baseStmt + "\n==>\n" + m_stmts.get(i) + "\n");
                }

                if (m_reportDiffExplainedPlan) {
                    m_reportWriter.write("\nExplained plan:\n" + pn1.toExplainPlanString() + "\n==>\n" + pn2.toExplainPlanString() + "\n");
                }

                m_reportWriter.write("Path to the config file :" + config + "\n" +
                                     "Path to the baseline file :" + m_baselinePath + config + ".plan" + i + "\n" +
                                     "Path to the current plan file :" + m_workPath + config + ".plan" + i +
                                     "\n\n----------------------------------------------------------------------\n");
            }
        }
        m_reportWriter.write("===================================================================" +
                            "End " + config + "\n");
        m_reportWriter.flush();
    }

    public static boolean diffInlineAndJoin(AbstractPlanNode oldpn1, AbstractPlanNode newpn2) {
        m_treeSizeDiff = 0;
        boolean noDiff = true;
        List<String> messages = new ArrayList<>();
        List<AbstractPlanNode> list1 = oldpn1.getPlanNodeList();
        List<AbstractPlanNode> list2 = newpn2.getPlanNodeList();
        int size1 = list1.size();
        int size2 = list2.size();
        m_treeSizeDiff = size1 - size2;
        diffPair intdiffPair = new diffPair(0,0);
        diffPair stringdiffPair = new diffPair(null,null);
        if (size1 != size2) {
            intdiffPair.set(size1, size2);
            messages.add("Plan tree size diff: " + intdiffPair.toString());
        }
        Map<String,ArrayList<Integer>> planNodesPosMap1 = new LinkedHashMap<String,ArrayList<Integer>> ();
        Map<String,ArrayList<AbstractPlanNode>> inlineNodesPosMap1 = new LinkedHashMap<String,ArrayList<AbstractPlanNode>> ();

        Map<String,ArrayList<Integer>> planNodesPosMap2 = new LinkedHashMap<String,ArrayList<Integer>> ();
        Map<String,ArrayList<AbstractPlanNode>> inlineNodesPosMap2 = new LinkedHashMap<String,ArrayList<AbstractPlanNode>> ();

        fetchPositionInfoFromList(list1, planNodesPosMap1, inlineNodesPosMap1);
        fetchPositionInfoFromList(list2, planNodesPosMap2, inlineNodesPosMap2);

        planNodePositionDiff(planNodesPosMap1, planNodesPosMap2, messages);
        inlineNodePositionDiff(inlineNodesPosMap1, inlineNodesPosMap2, messages);

        // join nodes diff
        List<AbstractPlanNode> joinNodes1 = getJoinNodes(list1);
        List<AbstractPlanNode> joinNodes2 = getJoinNodes(list2);
        size1 = joinNodes1.size();
        size2 = joinNodes2.size();
        if (size1 != size2) {
            intdiffPair.set(size1 , size2);
            messages.add("Join Nodes Number diff:\n" + intdiffPair.toString() + "\nSQL statement might be changed.");
            m_changedSQL = true;
            String str1 = "";
            String str2 = "";
            for (AbstractPlanNode pn : joinNodes1) {
                str1 = str1 + pn.toString() + ", ";
            }
            for (AbstractPlanNode pn : joinNodes2) {
                str2 = str2 + pn.toString() + ", ";
            }
            if (str1.length() > 1) {
                str1 = (str1.subSequence(0, str1.length()-2)).toString();
            }
            if (str2.length() > 1) {
                str2 = (str2.subSequence(0, str2.length()-2)).toString();
            }
            stringdiffPair.set(str1, str2);
            messages.add("Join Node List diff: " + "\n" + stringdiffPair.toString() + "\n");
        }
        else {
            for (int i = 0 ; i < size1 ; i++) {
                AbstractPlanNode pn1 = joinNodes1.get(i);
                AbstractPlanNode pn2 = joinNodes2.get(i);
                PlanNodeType pnt1 = pn1.getPlanNodeType();
                PlanNodeType pnt2 = pn2.getPlanNodeType();
                if (!pnt1.equals(pnt2)) {
                    stringdiffPair.set(pn1.toString(), pn2.toString());
                    messages.add("Join Node Type diff:\n" + stringdiffPair.toString());
                }
            }
        }

        for (String msg : messages) {
            if (msg.contains("diff") || msg.contains("Diff")) {
                noDiff = false;
                break;
            }
        }
        m_diffMessages.addAll(messages);
        return noDiff;
    }

    private static void fetchPositionInfoFromList(Collection<AbstractPlanNode> list,
            Map<String,ArrayList<Integer>> planNodesPosMap,
            Map<String,ArrayList<AbstractPlanNode>> inlineNodesPosMap) {
        for (AbstractPlanNode pn : list) {
            String nodeTypeStr = pn.getPlanNodeType().name();
            if (!planNodesPosMap.containsKey(nodeTypeStr)) {
                ArrayList<Integer> intList = new ArrayList<Integer>();
                intList.add(pn.getPlanNodeId());
                planNodesPosMap.put(nodeTypeStr, intList);
            }
            else {
                planNodesPosMap.get(nodeTypeStr).add(pn.getPlanNodeId());
            }
            // walk inline nodes
            for (AbstractPlanNode inlinepn : pn.getInlinePlanNodes().values()) {
                String inlineNodeTypeStr = inlinepn.getPlanNodeType().name();
                if (!inlineNodesPosMap.containsKey(inlineNodeTypeStr)) {
                    ArrayList<AbstractPlanNode> nodeList = new ArrayList<AbstractPlanNode>();
                    nodeList.add(pn);
                    inlineNodesPosMap.put(inlineNodeTypeStr, nodeList);
                }
                else {
                    inlineNodesPosMap.get(inlineNodeTypeStr).add(pn);
                }
            }
        }
    }

    private static void planNodePositionDiff(Map<String,ArrayList<Integer>> planNodesPosMap1,
            Map<String,ArrayList<Integer>> planNodesPosMap2, List<String> messages)
    {
        Set<String> typeWholeSet = new HashSet<String>();
        typeWholeSet.addAll(planNodesPosMap1.keySet());
        typeWholeSet.addAll(planNodesPosMap2.keySet());

        for (String planNodeTypeStr : typeWholeSet) {
            if ( ! planNodesPosMap1.containsKey(planNodeTypeStr) &&
                    planNodesPosMap2.containsKey(planNodeTypeStr)) {
                diffPair strPair = new diffPair(null, planNodesPosMap2.get(planNodeTypeStr).toString());
                messages.add(planNodeTypeStr + " diff: \n" + strPair.toString());
            }
            else if (planNodesPosMap1.containsKey(planNodeTypeStr) &&
                    ! planNodesPosMap2.containsKey(planNodeTypeStr)) {
                diffPair strPair = new diffPair(planNodesPosMap1.get(planNodeTypeStr).toString(), null);
                messages.add(planNodeTypeStr + " diff: \n" + strPair.toString());
            }
            else {
                diffPair strPair = new diffPair(planNodesPosMap1.get(planNodeTypeStr).toString(),
                        planNodesPosMap2.get(planNodeTypeStr).toString());
                if (!strPair.equals()) {
                    messages.add(planNodeTypeStr + " diff: \n" + strPair.toString());
                }
            }
        }
    }

    private static void inlineNodePositionDiff(Map<String,ArrayList<AbstractPlanNode>> inlineNodesPosMap1, Map<String,ArrayList<AbstractPlanNode>> inlineNodesPosMap2, List<String> messages)
    {
        Set<String> typeWholeSet = new HashSet<String>();
        typeWholeSet.addAll(inlineNodesPosMap1.keySet());
        typeWholeSet.addAll(inlineNodesPosMap2.keySet());

        for (String planNodeTypeStr : typeWholeSet) {
            if ( ! inlineNodesPosMap1.containsKey(planNodeTypeStr) &&
                    inlineNodesPosMap2.containsKey(planNodeTypeStr)) {
                diffPair strPair = new diffPair(null, inlineNodesPosMap2.get(planNodeTypeStr).toString());
                messages.add("Inline " + planNodeTypeStr + " diff: \n" + strPair.toString());
            }
            else if (inlineNodesPosMap1.containsKey(planNodeTypeStr) &&
                    ! inlineNodesPosMap2.containsKey(planNodeTypeStr)) {
                diffPair strPair = new diffPair(inlineNodesPosMap1.get(planNodeTypeStr).toString(), null);
                messages.add("Inline " + planNodeTypeStr + " diff: \n" + strPair.toString());
            }
            else {
                diffPair strPair = new diffPair(inlineNodesPosMap1.get(planNodeTypeStr).toString(), inlineNodesPosMap2.get(planNodeTypeStr).toString());
                if ( ! strPair.equals()) {
                    messages.add("Inline " + planNodeTypeStr + " diff: \n" + strPair.toString());
                }
            }
        }
    }

    private static void scanNodeDiffModule(int leafID, AbstractScanPlanNode spn1,
            AbstractScanPlanNode spn2, ArrayList<String> messages)
    {
        diffPair stringdiffPair = new diffPair("", "");
        String table1 = "";
        String table2 = "";
        String nodeType1 = "";
        String nodeType2 = "";
        String index1 = "";
        String index2 = "";
        if (spn1 == null && spn2 != null) {
            table2 = spn2.getTargetTableName();
            nodeType2 = spn2.getPlanNodeType().toString();
            if (nodeType2.equalsIgnoreCase(PlanNodeType.INDEXSCAN.name())) {
                index2 = ((IndexScanPlanNode)spn2).getTargetIndexName();
            }
        }
        else if (spn2 == null && spn1 != null) {
            table1 = spn1.getTargetTableName();
            nodeType1 = spn1.getPlanNodeType().toString();
            if (nodeType1.equalsIgnoreCase(PlanNodeType.INDEXSCAN.name())) {
                index1 = ((IndexScanPlanNode)spn1).getTargetIndexName();
            }
        }
        // both null is not possible
        else {
            table1 = spn1.getTargetTableName();
            table2 = spn2.getTargetTableName();
            nodeType1 = spn1.getPlanNodeType().toString();
            nodeType2 = spn2.getPlanNodeType().toString();
            if (nodeType1.equalsIgnoreCase(PlanNodeType.INDEXSCAN.name())) {
                index1 = ((IndexScanPlanNode)spn1).getTargetIndexName();
            }
            if (nodeType2.equalsIgnoreCase(PlanNodeType.INDEXSCAN.name())) {
                index2 = ((IndexScanPlanNode)spn2).getTargetIndexName();
            }
        }
        if (!table1.equals(table2)) {
            stringdiffPair.set(table1.equals("") ? null : nodeType1 + " on " + table1,
                                table2.equals("") ? null : nodeType2 + " on " + table2);
            messages.add("Table diff at leaf " + leafID + ":" + "\n" + stringdiffPair.toString());
        }
        else if (!nodeType1.equals(nodeType2)) {
            stringdiffPair.set(nodeType1 + " on " + table1, nodeType2 + " on " + table2);
            messages.add("Scan diff at leaf " + leafID + " :" + "\n" + stringdiffPair.toString());
        }
        else if (nodeType1.equalsIgnoreCase(PlanNodeType.INDEXSCAN.name())) {
            if (!index1.equals(index2)) {
                stringdiffPair.set(index1, index2);
                messages.add("Index diff at leaf " + leafID + " :" + "\n" + stringdiffPair.toString());
            }
            else {
                messages.add("Same at leaf " + leafID);
            }
        }
        // either index scan using same index or seqscan on same table
        else {
            messages.add("Same at leaf " + leafID);
        }
    }

    public static boolean diffScans(AbstractPlanNode oldpn, AbstractPlanNode newpn)
    {
        m_changedSQL = false;
        boolean noDiff = true;
        List<AbstractScanPlanNode> list1 = oldpn.getScanNodeList();
        List<AbstractScanPlanNode> list2 = newpn.getScanNodeList();
        int size1 = list1.size();
        int size2 = list2.size();
        int max = Math.max(size1, size2);
        int min = Math.min(size1, size2);
        diffPair intdiffPair = new diffPair(0,0);
        ArrayList<String> messages = new ArrayList<String>();
        if (max == 0) {
            messages.add("0 scan statement");
        }
        else {
            AbstractScanPlanNode spn1 = null;
            AbstractScanPlanNode spn2 = null;
            if (size1 != size2) {
                intdiffPair.set(size1, size2);
                messages.add("Scan time diff : " + "\n" + intdiffPair.toString() +
                       "\nSQL statement might be changed");
                m_changedSQL = true;
                for (int i = 0; i < min; i++) {
                    spn1 = list1.get(i);
                    spn2 = list2.get(i);
                    scanNodeDiffModule(i, spn1, spn2, messages);
                }
                // lists size are different
                if (size2 < max) {
                    for (int i = min; i < max; i++) {
                        spn1 = list1.get(i);
                        spn2 = null;
                        scanNodeDiffModule(i, spn1, spn2, messages);
                    }
                }
                else if (size1 < max) {
                    for (int i = min; i < max; i++) {
                        spn1 = null;
                        spn2 = list2.get(i);
                        scanNodeDiffModule(i, spn1, spn2, messages);
                    }
                }
            }
            else {
                messages.add("same leaf size");
                if (max == 1) {
                    messages.add("Single scan plan");
                    spn1 = list1.get(0);
                    spn2 = list2.get(0);
                    scanNodeDiffModule(0, spn1, spn2, messages);
                }
                else {
                    messages.add("Join query");
                    for (int i = 0; i < max; i++) {
                        spn1 = list1.get(i);
                        spn2 = list2.get(i);
                        scanNodeDiffModule(i, spn1, spn2, messages);
                    }
                }
            }
        }
        for (String msg : messages) {
            if (msg.contains("diff") || msg.contains("Diff")) {
                noDiff = false;
                break;
            }
        }
        m_diffMessages.addAll(messages);
        return noDiff;
    }

    // return true is there are no diff
    // false if there's any diff
    public static boolean diff(AbstractPlanNode oldpn, AbstractPlanNode newpn, boolean print)
    {
        m_diffMessages.clear();
        boolean noDiff1 = diffScans(oldpn, newpn);
        boolean noDiff2 = diffInlineAndJoin(oldpn, newpn);
        if (noDiff1 && noDiff2) {
            return true;
        }
        if (print) {
            for (String msg : m_diffMessages) {
                System.out.println(msg);
            }
        }
        return false;
    }
}
