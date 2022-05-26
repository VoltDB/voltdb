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

package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestBooleanLiteralsSuite extends RegressionSuite {

    static String[] conditions = {"1=1", "TRUE", "6-1>=0", "1=0", "FALSE", "1>2"};
    static boolean isConditionTrue(int condId) {
        return condId < 3;
    }
    static boolean isTablePartitioned(int tableId) {
        return tableId > 2;
    }

    private void populateTable(int tableId, Client client) throws Exception{
        String procName = "T" + tableId + ".Insert";
        for (int i=0; i<3; i++) {
            client.callProcedure(procName, 2*i+1);
        }
    }

    public void testBooleanLiteralsInQueries() throws Exception {
        Client client = getClient();
        for (int i=1; i<=4; i++) {
            populateTable(i, client);
        }

        Object[][] emptyTable = {};
        Object[][] tableResult = {{1}, {3}, {5}};
        Object[][] joinResult = {{1,1}, {1,3}, {1,5}, {3,1}, {3,3}, {3,5}, {5,1}, {5,3}, {5,5}};
        Object[][] caseWhenFalseResult = {{-1}, {-1}, {-1}};
        String whereQuery = "SELECT AINT FROM T%d WHERE %%s ORDER BY AINT";
        String joinQuery = "SELECT * FROM T%d a JOIN T%d b ON %%s ORDER BY a.AINT, b.AINT";
        String caseWhenQuery = "SELECT CASE WHEN %%s THEN AINT ELSE -1 END FROM T%d ORDER BY AINT";
        subTestBooleanLiteralsInQuery(String.format(whereQuery, 1),
                                      tableResult, emptyTable, client); // replicated
        subTestBooleanLiteralsInQuery(String.format(whereQuery, 3),
                                      tableResult, emptyTable, client); // partitioned
        subTestBooleanLiteralsInQuery(String.format(joinQuery, 1, 2),
                                      joinResult, emptyTable, client); // replicated join replicated.
        subTestBooleanLiteralsInQuery(String.format(joinQuery, 2, 3),
                                      joinResult, emptyTable, client); // replicated join partitioned.
        subTestBooleanLiteralsInQuery(String.format(joinQuery, 3, 2),
                                      joinResult, emptyTable, client); // partitioned join replicated.
        // We do not test a partitioned table joining another partitioned table on a constant boolean value here.
        // Instead, we test this case in TestPlansJoin for proper error message.
        // The query is not plannable because the planner cannot guarantee that all rows would be in a single partition.
        subTestBooleanLiteralsInQuery(String.format(caseWhenQuery, 1),
                                      tableResult, caseWhenFalseResult, client); // replicated
        subTestBooleanLiteralsInQuery(String.format(caseWhenQuery, 3),
                                      tableResult, caseWhenFalseResult, client); // partitioned
    }

    private void subTestBooleanLiteralsInQuery(String sqlBody,
                                               Object[][] conditionTrueResult,
                                               Object[][] conditionFalseResult,
                                               Client client) throws Exception {
        VoltTable vt;
        for (int i=0; i<conditions.length; i++) {
            String sql = String.format(sqlBody, conditions[i]);
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            assertContentOfTable((isConditionTrue(i) ? conditionTrueResult : conditionFalseResult), vt);
        }
    }

    public TestBooleanLiteralsSuite(String name) {
        super(name);
    }

    static String tableDDL = "CREATE TABLE T%d (" +
                             "AINT INTEGER DEFAULT 0 NOT NULL," +
                             "PRIMARY KEY (AINT) );";
    static String partitionDDL = "PARTITION TABLE T%d ON COLUMN AINT;";

    static private void addTableToProject(VoltProjectBuilder project, int tableId, boolean partitioned) {
        try {
            project.addLiteralSchema(String.format(tableDDL, tableId));
            if (partitioned) {
                project.addLiteralSchema(String.format(partitionDDL, tableId));
            }
        } catch (IOException e) {
            assertFalse(true);
        }
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestBooleanLiteralsSuite.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();
        // T1 and T2 are replicated table, while T3 and T4 are partitioned.
        for (int i=1; i<=4; i++) {
            addTableToProject(project, i, isTablePartitioned(i));
        }

        // CONFIG #1: Local Site/Partitions running on JNI backend
        config = new LocalCluster("bool-voltdbBackend.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        // alternative to enable for debugging */ config = new LocalCluster("IPC-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_IPC);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // CONFIG #2: HSQL
        config = new LocalCluster("bool-hsqlBackend.jar", 2, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
