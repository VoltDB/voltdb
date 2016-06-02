/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

    static String[] conditions = {"1=1", "1=0", "TRUE", "FALSE", "1>2", "6-1>=0"};

    private void populateTable(Client client, int tableId) throws Exception{
        String procName = "Insert" + tableId;
        for (int i=0; i<3; i++) {
            client.callProcedure(procName, 2*i+1);
        }
    }

    public void testBooleanLiteralsInWhere() throws Exception {
        String sqlBody;
        Client client = getClient();
        for (int i=1; i<=4; i++) {
            populateTable(client, i);
        }

        long[] resultsForWhere = {3, 0, 3, 0, 0, 3};
        long[] resultsForOn = {9, 0, 9, 0, 0, 9};
        long[] resultsForCaseWhen = {1, -1, 1, -1, -1, 1};
        String whereQuery = "SELECT AINT FROM T%d WHERE %%s";
        String joinQuery = "SELECT * FROM T%d a JOIN T%d b ON %%s";
        String caseWhenQuery = "SELECT CASE WHEN %%s THEN AINT ELSE -1 END FROM T%d ORDER BY AINT";
        subTestBooleanLiteralsInQuery(String.format(whereQuery, 1), resultsForWhere, client, true);
        subTestBooleanLiteralsInQuery(String.format(whereQuery, 3), resultsForWhere, client, true);
        subTestBooleanLiteralsInQuery(String.format(joinQuery, 1, 2), resultsForOn, client, true); // replicated join replicated.
        subTestBooleanLiteralsInQuery(String.format(joinQuery, 2, 3), resultsForOn, client, true); // replicated join partitioned.
        subTestBooleanLiteralsInQuery(String.format(joinQuery, 3, 2), resultsForOn, client, true); // partitioned join replicated.
//        subTestBooleanLiteralsInQuery(String.format(joinQuery, 3, 4) + " AND a.AINT=b.AINT", resultsForWhere, client, true); // partitioned join partitioned.
        subTestBooleanLiteralsInQuery(String.format(caseWhenQuery, 1), resultsForCaseWhen, client, false);
        subTestBooleanLiteralsInQuery(String.format(caseWhenQuery, 3), resultsForCaseWhen, client, false);
    }

    private void subTestBooleanLiteralsInQuery(String sqlBody, long[] expectedResults,
                                                   Client client, boolean examineCount) throws Exception {
        assertEquals(conditions.length, expectedResults.length);
        VoltTable vt;
        for (int i=0; i<conditions.length; i++) {
            String sql = String.format(sqlBody, conditions[i]);
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            vt.advanceRow();
            if (examineCount)
                assertEquals(expectedResults[i], vt.getRowCount());
            else
                assertEquals(expectedResults[i], vt.getLong(0));
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
            project.addStmtProcedure("Insert" + tableId, "INSERT INTO T" + tableId + " VALUES (?)");
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
            addTableToProject(project, i, i>2);
        }

        // CONFIG #1: Local Site/Partitions running on JNI backend
        config = new LocalCluster("bool-voltdbBackend.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        // alternative to enable for debugging */ config = new LocalCluster("IPC-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_IPC);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // CONFIG #2: HSQL
        config = new LocalCluster("bool-hsqlBackend.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
