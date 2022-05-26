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

package org.voltdb.compilereport;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import junit.framework.TestCase;

import org.voltdb.compiler.VoltCompiler;
import org.voltdb.utils.CatalogSizing;
import org.voltdb.utils.InMemoryJarfile;

import com.google_voltpatches.common.base.Charsets;

public class TestReportMaker extends TestCase {

    private static final int MAX_OVERHEAD = 4;

    private static String compileAndGenerateCatalogReport(String ddl, boolean isXDCR) throws IOException {
        // Let's try not to "drool" files into the current directory.
        // Generate random temporary names for the .jar and DDL files,
        // and delete them before we exit this method.  We will still
        // drool the catalog-report.html file, though (many tests seem
        // to do this).
        UUID uuid = UUID.randomUUID();
        String jarName = uuid + ".jar";
        String ddlName = uuid + ".sql";
        String report = null;
        PrintWriter ddlWriter = null;
        try {
            ddlWriter = new PrintWriter(ddlName);
            ddlWriter.println(ddl);
            ddlWriter.close();
            VoltCompiler vc = new VoltCompiler(true, isXDCR); // trick it into behaving like standalone
            boolean success = vc.compileFromDDL(jarName, ddlName);
            assertTrue("Catalog compilation failed!", success);

            InMemoryJarfile jarfile = new InMemoryJarfile(Files.readAllBytes(Paths.get(jarName)));
            report = new String(jarfile.get(VoltCompiler.CATLOG_REPORT), Charsets.UTF_8);
        }
        catch (Exception e) {
        }
        finally {
            if (ddlWriter != null)
                ddlWriter.close();
            Path ddlPath = Paths.get(ddlName);
            if (ddlPath.toFile().exists()) {
                Files.delete(ddlPath);
            }

            Path jarPath = Paths.get(jarName);
            if (jarPath.toFile().exists()) {
                Files.delete(jarPath);
            }
        }

        return report;
    }

    private void validateDeltas(int input, int testcase,
                                int byte_increment, int percent_increment)
    {
        if (byte_increment < 0) {
            System.out.println("Failing case " + testcase + " input " + input +
                               " byte_increment " + byte_increment);
        }
        assertTrue(byte_increment >= 0);
        if (byte_increment >= ((1<<19) + MAX_OVERHEAD)) {
            System.out.println("Failing case " + testcase + " input " + input +
                               " byte_increment " + byte_increment);
        }
        assertTrue(byte_increment < ((1<<19) + MAX_OVERHEAD));
        if (percent_increment >= 66) {
            System.out.println("Failing case " + testcase + " input " + input +
                    " percent_increment " + percent_increment);
        }
        assertTrue(percent_increment < 66);
    }

    private int validateAllocation(int input)
    {
        int result = CatalogSizing.testOnlyAllocationSizeForObject(input);
        // Add the minimum overhead size to the input to establish a baseline
        // against which the round-up effect can be measured.
        input += 4 + 8;
        int byte_overhead = result - input;
        int percent_overhead = byte_overhead * 100 / input;
        validateDeltas(input, 0, byte_overhead, percent_overhead);
        return result;
    }

    private void validateTrend(int input, int suite, int low, int medium, int high)
    {
        int byte_increment_ml = medium - low;
        int percent_increment_ml = byte_increment_ml * 100 / medium;
        int byte_increment_hm = high - medium;
        int percent_increment_hm = byte_increment_hm * 100 / medium;
        int byte_increment_hl = high - low;
        int percent_increment_hl = byte_increment_hl * 100 / medium;
        validateDeltas(input, suite+1, byte_increment_ml, percent_increment_ml);
        validateDeltas(input, suite+2, byte_increment_hm, percent_increment_hm);
        validateDeltas(input, suite+3, byte_increment_hl, percent_increment_hl);
    }

    private void validateAllocationSpan(int input)
    {
        int result = validateAllocation(input);
        int result_down = validateAllocation(input-1);
        int result_up = validateAllocation(input+1);
        int result_in = validateAllocation(input*7/8);
        int result_out = validateAllocation(input*8/7);
        assertTrue(result_up <= (1L<<20) + MAX_OVERHEAD);
        assertTrue(result_out <= (1L<<20) + MAX_OVERHEAD);
        validateTrend(input, 0, result_down, result, result_up);
        validateTrend(input, 4, result_in, result, result_out);
    }

    // This purposely duplicates test logic in the common/ThreadLocalPoolTest cpp unit test.
    // That test applies to the actual string allocation sizing logic.
    // This test applies to the java recap of that logic used for estimation purposes.
    // Ideally, they use compatible algorithms and so pass or fail the same sanity tests.
    // This does not guarantee that their algorithms are in synch, but that level of testing
    // would take more test-only JNI plumbing than seems warranted.
    public void testCatalogSizingStringAllocationReasonableness()
    {
        // CHEATING SLIGHTLY -- The tests are a little too stringent when applied
        // to the actual MIN_REQUEST value of 1.
        final int MIN_REQUEST = 2;

        // Extreme Inputs
        validateAllocation(MIN_REQUEST);
        validateAllocation(MIN_REQUEST+1);
        validateAllocation(1<<20);
        validateAllocation((1<<20) + MAX_OVERHEAD);

        // A Range of Fixed Inputs
        int fixedTrial[] = { 4, 7, 10, 13, 16,
                             1<<5, 1<<6, 1<<7, 1<<8, 1<<9, 1<<10, 1<<12, 1<<14, 1<<18,
                             3<<5, 3<<6, 3<<7, 3<<8, 3<<9, 3<<10, 3<<12, 3<<14, 3<<18,
                             5<<5, 5<<6, 5<<7, 5<<8, 5<<9, 5<<10, 5<<12, 5<<14 };
        for (int trial : fixedTrial) {
            validateAllocationSpan(trial);
        }

        // A Range of Random Inputs Skewed towards smaller human-scale string data allocations.
        int trialCount = 10000;
        for (int randTrial = 0; randTrial < trialCount; ++randTrial) {
            // Sum a small constant to avoid small extremes, a small linear component to get a wider range of
            // unique values, and a component with an inverse distribution to favor numbers nearer the low end.
            int skewedInt = (int) (MAX_OVERHEAD/2 + (Math.random() % (1<<10)) + (1<<19) / (1 + (Math.random() % (1<<19))));
            validateAllocationSpan(skewedInt);
        }
    }

    public void testEscapesRenderedText() throws IOException {
        final String ddl =
                        // The very large varchar column will generate a compiler warning
                        // Type needs to be converted to "VARCHAR(... bytes)"
                "CREATE TABLE FunkyDefaults ( " +
                "vc VARCHAR(262145) DEFAULT '<b>Hello, \"World\"!</b>', " +
                "i INTEGER " +
                "); " +
                "CREATE INDEX FunkyIndex on FUNKYDEFAULTS (CASE WHEN i < 10 THEN 0 ELSE 10 END CASE); " +
                "CREATE INDEX NormalIndex on FUNKYDEFAULTS (vc); " +
                "CREATE VIEW MyView (vc, TheCount, TheMax) AS " +
                "SELECT vc, COUNT(*), MAX(i) " +
                "FROM FunkyDefaults " +
                "WHERE i < 100 AND i > 50 AND vc LIKE '%\"!<b/b>' " +
                "GROUP BY vc; " +
                "CREATE PROCEDURE NeedsEscape AS " +
                "SELECT i FROM FUNKYDEFAULTS WHERE i<? AND i>?;";
        String report = compileAndGenerateCatalogReport(ddl, false);

        // Lock down all the places in ReportMaker
        // where we insert escape sequences for HTML entities:
        //   - In the DDL output in the Schema tab
        //   - The SQL statement text in the Procedures & SQL tab
        //   - The "explain plan" text under the SQL statement
        //   - DDL seems also to be in the Size Worksheet tab,
        //       but I can't see it rendered?
        //   - The Overview tab warnings section
        //   - The DDL Source tab

        // < and > in SQL statements should be escaped.
        // Procedures & SQL
        assertTrue(report.contains("WHERE i&lt;? AND i&gt;?"));
        assertFalse(report.contains("i<?"));
        assertFalse(report.contains("i>?"));

        // DEFAULT '<b>Hello, "World"!<b>
        // Should have its angle brackets and quotes escaped.  Table definitions are
        // visible in both the "Schema" and "DDL Source" tabs.
        assertTrue(report.contains("DEFAULT '&lt;b&gt;Hello, &quot;World&quot;!&lt;/b&gt;'"));
        assertFalse(report.contains("DEFAULT '<b>Hello, \"World\"!</b>'"));

        // "Explain Plan" output should also be escaped:
        // (spaces in explain plan are replaced by &nbsp;)
        assertTrue(report.contains("filter&nbsp;by&nbsp;"));
        assertTrue(report.contains("(I&nbsp;&gt;&nbsp;?1)"));
        assertTrue(report.contains("&nbsp;AND&nbsp;"));
        assertTrue(report.contains("(I&nbsp;&lt;&nbsp;?0)"));

        // Warnings in the Overview tab should have escaped ", &, <, >, etc.
        assertTrue(report.contains("To eliminate this warning, specify &quot;VARCHAR(262145 BYTES)&quot;"));
        assertFalse(report.contains("To eliminate this warning, specify \"VARCHAR(262145 BYTES)\""));
    }

    // Under active/active DR, create a DRed table without index will trigger warning
    public void testTableWithoutIndexGetFullTableScanWarning() throws IOException {
        final String tableName = "TABLE_WITHOUT_INDEX";
        final String ddl =
                "CREATE TABLE " + tableName + " ( " +
                "t1 INTEGER, " +
                "t2 BIGINT, " +
                "t3 VARCHAR(32) " +
                "); " +
                "DR TABLE " + tableName + "; ";
        String report = compileAndGenerateCatalogReport(ddl, true);

        assertTrue(report.contains("Table " + tableName + " doesn't have any unique index, it will cause full table scans to update/delete DR record and may become slower as table grow."));
    }

    public void testExplainViewTable() throws IOException {
        final String ddl =
            "CREATE TABLE VIEW_SOURCE (" +
            "    GROUPBY1 INT NOT NULL," +
            "    GROUPBY2 INT NOT NULL," +
            "    MINCOL DECIMAL NOT NULL," +
            "    SUMCOL FLOAT NOT NULL, " +
            "    MAXCOL TIMESTAMP NOT NULL," +
            "    COUNTCOL VARCHAR(128) NOT NULL" +
            ");" +
            "PARTITION TABLE VIEW_SOURCE ON COLUMN GROUPBY1;" +
            "CREATE INDEX IDXG1 ON VIEW_SOURCE(GROUPBY1, MINCOL);" +
            "CREATE VIEW VIEW_SEQSCAN (GROUPBY2, CNT, MINSEQ, SUMCOL, MAXSEQ, COUNTCOL) AS" +
            "    SELECT GROUPBY2, COUNT(*), MIN(MINCOL), SUM(SUMCOL), MAX(MAXCOL), COUNT(COUNTCOL)" +
            "    FROM VIEW_SOURCE GROUP BY GROUPBY2;" +
            "CREATE VIEW VIEW_IDXPLAN (GROUPBY1, CNT, MINIDX, SUMCOL, MAXPLAN, COUNTCOL) AS" +
            "    SELECT GROUPBY1, COUNT(*), MIN(MINCOL), SUM(SUMCOL), MAX(MAXCOL), COUNT(COUNTCOL)" +
            "    FROM VIEW_SOURCE GROUP BY GROUPBY1;";
        String report = compileAndGenerateCatalogReport(ddl, false);
        assertTrue(report.contains("<thead><tr><th>View Task</th><th>Execution Plan</th></tr>"));
        assertTrue(report.contains("<tr class='primaryrow2'><td>Refresh MIN column \"MINIDX\"</td><td>Built-in&nbsp;index&nbsp;scan&nbsp;&quot;IDXG1&quot;.</td></tr>"));
        assertTrue(report.contains("<tr class='primaryrow2'><td>Refresh MAX column \"MAXPLAN\"</td><td>RETURN&nbsp;RESULTS&nbsp;TO&nbsp;STORED&nbsp;PROCEDURE<br/>&nbsp;INDEX&nbsp;SCAN&nbsp;of&nbsp;&quot;VIEW_SOURCE&quot;&nbsp;using&nbsp;&quot;IDXG1&quot;<br/>&nbsp;range-scan&nbsp;on&nbsp;1&nbsp;of&nbsp;2&nbsp;cols&nbsp;from&nbsp;(GROUPBY1&nbsp;&gt;=&nbsp;?0)&nbsp;while&nbsp;(GROUPBY1&nbsp;=&nbsp;?0),&nbsp;filter&nbsp;by&nbsp;(MAXCOL&nbsp;&lt;=&nbsp;?1)<br/>&nbsp;&nbsp;inline&nbsp;Serial&nbsp;AGGREGATION&nbsp;ops:&nbsp;MAX(VIEW_SOURCE.MAXCOL)<br/></td></tr>"));
        assertTrue(report.contains("<tr class='primaryrow2'><td>Refresh MIN column \"MINSEQ\"</td><td>Built-in&nbsp;sequential&nbsp;scan.</td></tr>"));
        assertTrue(report.contains("<tr class='primaryrow2'><td>Refresh MAX column \"MAXSEQ\"</td><td>Built-in&nbsp;sequential&nbsp;scan.</td></tr>"));
    }

    public void testSelectWithSubquery() throws IOException {
        final String ddl =
            "CREATE TABLE TABLE_SOURCE1 (" +
            "    COLUMN1 INT NOT NULL," +
            "    COLUMN2 INT NOT NULL" +
            "); " +
            "CREATE TABLE TABLE_SOURCE2 (" +
            "    COLUMN1 INT NOT NULL," +
            "    COLUMN2 INT NOT NULL" +
            "); " +
            "CREATE INDEX IDXG1 ON TABLE_SOURCE1(COLUMN1); " +
            "CREATE PROCEDURE " +
            "   SELECT_FROM_TABLE_SOURCE1_TABLE_SOURCE2 AS " +
            "       SELECT COLUMN1 FROM TABLE_SOURCE1 WHERE COLUMN1 IN (SELECT COLUMN2 FROM TABLE_SOURCE2);";
        String report = compileAndGenerateCatalogReport(ddl, false);
        assertTrue(report.contains("<p>Read-only access to tables: <a href='#s-TABLE_SOURCE1'>TABLE_SOURCE1</a>, <a href='#s-TABLE_SOURCE2'>TABLE_SOURCE2</a></p>"));
    }

    public void testSelectWithScalarSubquery() throws IOException {
        final String ddl =
            "CREATE TABLE TABLE_SOURCE1 (" +
            "    COLUMN1 INT NOT NULL," +
            "    COLUMN2 INT NOT NULL" +
            "); " +
            "CREATE TABLE TABLE_SOURCE2 (" +
            "    COLUMN1 INT NOT NULL," +
            "    COLUMN2 INT NOT NULL" +
            "); " +
            "CREATE INDEX IDXG1 ON TABLE_SOURCE1(COLUMN1); " +
            "CREATE PROCEDURE SELECT_FROM_TABLE_SOURCE1_TABLE_SOURCE2 AS " +
            "   SELECT COLUMN1, (SELECT COLUMN2 FROM TABLE_SOURCE2 LIMIT 1) FROM TABLE_SOURCE1;";
        String report = compileAndGenerateCatalogReport(ddl, false);
        assertTrue(report.contains("<p>Read-only access to tables: <a href='#s-TABLE_SOURCE1'>TABLE_SOURCE1</a>, <a href='#s-TABLE_SOURCE2'>TABLE_SOURCE2</a></p>"));
    }

    public void testDeleteWithSubquery() throws IOException {
        final String ddl =
            "CREATE TABLE TABLE_SOURCE1 (" +
            "    COLUMN1 INT NOT NULL," +
            "    COLUMN2 INT NOT NULL" +
            "); " +
            "CREATE TABLE TABLE_SOURCE2 (" +
            "    COLUMN1 INT NOT NULL," +
            "    COLUMN2 INT NOT NULL" +
            "); " +
            "CREATE INDEX IDXG1 ON TABLE_SOURCE1(COLUMN1); " +
            "CREATE PROCEDURE DELETE_FROM_TABLE_SOURCE1_TABLE_SOURCE2 AS " +
            "   DELETE FROM TABLE_SOURCE1 WHERE COLUMN1 IN (SELECT COLUMN2 FROM TABLE_SOURCE2);";
        String report = compileAndGenerateCatalogReport(ddl, false);
        assertTrue(report.contains("<p>Read/Write by procedures: <a href='#p-DELETE_FROM_TABLE_SOURCE1_TABLE_SOURCE2'>DELETE_FROM_TABLE_SOURCE1_TABLE_SOURCE2</a></p>"));
        assertTrue(report.contains("<p>Read-only by procedures: <a href='#p-DELETE_FROM_TABLE_SOURCE1_TABLE_SOURCE2'>DELETE_FROM_TABLE_SOURCE1_TABLE_SOURCE2</a></p><p>No indexes defined on table.</p>"));
    }

    public void testUpdateWithSubquery() throws IOException {
        final String ddl =
            "CREATE TABLE TABLE_SOURCE1 (" +
            "    COLUMN1 INT NOT NULL," +
            "    COLUMN2 INT NOT NULL" +
            "); " +
            "CREATE TABLE TABLE_SOURCE2 (" +
            "    COLUMN1 INT NOT NULL," +
            "    COLUMN2 INT NOT NULL" +
            "); " +
            "CREATE INDEX IDXG1 ON TABLE_SOURCE1(COLUMN1); " +
            "CREATE PROCEDURE UPDATE_TABLE_SOURCE1_TABLE_SOURCE2 AS " +
            "   UPDATE TABLE_SOURCE1 SET COLUMN2 = 3 WHERE COLUMN1 IN (SELECT COLUMN2 FROM TABLE_SOURCE2);";
        String report = compileAndGenerateCatalogReport(ddl, false);
        assertTrue(report.contains("<p>Read/Write by procedures: <a href='#p-UPDATE_TABLE_SOURCE1_TABLE_SOURCE2'>UPDATE_TABLE_SOURCE1_TABLE_SOURCE2</a></p>"));
        assertTrue(report.contains("<p>Read-only by procedures: <a href='#p-UPDATE_TABLE_SOURCE1_TABLE_SOURCE2'>UPDATE_TABLE_SOURCE1_TABLE_SOURCE2</a></p><p>No indexes defined on table.</p>"));
    }

    public void testInsertWithSubquery() throws IOException {
        final String ddl =
            "CREATE TABLE TABLE_SOURCE1 (" +
            "    COLUMN1 INT NOT NULL," +
            "    COLUMN2 INT NOT NULL" +
            "); " +
            "CREATE TABLE TABLE_SOURCE2 (" +
            "    COLUMN1 INT NOT NULL," +
            "    COLUMN2 INT NOT NULL" +
            "); " +
            "CREATE INDEX IDXG1 ON TABLE_SOURCE1(COLUMN1); " +
            "CREATE PROCEDURE INSERT_TABLE_SOURCE1_TABLE_SOURCE2 AS " +
            "   INSERT INTO TABLE_SOURCE1 (COLUMN1, COLUMN2) VALUES ((SELECT COLUMN2 FROM TABLE_SOURCE2 LIMIT 1), 2);";
        String report = compileAndGenerateCatalogReport(ddl, false);
        assertTrue(report.contains("<p>Read/Write by procedures: <a href='#p-INSERT_TABLE_SOURCE1_TABLE_SOURCE2'>INSERT_TABLE_SOURCE1_TABLE_SOURCE2</a></p>"));
        assertTrue(report.contains("<p>Read-only by procedures: <a href='#p-INSERT_TABLE_SOURCE1_TABLE_SOURCE2'>INSERT_TABLE_SOURCE1_TABLE_SOURCE2</a></p><p>No indexes defined on table.</p>"));
    }

    // Under active/active DR, create a DRed table without index will trigger warning
    public void testTableWithIndexNoWarning() throws IOException {
        final String tableName = "TABLE_WITH_INDEX";

        final String nonUniqueIndexDDL =
                "CREATE TABLE " + tableName +" ( " +
                "p1 INTEGER, " +
                "p2 TIMESTAMP, " +
                "p3 VARCHAR(32) " +
                ");" +
                "CREATE INDEX tableIndex ON table_with_index ( p1 ); " +
                "DR TABLE " + tableName + ";";
        String report = compileAndGenerateCatalogReport(nonUniqueIndexDDL, true);
        assertTrue(report.contains("Table " + tableName + " doesn't have any unique index, it will cause full table scans to update/delete DR record and may become slower as table grow."));

        final String uniqueIndexDDL =
                "CREATE TABLE " + tableName +" ( " +
                "p1 INTEGER, " +
                "p2 TIMESTAMP, " +
                "p3 VARCHAR(32) " +
                ");" +
                "CREATE UNIQUE INDEX tableIndex ON table_with_index ( p1 ); " +
                "DR TABLE " + tableName + ";";
        report = compileAndGenerateCatalogReport(uniqueIndexDDL, true);
        assertFalse(report.contains("Table " + tableName + " doesn't have any unique index, it will cause full table scans to update/delete DR record and may become slower as table grow."));

        final String primayKeyDDL =
                "CREATE TABLE " + tableName +" ( " +
                "p1 INTEGER NOT NULL, " +
                "p2 TIMESTAMP, " +
                "p3 VARCHAR(32), " +
                "PRIMARY KEY ( p1 )" +
                ");" +
                "DR TABLE " + tableName + ";";
        report = compileAndGenerateCatalogReport(primayKeyDDL, true);
        assertFalse(report.contains("Table " + tableName + " doesn't have any unique index, it will cause full table scans to update/delete DR record and may become slower as table grow."));

        final String constaintDDL =
                "CREATE TABLE " + tableName +" ( " +
                "p1 INTEGER NOT NULL, " +
                "p2 TIMESTAMP, " +
                "p3 VARCHAR(32), " +
                "CONSTRAINT pkey PRIMARY KEY (p1) " +
                ");" +
                "DR TABLE " + tableName + ";";
        report = compileAndGenerateCatalogReport(constaintDDL, true);
        assertFalse(report.contains("Table " + tableName + " doesn't have any unique index, it will cause full table scans to update/delete DR record and may become slower as table grow."));

        final String uniqueDDL =
                "CREATE TABLE " + tableName +" ( " +
                "p1 INTEGER UNIQUE NOT NULL, " +
                "p2 TIMESTAMP UNIQUE, " +
                "p3 VARCHAR(32) " +
                ");" +
                "DR TABLE " + tableName + ";";
        report = compileAndGenerateCatalogReport(uniqueDDL, true);
        assertFalse(report.contains("Table " + tableName + " doesn't have any unique index, it will cause full table scans to update/delete DR record and may become slower as table grow."));

        final String assumeUniqueDDL =
                "CREATE TABLE " + tableName +" ( " +
                "p1 INTEGER ASSUMEUNIQUE NOT NULL, " +
                "p2 TIMESTAMP, " +
                "p3 VARCHAR(32) " +
                ");" +
                "DR TABLE " + tableName + ";";
        report = compileAndGenerateCatalogReport(assumeUniqueDDL, true);
        assertFalse(report.contains("Table " + tableName + " doesn't have any unique index, it will cause full table scans to update/delete DR record and may become slower as table grow."));
    }
}
