/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import com.google_voltpatches.common.base.Charsets;

public class TestReportMaker extends TestCase {

    private static String compileAndGenerateCatalogReport(String ddl) throws IOException {
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
            VoltCompiler vc = new VoltCompiler(true); // trick it into behaving like standalone
            boolean success = vc.compileFromDDL(jarName, ddlName);
            assertTrue("Catalog compilation failed!", success);
            report = new String(Files.readAllBytes(Paths.get("catalog-report.html")), Charsets.UTF_8);
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
        String report = compileAndGenerateCatalogReport(ddl);

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
        assertTrue(report.contains("filter&nbsp;by&nbsp;((I&nbsp;&lt;&nbsp;?0)&nbsp;AND&nbsp;(I&nbsp;&gt;&nbsp;?1))"));

        // Warnings in the Overview tab should have escaped ", &, <, >, etc.
        assertTrue(report.contains("To eliminate this warning, specify &quot;VARCHAR(262145 BYTES)&quot;"));
        assertFalse(report.contains("To eliminate this warning, specify \"VARCHAR(262145 BYTES)\""));
    }
}
