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

import org.voltdb.compiler.VoltCompiler;
import org.voltdb.utils.InMemoryJarfile;

import com.google_voltpatches.common.base.Charsets;

import junit.framework.TestCase;

public class TestDDLSource extends TestCase{

    private static String compileAndGenerateCatalogReport(String ddl) throws IOException {
        UUID uuid = UUID.randomUUID();
        String jarName = uuid + ".jar";
        String ddlName = uuid + ".sql";
        String report = null;
        PrintWriter ddlWriter = null;
        try {
            ddlWriter = new PrintWriter(ddlName);
            ddlWriter.println(ddl);
            ddlWriter.close();
            VoltCompiler vc = new VoltCompiler(true, false); // trick it into behaving like standalone
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

    public void testCreateTableDDL() throws IOException {
        final String ddlCreateTable =
                "create table AllTypes ("
                        + "clm_integer integer not null, "
                        + "clm_tinyint tinyint default 0, "
                        + "clm_smallint smallint default 0, "
                        + "clm_bigint bigint default 0, "
                        + "clm_string varchar(20) default null, "
                        + "clm_decimal decimal default null, "
                        + "clm_float float default null, "
                        + "clm_timestamp timestamp default null, "
                        + "clm_point geography_point default null, "
                        + "clm_geography geography default null, "
                        + "PRIMARY KEY(clm_integer) "
                        + ");";
        String reportCreateTable = compileAndGenerateCatalogReport(ddlCreateTable);
        String targetDDL = "CREATE TABLE ALLTYPES (\n"
                          + "   CLM_INTEGER integer NOT NULL,\n"
                          + "   CLM_TINYINT tinyint DEFAULT '0',\n"
                          + "   CLM_SMALLINT smallint DEFAULT '0',\n"
                          + "   CLM_BIGINT bigint DEFAULT '0',\n"
                          + "   CLM_STRING varchar(20),\n"
                          + "   CLM_DECIMAL decimal,\n"
                          + "   CLM_FLOAT float,\n"
                          + "   CLM_TIMESTAMP timestamp,\n"
                          + "   CLM_POINT GEOGRAPHY_POINT,\n"
                          + "   CLM_GEOGRAPHY GEOGRAPHY(32768),\n"
                          + "   PRIMARY KEY (CLM_INTEGER)\n"
                          + ");";
        assertTrue(reportCreateTable.contains(targetDDL));
    }

    public void testCreateViewDDL() throws IOException {
        final String ddlCreateTableAndView =
                "create table AllTypes ("
                        + "clm_integer integer not null, "
                        + "clm_tinyint tinyint default 0, "
                        + "clm_smallint smallint default 0, "
                        + "clm_bigint bigint default 0, "
                        + "clm_string varchar(20) default null, "
                        + "clm_decimal decimal default null, "
                        + "clm_float float default null, "
                        + "clm_timestamp timestamp default null, "
                        + "clm_point geography_point default null, "
                        + "clm_geography geography default null, "
                        + "PRIMARY KEY(clm_integer) "
                        + ");\n"
                 + "create view IntFamily (clm_tinyint, clm_smallint, clm_bigint, num) as \n"
                 + "  select clm_tinyint, clm_smallint, clm_bigint, count(*)"
                 + "   from ALLTypes"
                 + "   group by clm_tinyint, clm_smallint, clm_bigint;";
        String reportCreateTableAndView = compileAndGenerateCatalogReport(ddlCreateTableAndView);
        String targetDDL = "CREATE VIEW INTFAMILY (\n"
                          + "   CLM_TINYINT,\n"
                          + "   CLM_SMALLINT,\n"
                          + "   CLM_BIGINT,\n"
                          + "   NUM\n"
                          + ")  AS SELECT CLM_TINYINT,CLM_SMALLINT,CLM_BIGINT,COUNT(*) FROM ALLTYPES GROUP BY CLM_TINYINT,CLM_SMALLINT,CLM_BIGINT;";
        assertTrue(reportCreateTableAndView.contains(targetDDL));
    }

    public void testCreateViewsDDL() throws IOException {
        final String ddlCreateTableAndViews =
                "create table AllTypes ("
                        + "clm_integer integer not null, "
                        + "clm_tinyint tinyint default 0, "
                        + "clm_smallint smallint default 0, "
                        + "clm_bigint bigint default 0, "
                        + "clm_string varchar(20) default null, "
                        + "clm_decimal decimal default null, "
                        + "clm_float float default null, "
                        + "clm_timestamp timestamp default null, "
                        + "clm_point geography_point default null, "
                        + "clm_geography geography default null, "
                        + "PRIMARY KEY(clm_integer) "
                        + ");\n"
                 + "create view IntFamily (clm_tinyint, clm_smallint, clm_bigint, num) as\n"
                 + "  select clm_tinyint, clm_smallint, clm_bigint, count(*)\n"
                 + "   from ALLTypes\n"
                 + "   group by clm_tinyint, clm_smallint, clm_bigint;\n"
                 + "create view floatFamily (clm_decimal, clm_float, num) as\n"
                 + "  select clm_decimal, clm_float, count(*)\n"
                 + "   from AllTypes\n"
                 + "   group by clm_decimal, clm_float;";
        String reportCreateTableAndViews = compileAndGenerateCatalogReport(ddlCreateTableAndViews);
        String targetDDL1 = "CREATE VIEW INTFAMILY (\n"
                          + "   CLM_TINYINT,\n"
                          + "   CLM_SMALLINT,\n"
                          + "   CLM_BIGINT,\n"
                          + "   NUM\n"
                          + ")  AS SELECT CLM_TINYINT,CLM_SMALLINT,CLM_BIGINT,COUNT(*) FROM ALLTYPES GROUP BY CLM_TINYINT,CLM_SMALLINT,CLM_BIGINT;";
        String targetDDL2 = "CREATE VIEW FLOATFAMILY (\n"
                          + "   CLM_DECIMAL,\n"
                          + "   CLM_FLOAT,\n"
                          + "   NUM\n"
                          + ")  AS SELECT CLM_DECIMAL,CLM_FLOAT,COUNT(*) FROM ALLTYPES GROUP BY CLM_DECIMAL,CLM_FLOAT;";
        assertTrue(reportCreateTableAndViews.contains(targetDDL1));
        assertTrue(reportCreateTableAndViews.contains(targetDDL2));
    }

    public void testCreateTablePartitionDDL() throws IOException {
        final String ddlCreateTable =
                "create table AllTypes ("
                        + "clm_integer integer not null, "
                        + "clm_tinyint tinyint default 0, "
                        + "clm_smallint smallint default 0, "
                        + "clm_bigint bigint default 0, "
                        + "clm_string varchar(20) not null, "
                        + "clm_decimal decimal default null, "
                        + "clm_float float default null, "
                        + "clm_timestamp timestamp default null, "
                        + "clm_point geography_point default null, "
                        + "clm_geography geography default null, "
                        + "PRIMARY KEY(clm_integer) "
                        + ");\n"
                        + "partition table alltypes on column clm_integer;";
        String reportCreateTable = compileAndGenerateCatalogReport(ddlCreateTable);
        String targetDDL = "CREATE TABLE ALLTYPES (\n"
                          + "   CLM_INTEGER integer NOT NULL,\n"
                          + "   CLM_TINYINT tinyint DEFAULT '0',\n"
                          + "   CLM_SMALLINT smallint DEFAULT '0',\n"
                          + "   CLM_BIGINT bigint DEFAULT '0',\n"
                          + "   CLM_STRING varchar(20) NOT NULL,\n"
                          + "   CLM_DECIMAL decimal,\n"
                          + "   CLM_FLOAT float,\n"
                          + "   CLM_TIMESTAMP timestamp,\n"
                          + "   CLM_POINT GEOGRAPHY_POINT,\n"
                          + "   CLM_GEOGRAPHY GEOGRAPHY(32768),\n"
                          + "   PRIMARY KEY (CLM_INTEGER)\n"
                          + ");\n"
                          + "PARTITION TABLE ALLTYPES ON COLUMN CLM_INTEGER;";
        assertTrue(reportCreateTable.contains(targetDDL));
    }
}
