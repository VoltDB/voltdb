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

package org.voltdb.compiler;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import junit.framework.TestCase;

import org.apache.commons.lang3.StringUtils;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.utils.BuildDirectoryUtils;

/**
 * @author scooper
 *
 */
public class TestPartitionDDL extends TestCase {

    String testout_jar;

    @Override
    public void setUp() {
        testout_jar = BuildDirectoryUtils.getBuildDirectoryPath() + File.pathSeparator + "testout.jar";
    }

    @Override
    public void tearDown() {
        new File(testout_jar).delete();
    }

    class Item {
        final String[] m_strings;

        Item(final String... strings) {
            m_strings = strings;
        }

        String toDDL() {
            return null;
        }

        String toXML() {
            return null;
        }
    }

    class DDL extends Item {
        DDL(final String... ddlStrings) {
            super(ddlStrings);
        }
        @Override
        String toDDL() {
            return StringUtils.join(m_strings, '\n');
        }
    }

    class ReplicateDDL extends DDL {
        ReplicateDDL(final String tableName) {
            super(String.format("REPLICATE TABLE %s;", tableName));
        }
    }

    class PartitionDDL extends DDL {
        PartitionDDL(final String tableName, final String columnName) {
            super(String.format("PARTITION TABLE %s ON COLUMN %s;", tableName, columnName));
        }
    }

    class PartitionXML extends Item {
        PartitionXML(final String tableName, final String columnName) {
            super(tableName, columnName);
        }
        @Override
        String toXML() {
            return String.format("<partition table='%s' column='%s' />", m_strings[0], m_strings[1]);
        }
    }

    class Tester {
        final Item schemaItem;
        final static String partitionedProc = "org.voltdb.compiler.procedures.AddBook";
        final static String replicatedProc = "org.voltdb.compiler.procedures.EmptyProcedure";

        Tester() {
            schemaItem = new DDL(
                "create table books (",
                    "cash integer default 0 NOT NULL,",
                    "title varchar(10) default 'foo',",
                    "PRIMARY KEY(cash)",
                ");");
        }

        Tester(DDL schemaItemIn) {
            schemaItem = schemaItemIn;
        }

        String writeDDL(final Item... items) {
            StringBuilder sb = new StringBuilder();
            sb.append(schemaItem.toDDL());
            sb.append('\n');
            for (Item item : items) {
                String line = item.toXML();
                if (line == null) {
                    sb.append(item.toDDL());
                    sb.append('\n');
                }
            }
            final File ddlFile = VoltProjectBuilder.writeStringToTempFile(sb.toString());
            return ddlFile.getPath();
        }

        String writeXML(final boolean partitioned, final String ddlPath, final Item... items) {
            String xmlText =
                    "<?xml version=\"1.0\"?>\n" +
                    "<project>\n" +
                    "  <database>\n" +
                    "  <schemas>\n" +
                    "    <schema path='" + ddlPath + "' />\n" +
                    "  </schemas>\n";
                int nxml = 0;
                for (Item item : items) {
                    String line = item.toXML();
                    if (line != null) {
                        nxml++;
                        if (nxml == 1) {
                            xmlText += "  <partitions>\n";
                        }
                        xmlText += String.format("    %s\n", line);
                    }
                }
                if (nxml > 0) {
                    xmlText += "  </partitions>\n";
                }
            xmlText += "    <procedures>\n";
            xmlText += String.format("      <procedure class='%s' />\n",
                                    partitioned ? partitionedProc : replicatedProc);
            xmlText += "    </procedures>\n" +
                    "  </database>\n" +
                    "</project>\n";
            return VoltProjectBuilder.writeStringToTempFile(xmlText).getPath();
        }

        String getMessages(final VoltCompiler compiler, final boolean success) {
            ByteArrayOutputStream ss = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(ss);
            if (success) {
                compiler.summarizeSuccess(null, ps, testout_jar);
            }
            else {
                compiler.summarizeErrors(null, ps);
            }
            // For some reason linefeeds break the regex pattern matching.
            return ss.toString().trim().replace('\n', ' ');
        }

        // Must provide a failRegex if failure is expected.
        void test(final boolean partitioned,
                  final String failRegex,
                  final int additionalTables,
                  final Item... items) {
            // Generate DDL and XML files.
            String ddlPath = writeDDL(items);
            String xmlPath = writeXML(partitioned, ddlPath, items);

            // Compile the catalog.
            final VoltCompiler compiler = new VoltCompiler();
            boolean success = compiler.compileWithProjectXML(xmlPath, testout_jar);

            // Check for expected compilation success or failure.
            String s = getMessages(compiler, false);
            if (failRegex != null) {
                assertFalse("Expected compilation failure.", success);
                assertTrue(String.format("Expected error regex \"%s\" not matched.\n%s", failRegex, s),
                        s.matches(failRegex));
            }
            else {
                assertTrue(String.format("Unexpected compilation failure.\n%s", s), success);

                // Check that the catalog table is appropriately configured.
                Database db = compiler.m_catalog.getClusters().get("cluster").getDatabases().get("database");
                assertNotNull(db);
                assertEquals(1 + additionalTables, db.getTables().size());
                Table t = db.getTables().getIgnoreCase("books");
                assertNotNull(t);
                Column c = t.getPartitioncolumn();
                if (partitioned) {
                    assertNotNull(c);
                }
                else {
                    assertNull(c);
                }
            }
        }

        /**
         *  Call when expected result is a partitioned table.
         * @param items
         */
        void partitioned(final Item... items) {
            test(true, null, 0, items);
        }

        /**
         * Call when expected result is a replicated table.
         * @param items
         */
        void replicated(final Item... items) {
            test(false, null, 0, items);
        }

        /**
         * Call when expected result is a failure.
         * Checks error message against failRegex.
         * @param failRegex
         * @param items
         */
        void bad(final String failRegex, final Item... items) {
            test(false, failRegex, 0, items);
        }

        /**
         * Call when DDL has a view and it should pass.
         * @param items
         */
        void view_good(final boolean partitioned, final Item... items) {
            test(partitioned, null, 1, items);
        }

        /**
         * Call when DDL has a view and it should fail.
         * @param items
         */
        void view_bad(final String failRegex, final boolean partitioned, final Item... items) {
            test(partitioned, failRegex, 1, items);
        }
    }

    public void testGeneralDDLParsing() {
        Tester tester = new Tester();

        // Just the CREATE TABLE statement.
        tester.replicated();

        // Garbage statement added.
        tester.bad(".*unexpected token.*", new DDL("asldkf sadlfk;"));
    }

    public void testBadReplicate() {
        Tester tester = new Tester();

        // REPLICATE statement with no semi-colon.
        tester.bad(".*no semicolon found.*",
                new DDL("REPLICATE TABLE books"));

        // REPLICATE statement with missing argument.
        tester.bad(".*Invalid REPLICATE statement.*",
                new DDL("REPLICATE TABLE;"));

        // REPLICATE statement with too many arguments.
        tester.bad(".*Invalid REPLICATE statement.*",
                new DDL("REPLICATE TABLE books NOW;"));

        // REPLICATE with bad table clause.
        tester.bad(".*Invalid REPLICATE statement.*",
                new DDL("REPLICATE TABLEX books;"));

        //REPLICATE with bad table identifier
        tester.bad(".*Unknown indentifier in DDL.*",
                new DDL("REPLICATE TABLE 0books;"));
    }

    public void testGoodReplicate() {
        Tester tester = new Tester();

        // REPLICATE with annoying whitespace.
        tester.replicated(new DDL("\t\t  REPLICATE\r\nTABLE\nbooks\t\t\n  ;"));

        // REPLICATE with clean statement.
        tester.replicated(new ReplicateDDL("books"));
    }

    public void testBadPartition() {
        Tester tester = new Tester();

        // PARTITION statement with no semi-colon.
        tester.bad(".*no semicolon found.*",
                new DDL("PARTITION TABLE books ON COLUMN cash"));

        // PARTITION statement with missing arguments.
        tester.bad(".*Invalid PARTITION statement.*",
                new DDL("PARTITION TABLE;"));

        // PARTITION statement with too many arguments.
        tester.bad(".*Invalid PARTITION statement.*",
                new DDL("PARTITION TABLE books ON COLUMN cash COW;"));

        // PARTITION statement intermixed with procedure.
        tester.bad(".*Invalid PARTITION statement.*",
                new DDL("PARTITION TABLE books PROCEDURE bruha ON COLUMN cash;"));

        // PARTITION with bad table clause.
        tester.bad(".*Invalid PARTITION statement.*",
                new DDL("PARTITION TABLEX books ON COLUMN cash;"));
    }

    public void testGoodPartition() {
        Tester tester = new Tester();

        // PARTITION with annoying whitespace.
        tester.partitioned(new DDL("\t\t  PARTITION\r\nTABLE\nbooks\r\n\tON COLUMN cash\t\t\n  ;"));

        // PARTITION from DDL.
        tester.partitioned(new PartitionDDL("books", "cash"));

        // PARTITION from XML.
        tester.partitioned(new PartitionXML("books", "cash"));
    }
}
