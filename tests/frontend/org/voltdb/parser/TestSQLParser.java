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

package org.voltdb.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.voltdb.parser.SQLParser.FileOption;
import org.voltdb.parser.SQLParser.ParseRecallResults;

public class TestSQLParser extends TestCase {

    public void testAppearsToBeValidDDLBatchPositive() {

        // alter create drop export partition
        // (and sometimes import?)

        assertTrue(SQLParser.appearsToBeValidDDLBatch(
                "create table t (i integer);"));

        assertTrue(SQLParser.appearsToBeValidDDLBatch(
                "alter table t add column j double;"));

        assertTrue(SQLParser.appearsToBeValidDDLBatch(
                "drop index idx;"));

        assertTrue(SQLParser.appearsToBeValidDDLBatch(
                "partition table t on column i;"));

        assertTrue(SQLParser.appearsToBeValidDDLBatch(
                "export table ex_tbl;"));

        // Seems to be considered DDL.  Is that what we want?
        assertTrue(SQLParser.appearsToBeValidDDLBatch(
                "import table ex_tbl;"));

        // Now test multiple statements and comments...

        assertTrue(SQLParser.appearsToBeValidDDLBatch(
                "create table t (i integer);\n"
                + "create index idx on t (i);"));

        assertTrue(SQLParser.appearsToBeValidDDLBatch(
                "-- Here's some DDL...\n"
                + "create table t (i integer);\n"
                + "create index idx on t (i);"));

        assertTrue(SQLParser.appearsToBeValidDDLBatch(
                "-- Here's some DDL...\n"
                + "create table t (i integer); -- let's not forget the index...\n"
                + "create index idx on t (i);"));

        // This currently does not work.
        //        assertTrue(SQLParser.batchBeginsWithDDLKeyword(
        //                "/* here's some DDL: */\n"
        //                + "create table t (i integer); -- let's not forget the index...\n"
        //                + "create index idx on t (i);"));

        assertTrue(SQLParser.appearsToBeValidDDLBatch(
                "// here's some DDL; check it out!\n"
                + "create table t (i integer); -- let's not forget the index...\n"
                + "create index idx on t (i);"));

        // leading whitespace
        assertTrue(SQLParser.appearsToBeValidDDLBatch(
                "  \n"
                + "// here's some DDL; check it out!\n"
                + "  \n"
                + "create table t (i integer); -- let's not forget the index...\n"
                + "create index idx on t (i);"));

        // batches with no semantic content are considered trivially valid.
        assertTrue(SQLParser.appearsToBeValidDDLBatch(""));
        assertTrue(SQLParser.appearsToBeValidDDLBatch("  "));
        assertTrue(SQLParser.appearsToBeValidDDLBatch("-- hello  "));


    }

    public void testAppearsToBeValidDDLBatchNegative() {

        assertFalse(SQLParser.appearsToBeValidDDLBatch(
                "insert into t values (47);\n"
                + "partition table t on z;"));

        assertFalse(SQLParser.appearsToBeValidDDLBatch(
                "delete from t where i = 9;"));

        assertFalse(SQLParser.appearsToBeValidDDLBatch(
                "upsert into t values (32);\n"
                + "alter table t add column j bigint;"));

        assertFalse(SQLParser.appearsToBeValidDDLBatch(
                "update t set i = 70 where i > 69;"));

        assertFalse(SQLParser.appearsToBeValidDDLBatch(
                "update t set i = 70 where i > 69;\n"
                + "create table mytable (i integer);"));

        // Now some comments

        assertFalse(SQLParser.appearsToBeValidDDLBatch(
                "-- create table was done earlier...\n"
                + "update t set i = 70 where i > 69;"));

        assertFalse(SQLParser.appearsToBeValidDDLBatch(
                "// create table was done earlier...\n"
                + "update t set i = 70 where i > 69;"));

        // This passes only because the C-style comment
        // doesn't look like DDL--it isn't stripped out.
        assertFalse(SQLParser.appearsToBeValidDDLBatch(
                "/* create table was done earlier... */\n"
                + "update t set i = 70 where i > 69;"));

        assertFalse(SQLParser.appearsToBeValidDDLBatch(
                "  \n"
                + "select * from foo;"
                + "create table catdog (dogcat bigint);"));

        assertFalse(SQLParser.appearsToBeValidDDLBatch(
                "  \n"
                + "  -- hello world!!"
                + "     \t\n"
                + "select * from foo;"
                + "create table catdog (dogcat bigint);"));


        // Near misses that might appear in a ddl.sql file
        // but that cannot be batched

        assertFalse(SQLParser.appearsToBeValidDDLBatch(
                "load classes foo-bar.jar"));

        assertFalse(SQLParser.appearsToBeValidDDLBatch(
                "remove classes foo-bar.jar"));

        assertFalse(SQLParser.appearsToBeValidDDLBatch(
                "exec SelectAllRowsWithKey 10;"));

        assertFalse(SQLParser.appearsToBeValidDDLBatch(
                "file \"mysqlcommands.sql\";"));
    }

    private void assertThrowsParseException(String expectedMessage, String fileCommand) {
        try {
            SQLParser.parseFileStatement(fileCommand);
        }
        catch (SQLParser.Exception ex) {
            assertEquals(expectedMessage, ex.getMessage());
            return;
        }

        fail("Expected input \"" + fileCommand + "\" to fail with message \""
                + expectedMessage + "\", but it did not fail.");
    }

    public void testParseFileStatement() {
        SQLParser.FileInfo fi;

        // Plain file directive
        fi = SQLParser.parseFileStatement("  file 'foo.sql';");
        assertEquals(FileOption.PLAIN, fi.getOption());
        assertEquals("foo.sql", fi.getFile().getName());
        assertFalse(fi.isBatch());

        // Plain file directive
        // no quotes and trailing whitespace.
        fi = SQLParser.parseFileStatement("  file foo.sql  ");
        assertEquals(FileOption.PLAIN, fi.getOption());
        assertFalse(fi.isBatch());
        assertEquals("foo.sql", fi.getFile().getName());

        // file -batch directive
        fi = SQLParser.parseFileStatement("file -batch myddl.sql");
        assertEquals(FileOption.BATCH, fi.getOption());
        assertEquals("myddl.sql", fi.getFile().getName());
        assertTrue(fi.isBatch());

        // Plain file directive
        // quotes and trailing whitespace.
        // Whitespace in quotes is trimmed.  What are the rules here?
        // Please see ENG-7794.
        fi = SQLParser.parseFileStatement("  file '  foo.sql  '");
        assertEquals(FileOption.PLAIN, fi.getOption());
        assertFalse(fi.isBatch());
        assertEquals("foo.sql", fi.getFile().getName());
    }

    public void testParseFileStatementInlineBatch() {
        SQLParser.FileInfo fi = null;

        SQLParser.FileInfo parent = SQLParser.FileInfo.forSystemIn();


        fi = SQLParser.parseFileStatement(parent, "file -inlinebatch EOF");
        assertEquals(FileOption.INLINEBATCH, fi.getOption());
        assertEquals("EOF", fi.getDelimiter());
        assertTrue(fi.isBatch());

        fi = SQLParser.parseFileStatement(parent, "file -inlinebatch <<<<   ");
        assertEquals(FileOption.INLINEBATCH, fi.getOption());
        assertEquals("<<<<", fi.getDelimiter());
        assertTrue(fi.isBatch());

        // terminating semicolon is ignored, as bash does.
        // also try FILE parent
        SQLParser.FileInfo fileParent = SQLParser.parseFileStatement(parent, "file foo.sql ;");
        fi = SQLParser.parseFileStatement(fileParent, "file -inlinebatch EOF;");
        assertEquals(FileOption.INLINEBATCH, fi.getOption());
        assertEquals("EOF", fi.getDelimiter());
        assertTrue(fi.isBatch());

        // There can be whitespace around the semicolon
        fi = SQLParser.parseFileStatement(parent, "file -inlinebatch END_OF_THE_BATCH  ; ");
        assertEquals(FileOption.INLINEBATCH, fi.getOption());
        assertEquals("END_OF_THE_BATCH", fi.getDelimiter());
        assertTrue(fi.isBatch());

    }

    public void testParseFileStatementNegative() {
        assertThrowsParseException("Did not find valid delimiter for \"file -inlinebatch\" command.",
                "  file   -inlinebatch");

        // no embedded whitespace
        assertThrowsParseException("Did not find valid delimiter for \"file -inlinebatch\" command.",
                "  file   -inlinebatch  EOF EOF");

        // embedded semicolons not allowed
        assertThrowsParseException("Did not find valid delimiter for \"file -inlinebatch\" command.",
                "  file   -inlinebatch  EOF;EOF");

        assertThrowsParseException("Did not find valid delimiter for \"file -inlinebatch\" command.",
                "  file   -inlinebatch;");

        assertThrowsParseException("Did not find valid delimiter for \"file -inlinebatch\" command.",
                "  file   -inlinebatch EOF; hello");

        assertThrowsParseException("Did not find valid file name in \"file -batch\" command.",
                "  file   -batch");

        assertThrowsParseException("Did not find valid file name in \"file\" command.",
                "file");

        assertThrowsParseException("Did not find valid file name in \"file\" command.",
                "file;");

        assertThrowsParseException("Did not find valid file name in \"file\" command.",
                "file ");

        assertThrowsParseException("Did not find valid file name in \"file\" command.",
                "file  ");

        assertThrowsParseException("Did not find valid file name in \"file\" command.",
                "file '");

        assertThrowsParseException("Did not find valid file name in \"file\" command.",
                "file ''");

        assertThrowsParseException("Did not find valid file name in \"file\" command.",
                "file \"\"");

        assertThrowsParseException("Did not find valid file name in \"file\" command.",
                "file \";\"");

        assertThrowsParseException("Did not find valid file name in \"file\" command.",
                "file  ;");

        // This won't be regarded as a file command.
        assertEquals(null, SQLParser.parseFileStatement("filename"));

        // Edge case.
        assertEquals(null, SQLParser.parseFileStatement(""));
    }

    private static final Pattern RequiredWhitespace = Pattern.compile("\\s+");
    /**
     * Match statement against pattern for all VoltDB-specific statement preambles
     * @param statement  statement to match against
     * @return           upper case single-space-separated preamble token string or null if not a match
     */
    private static String parseVoltDBSpecificDdlStatementPreamble(String statement, boolean fudge)
    {
        Matcher matcher = SQLParser.matchAllVoltDBStatementPreambles(statement);
        if ( ! matcher.find()) {
            if (fudge) {
                String padded = statement.substring(0, statement.length()-1) + " ;";
                matcher = SQLParser.matchAllVoltDBStatementPreambles(padded);
                if ( ! matcher.find()) {
                    return null;
                }
            }
            else {
                return null;
            }
        }
        String cleanCommand = matcher.group(1);
        cleanCommand = cleanCommand.toUpperCase();
        if (fudge) {
            cleanCommand = RequiredWhitespace.matcher(cleanCommand).replaceAll(" ");
            if ("PROCEDURE".equals(cleanCommand) || "ROLE".equals(cleanCommand)) {
                return "CREATE " + cleanCommand;
            }
            // This kind of heavy lifting should REALLY be done by the pattern.
            if ("DROP".equals(cleanCommand)) {
                String cleanStatement =
                        RequiredWhitespace.matcher(statement.toUpperCase()).replaceAll(" ");
                if (cleanStatement.substring(0, "DROP ROLE".length()).
                        equals("DROP ROLE")) {
                    return "DROP ROLE";
                }
                else if (cleanStatement.substring(0, "DROP PROCEDURE".length()).
                        equals("DROP PROCEDURE")) {
                    return "DROP PROCEDURE";
                }
                return null;
            }
        }
        return cleanCommand;
    }

    private void expectFromAll(boolean fudge, String expected, String... candidates) {
        for (String candidate : candidates) {
            String got = parseVoltDBSpecificDdlStatementPreamble(candidate, fudge);
            // Guarding assert to makes breakpoints easier.
            if (got == null) {
                if (expected == null) {
                    continue;
                }
            }
            else if (got.equals(expected)) {
                continue;
            }
            // Retry before reporting failure for chance to debug.
            got = parseVoltDBSpecificDdlStatementPreamble(candidate, fudge);
            // sure to fail
            assertEquals("For input '" + candidate + "'" + (fudge ? " fudging " : " "),
                         expected, got);
        }
    }

    public void testParseVoltDBSpecificDDLStatementPreambles() {

        expectFromAll(true, "CREATE PROCEDURE",
                "CREATE PROCEDURE XYZ ...;",
                "Create Procedure 123 ...;",
                "CREATE PROCEDURE AS ...;",
                "CREATE\tPROCEDURE ALLOW ...;",
                "CREATE  PROCEDURE PARTITION ...;",
                "CREATE\t PROCEDURE ...;",
                "CREATE PROCEDURE\tOK...;",
                "CREATE PROCEDURE  ALRIGHT...;",
                "CREATE PROCEDURE;",

                "create procedure ;");


        expectFromAll(true, "CREATE ROLE",
                "CREATE ROLE XYZ ...;",
                "Create Role 123 ...;",
                "CREATE ROLE AS ...;",
                "CREATE\tROLE ALLOW ...;",
                "CREATE  ROLE PARTITION ...;",
                "CREATE\t ROLE ...;",
                "CREATE ROLE\tOK...;",
                "CREATE ROLE  ALRIGHT...;",
                "CREATE ROLE;",

                "create role ;");
    }

    public void testParseRecall()
    {
        parseRecallCase("RECALL 1", 1);
        parseRecallCase("  RECALL 2 ", 2);
        parseRecallCase("RECALL 33;", 33);
        parseRecallCase("recall 99 ;", 99);
        parseRecallCase("RECALL 100 ; ", 100);

        // Try too short commands.
        parseRecallErrorCase("RECALL");
        parseRecallErrorCase("RECALL ");
        parseRecallErrorCase("Recall;");
        parseRecallErrorCase("RECALL ;");

        // Try interspersed garbage.
        parseRecallErrorCase("RECALL abc 1");
        parseRecallErrorCase("RECALL 2 def");
        parseRecallErrorCase("RECALL 33;ghi");
        parseRecallErrorCase("RECALL 44 jkl;");

        parseRecallErrorCase("RECALL mno");
        parseRecallErrorCase("RECALL; pqr");
        parseRecallErrorCase("RECALL ;stu");

        // Try invalid keyword terminators
        parseRecallErrorCase("RECALL,1");
        parseRecallErrorCase("RECALL. 1");
        parseRecallErrorCase("RECALL'1;");
        parseRecallErrorCase("RECALL( 1;");
        parseRecallErrorCase("RECALL(1);");
        parseRecallErrorCase("RECALL- 1 ;");
        parseRecallErrorCase("RECALL,");
        parseRecallErrorCase("RECALL, ");
        parseRecallErrorCase("RECALL,;");
        parseRecallErrorCase("RECALL, ;");

        // Try imaginative usage.
        parseRecallErrorCase("RECALL 1 3;");
        parseRecallErrorCase("RECALL 1,3;");
        parseRecallErrorCase("RECALL 1, 3;");
        parseRecallErrorCase("RECALL 1-3;");
        parseRecallErrorCase("RECALL 1..3;");

        // Try invalid numerics
        parseRecallErrorCase("RECALL 0");
        parseRecallErrorCase("recall -2;");
        parseRecallErrorCase("RECALL 101");
        parseRecallErrorCase("recall 1000;");

        // confirm that the recall command parser does not overstep
        // its mandate and try to process anything but a recall command.
        assertNull(SQLParser.parseRecallStatement("RECAL", 99));
        assertNull(SQLParser.parseRecallStatement("recal 1", 99));
        assertNull(SQLParser.parseRecallStatement("RECALL1", 99));
        assertNull(SQLParser.parseRecallStatement("RECALLL", 99));
        assertNull(SQLParser.parseRecallStatement("RECALLL 1", 99));
        assertNull(SQLParser.parseRecallStatement("HELP;", 99));
        assertNull(SQLParser.parseRecallStatement("FILE ddl.sql", 99));
        assertNull(SQLParser.parseRecallStatement("@RECALL 1", 99));
        assertNull(SQLParser.parseRecallStatement("--recall 1", 99));
        assertNull(SQLParser.parseRecallStatement("ECALL 1", 99));
    }

    private void parseRecallCase(String lineText, int lineNumber)
    {
        ParseRecallResults result = SQLParser.parseRecallStatement(lineText, 99);
        assertNotNull(result);
        assertNull(result.getError());
        // Line number inputs are 1-based but getLine() results are 0-based
        assertEquals(lineNumber, result.getLine()+1);
    }

    private void parseRecallErrorCase(String lineText)
    {
        ParseRecallResults result = SQLParser.parseRecallStatement(lineText, 99);
        assertNotNull(result);
        assertNotNull(result.getError());
    }


}