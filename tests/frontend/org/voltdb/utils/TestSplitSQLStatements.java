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

package org.voltdb.utils;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.parser.SQLLexer;

/**
 * This class tests the method SQLLexer.splitStatements.
 */
public class TestSplitSQLStatements {

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
    }

    private void checkSplitter(final String inputStmts, final String... expectedStmts) {
        final List<String> strsOut = SQLLexer.splitStatements(inputStmts).getCompletelyParsedStmts();
        assertEquals(expectedStmts.length, strsOut.size());
        for (int i = 0; i < expectedStmts.length; ++i) {
            assertEquals(expectedStmts[i], strsOut.get(i));
        }
    }

    private void checkSplitterWithIncompleteStmt(String inputStmts, String... expectedStmts) {
        String incompleteStmt = SQLLexer.splitStatements(inputStmts).getIncompleteStmt();
        if (expectedStmts.length > 0) {
            checkSplitter(inputStmts, Arrays.copyOfRange(expectedStmts, 0, expectedStmts.length - 1));
        }

        assertEquals(expectedStmts[expectedStmts.length - 1], incompleteStmt);
    }

    /*
     * Tokens of interest:
     * - --    (single line comment)
     * - /*    (C-style multi-line comment begin)
     * - * /   (C-style multi-line comment end)
     * - ''    (single quote)
     * - ""    (double quote)
     * - AS    (precedes BEGIN token)
     * - BEGIN (multi-statement procedure)
     * - CASE  (CASE WHEN .. END)
     * - END   (can end either CASE or multi-statement procedure)
     * - ;     (statement terminator outside if multi-statement procedure)
     */

    @Test
    public void testBasic() {
        checkSplitter(";");
        checkSplitter("  ;  ");
        checkSplitter("foo;bar;", "foo", "bar");
    }

    @Test
    public void testDashDashComments() {
        // Make sure that we trim comments when splitting.
        checkSplitter("");
        checkSplitter("-", "-");
        checkSplitter("-\n", "-");
        checkSplitter("--");
        checkSplitter("--comment");
        checkSplitter("  --comment\n");

        // put various interesting tokens in comments
        checkSplitter("foo-- 'foo'\nbar", "foo\nbar");
        checkSplitter("foo-- \"foo\"\nbar", "foo\nbar");
        checkSplitter("foo-- /*foo*/\nbar", "foo\nbar");
        checkSplitter("foo-- --foo\nbar", "foo\nbar");
        checkSplitter("foo--foo\nbar", "foo\nbar");
        checkSplitter("foo-- as begin\nbar", "foo\nbar");
        checkSplitter("foo-- as begin\nbar", "foo\nbar");
        checkSplitter("foo-- case\nbar", "foo\nbar");
        checkSplitter("foo-- as begin\nbar", "foo\nbar");
        checkSplitter("foo-- end\nbar", "foo\nbar");
        checkSplitter("foo-- ;\nbar", "foo\nbar");
    }

    @Test
    public void testCStyleComments() {
        checkSplitter("/", "/");
        checkSplitter("*", "*");
        checkSplitter("/*");
        checkSplitter("/*/");
        checkSplitter("/*foo*/");
        checkSplitter("foo/*o*/bar", "foo bar");
        checkSplitter("/*foo\nbar*/\n");
        checkSplitter("foo/*foo\nbar*/bar\n", "foo bar");

        checkSplitter("foo/*/bar", "foo");
        checkSplitter("foo/**/bar", "foo bar");
        checkSplitter("foo/*--*/bar", "foo bar");
        checkSplitter("foo/* /* */bar", "foo bar");
        checkSplitter("foo/* as begin */bar", "foo bar");
        checkSplitter("foo/* begin */bar", "foo bar");
        checkSplitter("foo/* as */bar", "foo bar");
        checkSplitter("foo/* case */bar", "foo bar");
        checkSplitter("foo/* end */bar", "foo bar");
        checkSplitter("foo/* \"foo\" */bar", "foo bar");
        checkSplitter("foo/* 'foo' */bar", "foo bar");
        checkSplitter("foo/* 'foo' */bar", "foo bar");
        checkSplitter("foo/* foo; */bar", "foo bar");
    }

    @Test
    public void testSingleQuotedStrings() {
        checkSplitter("'", "'");
        checkSplitter("'foo'", "'foo'");
        checkSplitter("';'", "';'");
        checkSplitter("'foo--'", "'foo--'");
        checkSplitter("'--foo'", "'--foo'");
        checkSplitter("'/*'", "'/*'");
        checkSplitter("'/**/'", "'/**/'");
        checkSplitter("'/**/'", "'/**/'");
        checkSplitter("'/**/'", "'/**/'");
        checkSplitter("'\"'", "'\"'");
        checkSplitter("'begin'", "'begin'");
        checkSplitter("'as begin'", "'as begin'");
        checkSplitter("'as'", "'as'");
        checkSplitter("'case'", "'case'");
        checkSplitter("'end'", "'end'");
        checkSplitter("'foo;bar'", "'foo;bar'");

        // Check escaped characters and quotes
        checkSplitter("'''--foo'", "'''--foo'");
        checkSplitter("'--''foo'", "'--''foo'");
        checkSplitter("'--''foo'", "'--''foo'");
        checkSplitter("'''--''''foo'''", "'''--''''foo'''");
        checkSplitter("'foo'''--foo", "'foo'''");

        checkSplitter("'\\'--foo'", "'\\'--foo'");
        checkSplitter("'--\\'foo'", "'--\\'foo'");
        checkSplitter("'--\\'foo'", "'--\\'foo'");
        checkSplitter("'\\'--\\'\\'foo\\''", "'\\'--\\'\\'foo\\''");
        checkSplitter("'foo\\''--foo", "'foo\\''");

        checkSplitter("'''", "'''");
        checkSplitter("'\\", "'\\");
    }

    @Test
    public void testDoubleQuotedStrings() {
        checkSplitter("\"", "\"");
        checkSplitter("\"foo\"", "\"foo\"");
        checkSplitter("\" ;\"", "\" ;\"");
        checkSplitter("\"foo--\"", "\"foo--\"");
        checkSplitter("\"--foo\"", "\"--foo\"");
        checkSplitter("\"/*\"", "\"/*\"");
        checkSplitter("\"/**/\"", "\"/**/\"");
        checkSplitter("\"/**/\"", "\"/**/\"");
        checkSplitter("\"/**/\"", "\"/**/\"");
        checkSplitter("\"\"\"", "\"\"\"");
        checkSplitter("\"begin\"", "\"begin\"");
        checkSplitter("\"as begin\"", "\"as begin\"");
        checkSplitter("\"as\"", "\"as\"");
        checkSplitter("\"case\"", "\"case\"");
        checkSplitter("\"end\"", "\"end\"");
        checkSplitter("\"foo;bar\"", "\"foo;bar\"");

        // Check escaped characters and quotes
        checkSplitter("\"\"\"--foo\"", "\"\"\"--foo\"");
        checkSplitter("\"--\"\"foo\"", "\"--\"\"foo\"");
        checkSplitter("\"--\"\"foo\"", "\"--\"\"foo\"");
        checkSplitter("\"\"\"--\"\"\"\"foo\"\"\"", "\"\"\"--\"\"\"\"foo\"\"\"");
        checkSplitter("\"foo\"\"\"--foo", "\"foo\"\"\"");

        checkSplitter("\"\\\"--foo\"", "\"\\\"--foo\"");
        checkSplitter("\"--\\\"foo\"", "\"--\\\"foo\"");
        checkSplitter("\"--\\\"foo\"", "\"--\\\"foo\"");
        checkSplitter("\"\\\"--\\\"\\\"foo\\\"\"", "\"\\\"--\\\"\\\"foo\\\"\"");
        checkSplitter("\"foo\\\"\"--foo", "\"foo\\\"\"");

        checkSplitter("\"\"\"", "\"\"\"");
        checkSplitter("\"\\", "\"\\");
    }

    @Test
    public void testBegin() {
        checkSplitterWithIncompleteStmt("as begin", "as begin");
        checkSplitterWithIncompleteStmt("foo; as begin foo end; as begin", "foo", "as begin foo end", "as begin");

        checkSplitter("as begin foo; bar; end", "as begin foo; bar; end");
        checkSplitter("as foo begin foo; bar; end", "as foo begin foo", "bar", "end");

        // statement not completed.
        checkSplitter("as begin foo; bar;");

        // Comments between AS and BEGIN still trigger multi-statement procedure splitting
        checkSplitter("as --foo\nbegin foo; bar; end; foo", "as \nbegin foo; bar; end", "foo");
        checkSplitter("as /*foo*/\nbegin foo; bar; end; foo", "as  \nbegin foo; bar; end", "foo");

        checkSplitter("as as begin ; end ; foo", "as as begin ; end", "foo");
        checkSplitter("as begin begin ; end ; foo", "as begin begin ; end", "foo");

        // other stuff between AS and BEGIN means we aren't looking for multistatement procs.
        checkSplitter("as 'foo'\nbegin foo; bar; end; foo", "as 'foo'\nbegin foo", "bar", "end", "foo");
        checkSplitter("as \"foo\"\nbegin foo; bar; end; foo", "as \"foo\"\nbegin foo", "bar", "end", "foo");
        checkSplitter("as ; begin ; end ; foo", "as", "begin", "end", "foo");
        checkSplitter("as case begin ; end ; foo", "as case begin", "end", "foo");
        checkSplitter("as end begin ; end ; foo", "as end begin", "end", "foo");

        // check stuff between BEGIN and END
        checkSplitter("as begin --foo\nend", "as begin \nend");
        checkSplitter("as begin /*foo*/ end", "as begin   end");
        checkSplitter("as begin 'foo' end", "as begin 'foo' end");
        checkSplitter("as begin \"foo\" end", "as begin \"foo\" end");

        checkSplitter("as begin as end", "as begin as end");
        checkSplitter("as begin begin end", "as begin begin end");

        checkSplitterWithIncompleteStmt("as begin case end", "as begin case end");
        checkSplitter("as begin case end end", "as begin case end end");
        checkSplitter("as begin case case end end end", "as begin case case end end end");
        checkSplitterWithIncompleteStmt("as begin case case end end", "as begin case case end end");
    }

    @Test
    public void testCase() {
        checkSplitter("case foo end", "case foo end");
        checkSplitter("case --foo\n end", "case \n end");
        checkSplitter("case /*foo*/ end", "case   end");
        checkSplitter("case 'foo' end", "case 'foo' end");
        checkSplitter("case \"foo\" end", "case \"foo\" end");

        checkSplitter("case as end", "case as end");
        checkSplitter("case begin end", "case begin end");
        checkSplitterWithIncompleteStmt("case as begin end", "case as begin end");

        checkSplitter("case foo; end", "case foo", "end");
        checkSplitter("case end end", "case end end");
    }

    @Test
    public void testSQLSplitter() {
        checkSplitter("");
        checkSplitter(" ");
        checkSplitter(" ; ");
        checkSplitter("abc", "abc");
        checkSplitter(" ab c ", "ab c");
        checkSplitter(" ab ; c ", "ab", "c");
        checkSplitter(" ab ; c; ", "ab", "c");
        checkSplitter(" a\"b ; c \" ; ", "a\"b ; c \"");
        checkSplitter(" a\"b ; c 'd;ef' \" ; ", "a\"b ; c 'd;ef' \"");
        checkSplitter("abc;'--dashes';bar;", "abc", "'--dashes'", "bar");
        checkSplitter(" a\"b ; c \\\" 'd;ef' \" ; ", "a\"b ; c \\\" 'd;ef' \"");
        checkSplitter(" a'b ; c \\' \"d;ef\" ' ; ", "a'b ; c \\' \"d;ef\" '");
        checkSplitter("a;;b;;c;;", "a", "b", "c");
        checkSplitter("abc --;def\n;ghi", "abc", "ghi");
        checkSplitter("abc /*\";def\n;*/ghi", "abc  ghi");
        checkSplitter("a\r\nb;c\r\nd;", "a\r\nb", "c\r\nd");
        checkSplitter("--one\n--two\nreal", "real");
        checkSplitter("  --one\n  --two\nreal", "real");
        checkSplitter("  abc;  --def\n\n  /*ghi\njkl;*/", "abc");
        checkSplitter("  abc;/* comments*/  def;", "abc", "def");
        checkSplitter("  abc;'/*' comments*/  def;", "abc", "'/*' comments*/  def");
        checkSplitter("  abc;'/* comments*/'  def;", "abc", "'/* comments*/'  def");
        checkSplitter(" -- abc;/* comments*/\n  def;", "def");
        checkSplitter("  abc;/* comments*/\n  def;", "abc", "def");
        checkSplitter("  abc;/* comments ; with ;*/\n  def;", "abc", "def");
        checkSplitter("  abc;/* this is a long \n comment \n in 3 lines*/  def;", "abc", "def");
        checkSplitter("/* comments*/  abc;/* comments \n multiline*/  --def\n\n  /*ghi\njkl;*/", "abc");
        checkSplitter("testing comments in quotes /* not ending will remain '*/'", "testing comments in quotes  '");

        checkSplitter("SELECT * FROM table --UNION SELECT * FROM table2;", "SELECT * FROM table");
        checkSplitter("SELECT * FROM table --UNION --SELECT * FROM table2;", "SELECT * FROM table");

        String sql = " select -- comment no semicolon\n"
                + "* -- comment no semicolon\n"
                + "from -- comment no semicolon\n"
                + "table -- comment with semicolon;";
        checkSplitter(sql, "select \n* \nfrom \ntable");

        sql = "select -- comment no semicolon\n"
                + "* -- comment with this ; a semicolon inside\n"
                + "from -- comment with this ; a semicolon inside\n"
                + "table-- comment with semicolon;";
        checkSplitter(sql, "select \n* \nfrom \ntable");
    }

    @Test
    public void testProcSQLSplit() {
        checkSplitter("begi", "begi");
        checkSplitter("begin end", "begin end");
        checkSplitter("as begin end", "as begin end");

        // the next test will not return completely parsed statements
        // because begin has not end yet, they are incomplete
        String sql = "as begin en";
        SplitStmtResults parsedOut = SQLLexer.splitStatements(sql);
        assertEquals(0, parsedOut.getCompletelyParsedStmts().size());
        assertEquals(sql, parsedOut.getIncompleteStmt());

        sql = "create table begin (a int);";
        checkSplitter(sql, sql.substring(0, sql.length() - 1));

        sql = "create table begin (a int, begin int);";
        checkSplitter(sql, sql.substring(0, sql.length() - 1));

        sql = "CREATE PROCEDURE foo AS "
                + "BEGIN "
                + "SELECT * from t; "
                + "SELECT * from t; "
                + "END;";
        checkSplitter(sql, sql.substring(0, sql.length() - 1));

        // with white spaces between AS..BEGIN
        sql = "CREATE PROCEDURE foo AS  \t"
                + "BEGIN "
                + "SELECT * from t; "
                + "SELECT * from t; "
                + "END;";
        checkSplitter(sql, sql.substring(0, sql.length() - 1));

        sql = "CREATE PROCEDURE foo AS "
                + "BEGIN "
                + "SELECT * from t; "
                + "SELECT * from t; "
                + "END; "
                + "abc";
        checkSplitter(sql,
                "CREATE PROCEDURE foo AS BEGIN SELECT * from t; SELECT * from t; END", "abc");

        // BEGIN ... END; -- semi colon is needed after END
        sql = "CREATE PROCEDURE foo AS "
          + "BEGIN "
          + "SELECT * from t; "
          + "SELECT * from t; "
          + "END "
          + "abc; def";
        checkSplitter(sql,
          "CREATE PROCEDURE foo AS BEGIN SELECT * from t; SELECT * from t; END abc", "def");

        // there is no END statement for BEGIN, so the ; is included as the parsing of BEGIN is not complete
        sql = "CREATE PROCEDURE foo AS BEGIN SELECT * from t; SELECT * from t;";
        parsedOut = SQLLexer.splitStatements(sql);
        assertEquals(0, parsedOut.getCompletelyParsedStmts().size());
        assertEquals(sql, parsedOut.getIncompleteStmt());

        // enf is not end of statement for BEGIN, so the ; is included as the parsing of BEGIN is not complete
        sql = "CREATE PROCEDURE foo AS BEGIN SELECT * from t; SELECT * from t; ENF;";
        parsedOut = SQLLexer.splitStatements(sql);
        assertEquals(0, parsedOut.getCompletelyParsedStmts().size());
        assertEquals(sql, parsedOut.getIncompleteStmt());

        checkSplitter("CREATE PROCEDURE foo AS BEGIN SELECT * from t; SELECT * from t; ENF; end",
                "CREATE PROCEDURE foo AS BEGIN SELECT * from t; SELECT * from t; ENF; end");

        String sql1 = "abc";
        sql = "SELECT a, "
                + "CASE WHEN a > 100.00 "
                + "THEN 'Expensive'"
                + "ELSE 'Cheap'"
                + "END "
                + "FROM t;";
        checkSplitter(sql+sql1, sql.substring(0, sql.length()-1), sql1);

        sql = "SELECT a, "
                + "CASE WHEN a > 100.00 "
                + "THEN 'Expensive'"
                + "ELSE 'Cheap'"
                + "END "
                + "FROM t;";
        checkSplitter(sql+sql1, sql.substring(0, sql.length() - 1), sql1);

        sql = "create procedure thisproc as "
              + "begin "
              + "select * from t;"
              + "select * from r where f = 'foo';"
              + "select * from r where f = 'begin' or f = 'END';"
              + "end;";
        checkSplitter(sql+sql1, sql.substring(0, sql.length() - 1), sql1);

        // multiple multi stmt procs
        sql = "CREATE PROCEDURE foo AS begin SELECT * from t; "
                + "SELECT * from t; end;";
        checkSplitter(sql + sql, sql.substring(0, sql.length() - 1), sql.substring(0, sql.length() - 1));

        // new line after first statement in the multi stmt proc
        sql = "create procedure thisproc as "
                + "begin "
                + "select * from t;\n"
                + "select * from r where f = 'foo';"
                + "select * from r where f = 'begin' or f = 'END';"
                + "end;";
        checkSplitter(sql + sql1, sql.substring(0, sql.length() - 1), sql1);

        // semi colon in quoted string
        sql = "create procedure thisproc as "
                + "begin "
                + "select * from t;"
                + "select * from r where f = 'foo';"
                + "select * from r where f = 'beg;in' or f = 'END;';"
                + "end;";
        checkSplitter(sql + sql1, sql.substring(0, sql.length() - 1), sql1);

        // partition clause
        sql = "create procedure thisproc "
                + "partition on table t column a parameter 1 "
                + "allow operator as "
                + "begin "
                + "select * from t;"
                + "select * from r where f = 'foo';"
                + "end;";
        checkSplitter(sql + sql1, sql.substring(0, sql.length() - 1), sql1);

        // case inside longer strings
        sql = "create procedure p as begin "
                + "select emptycase from R; "
                + "select caseofbeer from R; "
                + "select suitcaseofbeer from R; "
                + "end;";
        checkSplitter(sql + sql1, sql.substring(0, sql.length() - 1), sql1);

        // case inside longer strings with numbers and letters
        sql = "create procedure p as begin "
                + "select empty1case from R; "
                + "select case2ofbeer from R; "
                + "select suit2case3ofbeer from R; "
                + "end;";
        checkSplitter(sql + sql1, sql.substring(0, sql.length() - 1), sql1);

        // end inside longer strings
        sql = "create procedure p as begin "
                + "select emptycase from R; "
                + "select caseofbeer from R; "
                + "select endofbeer from R; "
                + "select frontend from R; "
                + "end;";
        checkSplitter(sql, sql.substring(0, sql.length() - 1));

        // end inside longer strings with numbers and letters
        sql = "create procedure p as begin "
                + "select cas1end3ofbeer from R; "
                + "select end2ofbeer from R; "
                + "select front1end from R; "
                + "end;";
        checkSplitter(sql, sql.substring(0, sql.length() - 1));

        // begin as table name
        sql = "create procedure p as begin "
                + "select emptycase from begin; "
                + "select caseofbeer from R; "
                + "end;";
        checkSplitter(sql+sql1, sql.substring(0, sql.length() - 1), sql1);

        // begin as column name
        sql = "create procedure p as begin "
                + "select emptycase from S; "
                + "select begin from R; "
                + "end;";
        checkSplitter(sql+sql1, sql.substring(0, sql.length() - 1), sql1);

        // begin as table and column name
        sql = "create procedure p as begin "
                + "select begin.begin from begin; "
                + "select emptycase from R; "
                + "end;";
        checkSplitter(sql+sql1, sql.substring(0, sql.length() - 1), sql1);

        // begin as table and column name
        sql = "create procedure p as "
                + "select begin.begin from begin;";
        checkSplitter(sql+sql1, sql.substring(0, sql.length() - 1), sql1);

        // begin as table and column name with spaces after AS
        sql = "create procedure p as      "
                + "select begin.begin from begin;";
        checkSplitter(sql+sql1, sql.substring(0, sql.length() - 1), sql1);

        // prevent error propagating from one malformed statement to another
        // each statement should have a fresh value for checking in begin and in case...
        sql = "create table begin (a int);";
        sql1 = "create procedure p as select a, case when a > 10 then 'a' else 'b' end from begin;";
        checkSplitter(sql + sql, sql.substring(0, sql.length() - 1), sql.substring(0, sql.length() - 1));

        // parsing AS BEGIN will only end with END
        sql = "create table as begin (a int);";
        parsedOut = SQLLexer.splitStatements(sql + sql1);
        assertEquals(0, parsedOut.getCompletelyParsedStmts().size());
        assertEquals(sql + sql1, parsedOut.getIncompleteStmt());
    }

    @Test
    public void testProcSQLSplitWithComments() {

        String sql = "-- preceding comment\n"
                  + "create procedure thisproc as begin --one\n"
                  + "select * from t;select * from r where f = 'foo';\n"
                  + "-- mid-statement comment\n"
                  + "select * from r where f = 'begin' or f = 'END';end;\n"
                  + "-- trailing comment\n";
        String expected = "create procedure thisproc as begin \n"
                    + "select * from t;select * from r where f = 'foo';\n"
                    + "\n"
                    + "select * from r where f = 'begin' or f = 'END';end";
        checkSplitter(sql, expected);

        sql = "create procedure thisproc as "
                + "begin \n"
                + "select * from t; /*comment will still exist*/"
                + "select * from r where f = 'foo';"
                + "select * from r where f = 'begin' or f = 'END';"
                + "end;";
        expected = "create procedure thisproc as "
                + "begin \n"
                + "select * from t;  "
                + "select * from r where f = 'foo';"
                + "select * from r where f = 'begin' or f = 'END';"
                + "end";
        checkSplitter(sql, expected);

        sql = "select * from books;";
        String sql1 = "select title, case when cash > 100.00 "
                + "then case when cash > 1000.00 "
                + "then 'Super Expensive' else 'Pricy' end "
                + "'Expensive' else 'Cheap' end from books;";
        checkSplitter(sql + "/* comments will not exist if they are at beginning of statements */" + sql1,
                sql.substring(0, sql.length() - 1), sql1.substring(0, sql1.length() - 1));
    }

    @Test
    public void testProcSQLSplitWithCase() {

        String sql = "create procedure thisproc as "
                + "begin "
                + "SELECT a, "
                + "CASE WHEN a > 100.00 "
                + "THEN 'Expensive' "
                + "ELSE 'Cheap' "
                + "END "
                + "FROM t; "
                + "end;";
        checkSplitter(sql, sql.substring(0, sql.length() - 1));

        sql = "create procedure thisproc as "
                + "begin \n"
                + "select * from t; /*comment will still exist*/"
                + "select * from r where f = 'foo';"
                + "select * from r where f = 'begin' or f = 'END';"
                + "select a, "
                + "case when a > 100.00 then 'Expensive' else 'Cheap' end "
                + "from t;"
                + "end;";
        String expected = "create procedure thisproc as "
                + "begin \n"
                + "select * from t;  "
                + "select * from r where f = 'foo';"
                + "select * from r where f = 'begin' or f = 'END';"
                + "select a, "
                + "case when a > 100.00 then 'Expensive' else 'Cheap' end "
                + "from t;"
                + "end";
        checkSplitter(sql, expected);

        // nested CASE-WHEN-THEN-ELSE-END
        sql = "create procedure thisproc as "
                + "begin \n"
                + "select * from t; /*comment will still exist*/"
                + "select * from r where f = 'foo';"
                + "select * from r where f = 'begin' or f = 'END';"
                + "select a, "
                + "case when a > 100.00 then "
                + "case when a > 1000.00 then 'Super Expensive' else 'Pricy' end "
                + "'Expensive' else 'Cheap' end "
                + "from t;"
                + "end;";
        expected = "create procedure thisproc as "
                + "begin \n"
                + "select * from t;  "
                + "select * from r where f = 'foo';"
                + "select * from r where f = 'begin' or f = 'END';"
                + "select a, "
                + "case when a > 100.00 then "
                + "case when a > 1000.00 then 'Super Expensive' else 'Pricy' end "
                + "'Expensive' else 'Cheap' end "
                + "from t;"
                + "end";
        checkSplitter(sql, expected);

        // case with no whitespace before it
        sql = "create procedure thisproc as "
                + "begin "
                + "SELECT a, "
                + "100+CASE WHEN a > 100.00 "
                + "THEN 10 "
                + "ELSE 5 "
                + "END "
                + "FROM t; "
                + "end;";
        checkSplitter(sql, sql.substring(0, sql.length() - 1));

        // case/end with no whitespace before and after it
        sql = "create procedure thisproc as "
                + "begin "
                + "SELECT a, "
                + "10+case when id < 0 then (id+0)end+100 from aaa;"
                + "end;";
        checkSplitter(sql, sql.substring(0, sql.length() - 1));

        sql = "create procedure mumble as begin "
                + "select * from t order by case when t.a < 1 then t.a + 100 else t.a end; "
                + "select * from s order by case when s.a < 1 then s.a + 100 else s.a end; "
                + "end;";
        checkSplitter(sql, sql.substring(0, sql.length() - 1));

        String sql1 = "select * from R;";
        sql = "create procedure mumble as begin "
                + "select * from t order by case when t.a < 1 then t.a + 100 else t.a end; "
                + "select * from s order by case when s.a < 1 then s.a + 100 else s.a end; "
                + "end;";
        checkSplitter(sql+sql1, sql.substring(0, sql.length() - 1), sql1.substring(0, sql1.length() - 1));

        // to prevent propagating error from malformed case...end to next statement
        sql1 = "CREATE INDEX FunkyIndex on FUNKYDEFAULTS (CASE WHEN i < 10 THEN 0 ELSE 10 END CASE);";
        checkSplitter(sql1 + sql, sql1.substring(0, sql1.length() - 1), sql.substring(0, sql.length() - 1));
    }

}
