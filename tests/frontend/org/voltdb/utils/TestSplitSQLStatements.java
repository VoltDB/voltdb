/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.parser.SQLLexer;

public class TestSplitSQLStatements {

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
    }

    private void checkSplitter(final String strIn, final String... strsCmp) {
        final List<String> strsOut = SQLLexer.splitStatements(strIn);
        assertEquals(strsCmp.length, strsOut.size());
        for (int i = 0; i < strsCmp.length; ++i) {
            assertEquals(strsCmp[i], strsOut.get(i));
        }
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
        checkSplitter(" a\"b ; c \\\" 'd;ef' \" ; ", "a\"b ; c \\\" 'd;ef' \"");
        checkSplitter(" a'b ; c \\' \"d;ef\" ' ; ", "a'b ; c \\' \"d;ef\" '");
        checkSplitter("a;;b;;c;;", "a", "b", "c");
        checkSplitter("abc --;def\n;ghi", "abc --;def", "ghi");
        checkSplitter("abc /*\";def\n;*/ghi", "abc /*\";def\n;*/ghi");
        checkSplitter("a\r\nb;c\r\nd;", "a\r\nb", "c\r\nd");
        checkSplitter("--one\n--two\nreal", "--one", "--two", "real");
        checkSplitter("  --one\n  --two\nreal", "--one", "--two", "real");
        checkSplitter("  abc;  --def\n\n  /*ghi\njkl;*/", "abc", "--def", "/*ghi\njkl;*/");
    }

    @Test
    public void testProcSQLSplit() {
        checkSplitter("begi", "begi");
        checkSplitter("begin end", "begin end");
        checkSplitter("begin en", "begin en");
        checkSplitter("begin enf", "begin enf");

        checkSplitter("CREATE PROCEDURE foo as SELECT * from t;", "CREATE PROCEDURE foo as SELECT * from t");

        String sql = "CREATE PROCEDURE foo AS "
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
        checkSplitter("CREATE PROCEDURE foo BEGIN SELECT * from t; SELECT * from t;",
                "CREATE PROCEDURE foo BEGIN SELECT * from t; SELECT * from t;");

        // enf is not end of statement for BEGIN, so the ; is included as the parsing of BEGIN is not complete
        checkSplitter("CREATE PROCEDURE foo BEGIN SELECT * from t; SELECT * from t; ENF;",
                "CREATE PROCEDURE foo BEGIN SELECT * from t; SELECT * from t; ENF;");

        checkSplitter("CREATE PROCEDURE foo BEGIN SELECT * from t; SELECT * from t; ENF; end",
                "CREATE PROCEDURE foo BEGIN SELECT * from t; SELECT * from t; ENF; end");

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
        checkSplitter(sql+sql1, sql.substring(0, sql.length() - 1), sql1);

        // semi colon in quoted string
        sql = "create procedure thisproc as "
                + "begin "
                + "select * from t;"
                + "select * from r where f = 'foo';"
                + "select * from r where f = 'beg;in' or f = 'END;';"
                + "end;";
        checkSplitter(sql+sql1, sql.substring(0, sql.length() - 1), sql1);

        // partition clause
        sql = "create procedure thisproc "
                + "partition on table t column a parameter 1 "
                + "allow operator as "
                + "begin "
                + "select * from t;"
                + "select * from r where f = 'foo';"
                + "end;";
        checkSplitter(sql+sql1, sql.substring(0, sql.length() - 1), sql1);

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
    }

    @Test
    public void testProcSQLSplitWithComments() {

        String sql = "create procedure thisproc as "
                  + "begin --one\n"
                  + "select * from t;"
                  + "select * from r where f = 'foo';"
                  + "select * from r where f = 'begin' or f = 'END';"
                  + "end;";
        checkSplitter(sql, sql.substring(0, sql.length() - 1));

        sql = "create procedure thisproc as "
                + "begin \n"
                + "select * from t; /*comment will still exist*/"
                + "select * from r where f = 'foo';"
                + "select * from r where f = 'begin' or f = 'END';"
                + "end;";
        checkSplitter(sql, sql.substring(0, sql.length() - 1));
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
        checkSplitter(sql, sql.substring(0, sql.length() - 1));

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
        checkSplitter(sql, sql.substring(0, sql.length() - 1));

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
                + "select case when id < 0 then (id+0)end+100 from aaa;"
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
    }

}
