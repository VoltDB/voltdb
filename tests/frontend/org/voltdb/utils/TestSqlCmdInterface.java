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

package org.voltdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google_voltpatches.common.base.Joiner;

public class TestSqlCmdInterface
{
    private static boolean noBlockComment = true;
    private int ID = -1;
    private final static String sqlFile = "./tests/frontend/org/voltdb/utils/localQry.txt";
    private final String logFilename = "/tmp/logHelp.txt"; // For ENG-3440
    private static int numOfQueries = -1;
    private static String qryFrmFile = "";
    private final static String[] firstKeyQryWord =
            new String[]{"select", "insert", "delete", "update", "exec", "execute", "create", "alter", "drop"};


    @BeforeClass
    public static void prepare() throws FileNotFoundException {
        File fh = new File(sqlFile);
        if(fh.exists()) {
            String contents = readFile2String(fh);
            try {
                Pattern regex = Pattern.compile("(?:/\\*[^;]*?\\*/)", Pattern.DOTALL | Pattern.MULTILINE);

                Matcher regexMatcher = regex.matcher(contents);
                StringBuffer sb = new StringBuffer();
                while (regexMatcher.find()) {
                    regexMatcher.appendReplacement(sb, "");
                    noBlockComment = false;
                }
                // Add the last segment of input to the new String
                regexMatcher.appendTail(sb);
            } catch (PatternSyntaxException ex) {
                // Syntax error in the regular expression
                System.err.println(ex.getDescription());
                System.exit(1);
            }
            // To set two private members first: (int) numOfQueries & (String) qryFrmFile
            setQryString(fh);
        }
    }

    // To read the contents of the fileHandle into a string
    private static String readFile2String(File fileHandle) {
        //InputStream is = null;
        String buf = null;
        try {
            InputStream is = new FileInputStream(fileHandle);
            buf = new java.util.Scanner(is).useDelimiter("\\A").next();
        }
        catch (FileNotFoundException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        return buf;
    }

    // 1) To test a single select statement
    @Test
    public void testParseQuery1() {
        String raw = "select * from dummy";
        ID = 1;
        assertThis(raw, 1, ID);
    }

    // 2) To test 2 select statements which are separated by one or more white spaces
    @Test
    public void testParseQuery2() {
        String raw = "   select * From dummy      select * from    dummy2        ";
        ID = 2;
        String expected = trimKeyWordsLeadingSpaces(raw);
        //expected = expected.replaceAll("\\s+", " ");
        assertThis(raw, expected, 2, ID);
    }

    // 3) To test 2 select statements which are separated by one or more colons
    //    and zero or more white spaces
    @Test
    public void testParseQuery3() {
        String raw = "   select * from Dummy where id  =    1;;;;    " +
                      "   select * from          dummy2;        ";
        ID = 3;
        String expected = raw.replaceAll("\\;+", "");
        expected = trimKeyWordsLeadingSpaces(expected);
        assertThis(raw, expected, 2, ID);
    }

    // 4) To test 2 select statements separated by a comma which is illegal
    //    The sqlcmd parser can still separate two statements
    @Test
    public void testParseQuery4() {
        String raw = "   select * from dummy,    select * frOm dummy2,";
        ID = 4;
        String expected = trimKeyWordsLeadingSpaces(raw);
        assertThis(raw, expected, 2, ID);
    }

    // 5) To test 2 select statements in which one of them is incomplete
    @Test
    public void testParseQuery5() {
        String raw = "select * fRom dummy;;;;select *";
        ID = 5;
        String expected = raw.replaceAll("\\;+", " ");
        assertThis(raw, expected, 2, ID);
    }

    // 6) To test 2 select statements in which one of them is incomplete
    @Test
    public void testParseQuery6() {
        String raw = "     INSERT INTO      Dummy " +
                      "            vALUES (value1, NULL, null, '', ...)";
        ID = 6;
        String expected = trimKeyWordsLeadingSpaces(raw);
        assertThis(raw, expected, 1, ID);
    }

    // 7) To test 2 select statements with union
    //    VoltDB has not supported UNION, yet.
    //    So this is a negative test.
    @Test
    public void testParseQuery7() {
        String raw = "SELECT * FROM table UNION SELECT * FROM table2";
        String expected = raw;
        ID = 7;
        assertThis(raw, expected, 1, ID);
    }

    // 8) To test 2 select statements with --union
    //    Everything after --union should be ignored
    //    ENG-3354
    @Test
    public void testParseQuery8() {
        String raw = "SELECT * FROM table --UNION SELECT * FROM table2";
        ID = 8;
        String expected = raw;
        notAssertThis(raw, expected, 1, ID); // Disable this when ENG-3354 is fixed
        //assertThis(raw, expected, 1, ID);  // Enable this when ENG-3354 is fixed
    }

    // 9) To test 2 select statements with --union
    //    Slightly different from test case 8 - there is '--' directly
    //    in front of the key word 'select'. So the 2nd select statement
    //    is treated as a comment. This test should pass.
    @Test
    public void testParseQuery9() {
        String raw = "SELECT * FROM table --UNION --SELECT * FROM table2";
        ID = 9;
        String expected = raw;
        assertThis(raw, expected, 1, ID);
    }

    // 10) To test 2 select statements with --
    //     Slightly different from test case 9 - there is a space " " in between
    //     '--' and the 2nd select statement. In theory, this test should pass.
    @Test
    public void testParseQuery10() {
        String raw = "SELECT * FROM table -- SELECT * FROM table2";
        ID = 10;
        String expected = raw;
        notAssertThis(raw, expected, 1, ID); // Disable this when ENG-3354 is fixed
        //assertThis(raw, expected, 1, ID);  // Enable this when ENG-3354 is fixed

    }

    // As of today, 07/13/2012, sqlcmd does not support create, yet.
    // Just to check what's got returned.
    // 11) create table xxx (col1_name type(), col2_name type());
    @Test
    public void testParseQuery11() {
        String raw = "  create tAble xxx   (col1_name type(), col2_name type());";
        ID = 11;
        String expected = raw.replaceAll("\\;+", " ");
        expected = trimKeyWordsLeadingSpaces(expected);
        expected = expected.replaceAll("\\;+", " ");
        assertThis(raw, expected, 1, ID);
    }

    // 12) To test 2 sql statements starting with different key words
    //     which are separated by one or more white spaces.
    //     This test case is derived from case 2 & 11.
    @Test
    public void testParseQuery12() {
        String raw = " select * From dummy   create tAble xxx  (col1_name type(), col2_name type()) ;  ";
        ID = 2;
        String expected = raw.replaceAll("\\;+", " ");
        expected = trimKeyWordsLeadingSpaces(expected);
        assertThis(raw, expected, 2, ID);
    }

    // 13) To test 2 sql statements starting with different key words
    //     which are separated by a semicolon.
    //     This test case is derived from case 12.
    @Test
    public void testParseQuery13() {
        String raw = " select * From dummy;   create tAble xxx  (col1_name type(), col2_name type()) ;  ";
        ID = 2;
        String expected = raw.replaceAll("\\;+", " ");
        expected = trimKeyWordsLeadingSpaces(expected);
        assertThis(raw, expected, 2, ID);
    }

    // 14) To test a bogus string containing semicolon(s).
    @Test
    public void testParseQuery14() {
        // SQLCommand.mockVoltDBForTest(new ClientForTest());
        String raw = "   ssldgjdsgjdsjjg dskfkdskeevnskdh   ;   ksjghtrewoito dsfharw  ; ";
        ID = 14;
        String expected = raw;
        // sqlcmd always replace semicolons with ONE space
        expected = expected.replaceAll("\\s+\\;+\\s+", " ");
        // sqlcmd always trims the input string
        expected = expected.trim();
        assertThis(raw, expected, 2, ID);
    }

    // 15) select/delete/update/insert
    @Test
    public void testParseQuery15() {
        ID = 15;
        // raw1 contains two select statements separated by a space
        String raw1 = "select * from votes limit 10 select count(*) from votes ";
        String raw2 = "delete from votes where   PHONE_NUMBER = 3082086134      ";
        // raw3 contains two select statements separated by multiple spaces
        String raw3 = "select count(*) from votes   select count(*) from votes;";
        // The combination of raw5 & raw6 is just one sql statement
        String raw4 = "update votes set CONTESTANT_NUMBER = 7 ";
        String raw5 = "where PHONE_NUMBER = 2150002906 ";
        String raw6 = "insert into votes vAlues (2150000000, 'PA', 6)";
        String raw = raw1 + raw2 + raw3 + raw4 + raw5 + raw6;
        String copy = raw;
        copy = copy.replaceAll("\\s*\\;+\\s*", " ");
        copy = trimKeyWordsLeadingSpaces(copy);
        assertThis(raw, copy, 7, ID);
    }

    // The function 'alter table' is not supported, yet. Therefore, the
    // key word 'alter' is unrecognized. This test is kind of like test
    // case 8. More test cases are derived from this one.
    @Test
    public void testParseQuery16() {
        ID = 16;
        String raw1 = "select * from votes limit 12 ;";
        String raw2 = "delete from votes where PHONE_NUMBER = 3082086134 ";
        String raw3 = "   alter table xxxx rename to new_tbl_name ";
        String raw4 = "select cOunt(*) from dummy ";
        String raw = raw1 + raw2 + raw3 + raw4;
        String expected = raw.replaceAll("\\s*\\;+\\s*", " ");
        expected = trimKeyWordsLeadingSpaces(expected);
        // Since 'alter table' is not supported, yet. We expect only three valid queries
        // returned, although they will not be processed by the VoltDB engine.
        assertThis(raw, expected, 4, ID);
    }

    // This is derived from test case 16.
    // The only difference is that there is a semicolon in the end of 'raw2'.
    // So, should we expect 3 or 4 queries returned?
    @Test
    public void testParseQuery17() {
        ID = 17;
        String raw1 = "select * from votes limit 12 ;";
        String raw2 = "delete from votes where PHONE_NUMBER = 3082086134;";
        String raw3 = "alter table xxxx rename to new_tbl_name ";
        String raw4 = "select cOunt(*) from dummy ";
        String raw = raw1 + raw2 + raw3 + raw4;
        String expected = raw.replaceAll("\\s*\\;+\\s*", " ");
        expected = trimKeyWordsLeadingSpaces(expected);
        // Let's expect it returns 3 queries, just as we expected 3 in test case 16!!
        // But, unfortunately, it is not the case!!
        assertThis(raw, expected, 4, ID);
    } // end of testParseQuery17()

    // Starting to test stored procedures
    @Test
    public void testParseQuery18() {
        ID = 18;
        String raw = "select * from dummy       exec @SystemCatalog   tables;";
        String expected = raw.replaceAll("\\s*\\;+\\s*", " ");
        expected = trimKeyWordsLeadingSpaces(expected);
        assertThis(raw, expected, 2, ID);
    }

    // insert white spaces
    @Test
    public void testParseQuery19() {
        ID = 19;
        String raw = " insert into tablename (col1, col2) values ('   1st 2nd 3rd  ', '   ')";
        //String expected = raw.replaceAll("\\s*\\;+\\s*", " ");
        String expected = trimKeyWordsLeadingSpaces(raw);
        assertThis(raw, expected, 1, ID);
    }

    // insert NULLs
    @Test
    public void testParseQuery20() {
        ID = 20;
        String raw = " insert into votes (phone-number, state, CONTESTANT_NUMBER) ";
        raw += "values (978-475-      0001, 'MA', null)";
        //String expected = raw.replaceAll("\\s*\\;+\\s*", " ");
        String expected = trimKeyWordsLeadingSpaces(raw);
        assertThis(raw, expected, 1, ID);
    }

    // to get queries from a local file, which contains some line comments
    // starting with '--', and possibly block comments '/* ... *\/'
    @Test
    public void testParseQuery21() throws FileNotFoundException {
        ID = 21;
        //numOfQueries = 9; // test only
        String raw = SQLCommand.readScriptFile(sqlFile);
        assertThis(raw, qryFrmFile, numOfQueries, ID);
    }

    // To test parseQueryProcedureCallParameters()
    // To test a valid query: 'select * from dummy'
    @Test
    public void testParseQueryProcedureCallParameters22() {
        ID = 22;
        String query = "select * from dummy";
        String expected = query.replaceAll("\\s+", "");
        assertThis2(query, expected, 4, ID);
    }

    // To test parseQueryProcedureCallParameters()
    // To test a valid query: 'exec @SystemCatalog,      tables'
    @Test
    public void testParseQueryProcedureCallParameters23() {
        ID = 23;
        String query = "exec @SystemCatalog,     tables";
        String expected = query.replace("exec", "");
        expected = expected.replaceAll(",", "");
        expected = expected.replaceAll("\\s+", "");
        assertThis2(query, expected, 2, ID);
    }

    // To test parseQueryProcedureCallParameters()
    // To test a valid query: 'exec ,, @SystemCatalog,,,,tables'
    // This test case is PASSED, which is kind of a surprise and shows that syntax could be too loose
    @Test
    public void testParseQueryProcedureCallParameters24() {
        ID = 24;
        String query = "exec ,, @SystemCatalog,,,,tables";
        String expected = query.replace("exec", "");
        expected = expected.replaceAll(",", "");
        expected = expected.replaceAll("\\s+", "");
        assertThis2(query, expected, 2, ID);
    }

    // To test parseQueryProcedureCallParameters()
    // To test a valid query: 'exec,, @SystemCatalog,,,,tables'
    // This test case is FAILED, which is also a surprise, because test case 23 is PASSED.
    // This further demonstrates that syntax is too loose, but NOT flexible.
    // Bug 3422
    @Test
    public void testParseQueryProcedureCallParameters25() {
        ID = 25;
        String query = "exec,, @SystemCatalog,,,,tables";
        String expected = query.replace("exec", "");
        expected = expected.replaceAll(",", "");
        expected = expected.replaceAll("\\s+", "");
        //assertThis2(query, expected, 2, ID);   // Uncomment this line after 3422 is fixed
        notAssertThis2(query, expected, 2, ID);  // Comment out this line after 3422 is fixed
    }

    // To assert the help page printed by SQLCommand.printHelp() is identical to the
    // original static help file 'SQLCommandReadme.txt'. For ENG-3440
    @Test
    public void testPrintHelpMenu26() throws IOException {
        ID = 26;
        String msg = "\nTest ID: " + ID + "\n";
        String err1 = null, err2 = null;

        String orgReadme = "./src/frontend/org/voltdb/utils/" + SQLCommand.getReadme();
        FileOutputStream fos = new FileOutputStream(logFilename);
        SQLCommand.printHelp(fos);
        FileInputStream fstream1 = new FileInputStream(logFilename);
        FileInputStream fstream2 = new FileInputStream(orgReadme);

        DataInputStream in1 = new DataInputStream(fstream1);
        BufferedReader br1 = new BufferedReader(new InputStreamReader(in1));
        DataInputStream in2 = new DataInputStream(fstream2);
        BufferedReader br2 = new BufferedReader(new InputStreamReader(in2));

        String strLine1 = null, strLine2 = null;
        int cnt = 0;
        while((strLine1 = br1.readLine()) != null && (strLine2 = br2.readLine()) != null)   {
            err1 = "Expected Content: #" + strLine1 + "#\n";
            err1 = "  Actual Content: #" + strLine2 + "#\n";
            assertTrue(msg+err1, strLine1.equals(strLine2));
            cnt++;
        }
        err2 = "The value of line count cannot be zero! cnt = " + cnt + "\n";
        assertNotSame(msg+err2, 0, cnt);
    }

    // 27) Make sure we don't get fooled by store procedures that with names that start
    //     with SQL keywords
    @Test
    public void testSneakyNamedProcedure() {
        String query = "exec selectMasterDonner, 0, 1";
        ID = 27;
        String expected = trimKeyWordsLeadingSpaces(query);
        assertThis(query, expected, 1, ID);
        expected = query.replace("exec", "");
        expected = expected.replaceAll(",", "");
        expected = expected.replaceAll("\\s+", "");
        assertThis2(query, expected, 3, ID);
    }

    @Test
    public void testParseCreateView()
    {
        ID = 28;
        String create = "create view foo (bas, as) as select bar, count(*) from foo group by val";
        assertThis(create, create, 1, ID);
    }

    @Test
    public void testParseCreateStmtProcedure()
    {
        ID = 29;
        String create = "create procedure foo as select * from blat";
        assertThis(create, create, 1, ID);
        create = "create procedure foo as insert into blat values (?, ?)";
        assertThis(create, create, 1, ID);
        create = "create procedure foo as update into blat values (?, ?)";
        assertThis(create, create, 1, ID);
        create = "create procedure foo as delete into blat values (?, ?)";
        assertThis(create, create, 1, ID);
        create = "create procedure foo as SELECT * FROM table UNION SELECT * FROM table2";
        assertThis(create, create, 1, ID);
    }

    private static void setQryString(File QryFileHandle) throws FileNotFoundException {
        // Prepare a Scanner that will "scan" the document
        Scanner opnScanner = new Scanner(QryFileHandle);
        // Read each line in the file
        while(opnScanner.hasNext()) {
            String line = opnScanner.nextLine();
            String str1 = "^--num=\\d+$";
            // To filter out sql comments starting with '--'
            // Note that currently, we only filter out the comments lines with
            // leading '--'. For instance:
            // 1) --this commenting line will be filtered out
            String str2 = "-{2,}.*";
            boolean result = line.matches(str2);
            if(result == true) {
                if(line.matches(str1))
                    numOfQueries = Integer.parseInt(line.replaceAll("\\D+", ""));
            }
            else {
                qryFrmFile = qryFrmFile.concat(line).concat(" ");
            }
        }
        qryFrmFile = qryFrmFile.replaceAll("\\;+", " ");
        qryFrmFile = trimKeyWordsLeadingSpaces(qryFrmFile);
        //qryFrmFile += " #blah#"; // For testing only
    }

    private static String trimKeyWordsLeadingSpaces(String str) {
        str = str.toLowerCase();
        for(String keyWord :  firstKeyQryWord) {
            String raw = "\\s+" + keyWord;
            String cleaned = " " + keyWord;
            str = str.replaceAll(raw, cleaned);
        }
        return str.trim();
    }

    private void assertThis(String qryStr, int numOfQry, int testID) {
        List<String> parsed = SQLCommand.parseQuery(qryStr);
        String msg = "Test ID: " + testID + ". ";
        assertNotNull(msg + "SQLCommand.parseQuery returned a NULL obj!!", parsed);
        assertEquals(msg, numOfQry, parsed.size());
        String parsedString = Joiner.on(" ").join(parsed);
        if(noBlockComment == true)
            assertTrue(msg, qryStr.equalsIgnoreCase(parsedString));
    }

    private void assertThis(String qryStr, String cleanQryStr, int numOfQry, int testID) {
        List<String> parsed = SQLCommand.parseQuery(qryStr);
        String msg = "\nTest ID: " + testID + ". ";
        String err1 = "\nExpected # of queries: " + numOfQry + "\n";
        err1 += "Actual # of queries: " + parsed.size() + "\n";
        assertEquals(msg+err1, numOfQry, parsed.size());
        String parsedString = Joiner.on(" ").join(parsed);
        //String result = parsed.get(0);
        String err2 = "\nExpected queries: \n#" + cleanQryStr + "#\n";
        err2 += "Actual queries: \n#" + parsedString + "#\n";
        if(ID == 21)
            if(noBlockComment == true)
                // If there is sql comments in block(s), then skip the assertion below
                assertTrue(msg+err2, cleanQryStr.equalsIgnoreCase(parsedString));
            else
                assertFalse(msg+err2, cleanQryStr.equalsIgnoreCase(parsedString));
        else
               assertTrue(msg+err2, cleanQryStr.equalsIgnoreCase(parsedString));
    }

    private void assertThis2(String query, String cleanQryStr, int num, int testID) {
        Pattern ExecuteCall = SQLCommand.getExecuteCall();
        query = ExecuteCall.matcher(query).replaceFirst("");
        List<String> params = SQLCommand.parseProcedureCallParameters(query);
        String parsedString = Joiner.on("").join(params);
        String msg = "\nTest ID: " + testID + ". ";
        String err1 = "\nExpected # of queries: " + num + "\n";
        err1 += "Actual # of queries: " + params.size() + "\n";
        assertEquals(msg+err1, num, params.size());
        String err2 = "\nExpected queries: \n#" + cleanQryStr + "#\n";
        err2 += "Actual queries: \n#" + parsedString + "#\n";
        assertTrue(msg+err2, cleanQryStr.equalsIgnoreCase(parsedString));
    }

    private void notAssertThis(String qryStr, String cleanQryStr, int numOfQry, int testID) {
        List<String> parsed = SQLCommand.parseQuery(qryStr);
        String msg = "\nTest ID: " + testID + ". ";
        String err1 = "\nExpected # of queries: " + numOfQry + "\n";
        err1 += "Actual # of queries: " + parsed.size() + "\n";
        assertNotSame(msg+err1, numOfQry, parsed.size());
        String parsedString = Joiner.on(" ").join(parsed);
        String err2 = "\nExpected queries: \n#" + cleanQryStr + "#\n";
        err2 += "Actual queries: \n#" + parsedString + "#\n";
        assertTrue(msg+err2, cleanQryStr.equalsIgnoreCase(parsedString));
    }

    private void notAssertThis2(String query, String cleanQryStr, int num, int testID) {
        Pattern ExecuteCall = SQLCommand.getExecuteCall();
        query = ExecuteCall.matcher(query).replaceFirst("");
        List<String> params = SQLCommand.parseProcedureCallParameters(query);
        String parsedString = Joiner.on("").join(params);
        String msg = "\nTest ID: " + testID + ". ";
        String err1 = "\nExpected # of queries: " + num + "\n";
        err1 += "Actual # of queries: " + params.size() + "\n";
        assertNotSame(msg+err1, num, params.size());
        String err2 = "\nExpected queries: \n#" + cleanQryStr + "#\n";
        err2 += "Actual queries: \n#" + parsedString + "#\n";
           assertFalse(msg+err2, cleanQryStr.equalsIgnoreCase(parsedString));
    }
}
