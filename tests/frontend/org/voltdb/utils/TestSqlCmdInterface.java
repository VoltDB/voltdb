/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
/*
import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

import org.junit.Test;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.Clientcopys;
import org.voltdb.client.ClientcopysContext;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
*/

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Joiner;

public class TestSqlCmdInterface
{
	private	int randomNum = -1;
	private int ID = -1;
	private final static String filename = "./tests/frontend/org/voltdb/utils/localQry.txt";
	private static int numOfQueries = -1;
	private static String qryFrmFile = "";
	private final static String[] firstKeyQryWord = 
			new String[]{"select", "insert", "delete", "update", "exec", "execute"};
	
	/*
    copyic private class ClientForTest implements Client {
    	boolean connected = false;
    	
		@Override
		public void createConnection(String host) throws UnknownHostException,
				IOException {
			assertFalse(connected);
	    	connected = true;
		}

		@Override
		public void createConnection(String host, int port)
				throws UnknownHostException, IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public ClientResponse callProcedure(String procName,
				Object... parameters) throws IOException,
				NoConnectionsException, ProcCallException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean callProcedure(ProcedureCallback callback,
				String procName, Object... parameters) throws IOException,
				NoConnectionsException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean callProcedure(ProcedureCallback callback,
				int expectedSerializedSize, String procName,
				Object... parameters) throws IOException,
				NoConnectionsException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public int calculateInvocationSerializedSize(String procName,
				Object... parameters) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public ClientResponse updateApplicationCatalog(File catalogPath,
				File deploymentPath) throws IOException,
				NoConnectionsException, ProcCallException {
			assertTrue(false);
			return null;
		}

		@Override
		public boolean updateApplicationCatalog(ProcedureCallback callback,
				File catalogPath, File deploymentPath) throws IOException,
				NoConnectionsException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void drain() throws NoConnectionsException, InterruptedException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void close() throws InterruptedException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void backpressureBarrier() throws InterruptedException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public ClientcopysContext createcopysContext() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Object[] getInstanceId() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getBuildString() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void configureBlocking(boolean blocking) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public boolean blocking() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public int[] getThroughputAndOutstandingTxnLimits() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void writeSummaryCSV(Clientcopys copys, String path)
				throws IOException {
			// TODO Auto-generated method stub
			
		}
    
    } // end of copyic private class ClientForTest implements Client
      */
	
	@BeforeClass
	public static void prepare() throws FileNotFoundException {
		File fh = new File(filename);
		if(fh.exists()) {
			//System.out.println("File exist!! ==-->> " + filename);
			// To set two private members first: (int) numOfQueries & (String) qryFrmFile
			setQryString(fh);
		}
	}
	
	// 1) To test a single select copyement
    @Test
    public void testParseQuery1() {
    //	SQLCommand.mockVoltDBForTest(new ClientForTest());
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
                      "            vALUES (value1, value2, value3,...)";
        ID = 6;
        String expected = trimKeyWordsLeadingSpaces(raw);
        assertThis(raw, expected, 1, ID);
    }
   
    // 7) To test 2 select statements with union
    @Test
    public void testParseQuery7() {
        String raw = "SELECT * FROM table UNION SELECT * FROM table2";
        String expected = raw;
        ID = 7;
        assertThis(raw, expected, 1, ID);    
    }
    
    // 8) To test 2 select statements with --union
    //    Everything after --union should be ignored
    @Test
    public void testParseQuery8() {
        String raw = "SELECT * FROM table --UNION SELECT * FROM table2";
        String expected = raw;
        assertThis(raw, expected, 1, ID);
    }
    
    /*
    // 8) create table xxx (col1_name type(), col2_name type());
    @Test
    public void testParseQuery8() {
    	String raw = "  create tAble xxx   (col1_name type(), col2_name type());";
    	//copy = copy.replaceAll("\\s+", " ");
        String copy = raw.replaceAll("\\;+", " ");
        copy = copy.trim();
        assertThis(raw, copy, 1);    
    }
       
    // 9) select/delete/update/insert
    @Test
    public void testParseQuery9() {
    	String raw1 = "select * from votes limit 10 select count(*) from votes;";
    	String raw2 = "delete from votes where   PHONE_NUMBER = 3082086134;      ";
    	String raw3 = "select count(*) from votes | select count(*) from votes;";
    	String raw4 = "--update votes set CONTESTANT_NUMBER = 7 ";
    	String raw5 = "where PHONE_NUMBER = 2150002906;";
    	String raw6 = "insert into votes vAlues (2150000000, 'PA', 6);";
    	String raw = raw1 + raw2 + raw3 + raw4 + raw5 + raw6;
    	String copy = raw;
    	copy = copy.replaceAll("\\;+", " ");
    	copy = trimKeyWordsLeadingSpaces(copy);
    	assertThis(raw, copy, 7);
    }
    
    //alter table xxxx rename to new_tbl_name;
    // String value = System.getProperty("key", "defaultvalue");
    @Test
    public void testParseQuery10() {
      	String raw1 = "select * from votes limit 12 ;";
    	String raw2 = "delete from votes where PHONE_NUMBER = 3082086134";
    	String raw3 = "--alter table xxxx rename to new_tbl_name ";
    	String raw4 = "select cOunt(*) from dummy ";
    	String raw = raw1 + raw2 + raw3 + raw4;
    	String copy = raw;
    	copy = copy.replaceAll("\\;+", " ");
    	copy = trimKeyWordsLeadingSpaces(copy);
    	assertThis(raw, copy, 3);    	
    }
    
    // to get queries from a local file
    @Test
    public void testParseQuery11() throws FileNotFoundException {
  		//System.out.println("\n\n");
   		//System.out.println("11 Final string:\n" + qryFrmFile);
        List<String> parsed = SQLCommand.parseQuery(qryFrmFile);
        assertNotNull(parsed);
        assertEquals(numOfQueries, parsed.size());
        String parsedString = Joiner.on(" ").join(parsed);
        //System.out.println("11 parsedString:\n" + parsedString);
        //System.out.println("\n\n");
        //String parsedString = parsed.get(0);
        assertEquals(qryFrmFile, parsedString);    	
    }
    */
    
//    @Test
//    public void testParseQuery12() {
 //   	String line = SQLCommand.parseSQLFile(filename);
    	//System.out.println("\n12 queries from file " + filename + " :\n" + line);
   // 	assertThis(line, qryFrmFile, numOfQueries);
    	/*
    	List<String> parsed = SQLCommand.parseQuery(line);
    	String parsedString = Joiner.on(" ").join(parsed);
    	System.out.println("\n12 queries from file " + filename + " after parseQuery:\n" + parsedString);
    	//qryFrmFile = qryFrmFile.replaceAll("\\;+", " "); 
    	//qryFrmFile = qryFrmFile.replaceAll("\\s+", " ");
        //qryFrmFile = qryFrmFile.trim();
        System.out.println("12 Final string:\n" + qryFrmFile);
        assertEquals(numOfQueries, parsed.size());
    	assertEquals(qryFrmFile, parsedString);
    	*/
    //}
    /*
    @Test
    public void testParseQuery13() {
    	String line = SQLCommand.parseSQLFile(filename);
    	//System.out.println("queries from file " + filename + " : " + line);
    	List<String> parsed = SQLCommand.parseQuery(line);
    	String parsedString = Joiner.on(" ").join(parsed);
    	//System.out.println("queries from file " + filename + " after parseQuery:\n" + parsedString);
    	//qryFrmFile = qryFrmFile.replaceAll("\\;+", " "); 
    	//qryFrmFile = qryFrmFile.replaceAll("\\s+", " ");
        //qryFrmFile = qryFrmFile.trim();
        //System.out.println("Final string:\n" + qryFrmFile);
    	assertEquals(qryFrmFile, parsedString);
    	for(int i = 0;i < parsed.size();i++)
    		System.out.println(i + ", ready to run this query: " + parsed.get(i));
    		//SQLCommand.testExecuteQuery(parsed.get(i));
    }
    


	private String getMultipleSemiColonSeparatedQry() {
    	long seed = getASeed();
    	Random rnd=new Random(seed);
    	//System.out.println("rnd = " + rnd);
    	randomNum = rnd.nextInt(20);
    	return null;
	}

	private long getASeed() {
		RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
        //
        // Get name representing the running Java virtual machine.
        // It returns something like {PID}@{HOSTNAME}. Where the value
        // before the @ symbol is the PID.
        //
        String jvmName = bean.getName();
        // System.out.println("Name = " + jvmName);
 
        //
        // Extract the PID by splitting the string returned by the
        // bean.getName() method.
        //
        long pid = Long.valueOf(jvmName.split("@")[0]);
        System.out.println("PID  = " + pid);
        // To use this PID as a seed
		return pid;
	}
	

	@Test
    public void testParseQuery50() {
    //	SQLCommand.mockVoltDBForTest(new ClientForTest());
        String statements = "select * from dummy where id = 1";
        List<String> parsed = SQLCommand.parseQuery(statements);
        assertNotNull(parsed);
        assertEquals(1, parsed.size());
        String result = parsed.get(0);
        assertNotNull(result);
        assertEquals(statements, result);
    }
	*/

    
    private static void setQryString(File QryFileHandle) throws FileNotFoundException {
    	// Prepare a Scanner that will "scan" the document
        Scanner opnScanner = new Scanner(QryFileHandle);
        // Read each line in the file
        while(opnScanner.hasNext()) {
            // Read each line and display its value
	        String line = opnScanner.nextLine();
	        String str1 = "--num=\\d+$";
	        //String str2 = "^--\\w*\\W*\\d*\\D*";
	        String str2 = "^--\\w*\\W*\\d*\\D*";
	        boolean result = line.matches(str2);
	        if(result == true) {
	        	if(line.matches(str1)) {
	        		numOfQueries = Integer.parseInt(line.replaceAll("\\D+", ""));
	        		System.out.println("numOfQueries = " + numOfQueries);
	        	}
	        	else {
	        		System.out.println("Comments Line: " + line);
	        	}
	        }
	        else {
	        	qryFrmFile = qryFrmFile.concat(line).concat(" ");
	        }
	        // method not get the same values therefore it returns false
	        //System.out.println("Method returns:  " + result + ", Line: " + line);
	    }
        qryFrmFile = qryFrmFile.replaceAll("\\;+", " ");
        qryFrmFile = trimKeyWordsLeadingSpaces(qryFrmFile);
    	//qryFrmFile = qryFrmFile.replaceAll("\\s+", " ");
        //qryFrmFile = qryFrmFile.trim();
        System.out.println("Final string:\n" + qryFrmFile);
	}
	
    private static String trimKeyWordsLeadingSpaces(String str) {
    	str = str.toLowerCase();
		for(String keyWord :  firstKeyQryWord) {
			String raw = "\\s+" + keyWord;
			String cleaned = " " + keyWord;
			str = str.replaceAll(raw, cleaned);
			//System.out.println("In trimKeyWordsLeadingSpaces(), keyWord = " + keyWord);
		}
		return str.trim();
	}

	private void assertThis(String qryStr, int numOfQry, int testID) {
		List<String> parsed = SQLCommand.parseQuery(qryStr);
		String msg = "Test ID: " + testID + ". ";
        assertNotNull(msg + "SQLCommand.parseQuery returned a NULL obj!!", parsed);
        assertEquals(msg, numOfQry, parsed.size());        
        String parsedString = Joiner.on(" ").join(parsed);
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
        //assertEquals(cleanQryStr, parsedString);
        String err2 = "\nExpected queries: \n#" + cleanQryStr + "#\n";
        err2 += "Actual queries: \n#" + parsedString + "#\n";
        assertTrue(msg+err2, cleanQryStr.equalsIgnoreCase(parsedString));
        //assertTrue(cleanQryStr.equalsIgnoreCase(parsedString));

	}


	private void getQryList() {
		// TODO Auto-generated method stub
		System.out.println("In getQryList");
	}
    
   
}
