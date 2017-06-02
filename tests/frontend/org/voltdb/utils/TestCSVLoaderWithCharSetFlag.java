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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltcore.logging.VoltLogger;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestCSVLoaderWithCharSetFlag {

    protected static ServerThread localServer;
    protected static Client client;
    protected static final VoltLogger m_log = new VoltLogger("CONSOLE");

    protected static String userName = System.getProperty("user.name");
    protected static String reportDir = String.format("/tmp/%s_csv", userName);
    protected static String path_csv = String.format("%s/%s", reportDir, "test.csv");
    protected static String dbName = String.format("mydb_%s", userName);
    protected static String originEncoding = "utf8";

    public static void prepare() {
        if (!reportDir.endsWith("/"))
            reportDir += "/";
        File dir = new File(reportDir);
        try {
            if (!dir.exists()) {
                dir.mkdirs();
            }

        } catch (Exception x) {
            m_log.error(x.getMessage(), x);
            System.exit(-1);
        }
    }

    @BeforeClass
    public static void startDatabase() throws Exception
    {
        prepare();

        String pathToCatalog = Configuration.getPathToCatalogForTest("csv.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("csv.xml");
        VoltProjectBuilder builder = new VoltProjectBuilder();

        builder.addLiteralSchema(
                "create table BLAH ("
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
                + ");");
        builder.addPartitionInfo("BLAH", "clm_integer");
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);
        Configuration config = new Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;
        localServer = new ServerThread(config);
        client = null;

        localServer.start();
        localServer.waitForInitialization();

        client = ClientFactory.createClient(new ClientConfig());
        client.createConnection("localhost");
    }

    public void test_Interface(String[] my_options, String[] my_data, int invalidLineCnt,
            int validLineCnt) throws Exception {
        test_Interface(my_options, my_data, invalidLineCnt, validLineCnt, 0, new String[0]);
    }

    public void test_Interface(String[] my_options, String[] my_data, int invalidLineCnt,
            int validLineCnt, int validLineUpsertCnt, String[] validData) throws Exception {
        try{
            FileOutputStream fos = new FileOutputStream(path_csv);
            OutputStreamWriter osw = new OutputStreamWriter(fos, originEncoding);
            BufferedWriter out_csv = new BufferedWriter(osw);

            for (String aMy_data : my_data) {
                out_csv.write(aMy_data + "\n");
            }
            out_csv.flush();
            out_csv.close();
        }
        catch( Exception e) {
            System.err.print( e.getMessage() );
        }

        CSVLoader.testMode = true;
        CSVLoader.main( my_options );
        // do the test

        VoltTable modCount;
        modCount = client.callProcedure("@AdHoc", "SELECT * FROM BLAH;").getResults()[0];
        int rowct = modCount.getRowCount();

        // Call validate partitioning to check if we are good.
        VoltTable valTable;
        valTable = client.callProcedure("@ValidatePartitioning", null, null).getResults()[0];
        while (valTable.advanceRow()) {
            long miscnt = valTable.getLong("MISPARTITIONED_ROWS");
            assertEquals(miscnt, 0);
        }

        BufferedReader csvreport = new BufferedReader(new FileReader(CSVLoader.pathReportfile));
        int lineCount = 0;
        String line;
        String promptMsg = "Number of rows successfully inserted:";
        String promptFailMsg = "Number of rows that could not be inserted:";
        int invalidlinecnt = 0;

        while ((line = csvreport.readLine()) != null) {
            if (line.startsWith(promptMsg)) {
                String num = line.substring(promptMsg.length());
                lineCount = Integer.parseInt(num.replaceAll("\\s",""));
            }
            if( line.startsWith(promptFailMsg)){
                String num = line.substring(promptFailMsg.length());
                invalidlinecnt = Integer.parseInt(num.replaceAll("\\s",""));
            }
        }
        csvreport.close();
        assertEquals(lineCount-validLineUpsertCnt,  rowct);
        //assert validLineCnt specified equals the successfully inserted lineCount
        assertEquals(validLineCnt, lineCount);
        assertEquals(invalidLineCnt, invalidlinecnt);

        // validate upsert the correct data
        if (validData != null && validData.length > 0) {
            tearDown();
            setup();
            try{
                BufferedWriter out_csv = new BufferedWriter( new FileWriter( path_csv ) );
                for (String aMy_data : validData) {
                    out_csv.write(aMy_data + "\n");
                }
                out_csv.flush();
                out_csv.close();
            }
            catch( Exception e) {
                e.printStackTrace();
            }

            CSVLoader.testMode = true;
            CSVLoader.main( my_options );

            VoltTable validMod;
            validMod = client.callProcedure("@AdHoc", "SELECT * FROM BLAH;").getResults()[0];
            assertTrue(modCount.hasSameContents(validMod));
        }
    }

    @AfterClass
    public static void stopDatabase() throws InterruptedException
    {
        if (client != null) client.close();
        client = null;

        if (localServer != null) {
            localServer.shutdown();
            localServer.join();
        }
        localServer = null;
    }

    @Before
    public void setup() throws IOException, ProcCallException
    {
        final ClientResponse response = client.callProcedure("@AdHoc", "SELECT COUNT(*) FROM BLAH;");
        assertEquals(0, response.getResults()[0].asScalarLong());
    }

    @After
    public void tearDown() throws IOException, ProcCallException
    {
        final ClientResponse response = client.callProcedure("@AdHoc", "TRUNCATE TABLE BLAH;");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
    }

    @Test
    public void testMultiByteCharacterUTF8() throws Exception
    {
        // ENG-12324: csvloader --header doesn't work if header has spaces. This is essentially the same test as testHeaderColumnNumNotSame, but with some
        // strategically placed whitespace in the header.

    	originEncoding = "utf8";
    	createCSVFile("utf8");

        String []myOptions = {
                "-f" + path_csv,
                "--characterSet=utf8",
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--limitrows=100",
                "--header",
                "BlAh"
        };
        String FILENAME = "utf8_encoded_text.csv";
        List<String> list = new ArrayList<>();

		//read from the file
		BufferedReader br = null;
		FileReader fr = null;
		try {
			fr = new FileReader(FILENAME);
			br = new BufferedReader(fr);
			String sCurrentLine;
			br = new BufferedReader(new FileReader(FILENAME));
			while ((sCurrentLine = br.readLine()) != null) {
				list.add(sCurrentLine);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

		String[] myData2 = new String[list.size()];
		for(int i = 0; i < list.size(); i++) {
			myData2[i] = list.get(i);
		}

        int invalidLineCnt = 0;
        int validLineCnt = 8;
        test_Interface(myOptions, myData2, invalidLineCnt, validLineCnt );

        // query the database
        VoltTable table = client.callProcedure("@AdHoc", "SELECT * FROM BLAH ORDER BY CLM_INTEGER;").getResults()[0];
        String[] strings = {"first复杂的漢字查询内容","second","third",null,"abcdeg","sixth","seventh","first"};
        int pos = 0;
        while(table.advanceRow()) {
        	int i = table.getActiveRowIndex();
        	VoltTableRow row = table.fetchRow(i);
        	assertEquals(row.getString(4),strings[pos]);
        	pos++;
        }

        try {
        	File temp = new File(FILENAME);
        	if(temp.exists()) temp.delete();
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }

    @Test
    public void testUTF8WithGBKEncoding() throws Exception
    {
        // ENG-12324: csvloader --header doesn't work if header has spaces. This is essentially the same test as testHeaderColumnNumNotSame, but with some
        // strategically placed whitespace in the header.

    	originEncoding = "utf8";
    	createCSVFile("utf8");

        String []myOptions = {
                "-f" + path_csv,
                "--characterSet=gbk",
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--limitrows=100",
                "--header",
                "BlAh"
        };
        String FILENAME = "utf8_encoded_text.csv";
        List<String> list = new ArrayList<>();

		//read from the file
		BufferedReader br = null;
		FileReader fr = null;
		try {
			fr = new FileReader(FILENAME);
			br = new BufferedReader(fr);
			String sCurrentLine;
			br = new BufferedReader(new FileReader(FILENAME));
			while ((sCurrentLine = br.readLine()) != null) {
				list.add(sCurrentLine);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

		String[] myData2 = new String[list.size()];
		for(int i = 0; i < list.size(); i++) {
			myData2[i] = list.get(i);
		}

        int invalidLineCnt = 0;
        int validLineCnt = 8;
        test_Interface(myOptions, myData2, invalidLineCnt, validLineCnt );

        // query the database
        VoltTable table = client.callProcedure("@AdHoc", "SELECT * FROM BLAH ORDER BY CLM_INTEGER;").getResults()[0];
        String[] strings = {"first复杂的漢字查询内容","second","third",null,"abcdeg","sixth","seventh","first"};
        int pos = 0;
        while(table.advanceRow()) {
        	int i = table.getActiveRowIndex();
        	if(i==0) {
        		assertFalse(table.fetchRow(i).getString(4).equals(strings[pos]));
        		pos++;continue;
        	}
        	VoltTableRow row = table.fetchRow(i);
        	assertEquals(row.getString(4),strings[pos]);
        	pos++;
        }
        try {
        	File temp = new File(FILENAME);
        	if(temp.exists()) temp.delete();
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }

    @Test
    public void testGB2312WithGBKEncoding() throws Exception
    {
        // ENG-12324: csvloader --header doesn't work if header has spaces. This is essentially the same test as testHeaderColumnNumNotSame, but with some
        // strategically placed whitespace in the header.

    	originEncoding = "gb2312";
    	createCSVFile("gb2312");

        String []myOptions = {
                "-f" + path_csv,
                "--characterSet=gbk",	// use gbk to decode it
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--limitrows=100",
                "--header",
                "BlAh"
        };
        String FILENAME = "gb2312_encoded_text.csv";
        List<String> list = new ArrayList<>();

		//read from the file
		BufferedReader br = null;
		FileReader fr = null;
		try {
			fr = new FileReader(FILENAME);
			br = new BufferedReader(fr);
			String sCurrentLine;
			br = new BufferedReader(new FileReader(FILENAME));
			while ((sCurrentLine = br.readLine()) != null) {
				list.add(sCurrentLine);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

		String[] myData2 = new String[list.size()];
		for(int i = 0; i < list.size(); i++) {
			myData2[i] = list.get(i);
		}

        int invalidLineCnt = 0;
        int validLineCnt = 8;
        test_Interface(myOptions, myData2, invalidLineCnt, validLineCnt );

        // query the database
        VoltTable table = client.callProcedure("@AdHoc", "SELECT * FROM BLAH ORDER BY CLM_INTEGER;").getResults()[0];
        String[] strings = {"first复杂的漢字查询内容","second","third",null,"abcdeg","sixth","seventh","first"};
        int pos = 0;
        while(table.advanceRow()) {
        	int i = table.getActiveRowIndex();
        	if(i==0) {
        		// System.out.println(table.fetchRow(i).getString(4) + "in the table, " + strings[pos] + " expected");
        		assertFalse(table.fetchRow(i).getString(4).equals(strings[pos]));
        		pos++;continue;
        	}
        	VoltTableRow row = table.fetchRow(i);
        	assertEquals(row.getString(4),strings[pos]);
        	pos++;
        }
        try {
        	File temp = new File(FILENAME);
        	if(temp.exists()) temp.delete();
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }

    @Test
    public void testMultiByteCharacterGBK() throws Exception
    {
        // ENG-12324: csvloader --header doesn't work if header has spaces. This is essentially the same test as testHeaderColumnNumNotSame, but with some
        // strategically placed whitespace in the header.

    	originEncoding = "gbk";
    	createCSVFile("gbk");

        String []myOptions = {
                "-f" + path_csv,
                "--characterSet=gbk",
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--limitrows=100",
                "--header",
                "BlAh"
        };
        String FILENAME = "gbk_encoded_text.csv";
        List<String> list = new ArrayList<>();

		//read from the file
		BufferedReader br = null;
		FileReader fr = null;
		try {
			fr = new FileReader(FILENAME);
			br = new BufferedReader(fr);
			String sCurrentLine;
			br = new BufferedReader(new InputStreamReader(new FileInputStream(FILENAME), Charset.forName("GBK")));
			while ((sCurrentLine = br.readLine()) != null) {

				list.add(sCurrentLine);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

		String[] myData2 = new String[list.size()];
		for(int i = 0; i < list.size(); i++) {
			myData2[i] = list.get(i);
		}

        int invalidLineCnt = 0;
        int validLineCnt = 8;
        test_Interface(myOptions, myData2, invalidLineCnt, validLineCnt );

        // query the database
        VoltTable table = client.callProcedure("@AdHoc", "SELECT * FROM BLAH ORDER BY CLM_INTEGER;").getResults()[0];
        String[] strings = {"first复杂的漢字查询内容","second","third",null,"abcdeg","sixth","seventh","first"};
        int pos = 0;
        while(table.advanceRow()) {
        	int i = table.getActiveRowIndex();
        	VoltTableRow row = table.fetchRow(i);
        	//System.out.println(row.getString(4) + "   ");
        	assertEquals(row.getString(4),strings[pos]);
        	pos++;
        }
        try {
        	File temp = new File(FILENAME);
        	if(temp.exists()) temp.delete();
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }

    @Test
    public void testMultiByteCharacterGB1312() throws Exception
    {
        // ENG-12324: csvloader --header doesn't work if header has spaces. This is essentially the same test as testHeaderColumnNumNotSame, but with some
        // strategically placed whitespace in the header.

    	createCSVFile("gb2312");

        String []myOptions = {
                "-f" + path_csv,
                "--characterSet=gb2312",
                "--reportdir=" + reportDir,
                "--maxerrors=50",
                "--user=",
                "--password=",
                "--port=",
                "--separator=,",
                "--quotechar=\"",
                "--escape=\\",
                "--limitrows=100",
                "--header",
                "BlAh"
        };
        String FILENAME = "gb2312_encoded_text.csv";
        List<String> list = new ArrayList<>();

		//read from the file
		BufferedReader br = null;
		FileReader fr = null;
		try {
			fr = new FileReader(FILENAME);
			br = new BufferedReader(fr);
			String sCurrentLine;
			br = new BufferedReader(new InputStreamReader(new FileInputStream(FILENAME), Charset.forName("GBK")));
			while ((sCurrentLine = br.readLine()) != null) {

				list.add(sCurrentLine);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

		String[] myData2 = new String[list.size()];
		for(int i = 0; i < list.size(); i++) {
			myData2[i] = list.get(i);
		}

        int invalidLineCnt = 0;
        int validLineCnt = 8;
        test_Interface(myOptions, myData2, invalidLineCnt, validLineCnt );

        // query the database
        VoltTable table = client.callProcedure("@AdHoc", "SELECT * FROM BLAH ORDER BY CLM_INTEGER;").getResults()[0];
        String[] strings = {"first复杂的汉字查询内容","second","third",null,"abcdeg","sixth","seventh","first"};
        int pos = 0;
        while(table.advanceRow()) {
        	int i = table.getActiveRowIndex();
        	if(i==0) {
        		assertFalse(table.fetchRow(i).getString(4).equals(strings[pos]));
        		pos++;continue;
        	}
        	VoltTableRow row = table.fetchRow(i);
        	//System.out.println(row.getString(4) + "   ");
        	assertEquals(row.getString(4),strings[pos]);
        	pos++;
        }
        try {
        	File temp = new File(FILENAME);
        	if(temp.exists()) temp.delete();
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }

    private void createCSVFile(String encoding) {
    	String FILENAME = encoding+"_encoded_text.csv";
		BufferedWriter bw = null;
		FileWriter fw = null;


		//String filePath = "WTF.csv";

		String currentTime = String.valueOf(System.currentTimeMillis());

        String []myData = {
                "  clm_integer, clm_tinyint,clm_smallint,  clm_bigint  ,clm_string,clm_decimal,clm_float  , clm_timestamp,clm_point ,clm_geography",
                "1 ,1,1,11111111,"+"first复杂的漢字查询内容"+",1.10,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 1 0, 0 1, 0 0))\"",
                "2,2,2,222222,second,3.30,NULL,"+currentTime+",POINT(2 2),\"POLYGON((0 0, 2 0, 0 2, 0 0))\"",
                "3,3,3,333333, third ,NULL, 3.33,"+currentTime+",POINT(3 3),\"POLYGON((0 0, 3 0, 0 3, 0 0))\"",
                "4,4,4,444444, NULL ,4.40 ,4.44,"+currentTime+",POINT(4 4),\"POLYGON((0 0, 4 0, 0 4, 0 0))\"",
                "5,5,5,5555555,  \"abcde\"g, 5.50, 5.55,"+currentTime+",POINT(5 5),\"POLYGON((0 0, 5 0, 0 5, 0 0))\"",
                "6,6,NULL,666666, sixth, 6.60, 6.66,"+currentTime+",POINT(6 6),\"POLYGON((0 0, 6 0, 0 6, 0 0))\"",
                "7,NULL,7,7777777, seventh, 7.70, 7.77,"+currentTime+",POINT(7 7),\"POLYGON((0 0, 7 0, 0 7, 0 0))\"",
                "11, 1 ,1,\"1,000\",first,1.10,1.11,"+currentTime+",POINT(1 1),\"POLYGON((0 0, 8 0, 0 8, 0 0))\"",
        };

		try {
			if(encoding.equals("utf8") || encoding.equals("utf-8")) {
				bw = new BufferedWriter
						(new OutputStreamWriter(new FileOutputStream(FILENAME), Charset.forName("UTF-8")));
				for(String line: myData) {
					bw.write(line+"\n");
				}
			} else if(encoding.equals("gbk") || encoding.equals("GBK")) {
				FileOutputStream fos = new FileOutputStream(FILENAME);
				OutputStreamWriter osw = new OutputStreamWriter(fos, "gbk");
				bw = new BufferedWriter(osw);

				for(String str: myData) {
					//System.out.println(str);
					bw.write(str + "\n");
				}
				bw.flush();
				osw.flush();
				fos.flush();
			} else if(encoding.equals("gb2312") || encoding.equals("GB2312")) {
				FileOutputStream fos = new FileOutputStream(FILENAME);
				OutputStreamWriter osw = new OutputStreamWriter(fos, "gb2312");
				bw = new BufferedWriter(osw);

				for(String str: myData) {
					//System.out.println(str);
					bw.write(str + "\n");
				}
				bw.flush();
				osw.flush();
				fos.flush();
			}
			//System.out.println("Done Creating the file");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (bw != null)
					bw.close();
				if (fw != null)
					fw.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
    }

}
