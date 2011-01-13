/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.export;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map.Entry;

import org.voltdb.benchmark.ClientMain;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.utils.Pair;

public class ExportTester extends ClientMain {

    private Client client;
    private int m_idA;
    private int m_idB;
    private int m_countInvalid;
    private int clientNum;
    private int valueOffset = 10;

    // map from client id num to table to pair of sums for columns
    Hashtable<Integer, HashMap<Character, long[]>> expectedValues = null;
    final int verticaPort = 5433;
    final String dbName = "database";

    private enum Transaction {
        INSERT_A("Insert A"),
        INSERT_B("Insert B"),
        TEST_CONSTRAINT("Violated constraint");
        private Transaction(String displayName) {this.displayName = displayName; }
        public final String displayName;
    }

    public Class<?>[] getProcedures() {
        Class <?> procs[];
        procs = new Class<?>[] { InsertA.class, InsertB.class, InsertCD.class };
        return procs;
    }

    public Class<?>[] getSupplementalClasses() {
        Class<?> classes[];
        classes = new Class<?>[] {};
        return classes;
    }

    public String getDDLFilename() {
        return "ExportTester-ddl.sql";
    }

    public String getJARFilename() {
        return "ExportTester.jar";
    }

    public int getTotalTransactionTypes() {
        return Transaction.values().length;
    }

    public String getTransactionDescription(int txnType) {
        return Transaction.values()[txnType].displayName;
    }

    public boolean setupForBenchmark(Hashtable<String, String> args) {
        expectedValues = new Hashtable<Integer, HashMap<Character, long[]>>();
        return true;
    }

    /**
     * Make sure the data structures for keeping track of expected values
     * is all set for this client. If it is, do nothing. If not, create the
     * data structure.
     *
     * @param clientNum The id number for the client to setup.
     */
    void ensureSetupForClient(int clientNum) {
        HashMap<Character, long[]> map = expectedValues.get(clientNum);
        if (map == null) {
            map = new HashMap<Character, long[]>();
            for (char table = 'A'; table <= 'D'; table++) {
                long[] colVals = new long[2];
                colVals[0] = colVals[1] = 0;
                map.put(table, colVals);
            }
            expectedValues.put(clientNum, map);
        }
    }

    /**
     * Query Vertica and sum the columns of a table/client combo.
     *
     * @param stmt SQL Statement instance which connects to VerticaDB.
     * @param clientNum The id number of the client doing the query.
     * @param table The name of the table to query.
     * @return Two longs that represent the queried values.
     */
    long[] sumTableForClient(Statement stmt, int clientNum, char table) {
        long[] retval = new long[2];
        String sql = "select sum(" + table + "_ID) as X, " +
                "sum(" + table + "_VAL) as Y " +
                "from " + table + " " +
                "where " + table + "_CLIENT = " + String.valueOf(clientNum) + ";";
        try {
            ResultSet results = stmt.executeQuery(sql);
            if (!results.next()) return null;
            retval[0] = results.getLong("X");
            retval[1] = results.getLong("Y");
        } catch (SQLException e) {
            System.out.println("Error: " + e.getMessage());
            return null;
        }

        return retval;
    }

    /**
     * Check that the query results and the expected results are right for a
     * specific table and client.
     *
     * @param table The name of the table to check.
     * @param clientNum The id number of the client who inserted the data.
     * @param tableSums The queried sums of values in Vertica.
     * @param expectedIDSum The expected sum of the id column in Vertica.
     * @param expectedValueSum The expected sum of the val column in Vertica.
     * @return True if all values match, false otherwise.
     */
    boolean checkValues(char table, int clientNum, long[] tableSums, long[] expectedSums) {
        if (tableSums[0] > expectedSums[0]) {
            System.err.printf("Table %c id column for client %d sums to too large a value.\n", table, clientNum);
            System.err.printf("  Expected %d but query returned %d\n", expectedSums[0], tableSums[0]);
            return false;
        }
        if (tableSums[0] < expectedSums[0]) {
            System.err.printf("Table %c id column for client %d sums to too small a value.\n", table, clientNum);
            System.err.printf("  Expected %d but query returned %d\n", expectedSums[0], tableSums[0]);
            return false;
        }
        if (tableSums[1] > expectedSums[1]) {
            System.err.printf("Table %c val column for client %d sums to too large a value.\n", table, clientNum);
            System.err.printf("  Expected %d but query returned %d\n", expectedSums[1], tableSums[1]);
            return false;
        }
        if (tableSums[1] < expectedSums[1]) {
            System.err.printf("Table %c val column for client %d sums to too small a value.\n", table, clientNum);
            System.err.printf("  Expected %d but query returned %d\n", expectedSums[1], tableSums[1]);
            return false;
        }

        System.out.printf("Table %c values for client %d check out.\n", table, clientNum);
        return true;
    }

    public void finalizeBenchmark(Hashtable<String, String> args) {
        // get the vertica host and return if not provided
        String host = args.get("elhost");
        if (host == null) return;

        System.out.println("Verifying results at Vertica site:");

        // sleep for 10s to make sure all data has a chance to get to vertica
        System.out.println("Waiting 10 seconds for final \"tick\"s...");
        System.out.flush();
        try {
            Thread.sleep(6000);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }

        Connection vdbConn;
        Statement stmt;

        // build the JDBC connection string for vertica
        String jdbcConnectionString = "jdbc:vertica://" + host + ":" +
            String.valueOf(verticaPort) + "/" + dbName;

        try {
            // try to load the vertica JDBC driver
            Class.forName("com.vertica.Driver");
            // try to connect to vertica db
            vdbConn = DriverManager.getConnection(jdbcConnectionString, "dbadmin", "");
            // create extra JDBC junk
            stmt = vdbConn.createStatement();
        }
        catch (Exception e) {
            return;
        }

        // assume success until proven otherwise
        boolean success = true;

        // iterate over all client's counts for calls to executeTransaction(..)
        for (Entry<Integer, HashMap<Character, long[]>> entry : expectedValues.entrySet()) {
            int clientNum = entry.getKey();
            HashMap<Character, long[]> values = entry.getValue();

            // query vertica
            long[] valuesA = sumTableForClient(stmt, clientNum, 'A');
            long[] valuesB = sumTableForClient(stmt, clientNum, 'B');
            long[] valuesC = sumTableForClient(stmt, clientNum, 'C');
            long[] valuesD = sumTableForClient(stmt, clientNum, 'D');

            // check with expected
            long[] expectedValuesA = values.get('A');
            long[] expectedValuesB = values.get('B');
            long[] expectedValuesC = values.get('C');
            long[] expectedValuesD = values.get('D');

            // verify the numbers: if any fail, this method fails
            success &= checkValues('A', clientNum, valuesA, expectedValuesA);
            success &= checkValues('B', clientNum, valuesB, expectedValuesB);
            success &= checkValues('C', clientNum, valuesC, expectedValuesC);
            success &= checkValues('D', clientNum, valuesD, expectedValuesD);
        }

        if (success)
            System.out.println("*** Verified ***");

        try {
            stmt.close();
            vdbConn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public int executeTransaction() {
        int curr_txn_type = 0;
        ensureSetupForClient(clientNum);

        // Use table A to test a table that fills w/i upload threshold
        //  - generate more than 2MB of content per 4s interval to table A
        // Use table B to test a table that fills slower than upload thresh.
        //  - generate less than 2MB of content per 4s interval to table B.
        // Generate uniqueness constraint violations B ever 1k transactions.

        try {
            // every 1000 transactions insert into B
            if ((totalTxns() % 1000) == 0) {
                m_idB += 1;
                curr_txn_type = 1;
                try {
                    client.callProcedure("InsertB", clientNum, m_idB, (m_idB + valueOffset));
                    expectedValues.get(clientNum).get('B')[0] += m_idB;
                    expectedValues.get(clientNum).get('B')[1] += m_idB + valueOffset;
                }
                catch (ProcCallException e) {
                    throw new RuntimeException(e);
                }

                // violate a uniqueness constraint by
                // inserting the previous unique ids. (client num is unique to
                // current thread and key is clientnum, id.)
                curr_txn_type = 2;
                m_countInvalid += 1;
                try {
                    client.callProcedure("InsertB", clientNum, m_idB, (m_idB + valueOffset));
                    throw new RuntimeException("Insert was supposed to fail, but did not.");
                }
                catch (ProcCallException e) {}
            }
            // otherwise, execute transaction A (the frequent txn).
            // and transaction CD - the frequent cross table txn.
            // within CD, C inserts each transaction, D every 10,000 transactions.
            m_idA += 1;
            curr_txn_type = 0;
            try {
                client.callProcedure("InsertA", clientNum, m_idA, (m_idA + valueOffset));
                expectedValues.get(clientNum).get('A')[0] += m_idA;
                expectedValues.get(clientNum).get('A')[1] += m_idA + valueOffset;

                client.callProcedure("InsertCD", clientNum, m_idA, (m_idA + valueOffset));
                expectedValues.get(clientNum).get('C')[0] += m_idA;
                expectedValues.get(clientNum).get('C')[1] += m_idA + valueOffset;
                if ((m_idA % 10000) == 0) {
                    expectedValues.get(clientNum).get('D')[0] += m_idA;
                    expectedValues.get(clientNum).get('D')[1] += m_idA + valueOffset;
                }
            }
            catch (ProcCallException e) {
                throw new RuntimeException(e);
            }
        }
        catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }

        return curr_txn_type;
    }

    private int totalTxns() {
        return m_idA + m_idB + m_countInvalid;
    }

    private void initialize(int clientNum, Client client) {
        this.clientNum = clientNum;
        this.client = client;
        this.m_idA = 0;
        this.m_idB = 0;
        this.m_countInvalid = 1;  // keep mod happy
    }

    public ExportTester(int clientNum, Client client) {
        super(new String[] {});
        initialize(clientNum, client);
    }

    public ExportTester() {
        super(new String[] {});
        initialize(0, null);
    }

    public ArrayList<Pair<String, String>> getPartitionInfo() {
        ArrayList<Pair<String,String>> partitionInfo = new ArrayList<Pair<String,String>>();
        partitionInfo.add(Pair.of("A", "A_CLIENT"));
        partitionInfo.add(Pair.of("B", "B_CLIENT"));
        partitionInfo.add(Pair.of("C", "C_CLIENT"));
        return partitionInfo;
    }

    @Override
    protected String[] getTransactionDisplayNames() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected void runLoop() {
        // TODO Auto-generated method stub

    }

    @Override
    protected String getApplicationName() {
        return "ExportTester";
    }

    @Override
    protected String getSubApplicationName() {
        return "";
    }
}
