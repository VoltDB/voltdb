/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.regressionsuites;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.TimestampType;

/**
 * Tests of SQL statements that use User-Defined Aggregate Functions (UDAF's).
 */
public class TestUserDefinedAggregates extends TestUserDefinedFunctions {
	
	public TestUserDefinedAggregates(String name) {
        super(name);
    }

    private void testFunction(String functionCall, Object expected, VoltType returnType,
            String[] columnNames, String[] columnValues, String tableName) 
    		throws IOException, ProcCallException {
    	// If table not specified, randomly decide which one to test
        if (tableName == null) {
            tableName = "R1";
            if (random.nextInt(100) < 50) {
                tableName = "P1";
            }
        }

        // Set the expected result of the SELECT query using the UDF
        Object[][] expectedTable = new Object[1][2];
        expectedTable[0][0] = 0;
        expectedTable[0][1] = expected;

        // INSERT one row into the table that we are using for testing
        String allColumnNames  = "ID";
        String allColumnValues = "0";
        if (columnNames != null && columnNames.length > 0) {
            allColumnNames = "ID, " + String.join(",", columnNames);
        }
        if (columnValues != null && columnValues.length > 0) {
            allColumnValues = "0, " + String.join(",", columnValues);
        }
        Client client = getClient();
        String insertStatement = "INSERT INTO "+tableName
                + " ("+allColumnNames+") VALUES" + " ("+allColumnValues+")";
        ClientResponse cr = client.callProcedure("@AdHoc", insertStatement);
        assertEquals(insertStatement+" failed", ClientResponse.SUCCESS, cr.getStatus());

        // Get the actual result of the SELECT query using the UDF
        String selectStatement = "SELECT ID, "+functionCall+" FROM "+tableName+" WHERE ID = 0";
        cr = client.callProcedure("@AdHoc", selectStatement);
        assertEquals(selectStatement+" failed", ClientResponse.SUCCESS, cr.getStatus());
        VoltTable vt = cr.getResults()[0];

        // Compare the expected to the actual result
        if (VoltType.FLOAT.equals(returnType)) {
            RegressionSuite.assertApproximateContentOfTable(expectedTable, vt, EPSILON);
        } else {
            RegressionSuite.assertContentOfTable(expectedTable, vt);
        }

        // Clean-up
        String truncateStatement = "TRUNCATE TABLE "+tableName;
        cr = client.callProcedure("@AdHoc", truncateStatement);
        assertEquals(truncateStatement+" (clean-up) failed", ClientResponse.SUCCESS, cr.getStatus());
    }

	private void testFunction(String functionCall, Object expected, VoltType returnType,
            String[] columnNames, String[] columnValues)
            throws IOException, ProcCallException {
        testFunction(functionCall, expected, returnType, columnNames, columnValues, null);
    }

	public void testUavg() throws IOException, ProcCallException {
		String[] columnNames = {"Number"};
		String[] columnValues = {"1", "2", "3", "4"};
		testFunction("Uavg(Number)", 2.5, VoltType.FLOAT, columnNames, columnValues);
	}

	public void testUmedian() throws IOException, ProcCallException {
		String[] columnNames = {"Number"};
		String[] columnValues = {"2", "4", "5", "10"};
		testFunction("Umedian(Number)", 4.5D, VoltType.FLOAT, columnNames, columnValues);
	}

	public void testUmode() throws IOException, ProcCallException {
		String[] columnNames = {"Number"};
		String[] columnValues = {"1", "3", "3", "3", "5", "5", "7"};
		testFunction("Umode(Number)", 3.0D, VoltType.FLOAT, columnValues, columnNames);
	}

}