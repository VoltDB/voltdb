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

//	static public Test suite() {
//		MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestUserDefinedAggregates.class);
//
//		// build up a project builder for the workload
//        VoltProjectBuilder project = new VoltProjectBuilder();
//        project.setUseDDLSchema(true);
//
//        byte[] createFunctionsDDL = null;
//        try {
//            createFunctionsDDL = Files.readAllBytes(Paths.get("tests/testfuncs/org/voltdb_testfuncs/UserDefinedTestAggregates/UserDefinedTestAggregates-DDL.sql"));
//        } catch (IOException e) {
//            fail(e.getMessage());
//        }
//	}

}