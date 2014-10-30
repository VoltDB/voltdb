/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestMaxInSuite extends RegressionSuite {

	static final Class<?>[] PROCEDURES = {};

	public TestMaxInSuite(String name) {
		super(name);
	}

	public void testMaxIn() throws Exception {
		final Client client = this.getClient();

		ClientResponse resp = null;
		for (int i = 0; i < 10; i++) {
			resp = client.callProcedure("P1.insert", i, i);
			assertEquals(ClientResponse.SUCCESS, resp.getStatus());
		}

		StringBuilder stringBuilder = new StringBuilder(
				"select * from p1 where a2 in(");
		for (int i = 0; i < 6000; i++) {
			stringBuilder.append(i);
			if (i != 5999) {
				stringBuilder.append(",");
			}
		}
		stringBuilder.append(");");
		resp = client.callProcedure("@AdHoc", stringBuilder.toString());
		assertEquals(ClientResponse.SUCCESS, resp.getStatus());

		assertEquals(1, resp.getResults().length);
		VoltTable results = resp.getResults()[0];

		System.out.println(results.getRowCount());
		System.out.println(results.getColumnCount());
		System.out.println(results.fetchRow(0).getLong(0));

		assertEquals(10, results.getRowCount());
		assertEquals(2, results.getColumnCount());
		assertEquals(0, results.fetchRow(0).getLong(0));
	}

	static public junit.framework.Test suite() {
		VoltServerConfig config = null;
		final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
				TestMaxInSuite.class);

		final VoltProjectBuilder project = new VoltProjectBuilder();

		project.addStmtProcedure("CountP1", "select count(*) from p1;");
		// project.addStmtProcedure("CountR1", "select count(*) from r1;");//if
		// r1 doesn't exist , exception

		try {
			// a table that should generate procedures
			// use column names such that lexical order != column order.
			project.addLiteralSchema("CREATE TABLE p1(b1 INTEGER NOT NULL, a2 INTEGER NOT NULL, PRIMARY KEY (b1));");
			project.addPartitionInfo("p1", "b1");

		} catch (IOException error) {
			fail(error.getMessage());
		}

		// JNI
		config = new LocalCluster("testMax-onesite.jar", 1, 1, 0,
				BackendTarget.NATIVE_EE_JNI);
		boolean t1 = config.compile(project);
		assertTrue(t1);
		builder.addServerConfig(config);

		// CLUSTER
		config = new LocalCluster("testMax-cluster.jar", 2, 3, 1,
				BackendTarget.NATIVE_EE_JNI);
		boolean t2 = config.compile(project);
		assertTrue(t2);
		builder.addServerConfig(config);

		return builder;
	}
}
