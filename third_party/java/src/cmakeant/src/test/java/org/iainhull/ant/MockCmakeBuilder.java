/* cmakeant - copyright Iain Hull.
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iainhull.ant;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * A mock CmakeBuilder that uses CmakeBuilder to orchestrate the other
 * components: Variables, Params and GeneratorRules; then asserts the commands
 * that it would run are correct.
 * 
 * This is configured like the regular CmakeBuilder with the added of calls to
 * setAsserts, setExpectedSourceDir and setExpectedBinaryDir.
 * 
 * @author iain
 */
public class MockCmakeBuilder extends CmakeBuilder {
	public static final String GENERATOR = "generator";
	public static final String BUILD_TOOL = "buildtool";

	private AssertExecute[] asserts = {};
	private int index = 0;
	private CacheVariables cacheVariables = new CacheVariables();
	private File expectedSourceDir;
	private File expectedBinaryDir;

	public MockCmakeBuilder() {
		addCacheVariables(new Variable("CMAKE_BUILD_TOOL",
				Variable.STRING_TYPE, BUILD_TOOL), new Variable(
				"CMAKE_GENERATOR", Variable.STRING_TYPE, GENERATOR));
	}

	public void setAsserts(AssertExecute... asserts) {
		this.asserts = asserts;
		index = 0;
	}

	@Override
	int doExecute(List<String> commandLine, File workingDirectory)
			throws IOException {
		assertTrue(index < asserts.length);
		int ret = asserts[index].assertCommand(commandLine, workingDirectory);
		index++;
		return ret;
	}

	void testPaths() {
	}

	public void addCacheVariables(Variable... variables) {
		for (Variable v : variables) {
			cacheVariables.addVariable(v);
		}
	}

	public void setExpectedSourceDir(File sourceDir) {
		expectedSourceDir = sourceDir;
	}

	public void setExpectedBinaryDir(File binaryDir) {
		expectedBinaryDir = binaryDir;
	}

	@Override
	protected void testSourceDir(File sourceDir) {
		if (expectedSourceDir != null) {
			assertEquals(expectedSourceDir, sourceDir);
		} else {
			super.testSourceDir(sourceDir);
		}
	}

	@Override
	protected void testBinaryDir(File binaryDir) {
		if (expectedSourceDir != null) {
			assertEquals(expectedBinaryDir, binaryDir);
		} else {
			super.testSourceDir(binaryDir);
		}
	}

	@Override
	CacheVariables readCacheVariables(File binaryDir) {
		return cacheVariables;
	}
}
