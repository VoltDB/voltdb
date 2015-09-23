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

import java.io.File;

import org.junit.Before;
import org.junit.Test;


public class CmakeBuilderTest {

	private MockCmakeBuilder builder;
	
	
	@Before
	public void setUp() throws Exception {
		builder = new MockCmakeBuilder();
	}
	
	@Test
	public void testSimple() {
		File source = new File("source");
		File binary = new File("binary");
		
		builder.setAsserts(
			new AssertExecute.Command(
					binary, "cmake", source.toString()),
			new AssertExecute.Command(
					binary, MockCmakeBuilder.BUILD_TOOL));
		
		builder.setExpectedSourceDir(source);
		builder.setExpectedBinaryDir(binary);

		builder.setSrcdir(source);
		builder.setBindir(binary);
		
		builder.execute();
	}

	@Test
	public void testVariables() {
		File source = new File("source");
		File binary = new File("binary");
		
		Variable v1 = builder.createVariable();
		Variable v2 = builder.createVariable();
		
		v1.setName("One");
		v1.setValue("TheOne");
		
		v2.setName("Two");
		v2.setType(Variable.BOOL_TYPE);
		v2.setValue("ON");		
		
		builder.setAsserts(
				new AssertExecute.Command(
						binary, "cmake", "-D", v1.toString(), "-D", v2.toString(), source.toString()),
			new AssertExecute.Null() );
		
		builder.setExpectedSourceDir(source);
		builder.setExpectedBinaryDir(binary);

		builder.setSrcdir(source);
		builder.setBindir(binary);

		builder.execute();
	}
	
	@Test
	public void testGeneratorVariables() {
		File source = new File("source");
		File binary = new File("binary");
		
		Variable v1 = builder.createVariable();
		Variable v2 = builder.createVariable();
		
		v1.setName("One");
		v1.setValue("TheOne");
		
		v2.setName("Two");
		v2.setType(Variable.BOOL_TYPE);
		v2.setValue("ON");		
		
		GeneratorRule g = builder.createGenerator();
		g.setName("test generator");
		Variable v3 = builder.createVariable();
		
		v3.setName("One");
		v3.setValue("TheGeneratorOne");
		
		
		builder.setAsserts(
			new AssertExecute.Command(
				binary, "cmake", "-G", "test generator", "-D", v3.toString(), 
				"-D", v2.toString(), source.toString() ),
			new AssertExecute.Null() );
		
		builder.setExpectedSourceDir(source);
		builder.setExpectedBinaryDir(binary);

		builder.setSrcdir(source);
		builder.setBindir(binary);

		builder.execute();
	}
	
	@Test
	
	public void testGeneratorBindir() {
		File source = new File("source");
		File binary = new File("binary");
		File debug = new File("binary/debug");
		
		GeneratorRule g = builder.createGenerator();
		g.setName("test generator");
		g.setBuildtype(BuildType.DEBUG);
		g.setBindir(debug);
		
		builder.setAsserts(
			new AssertExecute.Command(
				debug, "cmake", "-G", "test generator", source.toString() ),
			new AssertExecute.Null() );
		
		builder.setExpectedSourceDir(source);
		builder.setExpectedBinaryDir(debug);

		builder.setSrcdir(source);
		builder.setBindir(binary);

		builder.execute();
	}

	@Test
	public void testCmakeonly() {
		File source = new File("source");
		File binary = new File("binary");
		
		GeneratorRule g = builder.createGenerator();
		g.setName("test generator");
		
		builder.setAsserts(
			new AssertExecute.Command(
				binary, "cmake", "-G", "test generator", source.toString() ));
		
		builder.setExpectedSourceDir(source);
		builder.setExpectedBinaryDir(binary);

		builder.setSrcdir(source);
		builder.setBindir(binary);
		builder.setCmakeonly(true);

		builder.execute();
	}
	
	@Test
	public void testExtraBuildArgs() {
		File source = new File("source");
		File binary = new File("binary");
		
		GeneratorRule g = builder.createGenerator();
		g.setName("test generator");
		g.setBuildargs("-j8 -k");
		
		builder.setAsserts(
			new AssertExecute.Command(
					binary, "cmake", "-G", "test generator", source.toString()),
			new AssertExecute.Command(
					binary, MockCmakeBuilder.BUILD_TOOL, "-j8", "-k"));
		
		builder.setExpectedSourceDir(source);
		builder.setExpectedBinaryDir(binary);

		builder.setSrcdir(source);
		builder.setBindir(binary);
		
		builder.execute();
		
	}
	
	@Test
	public void testCMakeBuildCommand() {
		File source = new File("source");
		File binary = new File("binary");
		
		builder.addCacheVariables(
				new Variable(Variable.CMAKE_MAJOR_VERSION, Variable.STRING_TYPE, "2"), 
				new Variable(Variable.CMAKE_MINOR_VERSION, Variable.STRING_TYPE, "8"));

		GeneratorRule g = builder.createGenerator();
		g.setName("test generator");
		g.setBuildargs("-j8 -k");
		
		builder.setAsserts(
			new AssertExecute.Command(
					binary, "cmake", "-G", "test generator", source.toString()),
			new AssertExecute.Command(
					binary, "cmake", "--build", binary.toString(), "--", "-j8", "-k"));
		
		builder.setExpectedSourceDir(source);
		builder.setExpectedBinaryDir(binary);

		builder.setSrcdir(source);
		builder.setBindir(binary);
		
		builder.execute();
		
	}
	
	/*
	 * --build
	 * --target
	 * --config
	 * --clean-first
	 * --use-stderr
	 * --
	 */
	
	@Test
	public void testCMakeBuildCommandWithTargetAndConfig() {
		File source = new File("source");
		File binary = new File("binary");
		
		builder.addCacheVariables(
				new Variable(Variable.CMAKE_MAJOR_VERSION, Variable.STRING_TYPE, "2"), 
				new Variable(Variable.CMAKE_MINOR_VERSION, Variable.STRING_TYPE, "8"));

		builder.setTarget("SomeTarget");
		builder.setBuildtype(BuildType.REL_WITH_DEB_INFO);
		GeneratorRule g = builder.createGenerator();
		g.setName("test generator");
		g.setBuildargs("-j8 -k");
		
		builder.setAsserts(
			new AssertExecute.Command(
					binary, "cmake", "-G", "test generator","-D", 
					Variable.CMAKE_BUILD_TYPE + ":STRING=" + BuildType.REL_WITH_DEB_INFO, 
					source.toString()),
			new AssertExecute.Command(
					binary, "cmake", "--build", binary.toString(), 
					"--target", "SomeTarget", 
					"--config", BuildType.REL_WITH_DEB_INFO,
					"--", "-j8", "-k"));
		
		builder.setExpectedSourceDir(source);
		builder.setExpectedBinaryDir(binary);

		builder.setSrcdir(source);
		builder.setBindir(binary);
		
		builder.execute();
		
	}

}
