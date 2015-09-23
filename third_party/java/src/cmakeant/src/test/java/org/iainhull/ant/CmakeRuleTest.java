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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;


public class CmakeRuleTest {

	private SimpleParams first;
	private SimpleParams second;
	private CompositeParams composite;

	@Before
	public void setUp() throws Exception {
		this.first = new SimpleParams();
		this.second = new SimpleParams();
		this.composite = new CompositeParams(first, second);
	}
	
	@Test
	public void testComposite() {
		File one = new File("one");
		File two = new File("two");
		
		assertNull(composite.getBindir());
		first.setBindir(one);
		assertEquals(one, composite.getBindir());
		second.setBindir(two);
		assertEquals(two, composite.getBindir());
		
		first.setBuildtype(BuildType.DEBUG);
		assertEquals(BuildType.DEBUG, composite.getBuildtype());
		second.setBuildtype(BuildType.RELEASE);
		assertEquals(BuildType.RELEASE, composite.getBuildtype());	
	}

	@Test
	public void testCompositeProperties() {
		Variable v1 = new Variable("a", Variable.STRING_TYPE, "a");
		Variable v2 = new Variable("b", Variable.STRING_TYPE, "b1");
		Variable v3 = new Variable("b", Variable.STRING_TYPE, "b2");
		Variable v4 = new Variable("c", Variable.STRING_TYPE, "c");
		
		Map<String, Variable> expected = new HashMap<String, Variable>();
		expected.put(v1.getName(), v1);
		expected.put(v2.getName(), v2);
		expected.put(v3.getName(), v3);
		expected.put(v4.getName(), v4);
		
		addVar(first, v1);
		addVar(first, v2);
		addVar(second, v3);
		addVar(second, v4);

		assertEquals(expected, composite.getVariables());
	}
	
	private void addVar(Params rule, Variable v) {
		Variable u = rule.createVariable();
		u.setName(v.getName());
		u.setType(v.getType());
		u.setValue(v.getValue());
	}
}
