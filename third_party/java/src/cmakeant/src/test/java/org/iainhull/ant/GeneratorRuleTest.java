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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;


public class GeneratorRuleTest {

	@Before
	public void setUp() throws Exception {
	}
	
	@Test
	public void testDefault() {
		GeneratorRule rule = new GeneratorRule(new CmakeBuilder());
		assertTrue(rule.isDefault());
		
		rule.setPlatform("Some platform");
		assertFalse(rule.isDefault());
	}

	@Test
	public void testMatches() {
		assertMatch("linux", "linux", true);
		assertMatch("linux", "linux", true);

		assertMatch("win", "Windows XP", true);
		assertMatch("Windows", "Windows XP", true);
		assertMatch("linux", "Windows XP", false);

		assertMatch("win", "Windows 2003 Server", true);
		assertMatch("Windows", "Windows 2003 Server", true);

	}

	private void assertMatch(String rulePlatform, String sysPlatform, boolean match) {
		GeneratorRule rule = new GeneratorRule(new CmakeBuilder());
		rule.setName("Test rule " + rulePlatform);
		rule.setPlatform(rulePlatform);
		assertTrue(rule.matches(sysPlatform) == match);
	}

}
