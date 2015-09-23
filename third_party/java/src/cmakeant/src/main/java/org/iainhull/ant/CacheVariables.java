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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;


public class CacheVariables {
	private static final String EQUALS = "=";
	private static final String COLON = ":";
	
	private Map<String, Variable> vars = new HashMap<String, Variable>();

	public CacheVariables(File cache) throws IOException {
		this(new FileReader(cache));
	}
	
	public CacheVariables(Reader reader) throws IOException {
		BufferedReader bufferedReader = new BufferedReader(reader);
		while(bufferedReader.ready()) {
			readLine(bufferedReader.readLine());
		}
	}

	public CacheVariables() {
	}

	private void readLine(String line) {
		if (!isLineEmpty(line) && !isLineComment(line)) {
			addVariable(line);
		}
	}
	
	public boolean hasVariable(String name) {
		return vars.containsKey(name);
	}

	public Variable getVariable(String name) {
		return vars.get(name);
	}
	
	public int getIntValue(String name, int defValue) {
		try {
			if (hasVariable(name)) {
				return vars.get(name).getIntValue();
			} else {
				return defValue;
			}
		} catch(NumberFormatException e) {
			return defValue;
		}
	}

	private void addVariable(String line) {
		int colonPos = line.indexOf(COLON);
		int equalPos = line.indexOf(EQUALS, colonPos);
		
		if (colonPos != -1 && equalPos != -1) {
			String name = line.substring(0, colonPos);
			String type = line.substring(colonPos + 1, equalPos);
			String value = line.substring(equalPos + 1);
			
			addVariable(new Variable(name, type, value));
		}
	}
	
	private boolean isLineComment(String line) {
		return line.startsWith("//") || line.startsWith("#");
	}

	private boolean isLineEmpty(String line) {
		return line == null || line.length() == 0;
	}

	public void addVariable(Variable v) {
		vars.put(v.getName(), v);	
	}
}
