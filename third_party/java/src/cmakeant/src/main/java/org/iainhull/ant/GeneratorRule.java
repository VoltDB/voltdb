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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A GeneratorRule specifies a CMake generator to use for a specific
 * platform.  It can also override other Cmake params if this generator
 * is selected.
 *   
 * @author iain.hull
 */
public class GeneratorRule implements Params {
	
	private String name;
	private String platform;
	private String buildArgs;
	private Params params;

	/**
	 * Create a new GeneratorRule.
	 */
	public GeneratorRule(CmakeBuilder builder) {
		this.params = new CompositeParams(builder, new SimpleParams());
	}
	
	/**
	 * Return the name of the GeneratorRule.
	 * 
	 * @return the name of the GeneratorRule.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Set the name of the GeneratorRule, this must a valid cmake generator
	 * name (see cmake -G parameter).
	 * 
	 * @return the name of the GeneratorRule.
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Return the platform this rule is enabled for.
	 * 
	 * @return the platform this rule is enabled for.
	 */
	public String getPlatform() {
		return platform;
	}

	/**
	 * Set the platform this rule is enabled for, this is a fuzzy match
	 * for the system platform returned by the java code 
	 * <code>System.getProperty("os.name")</code>.
	 * 
	 * @param platform  this rule is enabled for.
	 */
	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public boolean matches(String os) {
		if (platform == null)
			return true;

		String p = platform.toUpperCase();
		String o = os.toUpperCase();

		return p.indexOf(o) >= 0 || o.indexOf(p) >= 0;
	}

	public String toString() {
		return (isDefault() ? "<default>" : platform) + ": " + name;
	}

	public boolean isDefault() {
		return platform == null || platform.equals("");
	}

	public File getBindir() {
		return params.getBindir();
	}

	public String getBuildtype() {
		return params.getBuildtype();
	}

	public void setBindir(File binaryDir) {
		this.params.setBindir(binaryDir);
	}

	public void setBuildtype(String buildType) {
		this.params.setBuildtype(buildType);
	}
	
	public boolean isCleanfirst() {
		return this.params.isCleanfirst();
	}
	
	public void setCleanfirst(boolean cleanfirst) {
		this.params.setCleanfirst(cleanfirst);
	}

	public boolean isCleanfirstSet() {
		return this.params.isCleanfirstSet();
	}

	public Variable createVariable() {
		return params.createVariable();
	}	
	
	public Map<String, Variable> getVariables() {
		return params.getVariables();
	}

	public String getTarget() {
		return params.getTarget();
	}

	public void setTarget(String target) {
		params.setTarget(target);
	}

	public void setBuildargs(String buildArgs) {
		this.buildArgs = buildArgs;
	}
	
	public String getBuildargs() {
		return buildArgs;
	}

	public List<String> getBuildargsAsList() {		
		if (buildArgs != null) {
			return Arrays.<String>asList(buildArgs.split("[ \t]+"));			
		} else {
			return Collections.emptyList();			
		}
	}
}