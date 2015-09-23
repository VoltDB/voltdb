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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VisualStudioBuildCommand extends BuildCommand {
	protected final Map<String, String> workspaceExtentions;
	protected final WorkSpaceLocator locator;
	
	public VisualStudioBuildCommand(GeneratorRule generator, CacheVariables vars) {
		this(generator, vars, new WorkSpaceLocator(), createWorkspaceExtentions());
	}

	VisualStudioBuildCommand(GeneratorRule generator, CacheVariables vars, WorkSpaceLocator locator) {
		this(generator, vars, locator, createWorkspaceExtentions());
	}
	
	protected VisualStudioBuildCommand(GeneratorRule generator, CacheVariables vars, WorkSpaceLocator locator, Map<String, String> workspaceExtentions) {
		super(generator, vars);
		this.locator = locator;
		this.workspaceExtentions = workspaceExtentions;
	}

	
	@Override
	protected List<String> buildCommand() {
		List<String> ret = new ArrayList<String>();
		ret.add(makeCommand);
		ret.add(workspace(workspaceExtentions.get(cmakeGenerator))); 
		
		if (generator.isCleanfirst()) {
			ret.add("/Rebuild");
		} else {
			ret.add("/Build");
		}
		
		ret.add(defaultBuildType(generator.getBuildtype()));
		
		
		if (generator.getTarget() != null) {
			ret.add("/Project");
			ret.add(generator.getTarget());
		}
		
		ret.addAll(generator.getBuildargsAsList());
		return ret;
	}

	@Override
	protected boolean canBuild() {
		return workspaceExtentions.containsKey(cmakeGenerator);
	}
	
	@Override
	protected boolean canSkipCmakeStep() {
		return false;
	}
	
	protected String defaultBuildType(String buildType) {
		if (buildType != null) {
			return buildType;
		}
		return "Release";
	}
	
	private static Map<String, String> createWorkspaceExtentions() {
		Map<String, String> map = new HashMap<String, String>();
		map.put("Visual Studio 7", "sln");
		map.put("Visual Studio 7 .NET 2003", "sln");
		map.put("Visual Studio 8 2005", "sln");
		map.put("Visual Studio 8 2005 Win64", "sln");
		map.put("Visual Studio 9 2008", "sln");
		map.put("Visual Studio 9 2008 Win64", "sln");
	
		return Collections.unmodifiableMap(map);
	}
	
	protected String workspace(final String extension) {
		File binaryDir = generator.getBindir();
		return locator.findByExtension(binaryDir, extension);
	}
}
