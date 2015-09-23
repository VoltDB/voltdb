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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Vs6BuildCommand extends VisualStudioBuildCommand {

	public Vs6BuildCommand(GeneratorRule generator, CacheVariables vars) {
		this(generator, vars, new WorkSpaceLocator());
	}

	Vs6BuildCommand(GeneratorRule generator, CacheVariables vars, WorkSpaceLocator locator) {
		super(generator, vars, locator,
				createWorkspaceExtentions());
	}

	@Override
	protected List<String> buildCommand() {
		List<String> ret = new ArrayList<String>();
		String target = generator.getTarget();
		if (target == null) {
			target = "ALL";
		}

		ret.add(makeCommand);
		ret.add(workspace(workspaceExtentions.get(cmakeGenerator)));
		ret.add("/MAKE");
		ret.add(target + " - "
				+ defaultBuildType(generator.getBuildtype()).toString());
		ret.addAll(generator.getBuildargsAsList());
		
		return ret;
	}

	private static Map<String, String> createWorkspaceExtentions() {
		Map<String, String> map = new HashMap<String, String>();
		map.put("Visual Studio 6", "dsw");

		return Collections.unmodifiableMap(map);
	}
}
