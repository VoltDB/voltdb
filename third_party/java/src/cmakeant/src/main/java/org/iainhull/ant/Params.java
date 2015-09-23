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
import java.util.Map;

/**
 * A simple set of fields describing a cmake build, these can be applied to
 * CMakeBuilder or a GeneratorRule.
 * 
 * @author iain
 */
public interface Params {

	/**
	 * Get the cmake binary directory, where the cmake generated makefiles/
	 * projects are written.
	 * 
	 * @return the binary directory
	 */
	public File getBindir();

	/**
	 * Set the cmake binary directory, where the cmake generated makefiles/
	 * projects are written.
	 * 
	 * @param binaryDir the binary directory
	 */
	public void setBindir(File binaryDir);
	
	/**
	 * Returns the value for the <tt>CMAKE_BUILD_TYPE</tt> variable or null if unset. 
	 * @return  the value for the <tt>CMAKE_BUILD_TYPE</tt> variable or null if unset. 
	 */
	public String getBuildtype();
	
	/**
	 * Set the value for the <tt>CMAKE_BUILD_TYPE</tt> variable or null to unset.
	 * 
	 * @param buildType this is usually Debug, Release, RelWithDebInfo, MinSizeRel.
	 */
	public void setBuildtype(String buildType);
	
	public String getTarget();
	public void setTarget(String target);
	
	public boolean isCleanfirst();
	public void setCleanfirst(boolean cleanfirst);
	public boolean isCleanfirstSet();
	
	public Variable createVariable();
	public Map<String, Variable> getVariables();
}
