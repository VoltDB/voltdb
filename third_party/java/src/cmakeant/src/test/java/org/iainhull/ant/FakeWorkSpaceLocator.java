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

public class FakeWorkSpaceLocator extends WorkSpaceLocator {
	private String workspace;

	public FakeWorkSpaceLocator(String workspace) {
		this.workspace = workspace;
	}
	
	@Override
	public String findByExtension(File dir, String extension) {
		if (workspace == null)
			return super.findByExtension(dir, extension);
		else
			return new File(dir, workspace).toString();
	}
}
