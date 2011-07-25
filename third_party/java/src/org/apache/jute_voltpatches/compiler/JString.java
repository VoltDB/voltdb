/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jute_voltpatches.compiler;

/**
 *
 */
public class JString extends JCompType {

    /** Creates a new instance of JString */
    public JString() {
        super("char *", " ::std::string", "String", "String", "String");
    }

    @Override
    public String getSignature() {
        return "s";
    }

    @Override
    public String genJavaReadWrapper(String fname, String tag, boolean decl) {
        String ret = "";
        if (decl) {
            ret = "    String " + fname + ";\n";
        }
        return ret + "        " + fname + "=a_.readString(\"" + tag + "\");\n";
    }

    @Override
    public String genJavaWriteWrapper(String fname, String tag) {
        return "        a_.writeString(" + fname + ",\"" + tag + "\");\n";
    }
}
