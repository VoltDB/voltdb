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

/**
 * Describes a CMake variable, its name, type and value.
 * 
 * This is used to read the cmake cache and to set variables at cmake time.
 * 
 * @author iain.hull
 */
public class Variable {
    
    public static final String STRING_TYPE = "STRING";
    public static final String FILEPATH_TYPE = "FILEPATH";
    public static final String PATH_TYPE = "PATH";
    public static final String BOOL_TYPE = "BOOL";
    public static final String STATIC_TYPE = "STATIC";
    public static final String INTERNAL_TYPE = "INTERNAL";
    
    public static final String CMAKE_BUILD_TOOL = "CMAKE_BUILD_TOOL";
    public static final String CMAKE_MAKE_PROGRAM = "CMAKE_MAKE_PROGRAM";
    public static final String CMAKE_GENERATOR = "CMAKE_GENERATOR";
    public static final String CMAKE_MAJOR_VERSION = "CMAKE_MAJOR_VERSION";
    public static final String CMAKE_MINOR_VERSION = "CMAKE_MINOR_VERSION";
    public static final String CMAKE_CACHE_MAJOR_VERSION = "CMAKE_CACHE_MAJOR_VERSION";
    public static final String CMAKE_CACHE_MINOR_VERSION = "CMAKE_CACHE_MINOR_VERSION";
    public static final String CMAKE_BUILD_TYPE = "CMAKE_BUILD_TYPE";
    
    private String name;
    private String type = STRING_TYPE;
    private String value;
    
    public Variable() {
    }
    
    public Variable(String name, String type, String value) {
        this.name = name;
        this.type = type;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    /**
     * Returns the variable value as an integer.
     * 
     * @return the variable value as an integer.
     * 
     * @throws NumberFormatException if the value is not a valid integer.
     */
    public int getIntValue() {
        return Integer.parseInt(value);
    }

    public void setValue(String value) {
        this.value = value;
    }
    
    @Override
    public String toString() {
        return name + ":" + type + "=" + value;
    }

    @Override
    public boolean equals(Object rhs) {
        if (rhs instanceof Variable) {
            return equals((Variable) rhs);
        }
        return false;
    }
    
    public boolean equals(Variable rhs) {
        return equals(name, rhs.name) 
            && equals(type, rhs.type) 
            && equals(value, rhs.value);
    }
    
    private boolean equals(String lhs, String rhs) {
        if (lhs == null) {
            return rhs == null;
        }
        return lhs.equals(rhs);
    }
}
