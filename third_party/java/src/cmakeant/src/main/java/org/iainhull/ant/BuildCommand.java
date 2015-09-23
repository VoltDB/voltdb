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

import java.util.List;

import org.apache.tools.ant.BuildException;

/**
 * Static Facade for working with the CMake generated build files.
 * 
 * This could probably be improved by separating the static methods to  
 * instance methods of a new class.
 * 
 * @author iain
 */
public abstract class BuildCommand {
    
    /**
     * Construct the build command line based on CMake output.
     * 
     * @param generator
     * @param makeCommand
     * @param cmakeGenerator
     * @return the build command line for the CMake output.
     */
    public static List<String> inferCommand(GeneratorRule generator, CacheVariables vars) {
        BuildCommand [] commands = createBuildCommands(generator, vars);
        
        for (BuildCommand command : commands) {
            if (command.canBuild()) {
                return command.buildCommand();
            }
        }
        
        throw new BuildException("Cannot construct build command for: " + generator.getName());
    }

    /**
     * Test if the CMake generated build files support skipping the Cmake Step
     * if the build files are already generated.
     *    
     * @param generator
     * @param makeCommand
     * @param cmakeGenerator
     * @return true if the CMake generated build files support skipping the 
     *      Cmake Step if the build files are already generated.
     */
    public static boolean canSkipCmakeStep(GeneratorRule generator,
            CacheVariables vars) {
        BuildCommand [] commands = createBuildCommands(generator, vars);
        
        for (BuildCommand command : commands) {
            if (command.canBuild()) {
                return command.canSkipCmakeStep();
            }
        }
        
        throw new BuildException("Cannot construct build command for: " + generator.getName());
    }
    
    private static BuildCommand[] createBuildCommands(GeneratorRule generator,
            CacheVariables vars) {
        
        if (CMakeBuildCommand.isSupported(vars)) {
            return new BuildCommand [] { new CMakeBuildCommand(generator, vars) };
        } else {    
            return new BuildCommand [] {
                    new VisualStudioBuildCommand(generator, vars),
                    new Vs6BuildCommand(generator, vars),
                    new MakeBuildCommand(generator, vars) };
        }
    }

    protected final GeneratorRule generator;
    protected final String makeCommand;
    protected final String cmakeGenerator;

    protected BuildCommand(GeneratorRule generator, CacheVariables vars) {
        this.generator = generator;
        if (vars.getVariable(Variable.CMAKE_MAKE_PROGRAM) != null) {
            this.makeCommand = vars.getVariable(Variable.CMAKE_MAKE_PROGRAM).getValue();
        } else if (vars.getVariable(Variable.CMAKE_BUILD_TOOL) != null) {
            this.makeCommand = vars.getVariable(Variable.CMAKE_BUILD_TOOL).getValue();
        } else {
            throw new BuildException("Cannot find make program for: " + generator.getName()
                + " (neither " + Variable.CMAKE_MAKE_PROGRAM
                + " or " + Variable.CMAKE_BUILD_TOOL +" are defined in CMakeCache.txt)");
        }

        if (vars.getVariable(Variable.CMAKE_GENERATOR) != null) {
           this.cmakeGenerator = vars.getVariable(Variable.CMAKE_GENERATOR).getValue();
        } else {
            throw new BuildException("Cannot find cmake generator for: " + generator.getName()
                + " (" + Variable.CMAKE_GENERATOR + " is not defined in CMakeCache.txt)");
        }
    }

    /**
     * Return the command line to execute the build for this BuildCommand
     * @return the command line to execute the build for this BuildCommand
     */
    protected abstract List<String> buildCommand();
    
    /**
     * Return true if this BuildCommand can build this CMake output.
     * @return true if this BuildCommand can build this CMake output.
     */
    protected abstract boolean canBuild();
    
    /**
     * Return true if this Build Command can skip the CMake step if the CMake
     * output has already been generated.  
     * 
     * <p>This is not safe for Visual Studio Builds, as the project does not 
     * automatically reload if CMake changes the existing project.</p>
     * 
     * @return true if this Build Command can skip the CMake step.
     */
    protected abstract boolean canSkipCmakeStep();
}
