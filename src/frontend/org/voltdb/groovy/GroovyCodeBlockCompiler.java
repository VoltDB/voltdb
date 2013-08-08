/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.groovy;

import groovy.lang.GroovyClassLoader;
import groovy_voltpatches.util.DelegatingScript;

import java.io.File;
import java.util.List;

import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.customizers.ImportCustomizer;
import org.voltdb.compiler.CodeBlockCompilerException;

public class GroovyCodeBlockCompiler  {

    private final GroovyClassLoader gcl;

    private GroovyCodeBlockCompiler() {

        CompilerConfiguration conf = new CompilerConfiguration(CompilerConfiguration.DEFAULT);

        ImportCustomizer imports = new ImportCustomizer();
        imports.addStarImports("org.voltdb");
        imports.addImports("org.voltdb.groovy.TableBuilder","org.voltdb.groovy.Tuplerator");
        imports.addStaticStars("org.voltdb.VoltProcedure","org.voltdb.VoltType");

        conf.addCompilationCustomizers(imports);
        conf.getOptimizationOptions().put("indy", true);
        conf.setScriptBaseClass(DelegatingScript.class.getName());

        File groovyOut = new File("groovyout");
        if (!groovyOut.exists()) groovyOut.mkdir();
        if (!groovyOut.isDirectory() || !groovyOut.canRead() || !groovyOut.canWrite()) {
            throw new RuntimeException("Cannot access directory\"" + groovyOut + "\"");
        }
        List<String> classPath = conf.getClasspath();
        classPath.add(groovyOut.getName());
        conf.setClasspathList(classPath);

        conf.setTargetDirectory(groovyOut);

        gcl = new GroovyClassLoader(Thread.currentThread().getContextClassLoader(), conf);
    }

    public Class<?> parseCodeBlock(final String codeBlock, final String classNameForCodeBlock) {
        Class<?> parsedClazz;
        try {
            parsedClazz = gcl.parseClass(codeBlock,classNameForCodeBlock + ".groovy");
        } catch (CompilationFailedException e) {
            throw new CodeBlockCompilerException(e.getMessage());
        }
        return parsedClazz;
    }

    static public GroovyCodeBlockCompiler instance() {
        return Holder.instance;
    }

    final private static class Holder {
        final static GroovyCodeBlockCompiler instance = new GroovyCodeBlockCompiler();
    }

}
