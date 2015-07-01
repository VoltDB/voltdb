/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.io.IOException;
import java.util.List;

import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.customizers.ImportCustomizer;
import org.voltdb.VoltProcedure;
import org.voltdb.compiler.CodeBlockCompilerException;

/**
 * A singleton that holds an instance of a VoltDB groovy procedure script compiler. The compiler
 * is preconfigured to make each script a {@link VoltProcedure} delegate, and garnish it with
 * predefined includes.
 *
 *
 */
public class GroovyCodeBlockCompiler  {

    private final GroovyClassLoader gcl;

    private GroovyCodeBlockCompiler() {

        CompilerConfiguration conf = new CompilerConfiguration(CompilerConfiguration.DEFAULT);

        ImportCustomizer imports = new ImportCustomizer();
        imports.addStarImports("org.voltdb");
        imports.addImports(
                "org.voltdb.groovy.TableBuilder",
                "org.voltdb.groovy.Tuplerator",
                "org.voltdb.VoltProcedure.VoltAbortException"
                 );
        imports.addStaticStars("org.voltdb.VoltProcedure","org.voltdb.VoltType");

        conf.addCompilationCustomizers(imports);
        // conf.getOptimizationOptions().put("int", false);
        // conf.getOptimizationOptions().put("indy", true);
        conf.setScriptBaseClass(DelegatingScript.class.getName());

        File groovyOut = createGroovyOutDirectory();

        List<String> classPath = conf.getClasspath();
        classPath.add(groovyOut.getAbsolutePath());
        conf.setClasspathList(classPath);

        conf.setTargetDirectory(groovyOut);

        gcl = new GroovyClassLoader(Thread.currentThread().getContextClassLoader(), conf);
    }

    public Class<?> parseCodeBlock(final String codeBlock, final String classNameForCodeBlock) {
        Class<?> parsedClazz;
        try {
            parsedClazz = gcl.parseClass(codeBlock, classNameForCodeBlock + ".groovy");
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

    final static File createGroovyOutDirectory() {
        File groovyOut;
        try {
            groovyOut = File.createTempFile("groovyout", ".tmp");
        } catch (IOException e) {
            String tmpDN = System.getProperty("java.io.tmpdir", "[temp file system]");
            throw new RuntimeException("Groovy procedure compiler requires but lacks write access to \"" + tmpDN + "\"",e);
        }
        if (!groovyOut.delete() || !groovyOut.mkdir()) {
            throw new RuntimeException("Cannot create groovy procedure compiler output directory\"" + groovyOut + "\"");
        }
        if (   !groovyOut.isDirectory()
            || !groovyOut.canRead()
            || !groovyOut.canWrite()
            || !groovyOut.canExecute()) {
            throw new RuntimeException("Cannot access groovy procedure compiler output directory\"" + groovyOut + "\"");
        }
        return groovyOut;
    }
}
