/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.compiler.statements;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;

import org.voltcore.utils.CoreUtils;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Function;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.ProcedureCompiler;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;

/**
 * Process CREATE FUNCTION <function-name> FROM METHOD <class-name>.<method-name>
 */
public class CreateFunctionFromMethod extends StatementProcessor {

    static Set<Class<?>> m_allowedDataTypes = new HashSet<>();

    static {
        m_allowedDataTypes.add(byte.class);
        m_allowedDataTypes.add(byte[].class);
        m_allowedDataTypes.add(short.class);
        m_allowedDataTypes.add(int.class);
        m_allowedDataTypes.add(long.class);
        m_allowedDataTypes.add(double.class);
        m_allowedDataTypes.add(float.class);
        m_allowedDataTypes.add(Byte.class);
        m_allowedDataTypes.add(Byte[].class);
        m_allowedDataTypes.add(Short.class);
        m_allowedDataTypes.add(Integer.class);
        m_allowedDataTypes.add(Long.class);
        m_allowedDataTypes.add(Double.class);
        m_allowedDataTypes.add(Float.class);
        m_allowedDataTypes.add(BigDecimal.class);
        m_allowedDataTypes.add(String.class);
        m_allowedDataTypes.add(TimestampType.class);
        m_allowedDataTypes.add(GeographyPointValue.class);
        m_allowedDataTypes.add(GeographyValue.class);
    }

    public CreateFunctionFromMethod(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        // Matches if it is CREATE FUNCTION <name> FROM METHOD <class-name>.<method-name>
        Matcher statementMatcher = SQLParser.matchCreateFunctionFromMethod(ddlStatement.statement);
        if (! statementMatcher.matches()) {
            return false;
        }

        // Before we complete the user-defined function feature, executing UDF-related DDLs will
        // generate compiler warnings.
        m_compiler.addWarn("User-defined functions are not implemented yet.");

        String functionName = checkIdentifierStart(statementMatcher.group(1), ddlStatement.statement);
        String className = checkIdentifierStart(statementMatcher.group(2), ddlStatement.statement);
        String methodName = checkIdentifierStart(statementMatcher.group(3), ddlStatement.statement);
        CatalogMap<Function> functions = db.getFunctions();
        if (functions.get(functionName) != null) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Function name \"%s\" in CREATE FUNCTION statement already exists.",
                    functionName));
        }

        // Load class
        Class<?> funcClass;
        try {
            funcClass = Class.forName(className, true, m_classLoader);
        }
        catch (Throwable cause) {
            // We are here because either the class was not found or the class was found and
            // the initializer of the class threw an error we can't anticipate. So we will
            // wrap the error with a runtime exception that we can trap in our code.
            if (CoreUtils.isStoredProcThrowableFatalToServer(cause)) {
                throw (Error)cause;
            }
            else {
                throw m_compiler.new VoltCompilerException(String.format(
                        "Cannot load class for user-defined function: %s",
                        className), cause);
            }
        }

        if (Modifier.isAbstract(funcClass.getModifiers())) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Cannot define a function using an abstract class %s",
                    className));
        }

        // get the short name of the class (no package)
        String shortName = ProcedureCompiler.deriveShortProcedureName(className);

        // find the UDF method and get the params
        Method functionMethod = null;
        Method[] methods = funcClass.getDeclaredMethods();
        for (final Method m : methods) {
            String name = m.getName();
            if (name.equals(methodName)) {
                boolean found = true;
                StringBuilder warningMessage = new StringBuilder("Class " + shortName + " has a ");
                if (! Modifier.isPublic(m.getModifiers())) {
                    warningMessage.append("non-public ");
                    found = false;
                }
                if (Modifier.isStatic(m.getModifiers())) {
                    warningMessage.append("static ");
                    found = false;
                }
                if (m.getReturnType().equals(Void.TYPE)) {
                    warningMessage.append("void ");
                    found = false;
                }
                warningMessage.append(methodName);
                warningMessage.append("() method.");
                if (found) {
                    // if not null, then we've got more than one run method
                    if (functionMethod != null) {
                        String msg = "Class " + shortName + " has multiple methods named " + methodName;
                        msg += ". Only a single function method is supported.";
                        throw m_compiler.new VoltCompilerException(msg);
                    }
                    functionMethod = m;
                }
                else {
                    m_compiler.addWarn(warningMessage.toString());
                }
            }
        }

        if (functionMethod == null) {
            String msg = "Cannot find the implementation method " + methodName +
                    " for user-defined function " + functionName + " in class " + shortName;
            throw m_compiler.new VoltCompilerException(msg);
        }

        if (! m_allowedDataTypes.contains(functionMethod.getReturnType())) {
            String msg = String.format("Method %s.%s has an unspported return type %s",
                    shortName, methodName, functionMethod.getReturnType().getName());
            throw m_compiler.new VoltCompilerException(msg);
        }

        Class<?>[] paramTypes = functionMethod.getParameterTypes();
        for (int i = 0; i < paramTypes.length; i++) {
            Class<?> paramType = paramTypes[i];
            if (! m_allowedDataTypes.contains(paramType)) {
                String msg = String.format("Method %s.%s has an unspported parameter type %s at position %d",
                        shortName, methodName, paramType.getName(), i);
                throw m_compiler.new VoltCompilerException(msg);
            }
        }

        Function func = functions.add(functionName);
        func.setFunctionname(functionName);
        func.setClassname(className);
        func.setMethodname(methodName);
        return true;
    }

}
