/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.util.regex.Matcher;

import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.ProcedureCompiler;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;
import org.voltdb.compiler.statements.CreateFunction;

/**
 * Process CREATE FUNCTION <function-name> FROM METHOD <class-name>.<method-name>
 */
public class CreateFunctionFromMethod extends CreateFunction {

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

        // Clean up the names
        String functionName = checkIdentifierStart(statementMatcher.group(1), ddlStatement.statement).toLowerCase();
        // Class name and method name are case sensitive.
        String className = checkIdentifierStart(statementMatcher.group(2), ddlStatement.statement);
        String methodName = checkIdentifierStart(statementMatcher.group(3), ddlStatement.statement);

        // Check if the function is already defined
        if (isDefinedFunctionName(functionName)) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Function \"%s\" is already defined.",
                    functionName));
        }
        // Load the function class
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
        for (final Method m : funcClass.getDeclaredMethods()) {
            if (! m.getName().equals(methodName)) {
                continue;
            }
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

        if (functionMethod == null) {
            String msg = "Cannot find the implementation method " + methodName +
                    " for user-defined function " + functionName + " in class " + shortName;
            throw m_compiler.new VoltCompilerException(msg);
        }

        Class<?> returnTypeClass = functionMethod.getReturnType();
        if (! m_allowedDataTypes.contains(returnTypeClass)) {
            String msg = String.format("Method %s.%s has an unsupported return type %s",
                    shortName, methodName, returnTypeClass.getName());
            throw m_compiler.new VoltCompilerException(msg);
        }

        Class<?>[] paramTypeClasses = functionMethod.getParameterTypes();
        int paramCount = paramTypeClasses.length;
        for (int i = 0; i < paramCount; i++) {
            Class<?> paramTypeClass = paramTypeClasses[i];
            if (! m_allowedDataTypes.contains(paramTypeClass)) {
                String msg = String.format("Method %s.%s has an unsupported parameter type %s at position %d",
                        shortName, methodName, paramTypeClass.getName(), i);
                throw m_compiler.new VoltCompilerException(msg);
            }
        }

        try {
            funcClass.newInstance();
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(String.format("Error instantiating function \"%s\"", className), e);
        }

        // Add the description of the function to the VoltXMLElement
        // in m_schema.  We get the function id from FunctionForVoltDB.
        // This may be a new function id or an old one, if we are reading
        // old DDL.
        //
        // Note here that the integer values for the return type and for the parameter
        // types are the value of a **VoltType** enumeration.  When the UDF is registered with
        // FunctionForVoltDB the return type and parameter type values are from **HSQL**.
        // When the UDF is actually placed into the catalog we have to keep this straight.
        //
        // It turns out that we need to register these with the compiler here
        // as well.  They can't be used until the procedures are defined.  But
        // the error messages are misleading if we try to use one in an index expression
        // or a materialized view definition.
        VoltType voltReturnType = VoltType.typeFromClass(returnTypeClass);
        VoltType[] voltParamTypes = new VoltType[paramTypeClasses.length];
        VoltXMLElement funcXML = new VoltXMLElement("ud_function")
                                    .withValue("name", functionName)
                                    .withValue("className", className)
                                    .withValue("methodName", methodName)
                                    .withValue("returnType", String.valueOf(voltReturnType.getValue()));
        for (int i = 0; i < paramTypeClasses.length; i++) {
            VoltType voltParamType = VoltType.typeFromClass(paramTypeClasses[i]);
            VoltXMLElement paramXML = new VoltXMLElement("udf_ptype")
                                         .withValue("type", String.valueOf(voltParamType.getValue()));
            funcXML.children.add(paramXML);
            voltParamTypes[i] = voltParamType;
        }
        //
        // Register the function and get the function id.  This lets HSQL use the name in
        // indexes, materialized views and tuple limit delete statements.  These are not
        // valid, but it helps us give a nice error message.  Note that this definition
        // may revive a saved user defined function, and that nothing is put into the
        // catalog here.
        //
        int functionId = FunctionForVoltDB.registerTokenForUDF(functionName, -1, voltReturnType, voltParamTypes, false);
        funcXML.attributes.put("functionid", String.valueOf(functionId));

        m_logger.debug(String.format("Added XML for function \"%s\"", functionName));
        m_schema.children.add(funcXML);
        return true;
    }
}
