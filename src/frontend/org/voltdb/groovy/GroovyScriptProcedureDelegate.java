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

import groovy.lang.Binding;
import groovy.lang.Closure;
import groovy.lang.Script;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

import com.google.common.collect.ImmutableMap;

public class GroovyScriptProcedureDelegate extends VoltProcedure implements GroovyCodeBlockConstants {

    public final Closure<Object> m_closure;
    protected final String m_procedureName;
    protected final Class<?>[] m_parameterTypes;
    protected final Map<String, SQLStmt> m_statementMap;

    public GroovyScriptProcedureDelegate(Class<?> scriptClass)  {
        String shortName = scriptClass.getSimpleName();

        // all groovy scripts have an implicit run method defined
        Method run;
        try {
            run = scriptClass.getMethod("run", (Class<?>[]) null);
        } catch (NoSuchMethodException e) {
            throw new SetupException(
                    "Procedure \"" + scriptClass.getName() + "\" is not a groovy script", e
                    );
        }
        Script script;
        try {
            script = (Script)scriptClass.newInstance();
        } catch (InstantiationException e) {
            throw new SetupException(
                    "Error instantiating the code block script for \"" + shortName + "\"", e
                    );
        } catch (IllegalAccessException e) {
            throw new SetupException(
                    "Error instantiating the code block script for \"" + shortName + "\"", e
                    );
        } catch (ClassCastException e) {
            throw new SetupException(
                    "Procedure \"" + shortName + "\" is not a groovy script", e
                    );
        }
        // inject the required volt binding
        Binding binding = new Binding();
        binding.setVariable(GVY_PROCEDURE_INSTANCE_VAR, this);
        script.setBinding(binding);

        try {
            run.invoke(script, (Object[]) null);
        } catch (IllegalAccessException e) {
            throw new SetupException(
                    "Error running the code block script for \"" + shortName+ "\"", e
                    );
        } catch (InvocationTargetException e) {
            throw new SetupException(
                    "Error running the code block script for \"" + shortName + "\"",
                    e.getTargetException() == null ? e : e.getTargetException()
                    );
        }
        Object transactOn = binding.getVariable(GVY_PROCEDURE_ENTRY_CLOSURE);
        if (transactOn == null || ! (transactOn instanceof Closure)) {
            throw new SetupException(String.format(
                    "Procedure \"%s\" code block does not contain the required \"%s\" closure",
                    shortName, GVY_PROCEDURE_ENTRY_CLOSURE
                    ));
        }

        @SuppressWarnings("unchecked")
        Map<String,Object> bindings = (Map<String,Object>)binding.getVariables();
        ImmutableMap.Builder<String, SQLStmt> builder = ImmutableMap.builder();

        for (Map.Entry<String, Object>entry: bindings.entrySet()) {
            if (entry.getValue() != null && entry.getValue() instanceof SQLStmt) {
                SQLStmt statement = SQLStmt.class.cast(entry.getValue());
                builder.put(entry.getKey(), statement);
            }
        }

        @SuppressWarnings("unchecked")
        Closure<Object> procedureInvocationClosure = (Closure<Object>)transactOn;

        m_procedureName = shortName;
        m_closure = procedureInvocationClosure;
        m_parameterTypes = procedureInvocationClosure.getParameterTypes();
        m_statementMap = builder.build();
    }

    public String getProcedureName() {
        return m_procedureName;
    }

    public Class<?>[] getParameterTypes() {
        return m_parameterTypes;
    }

    public Map<String,SQLStmt> getStatementMap() {
        return m_statementMap;
    }

    public static class SetupException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public SetupException() {
        }
        public SetupException(String message, Throwable cause) {
            super(message, cause);
        }
        public SetupException(String message) {
            super(message);
        }
        public SetupException(Throwable cause) {
            super(cause);
        }
    }
}
