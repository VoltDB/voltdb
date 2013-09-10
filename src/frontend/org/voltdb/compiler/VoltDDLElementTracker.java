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

package org.voltdb.compiler;

import static org.voltdb.compiler.ProcedureCompiler.deriveShortProcedureName;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.voltdb.compiler.VoltCompiler.ProcedureDescriptor;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;

/**
 * Maintains and validates partition info.
 * Maps between table names and partition column names,
 * and procedures and their respective partition info
 * Column name is null if replicated.
 */
public class VoltDDLElementTracker {
    final VoltCompiler m_compiler;
    final Map<String, String> m_partitionMap = new HashMap<String, String>();
    final Map<String, ProcedureDescriptor> m_procedureMap =
            new HashMap<String, ProcedureDescriptor>();
    final Set<String> m_exports = new HashSet<String>();
    // additional non-procedure classes for the jar
    String[] m_extraClassses = new String[0];

    /**
     * Constructor needs a compiler instance to throw VoltCompilerException.
     * @param compiler VoltCompiler instance
     */
    public VoltDDLElementTracker(VoltCompiler compiler) {
        m_compiler = compiler;
    }

    /**
     * Add a table/column partition mapping for a PARTITION/REPLICATE statements.
     * Validate input data and reject duplicates.
     *
     * @param tableName table name
     * @param colName column name
     * @throws VoltCompilerException
     */
    void put(String tableName, String colName) throws VoltCompilerException
    {
        // where is table and column validity checked?
        if (tableName.length() == 0) {
            throw m_compiler.new VoltCompilerException("PARTITION or REPLICATE has no TABLE specified");
        }

        if (m_partitionMap.containsKey(tableName.toLowerCase())) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Partitioning already specified for table \"%s\"", tableName));
        }

        m_partitionMap.put(tableName.toLowerCase(), colName);
    }

    /**
     * Add additional non-procedure classes for the jar.
     */
    void addExtraClasses(String[] classNames) {
        m_extraClassses = classNames;
    }

    /**
     * Tracks the given procedure descriptor if it is not already tracked
     * @param descriptor a {@link VoltCompiler.ProcedureDescriptor}
     * @throws VoltCompilerException if it is already tracked
     */
    void add(ProcedureDescriptor descriptor) throws VoltCompilerException
    {
        assert descriptor != null;

        String className = descriptor.m_className;
        assert className != null && ! className.trim().isEmpty();

        String shortName = deriveShortProcedureName(className);

        if( m_procedureMap.containsKey(shortName)) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Procedure \"%s\" is already defined", className));
        }

        m_procedureMap.put(shortName, descriptor);
    }

    /**
     * Associates the given partition info to the given tracked procedure
     * @param procedureName the short name of the procedure name
     * @param partitionInfo the partition info to associate with the procedure
     * @throws VoltCompilerException when there is no corresponding tracked
     *   procedure
     */
    void addProcedurePartitionInfoTo( String procedureName, String partitionInfo)
            throws VoltCompilerException {

        assert procedureName != null && ! procedureName.trim().isEmpty();
        assert partitionInfo != null && ! partitionInfo.trim().isEmpty();

        ProcedureDescriptor descriptor = m_procedureMap.get(procedureName);
        if( descriptor == null) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Partition in referencing an undefined procedure \"%s\"",
                    procedureName));
        }

        // need to re-instantiate as descriptor fields are final
        if( descriptor.m_singleStmt == null) {
            // the longer form costructor asserts on singleStatement
            descriptor = m_compiler.new ProcedureDescriptor(
                    descriptor.m_authGroups,
                    descriptor.m_class,
                    partitionInfo,
                    descriptor.m_language);
        }
        else {
            descriptor = m_compiler.new ProcedureDescriptor(
                    descriptor.m_authGroups,
                    descriptor.m_className,
                    descriptor.m_singleStmt,
                    descriptor.m_joinOrder,
                    partitionInfo,
                    false,
                    descriptor.m_language,
                    descriptor.m_class);
        }
        m_procedureMap.put(procedureName, descriptor);
    }

    /**
     * gets the list of tracked procedure descriptors
     * @return the list of tracked procedure descriptors
     */
    Collection<ProcedureDescriptor> getProcedureDescriptors() {
        return m_procedureMap.values();
    }

    /**
     * Track an exported table
     * @param tableName a table name
     * @throws VoltCompilerException when the given table is already exported
     */
    void addExportedTable( String tableName)
        throws VoltCompilerException
    {
        assert tableName != null && ! tableName.trim().isEmpty();

        if( m_exports.contains(tableName)) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Table \"%s\" is already exported", tableName
                    ));
        }

        m_exports.add(tableName);
    }

    /**
     * Get a collection with tracked table exports
     * @return a collection with tracked table exports
     */
    Collection<String> getExportedTables() {
        return m_exports;
    }

}