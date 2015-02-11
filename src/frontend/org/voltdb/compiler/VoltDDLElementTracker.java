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

package org.voltdb.compiler;

import static org.voltdb.compiler.ProcedureCompiler.deriveShortProcedureName;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

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
    // map from export group name to a sorted set of table names in that group
    final NavigableMap<String, NavigableSet<String>> m_exportsByTargetName = new TreeMap<>();
    // additional non-procedure classes for the jar
    final Set<String> m_extraClassses = new TreeSet<String>();
    final Set<String> m_importLines = new TreeSet<String>();

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
     */
    void addPartition(String tableName, String colName)
    {
        if (m_partitionMap.containsKey(tableName.toLowerCase())) {
            m_compiler.addInfo(String.format("Replacing partition column %s on table %s with column %s\n",
                        m_partitionMap.get(tableName.toLowerCase()), tableName,
                        colName));
        }

        m_partitionMap.put(tableName.toLowerCase(), colName.toLowerCase());
    }

    void removePartition(String tableName)
    {
        m_partitionMap.remove(tableName);
    }

    /**
     * Add additional non-procedure classes for the jar.
     */
    void addExtraClasses(Set<String> classNames) {
        m_extraClassses.addAll(classNames);
    }

    void addImportLine(String importLine) {
        m_importLines.add(importLine);
    }

    /**
     * Tracks the given procedure descriptor if it is not already tracked
     * @param descriptor a {@link VoltCompiler.ProcedureDescriptor}
     * @return name added to procedure map
     * @throws VoltCompilerException if it is already tracked
     */
    String add(ProcedureDescriptor descriptor) throws VoltCompilerException
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

        return shortName;
    }

    /**
     * Searches for and removes the Procedure provided in prior DDL statements
     * @param Name of procedure being removed
     * @throws VoltCompilerException if the procedure does not exist
     */
    void removeProcedure(String procName, boolean ifExists) throws VoltCompilerException
    {
        assert procName != null && ! procName.trim().isEmpty();

        String shortName = deriveShortProcedureName(procName);

        if( m_procedureMap.containsKey(shortName)) {
            m_procedureMap.remove(shortName);
        }
        else if (!ifExists) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Dropped Procedure \"%s\" is not defined", procName));
        }
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
                    "Partition references an undefined procedure \"%s\"",
                    procedureName));
        }

        // need to re-instantiate as descriptor fields are final
        if( descriptor.m_singleStmt == null) {
            // the longer form costructor asserts on singleStatement
            descriptor = m_compiler.new ProcedureDescriptor(
                    descriptor.m_authGroups,
                    descriptor.m_class,
                    partitionInfo,
                    descriptor.m_language,
                    descriptor.m_scriptImpl);
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
                    descriptor.m_scriptImpl,
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
     * @param targetName
     * @throws VoltCompilerException when the given table is already exported
     */
    void addExportedTable(String tableName, String targetName)
    {
        assert tableName != null && ! tableName.trim().isEmpty();
        assert targetName != null && ! targetName.trim().isEmpty();

        // store uppercase in the catalog as typename
        targetName = targetName.toUpperCase();

        // insert the table's name into the export group
        NavigableSet<String> tableGroup = m_exportsByTargetName.get(targetName);
        if (tableGroup == null) {
            tableGroup = new TreeSet<String>();
            m_exportsByTargetName.put(targetName, tableGroup);
        }
        tableGroup.add(tableName);
    }

    void removeExportedTable(String tableName)
    {
        for (Entry<String, NavigableSet<String>> groupTables : m_exportsByTargetName.entrySet()) {
            if(groupTables.getValue().remove(tableName)) {
                break;
            }
        }
    }

    /**
     * Get a collection with tracked table exports
     * @return a collection with tracked table exports
     */
    NavigableMap<String, NavigableSet<String>> getExportedTables() {
        return m_exportsByTargetName;
    }

}
