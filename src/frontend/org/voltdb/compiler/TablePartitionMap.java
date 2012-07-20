/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.compiler;

import java.util.HashMap;
import java.util.Map;

import org.voltdb.compiler.VoltCompiler.VoltCompilerException;

/**
 * Maintains and validates table partition info.
 * Maps between table names and partition column names.
 * Column name is null if replicated.
 */
public class TablePartitionMap {
    final VoltCompiler m_compiler;
    final Map<String, String> m_map = new HashMap<String, String>();

    /**
     * Constructor needs a compiler instance to throw VoltCompilerException.
     * @param compiler VoltCompiler instance
     */
    public TablePartitionMap(VoltCompiler compiler) {
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

        if (m_map.containsKey(tableName)) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Partitioning already specified for table \"%s\"", tableName));
        }

        m_map.put(tableName, colName);
    }
}