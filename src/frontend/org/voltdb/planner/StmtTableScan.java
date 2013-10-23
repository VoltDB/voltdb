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

package org.voltdb.planner;

import java.util.HashSet;

import org.voltdb.catalog.Table;
import org.voltdb.plannodes.SchemaColumn;

/**
 * StmtTableScan caches data related to a given instance of a table within the statement scope
 */
public class StmtTableScan {

    public static final int NULL_ALIAS_INDEX = -1;
    // Catalog table
    public Table m_table = null;
    // table alias
    public String m_tableAlias = null;
    // Store a table-hashed list of the columns actually used by this statement.
    public HashSet<SchemaColumn> m_scanColumns = null ;

}
