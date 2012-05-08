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

package adhocbenchmark;

/**
 * Helper interface that provides useful methods for building queries.
 */
public interface QueryTestHelper {
    /**
     * Generate table name from prefix and zero-based table index.
     * @param iTable  zero-based table index
     * @return  table name
     */
    String tableName(int iTable);

    /**
     * Generate column name from prefix and zero-based column index.
     * @param iColumn  zero-based column index
     * @return  column name
     */
    String columnName(int iColumn);

    /**
     * Generate table.column name from prefix and zero-based table and column index.
     * @param iTable  zero-based table index
     * @param iColumn  zero-based column index
     * @return  table.column name
     */
    String tableColumnName(int iTable, int iColumn);

    /**
     * Get an indexed shuffled number.
     * @param i  index into array of shuffled numbers
     * @return  shuffled number at that index
     */
    int getShuffledNumber(final int i);
}
