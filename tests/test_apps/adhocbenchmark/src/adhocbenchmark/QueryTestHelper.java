/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
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
