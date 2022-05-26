/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
package org.voltdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.voltdb.VoltTable.ColumnInfo;

public class MockStatsSource extends StatsSource{
    public static List<VoltTable.ColumnInfo> columns;
    public static int delay = 0;

    private final Object retvals[][];
    private final List<VoltTable.ColumnInfo> instanceColumns;
    public MockStatsSource(Object retvals[][]) {
        super(false);
        this.retvals = retvals;
        this.instanceColumns = columns;
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columnsList) {
        columnsList.addAll( columns );
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return new Iterator<Object>() {
            int index = 0;
            @Override
            public boolean hasNext() {
                return index < retvals.length;
            }

            @Override
            public Object next() {
                if (index < retvals.length){
                    return index++;
                }
                throw new NoSuchElementException();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }

    @Override
    protected int updateStatsRow(Object rowKey, Object rowValues[]) {
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        int index = (Integer)rowKey;
        for (int ii = 0; ii < retvals[index].length; ii++) {
            rowValues[columnNameToIndex.get(instanceColumns.get(ii).name)] = retvals[index][ii];
        }
        return instanceColumns.size();
    }
}
