/* Copyright (c) 2001-2014, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.navigator;

import java.io.IOException;

import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.QueryExpression;
import org.hsqldb_voltpatches.QuerySpecification;
import org.hsqldb_voltpatches.Row;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.SortAndSlice;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.TableBase;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.result.ResultMetaData;
import org.hsqldb_voltpatches.rowio.RowInputInterface;
import org.hsqldb_voltpatches.rowio.RowOutputInterface;

/**
 * Implementation of RowSetNavigator using a table as the data store.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.2
 * @since 1.9.0
 */
public class RowSetNavigatorDataTable extends RowSetNavigatorData {

    final Session          session;
    public TableBase       table;
    public PersistentStore store;
    RowIterator            iterator;
    Row                    currentRow;
    int                    maxMemoryRowCount;
    boolean                isClosed;
    int                    visibleColumnCount;
    boolean                isAggregate;
    boolean                isSimpleAggregate;
    Object[]               simpleAggregateData;
    Object[]               tempRowData;

    //
    boolean reindexTable;

    //
    private Index mainIndex;
    private Index fullIndex;
    private Index orderIndex;
    private Index groupIndex;
    private Index idIndex;

    public RowSetNavigatorDataTable(Session session,
                                    QuerySpecification select) {

        super(session);

        this.session       = session;
        this.rangePosition = select.resultRangePosition;
        maxMemoryRowCount  = session.getResultMemoryRowCount();
        visibleColumnCount = select.indexLimitVisible;
        table              = select.resultTable.duplicate();
        table.store = store = session.sessionData.getNewResultRowStore(table,
                !select.isAggregated);
        isAggregate       = select.isAggregated;
        isSimpleAggregate = select.isAggregated && !select.isGrouped;
        reindexTable      = select.isGrouped;
        mainIndex         = select.mainIndex;
        fullIndex         = select.fullIndex;
        orderIndex        = select.orderIndex;
        groupIndex        = select.groupIndex;
        idIndex           = select.idIndex;
        tempRowData       = new Object[1];
    }

    public RowSetNavigatorDataTable(Session session,
                                    QuerySpecification select,
                                    RowSetNavigatorData navigator) {

        this(session, (QuerySpecification) select);

        navigator.reset();

        while (navigator.hasNext()) {
            add(navigator.getNext());
        }
    }

    public RowSetNavigatorDataTable(Session session,
                                    QueryExpression queryExpression) {

        super(session);

        this.session       = session;
        maxMemoryRowCount  = session.getResultMemoryRowCount();
        table              = queryExpression.resultTable.duplicate();
        visibleColumnCount = table.getColumnCount();
        table.store = store = session.sessionData.getNewResultRowStore(table,
                true);
        mainIndex = queryExpression.mainIndex;
        fullIndex = queryExpression.fullIndex;
    }

    public RowSetNavigatorDataTable(Session session, Table table) {

        super(session);

        this.session       = session;
        maxMemoryRowCount  = session.getResultMemoryRowCount();
        this.table         = table;
        visibleColumnCount = table.getColumnCount();
        mainIndex          = table.getPrimaryIndex();
        fullIndex          = table.getFullIndex(session);
        store              = table.getRowStore(session);
        this.size          = (int) mainIndex.size(session, store);

        reset();
    }

    public void sortFull(Session session) {

        if (reindexTable) {
            store.indexRows(session);
        }

        mainIndex = fullIndex;

        if (iterator != null) {
            iterator.release();
        }

        reset();
    }

    public void sortOrder(Session session) {

        if (orderIndex != null) {
            if (reindexTable) {
                store.indexRows(session);
            }

            mainIndex = orderIndex;

            if (iterator != null) {
                iterator.release();
            }

            reset();
        }
    }

    public void sortOrderUnion(Session session, SortAndSlice sortAndSlice) {

        if (sortAndSlice.index != null) {
            mainIndex = sortAndSlice.index;

            reset();
        }
    }

    public void add(Object[] data) {

        try {
            Row row = (Row) store.getNewCachedObject(session, data, false);

            store.indexRow(session, row);

            size++;
        } catch (HsqlException e) {}
    }

    void addAdjusted(Object[] data, int[] columnMap) {

        try {
            if (columnMap == null) {
                data = (Object[]) ArrayUtil.resizeArrayIfDifferent(data,
                        visibleColumnCount);
            } else {
                Object[] newData = new Object[visibleColumnCount];

                ArrayUtil.projectRow(data, columnMap, newData);

                data = newData;
            }

            add(data);
        } catch (HsqlException e) {}
    }

    public void update(Object[] oldData, Object[] newData) {

        if (isSimpleAggregate) {
            return;
        }

        RowIterator it = groupIndex.findFirstRow(session, store, oldData);

        if (it.hasNext()) {
            Row row = it.getNextRow();

            it.removeCurrent();
            it.release();

            size--;

            add(newData);
        }
    }

    public boolean absolute(int position) {
        return super.absolute(position);
    }

    public Object[] getCurrent() {
        return currentRow.getData();
    }

    public Row getCurrentRow() {
        return currentRow;
    }

    public boolean next() {

        boolean result = super.next();

        currentRow = iterator.getNextRow();

        return result;
    }

    public void removeCurrent() {

        if (currentRow != null) {
            iterator.removeCurrent();

            currentRow = null;

            currentPos--;
            size--;
        }
    }

    public void reset() {

        super.reset();

        if (iterator != null) {
            iterator.release();
        }

        iterator = mainIndex.firstRow(session, store, 0);
    }

    public void release() {

        if (isClosed) {
            return;
        }

        iterator.release();
        store.release();

        isClosed = true;
    }

    public void clear() {

        table.clearAllData(store);

        size = 0;

        reset();
    }

    public boolean isMemory() {
        return store.isMemory();
    }

    public void read(RowInputInterface in,
                     ResultMetaData meta) throws IOException {}

    public void write(RowOutputInterface out,
                      ResultMetaData meta) throws IOException {

        reset();
        out.writeLong(id);
        out.writeInt(size);
        out.writeInt(0);    // offset
        out.writeInt(size);

        while (hasNext()) {
            Object[] data = getNext();

            out.writeData(meta.getExtendedColumnCount(), meta.columnTypes,
                          data, null, null);
        }

        reset();
    }

    public Object[] getData(Long rowId) {

        tempRowData[0] = rowId;

        RowIterator it = idIndex.findFirstRow(session, store, tempRowData,
                                              idIndex.getDefaultColumnMap());

        return it.getNext();
    }

    public void copy(RowSetNavigatorData other, int[] rightColumnIndexes) {

        while (other.hasNext()) {
            Object[] currentData = other.getNext();

            addAdjusted(currentData, rightColumnIndexes);
        }

        other.release();
    }

    public void union(Session session, RowSetNavigatorData other) {

        Object[] currentData;
        int      colCount = table.getColumnTypes().length;

        removeDuplicates(session);
        other.reset();

        while (other.hasNext()) {
            currentData = other.getNext();

            RowIterator it = findFirstRow(currentData);

            if (!it.hasNext()) {
                currentData =
                    (Object[]) ArrayUtil.resizeArrayIfDifferent(currentData,
                        colCount);

                add(currentData);
            }
        }

        other.release();
    }

    public void intersect(Session session, RowSetNavigatorData other) {

        removeDuplicates(session);
        other.sortFull(session);

        while (hasNext()) {
            Object[] currentData = getNext();
            boolean  hasRow      = other.containsRow(currentData);

            if (!hasRow) {
                removeCurrent();
            }
        }

        other.release();
    }

    public void intersectAll(Session session, RowSetNavigatorData other) {

        Object[]    compareData = null;
        RowIterator it;
        Row         otherRow  = null;
        Object[]    otherData = null;

        sortFull(session);
        other.sortFull(session);

        it = fullIndex.emptyIterator();

        while (hasNext()) {
            Object[] currentData = getNext();
            boolean newGroup =
                compareData == null
                || fullIndex.compareRowNonUnique(
                    session, currentData, compareData,
                    fullIndex.getColumnCount()) != 0;

            if (newGroup) {
                compareData = currentData;
                it          = other.findFirstRow(currentData);
            }

            otherRow  = it.getNextRow();
            otherData = otherRow == null ? null
                                         : otherRow.getData();

            if (otherData != null
                    && fullIndex.compareRowNonUnique(
                        session, currentData, otherData,
                        fullIndex.getColumnCount()) == 0) {
                continue;
            }

            removeCurrent();
        }

        other.release();
    }

    public void except(Session session, RowSetNavigatorData other) {

        removeDuplicates(session);
        other.sortFull(session);

        while (hasNext()) {
            Object[] currentData = getNext();
            boolean  hasRow      = other.containsRow(currentData);

            if (hasRow) {
                removeCurrent();
            }
        }

        other.release();
    }

    public void exceptAll(Session session, RowSetNavigatorData other) {

        Object[]    compareData = null;
        RowIterator it;
        Row         otherRow  = null;
        Object[]    otherData = null;

        sortFull(session);
        other.sortFull(session);

        it = fullIndex.emptyIterator();

        while (hasNext()) {
            Object[] currentData = getNext();
            boolean newGroup =
                compareData == null
                || fullIndex.compareRowNonUnique(
                    session, currentData, compareData,
                    fullIndex.getColumnCount()) != 0;

            if (newGroup) {
                compareData = currentData;
                it          = other.findFirstRow(currentData);
            }

            otherRow  = it.getNextRow();
            otherData = otherRow == null ? null
                                         : otherRow.getData();

            if (otherData != null
                    && fullIndex.compareRowNonUnique(
                        session, currentData, otherData,
                        fullIndex.getColumnCount()) == 0) {
                removeCurrent();
            }
        }

        other.release();
    }

    public boolean hasUniqueNotNullRows(Session session) {

        sortFull(session);

        Object[] lastRowData = null;

        while (hasNext()) {
            Object[] currentData = getNext();

            if (hasNull(currentData)) {
                continue;
            }

            if (lastRowData != null
                    && fullIndex.compareRow(session, lastRowData, currentData)
                       == 0) {
                return false;
            } else {
                lastRowData = currentData;
            }
        }

        return true;
    }

    public void removeDuplicates(Session session) {

        sortFull(session);

        Object[] lastRowData = null;

        while (next()) {
            Object[] currentData = getCurrent();

            if (lastRowData != null
                    && fullIndex.compareRow(session, lastRowData, currentData)
                       == 0) {
                removeCurrent();
            } else {
                lastRowData = currentData;
            }
        }

        reset();
    }

    public void trim(int limitstart, int limitcount) {

        if (size == 0) {
            return;
        }

        if (limitstart >= size) {
            clear();

            return;
        }

        if (limitstart != 0) {
            reset();

            for (int i = 0; i < limitstart; i++) {
                next();
                removeCurrent();
            }
        }

        if (limitcount == 0 || limitcount >= size) {
            return;
        }

        reset();

        for (int i = 0; i < limitcount; i++) {
            next();
        }

        while (hasNext()) {
            next();
            removeCurrent();
        }
    }

    boolean hasNull(Object[] data) {

        for (int i = 0; i < visibleColumnCount; i++) {
            if (data[i] == null) {
                return true;
            }
        }

        return false;
    }

    /**
     * Special case for isSimpleAggregate cannot use index lookup.
     */
    public Object[] getGroupData(Object[] data) {

        if (isSimpleAggregate) {
            if (simpleAggregateData == null) {
                simpleAggregateData = data;

                return null;
            }

            return simpleAggregateData;
        }

        RowIterator it = groupIndex.findFirstRow(session, store, data);

        if (it.hasNext()) {
            Row row = it.getNextRow();

            if (isAggregate) {
                row.setChanged(true);
            }

            return row.getData();
        }

        return null;
    }

    boolean containsRow(Object[] data) {

        RowIterator it     = mainIndex.findFirstRow(session, store, data);
        boolean     result = it.hasNext();

        it.release();

        return result;
    }

    RowIterator findFirstRow(Object[] data) {
        return mainIndex.findFirstRow(session, store, data);
    }
}
