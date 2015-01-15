/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2009, The HSQL Development Group
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


package org.hsqldb_voltpatches.index;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;
import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.OpTypes;
import org.hsqldb_voltpatches.Row;
import org.hsqldb_voltpatches.RowAVL;
import org.hsqldb_voltpatches.SchemaObject;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.TableBase;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.navigator.RowIterator;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.types.Type;

// fredt@users 20020221 - patch 513005 by sqlbob@users - corrections
// fredt@users 20020225 - patch 1.7.0 - changes to support cascading deletes
// tony_lai@users 20020820 - patch 595052 - better error message
// fredt@users 20021205 - patch 1.7.2 - changes to method signature
// fredt@users - patch 1.8.0 - reworked the interface and comparison methods
// fredt@users - patch 1.8.0 - improved reliability for cached indexes
// fredt@users - patch 1.9.0 - iterators and concurrency

/**
 * Implementation of an AVL tree with parent pointers in nodes. Subclasses
 * of Node implement the tree node objects for memory or disk storage. An
 * Index has a root Node that is linked with other nodes using Java Object
 * references or file pointers, depending on Node implementation.<p>
 * An Index object also holds information on table columns (in the form of int
 * indexes) that are covered by it.<p>
 *
 *  New class derived from Hypersonic SQL code and enhanced in HSQLDB. <p>
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since Hypersonic SQL
 */
public class IndexAVL implements Index {

    // fields
    private final long      persistenceId;
    private final HsqlName  name;
    private final boolean[] colCheck;
    private final int[]     colIndex;
    private final int[]     defaultColMap;
    private final Type[]    colTypes;
    private final boolean[] colDesc;
    private final boolean[] nullsLast;
    private final int[]     pkCols;
    private final Type[]    pkTypes;
    private final boolean   isUnique;    // DDL uniqueness
    private final boolean   useRowId;
    private final boolean   isConstraint;
    private final boolean   isForward;
    private int             depth;
    private static final IndexRowIterator emptyIterator =
        new IndexRowIterator(null, (PersistentStore) null, null, null);
    private final TableBase table;
    private int             position;

    //
    ReadWriteLock lock      = new ReentrantReadWriteLock();
    Lock          readLock  = lock.readLock();
    Lock          writeLock = lock.writeLock();

    //
    public static final Index[] emptyArray = new Index[]{};

    /**
     * Set a node as child of another
     *
     * @param x parent node
     * @param isleft boolean
     * @param n child node
     *
     */
    private static NodeAVL set(PersistentStore store, NodeAVL x,
                               boolean isleft, NodeAVL n) {

        if (isleft) {
            x = x.setLeft(store, n);
        } else {
            x = x.setRight(store, n);
        }

        if (n != null) {
            n.setParent(store, x);
        }

        return x;
    }

    /**
     * Returns either child node
     *
     * @param x node
     * @param isleft boolean
     *
     * @return child node
     *
     */
    private static NodeAVL child(PersistentStore store, NodeAVL x,
                                 boolean isleft) {
        return isleft ? x.getLeft(store)
                      : x.getRight(store);
    }

    private static void getColumnList(Table t, int[] col, int len,
                                      StringBuffer a) {

        a.append('(');

        for (int i = 0; i < len; i++) {
            a.append(t.getColumn(col[i]).getName().statementName);

            if (i < len - 1) {
                a.append(',');
            }
        }

        a.append(')');
    }

    /**
     * compares two full table rows based on a set of columns
     *
     * @param a a full row
     * @param b a full row
     * @param cols array of column indexes to compare
     * @param coltypes array of column types for the full row
     *
     * @return comparison result, -1,0,+1
     */
    public static int compareRows(Object[] a, Object[] b, int[] cols,
                                  Type[] coltypes) {

        int fieldcount = cols.length;

        for (int j = 0; j < fieldcount; j++) {
            int i = coltypes[cols[j]].compare(a[cols[j]], b[cols[j]]);

            if (i != 0) {
                return i;
            }
        }

        return 0;
    }

    /**
     * Constructor declaration
     *
     * @param name HsqlName of the index
     * @param id persistnece id
     * @param table table of the index
     * @param columns array of column indexes
     * @param descending boolean[]
     * @param nullsLast boolean[]
     * @param colTypes array of column types
     * @param unique is this a unique index
     * @param constraint does this index belonging to a constraint
     * @param forward is this an auto-index for an FK that refers to a table
     *   defined after this table
     */
    public IndexAVL(HsqlName name, long id, TableBase table, int[] columns,
                    boolean[] descending, boolean[] nullsLast,
                    Type[] colTypes, boolean pk, boolean unique,
                    boolean constraint, boolean forward) {

        persistenceId  = id;
        this.name      = name;
        this.colIndex  = columns;
        this.colTypes  = colTypes;
        this.colDesc   = descending == null ? new boolean[columns.length]
                                            : descending;
        this.nullsLast = nullsLast == null ? new boolean[columns.length]
                                           : nullsLast;
        isUnique       = unique;
        isConstraint   = constraint;
        isForward      = forward;
        this.table     = table;
        this.pkCols    = table.getPrimaryKey();
        this.pkTypes   = table.getPrimaryKeyTypes();
        useRowId = (!isUnique && pkCols.length == 0) || (colIndex.length == 0);
        colCheck       = table.getNewColumnCheckList();

        ArrayUtil.intIndexesToBooleanArray(colIndex, colCheck);

        defaultColMap = new int[columns.length];

        ArrayUtil.fillSequence(defaultColMap);
    }

    // SchemaObject implementation
    public int getType() {
        return SchemaObject.INDEX;
    }

    public HsqlName getName() {
        return name;
    }

    public HsqlName getCatalogName() {
        return name.schema.schema;
    }

    public HsqlName getSchemaName() {
        return name.schema;
    }

    public Grantee getOwner() {
        return name.schema.owner;
    }

    public OrderedHashSet getReferences() {
        return new OrderedHashSet();
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session) {}

    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        sb = new StringBuffer(64);

        sb.append(Tokens.T_CREATE).append(' ');

        if (isUnique()) {
            sb.append(Tokens.T_UNIQUE).append(' ');
        }

        sb.append(Tokens.T_INDEX).append(' ');
        sb.append(getName().statementName);
        sb.append(' ').append(Tokens.T_ON).append(' ');
        sb.append(((Table) table).getName().getSchemaQualifiedStatementName());

        int[] col = getColumns();
        int   len = getVisibleColumns();

        getColumnList(((Table) table), col, len, sb);

        return sb.toString();
    }

    // IndexInterface
    public RowIterator emptyIterator() {
        return emptyIterator;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public long getPersistenceId() {
        return persistenceId;
    }

    /**
     * Returns the count of visible columns used
     */
    public int getVisibleColumns() {
        return colIndex.length;
    }

    /**
     * Returns the count of visible columns used
     */
    public int getColumnCount() {
        return colIndex.length;
    }

    /**
     * Is this a UNIQUE index?
     */
    public boolean isUnique() {
        return isUnique;
    }

    /**
     * Does this index belong to a constraint?
     */
    public boolean isConstraint() {
        return isConstraint;
    }

    /**
     * Returns the array containing column indexes for index
     */
    public int[] getColumns() {
        return colIndex;
    }

    /**
     * Returns the array containing column indexes for index
     */
    public Type[] getColumnTypes() {
        return colTypes;
    }

    public boolean[] getColumnDesc() {
        return colDesc;
    }

    /**
     * Returns a value indicating the order of different types of index in
     * the list of indexes for a table. The position of the groups of Indexes
     * in the list in ascending order is as follows:
     *
     * primary key index
     * unique constraint indexes
     * autogenerated foreign key indexes for FK's that reference this table or
     *  tables created before this table
     * user created indexes (CREATE INDEX)
     * autogenerated foreign key indexes for FK's that reference tables created
     *  after this table
     *
     * Among a group of indexes, the order is based on the order of creation
     * of the index.
     *
     * @return ordinal value
     */
    public int getIndexOrderValue() {

        if (isConstraint) {
            return isForward ? 4
                             : isUnique ? 0
                                        : 1;
        } else {
            return 2;
        }
    }

    public boolean isForward() {
        return isForward;
    }

    /**
     * Returns the node count.
     */
    public int size(PersistentStore store) {

        int count = 0;

        readLock.lock();

        try {
            RowIterator it = firstRow(null, store);

            while (it.hasNext()) {
                it.getNextRow();

                count++;
            }

            return count;
        } finally {
            readLock.unlock();
        }
    }

    public int sizeEstimate(PersistentStore store) {
        return (int) (1L << depth);
    }

    public boolean isEmpty(PersistentStore store) {

        readLock.lock();

        try {
            return getAccessor(store) == null;
        } finally {
            readLock.unlock();
        }
    }

    public void checkIndex(PersistentStore store) {

        readLock.lock();

        try {
            NodeAVL p = getAccessor(store);
            NodeAVL f = null;

            while (p != null) {
                f = p;

                checkNodes(store, p);

                p = p.getLeft(store);
            }

            p = f;

            while (f != null) {
                checkNodes(store, f);

                f = next(store, f);
            }
        } finally {
            readLock.unlock();
        }
    }

    private void checkNodes(PersistentStore store, NodeAVL p) {

        NodeAVL l = p.getLeft(store);
        NodeAVL r = p.getRight(store);

        if (l != null && l.getBalance() == -2) {
            System.out.print("broken index - deleted");
        }

        if (r != null && r.getBalance() == -2) {
            System.out.print("broken index -deleted");
        }

        if (l != null && !p.equals(l.getParent(store))) {
            System.out.print("broken index - no parent");
        }

        if (r != null && !p.equals(r.getParent(store))) {
            System.out.print("broken index - no parent");
        }
    }

    /**
     * Insert a node into the index
     */
    public void insert(Session session, PersistentStore store, Row row) {

        NodeAVL n;
        NodeAVL x;
        boolean isleft  = true;
        int     compare = -1;

        writeLock.lock();

        try {
            n = getAccessor(store);
            x = n;

            if (n == null) {
                store.setAccessor(this, ((RowAVL) row).getNode(position));

                return;
            }

            while (true) {
                Row currentRow = n.getRow(store);

                compare = compareRowForInsertOrDelete(session, row,
                                                      currentRow);

                if (compare == 0) {
                    throw Error.error(ErrorCode.X_23505);
                }

                isleft = compare < 0;
                x      = n;
                n      = child(store, x, isleft);

                if (n == null) {
                    break;
                }
            }

            x = set(store, x, isleft, ((RowAVL) row).getNode(position));

            balance(store, x, isleft);
        } finally {
            writeLock.unlock();
        }
    }

    public void delete(PersistentStore store, Row row) {

        if (!row.isInMemory()) {
            row = (Row) store.get(row, false);
        }

        NodeAVL node = ((RowAVL) row).getNode(position);

        delete(store, node);
    }

    public void delete(PersistentStore store, NodeAVL x) {

        if (x == null) {
            return;
        }

        NodeAVL n;

        writeLock.lock();

        try {
            if (x.getLeft(store) == null) {
                n = x.getRight(store);
            } else if (x.getRight(store) == null) {
                n = x.getLeft(store);
            } else {
                NodeAVL d = x;

                x = x.getLeft(store);

                while (true) {
                    NodeAVL temp = x.getRight(store);

                    if (temp == null) {
                        break;
                    }

                    x = temp;
                }

                // x will be replaced with n later
                n = x.getLeft(store);

                // swap d and x
                int b = x.getBalance();

                x = x.setBalance(store, d.getBalance());
                d = d.setBalance(store, b);

                // set x.parent
                NodeAVL xp = x.getParent(store);
                NodeAVL dp = d.getParent(store);

                if (d.isRoot()) {
                    store.setAccessor(this, x);
                }

                x = x.setParent(store, dp);

                if (dp != null) {
                    if (dp.isRight(d)) {
                        dp = dp.setRight(store, x);
                    } else {
                        dp = dp.setLeft(store, x);
                    }
                }

                // relink d.parent, x.left, x.right
                if (d.equals(xp)) {
                    d = d.setParent(store, x);

                    if (d.isLeft(x)) {
                        x = x.setLeft(store, d);

                        NodeAVL dr = d.getRight(store);

                        x = x.setRight(store, dr);
                    } else {
                        x = x.setRight(store, d);

                        NodeAVL dl = d.getLeft(store);

                        x = x.setLeft(store, dl);
                    }
                } else {
                    d  = d.setParent(store, xp);
                    xp = xp.setRight(store, d);

                    NodeAVL dl = d.getLeft(store);
                    NodeAVL dr = d.getRight(store);

                    x = x.setLeft(store, dl);
                    x = x.setRight(store, dr);
                }

                // apprently no-ops
                x.getRight(store).setParent(store, x);
                x.getLeft(store).setParent(store, x);

                // set d.left, d.right
                d = d.setLeft(store, n);

                if (n != null) {
                    n = n.setParent(store, d);
                }

                d = d.setRight(store, null);
                x = d;
            }

            boolean isleft = x.isFromLeft(store);

            replace(store, x, n);

            n = x.getParent(store);

            x.delete();

            while (n != null) {
                x = n;

                int sign = isleft ? 1
                                  : -1;

                switch (x.getBalance() * sign) {

                    case -1 :
                        x = x.setBalance(store, 0);
                        break;

                    case 0 :
                        x = x.setBalance(store, sign);

                        return;

                    case 1 :
                        NodeAVL r = child(store, x, !isleft);
                        int     b = r.getBalance();

                        if (b * sign >= 0) {
                            replace(store, x, r);

                            x = set(store, x, !isleft,
                                    child(store, r, isleft));
                            r = set(store, r, isleft, x);

                            if (b == 0) {
                                x = x.setBalance(store, sign);
                                r = r.setBalance(store, -sign);

                                return;
                            }

                            x = x.setBalance(store, 0);
                            r = r.setBalance(store, 0);
                            x = r;
                        } else {
                            NodeAVL l = child(store, r, isleft);

                            replace(store, x, l);

                            b = l.getBalance();
                            r = set(store, r, isleft,
                                    child(store, l, !isleft));
                            l = set(store, l, !isleft, r);
                            x = set(store, x, !isleft,
                                    child(store, l, isleft));
                            l = set(store, l, isleft, x);
                            x = x.setBalance(store, (b == sign) ? -sign
                                                                : 0);
                            r = r.setBalance(store, (b == -sign) ? sign
                                                                 : 0);
                            l = l.setBalance(store, 0);
                            x = l;
                        }
                }

                isleft = x.isFromLeft(store);
                n      = x.getParent(store);
            }
        } finally {
            writeLock.unlock();
        }
    }

    public boolean exists(Session session, PersistentStore store,
                          Object[] rowdata, int[] rowColMap) {
        return findNode(session, store, rowdata, rowColMap, rowColMap.length)
               != null;
    }

    /**
     * Return the first node equal to the indexdata object. The rowdata has
     * the same column mapping as this index.
     *
     * @param session session object
     * @param store store object
     * @param rowdata array containing index column data
     * @param match count of columns to match
     * @return iterator
     */
    public RowIterator findFirstRow(Session session, PersistentStore store,
                                    Object[] rowdata, int match) {

        NodeAVL node = findNode(session, store, rowdata, defaultColMap, match);

        return getIterator(session, store, node);
    }

    /**
     * Return the first node equal to the rowdata object.
     * The rowdata has the same column mapping as this table.
     *
     * @param session session object
     * @param store store object
     * @param rowdata array containing table row data
     * @return iterator
     */
    public RowIterator findFirstRow(Session session, PersistentStore store,
                                    Object[] rowdata) {

        NodeAVL node = findNode(session, store, rowdata, colIndex,
                                colIndex.length);

        return getIterator(session, store, node);
    }

    /**
     * Return the first node equal to the rowdata object.
     * The rowdata has the column mapping privided in rowColMap.
     *
     * @param session session object
     * @param store store object
     * @param rowdata array containing table row data
     * @return iterator
     */
    public RowIterator findFirstRow(Session session, PersistentStore store,
                                    Object[] rowdata, int[] rowColMap) {

        NodeAVL node = findNode(session, store, rowdata, rowColMap,
                                rowColMap.length);

        return getIterator(session, store, node);
    }

    /**
     * Finds the first node that is larger or equal to the given one based
     * on the first column of the index only.
     *
     * @param session session object
     * @param store store object
     * @param value value to match
     * @param compare comparison Expression type
     *
     * @return iterator
     */
    public RowIterator findFirstRow(Session session, PersistentStore store,
                                    Object value, int compare) {

        readLock.lock();

        try {
            if (compare == OpTypes.SMALLER
                    || compare == OpTypes.SMALLER_EQUAL) {
                return findFirstRowNotNull(session, store);
            }

            boolean isEqual = compare == OpTypes.EQUAL
                              || compare == OpTypes.IS_NULL;
            NodeAVL x     = getAccessor(store);
            int     iTest = 1;

            if (compare == OpTypes.GREATER) {
                iTest = 0;
            }

            if (value == null && !isEqual) {
                return emptyIterator;
            }

            // this method returns the correct node only with the following conditions
            boolean check = compare == OpTypes.GREATER
                            || compare == OpTypes.EQUAL
                            || compare == OpTypes.GREATER_EQUAL;

            if (!check) {
                Error.runtimeError(ErrorCode.U_S0500, "Index.findFirst");
            }

            while (x != null) {
                boolean t = colTypes[0].compare(
                    value, x.getRow(store).getData()[colIndex[0]]) >= iTest;

                if (t) {
                    NodeAVL r = x.getRight(store);

                    if (r == null) {
                        break;
                    }

                    x = r;
                } else {
                    NodeAVL l = x.getLeft(store);

                    if (l == null) {
                        break;
                    }

                    x = l;
                }
            }

/*
        while (x != null
                && Column.compare(value, x.getData()[colIndex_0], colType_0)
                   >= iTest) {
            x = next(x);
        }
*/
            while (x != null) {
                Object colvalue = x.getRow(store).getData()[colIndex[0]];
                int    result   = colTypes[0].compare(value, colvalue);

                if (result >= iTest) {
                    x = next(store, x);
                } else {
                    if (isEqual) {
                        if (result != 0) {
                            x = null;
                        }
                    } else if (colvalue == null) {
                        x = next(store, x);

                        continue;
                    }

                    break;
                }
            }

// MVCC
            if (session == null || x == null) {
                return getIterator(session, store, x);
            }

            while (x != null) {
                Row row = x.getRow(store);

                if (compare == OpTypes.EQUAL
                        && colTypes[0].compare(
                            value, row.getData()[colIndex[0]]) != 0) {
                    x = null;

                    break;
                }

                if (session.database.txManager.canRead(session, row)) {
                    break;
                }

                x = next(store, x);
            }

            return getIterator(session, store, x);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Finds the first node where the data is not null.
     *
     * @return iterator
     */
    public RowIterator findFirstRowNotNull(Session session,
                                           PersistentStore store) {

        readLock.lock();

        try {
            NodeAVL x = getAccessor(store);

            while (x != null) {
                boolean t = colTypes[0].compare(
                    null, x.getRow(store).getData()[colIndex[0]]) >= 0;

                if (t) {
                    NodeAVL r = x.getRight(store);

                    if (r == null) {
                        break;
                    }

                    x = r;
                } else {
                    NodeAVL l = x.getLeft(store);

                    if (l == null) {
                        break;
                    }

                    x = l;
                }
            }

            while (x != null) {
                Object colvalue = x.getRow(store).getData()[colIndex[0]];

                if (colvalue == null) {
                    x = next(store, x);
                } else {
                    break;
                }
            }

// MVCC
            while (session != null && x != null) {
                Row row = x.getRow(store);

                if (session.database.txManager.canRead(session, row)) {
                    break;
                }

                x = next(store, x);
            }

            return getIterator(session, store, x);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns the row for the first node of the index
     *
     * @return Iterator for first row
     */
    public RowIterator firstRow(Session session, PersistentStore store) {

        int tempDepth = 0;

        readLock.lock();

        try {
            NodeAVL x = getAccessor(store);
            NodeAVL l = x;

            while (l != null) {
                x = l;
                l = x.getLeft(store);

                tempDepth++;
            }

            while (session != null && x != null) {
                Row row = x.getRow(store);

                if (session.database.txManager.canRead(session, row)) {
                    break;
                }

                x = next(store, x);
            }

            return getIterator(session, store, x);
        } finally {
            depth = tempDepth;

            readLock.unlock();
        }
    }

    public RowIterator firstRow(PersistentStore store) {

        int tempDepth = 0;

        readLock.lock();

        try {
            NodeAVL x = getAccessor(store);
            NodeAVL l = x;

            while (l != null) {
                x = l;
                l = x.getLeft(store);

                tempDepth++;
            }

            return getIterator(null, store, x);
        } finally {
            depth = tempDepth;

            readLock.unlock();
        }
    }

    /**
     * Returns the row for the last node of the index
     *
     * @return last row
     */
    public Row lastRow(Session session, PersistentStore store) {

        readLock.lock();

        try {
            NodeAVL x = getAccessor(store);
            NodeAVL l = x;

            while (l != null) {
                x = l;
                l = x.getRight(store);
            }

            while (session != null && x != null) {
                Row row = x.getRow(store);

                if (session.database.txManager.canRead(session, row)) {
                    break;
                }

                x = last(store, x);
            }

            return x == null ? null
                             : x.getRow(store);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns the node after the given one
     *
     * @param x node
     *
     * @return next node
     */
    private NodeAVL next(Session session, PersistentStore store, NodeAVL x) {

        if (x == null) {
            return null;
        }

        readLock.lock();

        try {
            while (true) {
                x = next(store, x);

                if (x == null) {
                    return x;
                }

                if (session == null) {
                    return x;
                }

                Row row = x.getRow(store);

                if (session.database.txManager.canRead(session, row)) {
                    return x;
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    private NodeAVL next(PersistentStore store, NodeAVL x) {

        NodeAVL r = x.getRight(store);

        if (r != null) {
            x = r;

            NodeAVL l = x.getLeft(store);

            while (l != null) {
                x = l;
                l = x.getLeft(store);
            }

            return x;
        }

        NodeAVL ch = x;

        x = x.getParent(store);

        while (x != null && ch.equals(x.getRight(store))) {
            ch = x;
            x  = x.getParent(store);
        }

        return x;
    }

    private NodeAVL last(PersistentStore store, NodeAVL x) {

        if (x == null) {
            return null;
        }

        readLock.lock();

        try {
            NodeAVL left = x.getLeft(store);

            if (left != null) {
                x = left;

                NodeAVL right = x.getRight(store);

                while (right != null) {
                    x     = right;
                    right = x.getRight(store);
                }

                return x;
            }

            NodeAVL ch = x;

            x = x.getParent(store);

            while (x != null && ch.equals(x.getLeft(store))) {
                ch = x;
                x  = x.getParent(store);
            }

            return x;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Replace x with n
     *
     * @param x node
     * @param n node
     */
    private void replace(PersistentStore store, NodeAVL x, NodeAVL n) {

        if (x.isRoot()) {
            if (n != null) {
                n = n.setParent(store, null);
            }

            store.setAccessor(this, n);
        } else {
            set(store, x.getParent(store), x.isFromLeft(store), n);
        }
    }

    /**
     * Compares two table rows based on the columns of this index. The rowColMap
     * parameter specifies which columns of the other table are to be compared
     * with the colIndex columns of this index. The rowColMap can cover all
     * or only some columns of this index.
     *
     * @param a row from another table
     * @param rowColMap column indexes in the other table
     * @param b a full row in this table
     *
     * @return comparison result, -1,0,+1
     */
    public int compareRowNonUnique(Object[] a, int[] rowColMap, Object[] b) {

        int fieldcount = rowColMap.length;

        for (int j = 0; j < fieldcount; j++) {
            int i = colTypes[j].compare(a[rowColMap[j]], b[colIndex[j]]);

            if (i != 0) {
                return i;
            }
        }

        return 0;
    }

    public int compareRowNonUnique(Object[] a, int[] rowColMap, Object[] b,
                                   int fieldCount) {

        for (int j = 0; j < fieldCount; j++) {
            int i = colTypes[j].compare(a[rowColMap[j]], b[colIndex[j]]);

            if (i != 0) {
                return i;
            }
        }

        return 0;
    }

    /**
     * As above but use the index column data
     */
    public int compareRowNonUnique(Object[] a, Object[] b, int fieldcount) {

        for (int j = 0; j < fieldcount; j++) {
            int i = colTypes[j].compare(a[j], b[colIndex[j]]);

            if (i != 0) {
                return i;
            }
        }

        return 0;
    }

    /**
     * Compare two rows of the table for inserting rows into unique indexes
     * Supports descending columns.
     *
     * @param newRow data
     * @param existingRow data
     * @return comparison result, -1,0,+1
     */
    private int compareRowForInsertOrDelete(Session session, Row newRow,
            Row existingRow) {

        Object[] a       = newRow.getData();
        Object[] b       = existingRow.getData();
        int      j       = 0;
        boolean  hasNull = false;

        for (; j < colIndex.length; j++) {
            Object  currentvalue = a[colIndex[j]];
            Object  othervalue   = b[colIndex[j]];
            int     i = colTypes[j].compare(currentvalue, othervalue);
            boolean nulls        = currentvalue == null || othervalue == null;

            if (i != 0) {
                if (colDesc[j] && !nulls) {
                    i = -i;
                }

                if (nullsLast[j] && nulls) {
                    i = -i;
                }

                return i;
            }

            if (currentvalue == null) {
                hasNull = true;
            }
        }

        if (isUnique && !useRowId && !hasNull) {
            if (session == null
                    || session.database.txManager.canRead(session,
                        existingRow)) {

                //* debug 190
//                session.database.txManager.canRead(session, existingRow);
                return 0;
            } else {
                int difference = newRow.getPos() - existingRow.getPos();

                return difference;
            }
        }

        for (j = 0; j < pkCols.length; j++) {
            Object currentvalue = a[pkCols[j]];
            int    i = pkTypes[j].compare(currentvalue, b[pkCols[j]]);

            if (i != 0) {
                return i;
            }
        }

        if (useRowId) {
            int difference = newRow.getPos() - existingRow.getPos();

            if (difference < 0) {
                difference = -1;
            } else if (difference > 0) {
                difference = 1;
            }

            return difference;
        }

        if (session == null
                || session.database.txManager.canRead(session, existingRow)) {
            return 0;
        } else {
            int difference = newRow.getPos() - existingRow.getPos();

            if (difference < 0) {
                difference = -1;
            } else if (difference > 0) {
                difference = 1;
            }

            return difference;
        }
    }

    /**
     * Finds a match with a row from a different table
     *
     * @param rowdata array containing data for the index columns
     * @param rowColMap map of the data to columns
     * @param first true if the first matching node is required, false if any node
     * @return matching node or null
     */
    private NodeAVL findNode(Session session, PersistentStore store,
                             Object[] rowdata, int[] rowColMap,
                             int fieldCount) {

        readLock.lock();

        try {
            NodeAVL x = getAccessor(store);
            NodeAVL n;
            NodeAVL result = null;

            while (x != null) {
                int i = this.compareRowNonUnique(rowdata, rowColMap,
                                                 x.getRow(store).getData(),
                                                 fieldCount);

                if (i == 0) {
                    result = x;
                    n      = x.getLeft(store);
                } else if (i > 0) {
                    n = x.getRight(store);
                } else {
                    n = x.getLeft(store);
                }

                if (n == null) {
                    break;
                }

                x = n;
            }

            // MVCC 190
            if (session == null) {
                return result;
            }

            while (result != null) {
                Row row = result.getRow(store);

                if (compareRowNonUnique(
                        rowdata, rowColMap, row.getData(), fieldCount) != 0) {
                    result = null;

                    break;
                }

                if (session.database.txManager.canRead(session, row)) {
                    break;
                }

                result = next(store, result);
            }

            return result;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Balances part of the tree after an alteration to the index.
     */
    private void balance(PersistentStore store, NodeAVL x, boolean isleft) {

        while (true) {
            int sign = isleft ? 1
                              : -1;

            switch (x.getBalance() * sign) {

                case 1 :
                    x = x.setBalance(store, 0);

                    return;

                case 0 :
                    x = x.setBalance(store, -sign);
                    break;

                case -1 :
                    NodeAVL l = child(store, x, isleft);

                    if (l.getBalance() == -sign) {
                        replace(store, x, l);

                        x = set(store, x, isleft, child(store, l, !isleft));
                        l = set(store, l, !isleft, x);
                        x = x.setBalance(store, 0);
                        l = l.setBalance(store, 0);
                    } else {
                        NodeAVL r = child(store, l, !isleft);

                        replace(store, x, r);

                        l = set(store, l, !isleft, child(store, r, isleft));
                        r = set(store, r, isleft, l);
                        x = set(store, x, isleft, child(store, r, !isleft));
                        r = set(store, r, !isleft, x);

                        int rb = r.getBalance();

                        x = x.setBalance(store, (rb == -sign) ? sign
                                                              : 0);
                        l = l.setBalance(store, (rb == sign) ? -sign
                                                             : 0);
                        r = r.setBalance(store, 0);
                    }

                    return;
            }

            if (x.isRoot()) {
                return;
            }

            isleft = x.isFromLeft(store);
            x      = x.getParent(store);
        }
    }

    private NodeAVL getAccessor(PersistentStore store) {

        NodeAVL node = (NodeAVL) store.getAccessor(this);

        return node;
    }

    private IndexRowIterator getIterator(Session session,
                                         PersistentStore store, NodeAVL x) {

        if (x == null) {
            return emptyIterator;
        } else {
            IndexRowIterator it = new IndexRowIterator(session, store, this,
                x);

            return it;
        }
    }

    public static final class IndexRowIterator implements RowIterator {

        final Session         session;
        final PersistentStore store;
        final IndexAVL        index;
        NodeAVL               nextnode;
        Row                   lastrow;
        IndexRowIterator      last;
        IndexRowIterator      next;
        IndexRowIterator      lastInSession;
        IndexRowIterator      nextInSession;

        /**
         * When session == null, rows from all sessions are returned
         */
        public IndexRowIterator(Session session, PersistentStore store,
                                IndexAVL index, NodeAVL node) {

            this.session = session;
            this.store   = store;
            this.index   = index;

            if (index == null) {
                return;
            }

            nextnode = node;
        }

        public boolean hasNext() {
            return nextnode != null;
        }

        public Row getNextRow() {

            if (nextnode == null) {
                release();

                return null;
            }

            lastrow  = nextnode.getRow(store);
            nextnode = index.next(session, store, nextnode);

            if (nextnode == null) {
                release();
            }

            return lastrow;
        }

        public void remove() {
            store.delete(lastrow);
        }

        public void release() {}

        public boolean setRowColumns(boolean[] columns) {
            return false;
        }

        public long getPos() {
            return nextnode.getPos();
        }
    }

    /************************* Volt DB Extensions *************************/

    private org.hsqldb_voltpatches.Expression[]    exprs; // A VoltDB extension to support indexed expressions
    private boolean         isAssumeUnique;  // A VoltDB extension to allow unique index on partitioned table without partition column included.

    /**
     * VoltDB-specific Expression Index Constructor supports indexed expressions
     *
     * @param name HsqlName of the index
     * @param id persistnece id
     * @param tableRep table of the index
     * @param cols
     * @param expressions
     * @param unique is this a unique index
     * @param constraint does this index belonging to a constraint
     */
    @Override
    public IndexAVL withExpressions(org.hsqldb_voltpatches.Expression[] expressions) {
        exprs = expressions;
        return this;
    }

    List<String> getColumnNameList() {

        List<String> columnNameList = new ArrayList<String>();
        Table t2 = (Table) table;

        for (int j = 0; j < colIndex.length; ++j) {
            columnNameList.add(
                t2.getColumn(colIndex[j]).getName().statementName);
        }

        return columnNameList;
    }

    /**
     * VoltDB added method to get a non-catalog-dependent
     * representation of this HSQLDB object.
     * @param session The current Session object may be needed to resolve
     * some names.
     * @return XML, correctly indented, representing this object.
     * @throws HSQLParseException
     */
    @Override
    public org.hsqldb_voltpatches.VoltXMLElement voltGetIndexXML(Session session, String tableName)
            throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException {
        org.hsqldb_voltpatches.VoltXMLElement index = new org.hsqldb_voltpatches.VoltXMLElement("index");

        String indexName = getName().name;
        String autoGenIndexName = null;
        if (indexName.startsWith("SYS_IDX_")) {
            if (indexName.startsWith("SYS_PK_", 8)) {
                autoGenIndexName = HSQLInterface.AUTO_GEN_PRIMARY_KEY_PREFIX + tableName;
            }
            else if (indexName.startsWith("SYS_CT_", 8)) {
                autoGenIndexName = HSQLInterface.AUTO_GEN_CONSTRAINT_PREFIX + tableName;
            }
            else {
                if (indexName.length() == 13) {
                    // Raw SYS_IDX_XXXXX
                    autoGenIndexName = HSQLInterface.AUTO_GEN_IDX_PREFIX + tableName;
                }
                else {
                    // Explicitly named constraint wrapped by SYS_IDX_
                    autoGenIndexName = HSQLInterface.AUTO_GEN_CONSTRAINT_WRAPPER_PREFIX +
                            indexName.substring(8, indexName.length()-6);
                    indexName = autoGenIndexName;
                }
            }
        }
        else {
            autoGenIndexName = "";
        }

        // Support indexed expressions
        int exprHash = 0;
        if (exprs != null) {
            org.hsqldb_voltpatches.VoltXMLElement indexedExprs = new org.hsqldb_voltpatches.VoltXMLElement("exprs");
            index.children.add(indexedExprs);
            String hashExprString = new String();
            String sep = "";
            for (org.hsqldb_voltpatches.Expression expression : exprs) {
                org.hsqldb_voltpatches.VoltXMLElement xml = expression.voltGetExpressionXML(session, (Table) table);
                indexedExprs.children.add(xml);
                hashExprString += sep + expression.getSQL();
                sep = ",";
            }
            if (!autoGenIndexName.equals("")) {
                byte[] bytes = hashExprString.getBytes();
                int offset = 0;
                for (int ii = 0; ii < bytes.length; ii++) {
                    exprHash = 31 * exprHash + bytes[offset++];
                }
            }
        }
        index.attributes.put("assumeunique", isAssumeUnique() ? "true" : "false");

        Object[] columnList = getColumnNameList().toArray();
        if (columnList.length > 0) {
            if (!autoGenIndexName.equals("") &&
                    !autoGenIndexName.startsWith(HSQLInterface.AUTO_GEN_CONSTRAINT_WRAPPER_PREFIX)) {
                autoGenIndexName += "_" + StringUtils.join(columnList, "_");
                if (exprs != null) {
                    autoGenIndexName += "_" + java.lang.Math.abs(exprHash % 100000);
                }
                indexName = autoGenIndexName;
            }
            index.attributes.put("name", indexName);
            index.attributes.put("columns", StringUtils.join(columnList, ","));
        }
        else {
            index.attributes.put("name", autoGenIndexName.equals("") ? indexName : autoGenIndexName);
            index.attributes.put("columns", "");
        }
        index.attributes.put("unique", isUnique() ? "true" : "false");

        return index;
    }

    /**
     * VoltDB added method to get a list of indexed expressions that contain one or more non-columns.
     * @return the list of expressions, or null if indexing only plain column value(s).
     */
    @Override
    public org.hsqldb_voltpatches.Expression[] getExpressions() {
        return exprs;
    }

    @Override
    public boolean isAssumeUnique() {
        return isAssumeUnique;
    }

    @Override
    public Index setAssumeUnique(boolean assumeUnique) {
        this.isAssumeUnique = assumeUnique;
        return this;
    }
    /**********************************************************************/
}
