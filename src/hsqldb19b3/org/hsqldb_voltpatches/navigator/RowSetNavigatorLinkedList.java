/* Copyright (c) 2001-2009, The HSQL Development Group
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
import java.util.NoSuchElementException;

import org.hsqldb_voltpatches.Row;
import org.hsqldb_voltpatches.lib.HsqlLinkedList;
import org.hsqldb_voltpatches.lib.HsqlLinkedList.Node;
import org.hsqldb_voltpatches.result.ResultMetaData;
import org.hsqldb_voltpatches.rowio.RowInputInterface;
import org.hsqldb_voltpatches.rowio.RowOutputInterface;

/*
 * All-in-memory implementation of RowSetNavigator for simple client or server
 * side result sets. These are the result sets used for batch or other internal
 * operations.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class RowSetNavigatorLinkedList extends RowSetNavigator {

    HsqlLinkedList list;
    final Node     root;
    Node           previous;
    Node           current;

    public RowSetNavigatorLinkedList() {

        list    = new HsqlLinkedList();
        root    = list.getHeadNode();
        current = root;
    }

    /**
     * Returns the current row object. Type of object is implementation defined.
     */
    public Object[] getCurrent() {
        return ((Row) current.data).getData();
    }

    public Row getCurrentRow() {
        return (Row) current.data;
    }

    public void remove() {

        // avoid consecutive removes without next()
        if (previous == null) {
            throw new NoSuchElementException();
        }

        if (currentPos < size && currentPos != -1) {
            list.removeAfter(previous);

            current = previous;

            size--;
            currentPos--;

            return;
        }

        throw new NoSuchElementException();
    }

    public boolean next() {

        boolean result = super.next();

        if (result) {
            previous = current;
            current  = current.next;
        }

        return result;
    }

    public void reset() {

        super.reset();

        current  = root;
        previous = null;
    }

    // reading and writing
    public void write(RowOutputInterface out,
                      ResultMetaData meta) throws IOException {

        beforeFirst();
        out.writeLong(id);
        out.writeInt(size);
        out.writeInt(0);    // offset
        out.writeInt(size);

        while (hasNext()) {
            Object[] data = getNext();

            out.writeData(meta.getColumnCount(), meta.columnTypes, data, null,
                          null);
        }

        beforeFirst();
    }

    public void read(RowInputInterface in,
                     ResultMetaData meta) throws IOException {

        id = in.readLong();

        int count = in.readInt();

        in.readInt();    // offset
        in.readInt();    // size again

        while (count-- > 0) {
            add(in.readData(meta.columnTypes));
        }
    }

    public void clear() {

        reset();
        list.clear();

        size = 0;
    }

    /**
     *  Method declaration
     *
     * @param  d
     */
    public void add(Object d) {

        list.add(d);

        size++;
    }
}
