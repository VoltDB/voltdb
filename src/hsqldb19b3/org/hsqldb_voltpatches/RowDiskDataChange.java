/* Copyright (c) 2001-2011, The HSQL Development Group
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


package org.hsqldb_voltpatches;

import java.io.IOException;

import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.rowio.RowInputBinary;
import org.hsqldb_voltpatches.rowio.RowInputInterface;
import org.hsqldb_voltpatches.rowio.RowOutputBinary;
import org.hsqldb_voltpatches.rowio.RowOutputInterface;
import org.hsqldb_voltpatches.types.ArrayType;
import org.hsqldb_voltpatches.types.Type;

/**
 * @author Fred Toussi (fredt@users dot sourceforge dot net)
 * @version 2.3.0
 * @since 2.2.7
 */
public class RowDiskDataChange extends RowAVLDisk {

    public final static int COL_POS_ROW_NUM     = 0;
    public final static int COL_POS_ROW_ID      = 1;
    public final static int COL_POS_TABLE_ID    = 2;
    public final static int COL_POS_SCHEMA_NAME = 3;
    public final static int COL_POS_TABLE_NAME  = 4;
    public final static int COL_POS_IS_UPDATE   = 5;

    //
    final static Type[] arrayType = new Type[]{
        new ArrayType(Type.SQL_INTEGER, Integer.MAX_VALUE) };
    Table    targetTable;
    Object[] updateData;
    int[]    updateColMap;

    /**
     *  Constructor for new Rows.  Variable hasDataChanged is set to true in
     *  order to indicate the data needs saving.
     *
     * @param t table
     * @param data row data
     */
    public RowDiskDataChange(TableBase t, Object[] data,
                             PersistentStore store, Table targetTable) {

        super(t, data, store);

        this.targetTable = targetTable;
    }

    /**
     *  Constructor when read from the disk into the Cache.
     *
     * @param t table
     * @param in data source
     * @throws IOException
     */
    public RowDiskDataChange(Session session, TableBase t,
                             RowInputInterface in) throws IOException {

        super(t, in);

        targetTable = t.database.schemaManager.getTable(session,
                (String) rowData[COL_POS_TABLE_NAME],
                (String) rowData[COL_POS_SCHEMA_NAME]);

        if ((Boolean) rowData[COL_POS_IS_UPDATE]) {
            updateData = in.readData(targetTable.colTypes);

            RowInputBinary bin = (RowInputBinary) in;

            if (bin.readNull()) {
                updateColMap = null;
            } else {
                updateColMap = bin.readIntArray();
            }
        } else {
            updateData   = null;
            updateColMap = null;
        }
    }

    public void write(RowOutputInterface out) {

        writeNodes(out);

        if (hasDataChanged) {
            out.writeData(this, table.colTypes);

            if (updateData != null) {
                Type[] targetTypes = targetTable.colTypes;

                out.writeData(targetTypes.length, targetTypes, updateData,
                              null, null);

                RowOutputBinary bout = (RowOutputBinary) out;

                if (updateColMap == null) {
                    bout.writeNull(Type.SQL_ARRAY_ALL_TYPES);
                } else {
                    bout.writeArray(updateColMap);
                }
            }

            out.writeEnd();

            hasDataChanged = false;
        }
    }

    public Object[] getUpdateData() {
        return updateData;
    }

    public int[] getUpdateColumnMap() {
        return updateColMap;
    }

    public void setTargetTable(Table table) {
        targetTable = table;
    }

    public void setUpdateData(Object[] data) {
        updateData = data;
    }

    public void setUpdateColumnMap(int[] colMap) {
        updateColMap = colMap;
    }

    public int getRealSize(RowOutputInterface out) {

        RowOutputBinary bout = (RowOutputBinary) out;
        int             size = out.getSize(this);

        if (updateData != null) {
            size += bout.getSize(updateData, targetTable.getColumnCount(),
                                 targetTable.getColumnTypes());

            if (updateColMap != null) {
                size += bout.getSize(updateColMap);
            }
        }

        return size;
    }
}
