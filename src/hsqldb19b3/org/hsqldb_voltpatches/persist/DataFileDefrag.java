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


package org.hsqldb_voltpatches.persist;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.TableBase;
import org.hsqldb_voltpatches.lib.DoubleIntIndex;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.StopWatch;
import org.hsqldb_voltpatches.lib.Storage;
import org.hsqldb_voltpatches.navigator.RowIterator;
import org.hsqldb_voltpatches.rowio.RowInputInterface;
import org.hsqldb_voltpatches.rowio.RowOutputBinary;
import org.hsqldb_voltpatches.rowio.RowOutputInterface;

// oj@openoffice.org - changed to file access api

/**
 *  Routine to defrag the *.data file.
 *
 *  This method iterates over the primary index of a table to find the
 *  disk position for each row and stores it, together with the new position
 *  in an array.
 *
 *  A second pass over the primary index writes each row to the new disk
 *  image after translating the old pointers to the new.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version    1.9.0
 * @since      1.7.2
 */
final class DataFileDefrag {

    BufferedOutputStream fileStreamOut;
    long                 fileOffset;
    StopWatch            stopw = new StopWatch();
    String               filename;
    int[][]              rootsList;
    Database             database;
    DataFileCache        cache;
    int                  scale;
    DoubleIntIndex       transactionRowLookup;

    DataFileDefrag(Database db, DataFileCache cache, String filename) {

        this.database = db;
        this.cache    = cache;
        this.scale    = cache.cacheFileScale;
        this.filename = filename;
    }

    void process() throws IOException {

        boolean complete = false;

        Error.printSystemOut("Defrag Transfer begins");

        transactionRowLookup = database.txManager.getTransactionIDList();

        HsqlArrayList allTables = database.schemaManager.getAllTables();

        rootsList = new int[allTables.size()][];

        Storage dest = null;

        try {
            OutputStream fos =
                database.getFileAccess().openOutputStreamElement(filename
                    + ".new");

            fileStreamOut = new BufferedOutputStream(fos, 1 << 12);

            for (int i = 0; i < DataFileCache.INITIAL_FREE_POS; i++) {
                fileStreamOut.write(0);
            }

            fileOffset = DataFileCache.INITIAL_FREE_POS;

            for (int i = 0, tSize = allTables.size(); i < tSize; i++) {
                Table t = (Table) allTables.get(i);

                if (t.getTableType() == TableBase.CACHED_TABLE) {
                    int[] rootsArray = writeTableToDataFile(t);

                    rootsList[i] = rootsArray;
                } else {
                    rootsList[i] = null;
                }

                Error.printSystemOut(t.getName().name + " complete");
            }

            writeTransactionRows();
            fileStreamOut.flush();
            fileStreamOut.close();

            fileStreamOut = null;

            // write out the end of file position
            dest = ScaledRAFile.newScaledRAFile(
                database, filename + ".new", false,
                ScaledRAFile.DATA_FILE_RAF,
                database.getURLProperties().getProperty("storage_class_name"),
                database.getURLProperties().getProperty("storage_key"));

            dest.seek(DataFileCache.LONG_FREE_POS_POS);
            dest.writeLong(fileOffset);
            dest.close();

            dest = null;

            for (int i = 0, size = rootsList.length; i < size; i++) {
                int[] roots = rootsList[i];

                if (roots != null) {
                    Error.printSystemOut(
                        org.hsqldb_voltpatches.lib.StringUtil.getList(roots, ",", ""));
                }
            }

            complete = true;
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, filename + ".new");
        } catch (OutOfMemoryError e) {
            throw Error.error(ErrorCode.OUT_OF_MEMORY);
        } finally {
            if (fileStreamOut != null) {
                fileStreamOut.close();
            }

            if (dest != null) {
                dest.close();
            }

            if (!complete) {
                database.getFileAccess().removeElement(filename + ".new");
            }
        }

        //Error.printSystemOut("Transfer complete: ", stopw.elapsedTime());
    }

    /**
     * called from outside after the complete end of defrag
     */
    void updateTableIndexRoots() {

        HsqlArrayList allTables = database.schemaManager.getAllTables();

        for (int i = 0, size = allTables.size(); i < size; i++) {
            Table t = (Table) allTables.get(i);

            if (t.getTableType() == TableBase.CACHED_TABLE) {
                int[] rootsArray = rootsList[i];

                t.setIndexRoots(rootsArray);
            }
        }
    }

    /**
     * called from outside after the complete end of defrag
     */
    void updateTransactionRowIDs() {
        database.txManager.convertTransactionIDs(transactionRowLookup);
    }

    int[] writeTableToDataFile(Table table) throws IOException {

        Session session = database.getSessionManager().getSysSession();
        PersistentStore    store  = session.sessionData.getRowStore(table);
        RowOutputInterface rowOut = new RowOutputBinary();
        DoubleIntIndex pointerLookup =
            new DoubleIntIndex(table.getPrimaryIndex().sizeEstimate(store),
                               false);
        int[] rootsArray = table.getIndexRootsArray();
        long  pos        = fileOffset;
        int   count      = 0;

        pointerLookup.setKeysSearchTarget();
        Error.printSystemOut("lookup begins: " + stopw.elapsedTime());

        RowIterator it = table.rowIterator(session);

        for (; it.hasNext(); count++) {
            CachedObject row = it.getNextRow();

            pointerLookup.addUnsorted(row.getPos(), (int) (pos / scale));

            if (count % 50000 == 0) {
                Error.printSystemOut("pointer pair for row " + count + " "
                                     + row.getPos() + " " + pos);
            }

            pos += row.getStorageSize();
        }

        Error.printSystemOut(table.getName().name + " list done ",
                             stopw.elapsedTime());

        count = 0;
        it    = table.rowIterator(session);

        for (; it.hasNext(); count++) {
            CachedObject row = it.getNextRow();

            rowOut.reset();
            row.write(rowOut, pointerLookup);
            fileStreamOut.write(rowOut.getOutputStream().getBuffer(), 0,
                                rowOut.size());

            fileOffset += row.getStorageSize();

            if ((count) % 50000 == 0) {
                Error.printSystemOut(count + " rows " + stopw.elapsedTime());
            }
        }

        for (int i = 0; i < rootsArray.length; i++) {
            if (rootsArray[i] == -1) {
                continue;
            }

            int lookupIndex =
                pointerLookup.findFirstEqualKeyIndex(rootsArray[i]);

            if (lookupIndex == -1) {
                throw Error.error(ErrorCode.DATA_FILE_ERROR);
            }

            rootsArray[i] = pointerLookup.getValue(lookupIndex);
        }

        setTransactionRowLookups(pointerLookup);
        Error.printSystemOut(table.getName().name + " : table converted");

        return rootsArray;
    }

    void setTransactionRowLookups(DoubleIntIndex pointerLookup) {

        for (int i = 0, size = transactionRowLookup.size(); i < size; i++) {
            int key         = transactionRowLookup.getKey(i);
            int lookupIndex = pointerLookup.findFirstEqualKeyIndex(key);

            if (lookupIndex != -1) {
                transactionRowLookup.setValue(
                    i, pointerLookup.getValue(lookupIndex));
            }
        }
    }

    void writeTransactionRows() {

        for (int i = 0, size = transactionRowLookup.size(); i < size; i++) {
            if (transactionRowLookup.getValue(i) != 0) {
                continue;
            }

            int key = transactionRowLookup.getKey(i);

            try {
                transactionRowLookup.setValue(i, (int) (fileOffset / scale));

                RowInputInterface rowIn = cache.readObject(key);

                fileStreamOut.write(rowIn.getBuffer(), 0, rowIn.getSize());

                fileOffset += rowIn.getSize();
            } catch (HsqlException e) {}
            catch (IOException e) {}
        }
    }
}
