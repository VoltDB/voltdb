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

import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.TableBase;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.lib.LongKeyHashMap;
import org.hsqldb_voltpatches.HsqlException;

/**
 * Collection of PersistenceStore itmes currently used by a session.
 * An item is retrieved based on key returned by
 * TableBase.getPersistenceId().
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class PersistentStoreCollectionSession
implements PersistentStoreCollection {

    private final Session        session;
    private final LongKeyHashMap rowStoreMapSession     = new LongKeyHashMap();
    private LongKeyHashMap       rowStoreMapTransaction = new LongKeyHashMap();
    private LongKeyHashMap       rowStoreMapStatement   = new LongKeyHashMap();

    public PersistentStoreCollectionSession(Session session) {
        this.session = session;
    }

    public void setStore(Object key, PersistentStore store) {

        TableBase table = (TableBase) key;

        switch (table.persistenceScope) {

            case TableBase.SCOPE_STATEMENT :
                if (store == null) {
                    rowStoreMapStatement.remove(table.getPersistenceId());
                } else {
                    rowStoreMapStatement.put(table.getPersistenceId(), store);
                }
                break;

            case TableBase.SCOPE_TRANSACTION :
                if (store == null) {
                    rowStoreMapTransaction.remove(table.getPersistenceId());
                } else {
                    rowStoreMapTransaction.put(table.getPersistenceId(),
                                               store);
                }
                break;

            case TableBase.SCOPE_SESSION :
                if (store == null) {
                    rowStoreMapSession.remove(table.getPersistenceId());
                } else {
                    rowStoreMapSession.put(table.getPersistenceId(), store);
                }
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "PersistentStoreCollection");
        }
    }

    public PersistentStore getStore(Object key) {

        try {
            TableBase       table = (TableBase) key;
            PersistentStore store;

            switch (table.persistenceScope) {

                case TableBase.SCOPE_STATEMENT :
                    store = (PersistentStore) rowStoreMapStatement.get(
                        table.getPersistenceId());

                    if (store == null) {
                        store = session.database.logger.newStore(session,
                                this, table, true);
                    }

                    return store;

                case TableBase.SCOPE_TRANSACTION :
                    store = (PersistentStore) rowStoreMapTransaction.get(
                        table.getPersistenceId());

                    if (store == null) {
                        store = session.database.logger.newStore(session,
                                this, table, true);
                    }

                    return store;

                case TableBase.SCOPE_SESSION :
                    store = (PersistentStore) rowStoreMapSession.get(
                        table.getPersistenceId());

                    if (store == null) {
                        store = session.database.logger.newStore(session,
                                this, table, true);
                    }

                    return store;
            }
        } catch (HsqlException e) {}

        throw Error.runtimeError(ErrorCode.U_S0500, "PSCS");
    }

    public void clearAllTables() {

        clearSessionTables();
        clearTransactionTables();
        clearStatementTables();
    }

    public void clearResultTables(long actionTimestamp) {

        if (rowStoreMapSession.isEmpty()) {
            return;
        }

        Iterator it = rowStoreMapSession.values().iterator();

        while (it.hasNext()) {
            RowStoreAVL store = (RowStoreAVL) it.next();

            if (store.timestamp == actionTimestamp) {

                store.release();
            }
        }
    }

    public void clearSessionTables() {

        if (rowStoreMapSession.isEmpty()) {
            return;
        }

        Iterator it = rowStoreMapSession.values().iterator();

        while (it.hasNext()) {
            PersistentStore store = (PersistentStore) it.next();

            store.release();
        }
    }

    public void clearTransactionTables() {

        if (rowStoreMapTransaction.isEmpty()) {
            return;
        }

        Iterator it = rowStoreMapTransaction.values().iterator();

        while (it.hasNext()) {
            PersistentStore store = (PersistentStore) it.next();

            store.release();
        }
    }

    public void clearStatementTables() {

        if (rowStoreMapStatement.isEmpty()) {
            return;
        }

        Iterator it = rowStoreMapStatement.values().iterator();

        while (it.hasNext()) {
            PersistentStore store = (PersistentStore) it.next();

            store.release();
        }
    }
}
