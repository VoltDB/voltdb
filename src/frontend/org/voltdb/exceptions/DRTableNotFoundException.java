/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.exceptions;

import java.nio.ByteBuffer;

import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;

/**
 * Exception thrown by native Execution Engine
 * when a table cannot be found for DR table hash that come from the remote cluster
 */
public class DRTableNotFoundException extends SerializableException {
    public static final long serialVersionUID = 0L;

    private long m_hash; // the hash value from the remote cluster for which it failed
    private long m_remoteTxnUniqueId; // the remote cluster's txn id
    private int m_catalogVersion; // local catalog version when this error was generated

    public DRTableNotFoundException() {
        super();
    }

    public DRTableNotFoundException(ByteBuffer b) {
        super(b);
        this.m_hash = b.getLong();
        this.m_remoteTxnUniqueId = b.getLong();
        this.m_catalogVersion = b.getInt();
    }

    @Override
    protected SerializableExceptions getExceptionType() {
        return SerializableExceptions.DrTableNotFoundException;
    }

    public void setRemoteTxnUniqueId(long remoteTxnUniqueId) {
        m_remoteTxnUniqueId = remoteTxnUniqueId;
    }

    public void setCatalogVersion(int catalogVersion) {
        m_catalogVersion = catalogVersion;
    }

    @Override
    public void setClientResponseResults(ClientResponseImpl cr) {
        cr.setResultTables(new VoltTable[] { getClientResponseTable(m_remoteTxnUniqueId, m_catalogVersion) });
    }

    public static VoltTable getClientResponseTable(long remoteTxnUniqueId, int catalogVersion) {
        ColumnInfo[] resultColumns = new ColumnInfo[] {
                new ColumnInfo("SOURCE_UNIQUEID", VoltType.BIGINT),
                new ColumnInfo("CATALOG_VERSION", VoltType.INTEGER)
        };
        VoltTable result = new VoltTable(resultColumns);
        result.addRow(remoteTxnUniqueId, catalogVersion);
        return result;
    }

    @Override
    protected int p_getSerializedSize() {
        return 20;
    }

    @Override
    protected void p_serializeToBuffer(ByteBuffer b) {
        b.putLong(m_hash);
        b.putLong(m_remoteTxnUniqueId);
        b.putInt(m_catalogVersion);
    }

    @Override
    public byte getClientResponseStatus() {
        return ClientResponse.DR_TABLE_HASH_NOT_FOUND;
    }

    @Override
    public String getShortStatusString() {
        return "TABLE NOT FOUND FOR REMOTE TABLE HASH";
    }
}
