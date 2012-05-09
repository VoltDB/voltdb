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
package org.voltdb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.Callable;

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.Pair;
import org.voltdb.utils.VoltTableUtil;
import org.voltcore.utils.DBBPool.BBContainer;

/*
 * Filter that converts snapshot data to CSV format
 */
public class CSVSnapshotFilter implements SnapshotDataFilter {
    private final char m_delimiter;
    private final char m_fullDelimiters[];
    private final byte m_schemaBytes[];
    private final ArrayList<VoltType> m_columnTypes;
    private int m_lastNumCharacters = 64 * 1024;

    public CSVSnapshotFilter(
            VoltTable vt,
            char delimiter,
            char fullDelimiters[]) {
        m_columnTypes = new ArrayList<VoltType>(vt.getColumnCount());
        for (int ii = 0; ii < vt.getColumnCount(); ii++) {
            m_columnTypes.add(vt.getColumnType(ii));
        }
        m_fullDelimiters = fullDelimiters;
        m_delimiter = delimiter;
        m_schemaBytes = vt.getSchemaBytes();
    }

    @Override
    public Callable<BBContainer> filter(final Callable<BBContainer> input) {
        return new Callable<BBContainer>() {
            @Override
            public BBContainer call() throws Exception {
                final BBContainer cont = input.call();
                if (cont == null) {
                    return null;
                }
                try {
                    ByteBuffer buf = ByteBuffer.allocate(m_schemaBytes.length + cont.b.remaining());
                    buf.put(m_schemaBytes);
                    final int rowCountPosition = buf.position();
                    buf.position(rowCountPosition + 4);
                    cont.b.position(12);
                    cont.b.limit(cont.b.limit() - 4);
                    buf.put(cont.b);
                    cont.b.limit(cont.b.limit() + 4);
                    final int rowCount = cont.b.getInt();
                    buf.putInt(rowCountPosition, rowCount);

                    VoltTable vt = PrivateVoltTableFactory.createVoltTableFromBuffer(buf, true);
                    Pair<Integer, byte[]> p =
                                    VoltTableUtil.toCSV(
                                            vt,
                                            m_columnTypes,
                                            m_delimiter,
                                            m_fullDelimiters,
                                            m_lastNumCharacters);
                    m_lastNumCharacters = p.getFirst();
                    return DBBPool.wrapBB(ByteBuffer.wrap(p.getSecond()));
                } finally {
                    cont.discard();
                }
            }
        };
    }

}
