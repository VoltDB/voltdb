/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
package org.voltdb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.Callable;

import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.utils.VoltTableUtil;

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
        m_schemaBytes = PrivateVoltTableFactory.getSchemaBytes(vt);
    }

    @Override
    public Callable<BBContainer> filter(final Callable<BBContainer> input) {
        return new Callable<BBContainer>() {
            @Override
            public BBContainer call() throws Exception {
                BBContainer cont = input.call();
                if (cont == null) {
                    return null;
                }
                try {
                    ByteBuffer buf = ByteBuffer.allocate(m_schemaBytes.length + cont.b().remaining() - 4);
                    buf.put(m_schemaBytes);
                    cont.b().position(4);
                    buf.put(cont.b());

                    VoltTable vt = PrivateVoltTableFactory.createVoltTableFromBuffer(buf, true);
                    Pair<Integer, byte[]> p =
                                    VoltTableUtil.toCSV(
                                            vt,
                                            m_columnTypes,
                                            m_delimiter,
                                            m_fullDelimiters,
                                            m_lastNumCharacters);
                    m_lastNumCharacters = p.getFirst();
                    final BBContainer origin = cont;
                    cont = null;
                    return new BBContainer( ByteBuffer.wrap(p.getSecond())) {
                        @Override
                        public void discard() {
                            checkDoubleFree();
                            origin.discard();
                        }
                    };
                } finally {
                    if (cont != null) {
                        cont.discard();
                    }
                }
            }
        };
    }

}
