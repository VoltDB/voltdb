/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.export;

import java.io.IOException;

import org.voltdb.messaging.FastSerializer;

public class ExportAdvertisement {

    final long m_generationId;
    final long m_partitionId;
    final String m_signature;

    public ExportAdvertisement(long generation, int partition, String signature) {
        m_generationId = generation;
        m_partitionId = partition;
        m_signature = signature;
    }

    public void serialize(FastSerializer fs) throws IOException {
        String advert = m_generationId + "-" + m_partitionId + "-" + m_signature;
        fs.writeString(advert);
    }
}

