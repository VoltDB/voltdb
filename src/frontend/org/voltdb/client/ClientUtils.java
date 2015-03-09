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

package org.voltdb.client;

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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Helper methods duplicated from MiscUtils to avoid linking with
 * some of the stuff MiscUtils brings in.
 *
 */
public class ClientUtils {

    /**
     * I heart commutativity
     * @param buffer ByteBuffer assumed position is at end of data
     * @return the cheesy checksum of this VoltTable
     */
    public static final long cheesyBufferCheckSum(ByteBuffer buffer) {
        final int mypos = buffer.position();
        buffer.position(0);
        long checksum = 0;
        if (buffer.hasArray()) {
            final byte bytes[] = buffer.array();
            final int end = buffer.arrayOffset() + mypos;
            for (int ii = buffer.arrayOffset(); ii < end; ii++) {
                checksum += bytes[ii];
            }
        } else {
            for (int ii = 0; ii < mypos; ii++) {
                checksum += buffer.get();
            }
        }
        buffer.position(mypos);
        return checksum;
    }

    /**
     * Serialize a file into bytes. Used to serialize catalog and deployment
     * file for UpdateApplicationCatalog on the client.
     *
     * @param path
     * @return a byte array of the file
     * @throws IOException
     *             If there are errors reading the file
     */
    public static byte[] fileToBytes(File path) throws IOException {
        FileInputStream fin = new FileInputStream(path);
        byte[] buffer = new byte[(int) fin.getChannel().size()];
        try {
            if (fin.read(buffer) == -1) {
                throw new IOException("File " + path.getAbsolutePath() + " is empty");
            }
        } finally {
            fin.close();
        }
        return buffer;
    }
}
