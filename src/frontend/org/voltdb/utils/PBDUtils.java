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

package org.voltdb.utils;

import org.voltcore.utils.DeferredSerialization;

import java.io.IOException;
import java.nio.ByteBuffer;

public class PBDUtils {
    public static int writeDeferredSerialization(ByteBuffer mbuf, DeferredSerialization ds) throws IOException
    {
        int written = 0;
        try {
            final int objSizePosition = mbuf.position();
            mbuf.position(mbuf.position() + PBDSegment.OBJECT_HEADER_BYTES);
            final int objStartPosition = mbuf.position();
            ds.serialize(mbuf);
            written = mbuf.position() - objStartPosition;
            mbuf.putInt(objSizePosition, written);
            mbuf.putInt(objSizePosition + 4, PBDSegment.NO_FLAGS);
        } finally {
            ds.cancel();
        }
        return written;
    }
}
