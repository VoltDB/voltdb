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

package org.voltdb;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import org.apache.hadoop_voltpatches.util.PureJavaCrc32C;

public class HybridCrc32 extends PureJavaCrc32C {
    private static final int NATIVE_REDIRECT_SIZE = 150;


    @Override
    public void update(byte[] b) {
        if (b.length > NATIVE_REDIRECT_SIZE) {
            CRC32 nativeCRC = new CRC32();
            nativeCRC.update(b, 0, b.length);
            int crc = (int) nativeCRC.getValue();
            update(crc);
        }
        else {
            super.update(b, 0, b.length);
        }
    }

    public void update(ByteBuffer b) {
        if (b.remaining() > NATIVE_REDIRECT_SIZE) {
          CRC32 nativeCRC = new CRC32();
          nativeCRC.update(b);
          update((int) nativeCRC.getValue());
          return;
        }
        int len = b.remaining();
        int localCrc = crc;
        while(len > 7) {
          int c0 = b.get() ^ localCrc;
          int c1 = b.get() ^ (localCrc >>>= 8);
          int c2 = b.get() ^ (localCrc >>>= 8);
          int c3 = b.get() ^ (localCrc >>>= 8);
          localCrc = (T8_7[c0 & 0xff] ^ T8_6[c1 & 0xff])
              ^ (T8_5[c2 & 0xff] ^ T8_4[c3 & 0xff]);

          localCrc ^= (T8_3[b.get() & 0xff] ^ T8_2[b.get() & 0xff])
               ^ (T8_1[b.get() & 0xff] ^ T8_0[b.get() & 0xff]);

          len -= 8;
        }
        while(len > 0) {
          localCrc = (localCrc >>> 8) ^ T8_0[(localCrc ^ b.get()) & 0xff];
          len--;
        }

        // Publish crc out to object
        crc = localCrc;
    }

    @Override
    public void update(byte[] b, int off, int len) {
        if (len > NATIVE_REDIRECT_SIZE) {
            CRC32 nativeCRC = new CRC32();
            nativeCRC.update(b, off, len);
            update((int) nativeCRC.getValue());
        }
        else {
            super.update(b, off, len);
        }
    }

    public void updateFromPosition(int off, ByteBuffer b) {
        b.limit(b.position());
        b.position(off);
        update(b);
        assert(b.remaining() == 0);
        b.limit(b.capacity());
    }
}
