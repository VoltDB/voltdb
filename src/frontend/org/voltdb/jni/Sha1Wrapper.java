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

package org.voltdb.jni;

import java.util.Arrays;

import org.voltdb.utils.Encoder;

import com.google_voltpatches.common.primitives.Ints;

/**
 * Turns a 20-byte SHA-1 hash into something you can use in a tree or
 * hash-based collection.
 *
 */
public class Sha1Wrapper implements Comparable<Sha1Wrapper> {
    public final byte[] hashBytes;

    public Sha1Wrapper(byte[] hashBytes) {
        assert(hashBytes != null);
        assert(hashBytes.length == 20); // sha1 is 20b
        this.hashBytes = hashBytes;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if ((obj instanceof Sha1Wrapper) == false) return false;

        return Arrays.equals(hashBytes, ((Sha1Wrapper) obj).hashBytes);
    }

    @Override
    public int hashCode() {
        return Ints.fromByteArray(hashBytes);
    }

    @Override
    public String toString() {
        return Encoder.hexEncode(hashBytes);
    }

    /** Not totally sure if this is a sensible ordering */
    @Override
    public int compareTo(Sha1Wrapper arg0) {
        if (arg0 == null) return 1;
        for (int i = 0; i < 20; i++) {
            int cmp = hashBytes[i] - arg0.hashBytes[i];
            if (cmp != 0) return cmp;
        }
        return 0;
    }
}
