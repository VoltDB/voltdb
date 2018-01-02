/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.iv2;

import java.nio.ByteBuffer;

import org.voltcore.logging.VoltLogger;
import org.voltdb.HybridCrc32;

/**
 * This class expands the determinism hash with an array
 * of hashes. For speed and memory reasons, this class is a simple int
 * array, even though it's really three ints and then an array of int pairs.
 *
 * The first int is an overall hash, equivalent to the old hash value, but
 * also including the catalog version of the procedure.
 *
 * Then catalog version (not hashed), then the number of DML statements run
 * by the proc (not hashed).
 *
 * Then a list of pairs, up to MAX_STATEMENTS_WITH_DETAIL long.
 *  a) The hash of the SQL text
 *  b) The hash of the parameter values for that SQL statement.
 *
 * This array is passed around in ClientResponseImpl internally, where we
 * used to pass a single long.
 *
 * It is not yet used for replay or command logging.
 *
 * Use the static helper function in this class to check two arrays and print
 * helpful output.
 */
public class DeterminismHash {

    protected static final VoltLogger m_tmLog = new VoltLogger("TM");

    // HEADER IS:
    // 1) total hash
    // 2) catalog version
    // 3) hash count (each statement has two hashes: SQL hash and parameter hash)
    public final static int HEADER_OFFSET = 3;

    public final static int MAX_HASHES_COUNT = Integer.getInteger("MAX_STATEMENTS_WITH_DETAIL", 32) * 2;

    int m_catalogVersion = 0;
    int m_hashCount = 0;

    final int[] m_hashes = new int[MAX_HASHES_COUNT + HEADER_OFFSET];

    protected final HybridCrc32 m_inputCRC = new HybridCrc32();

    public void reset(int catalogVersion) {
        m_catalogVersion = catalogVersion;
        m_inputCRC.reset();
        m_hashCount = 0;
    }

    /**
     * Serialize the running hashes to an array and complete the overall
     * hash for the first int value in the array.
     */
    public int[] get() {
        int includedHashes = Math.min(m_hashCount, MAX_HASHES_COUNT);
        int[] retval = new int[includedHashes + HEADER_OFFSET];
        System.arraycopy(m_hashes, 0, retval, HEADER_OFFSET, includedHashes);

        m_inputCRC.update(m_hashCount);
        m_inputCRC.update(m_catalogVersion);
        retval[0] = (int) m_inputCRC.getValue();
        retval[1] = m_catalogVersion;
        retval[2] = m_hashCount;
        return retval;
    }

    /**
     * Update the overall hash. Add a pair of ints to the array
     * if the size isn't too large.
     */
    public void offerStatement(int stmtHash, int offset, ByteBuffer psetBuffer) {
        m_inputCRC.update(stmtHash);
        m_inputCRC.updateFromPosition(offset, psetBuffer);

        if (m_hashCount < MAX_HASHES_COUNT) {
            m_hashes[m_hashCount] = stmtHash;
            m_hashes[m_hashCount + 1] = (int) m_inputCRC.getValue();
        }
        m_hashCount += 2;
    }

    /**
     * Compare two hash arrays return true if the same.
     *
     * For now, just compares first integer value in array.
     */
    public static boolean compareHashes(int[] leftHashes, int[] rightHashes) {
        assert(leftHashes != null);
        assert(rightHashes != null);
        assert(leftHashes.length >= 3);
        assert(rightHashes.length >= 3);

        return leftHashes[0] == rightHashes[0];
    }

    /**
     * Log the contents of the hash array
     */
    public static String description(int[] hashes) {
        assert(hashes != null);
        assert(hashes.length >= 3);
        StringBuilder sb = new StringBuilder();

        sb.append("Full Hash ").append(hashes[0]);
        sb.append(", Catalog Version ").append(hashes[1]);
        sb.append(", Statement Count ").append(hashes[2] / 2);

        int includedHashes = Math.min(hashes[2], MAX_HASHES_COUNT);
        for (int i = HEADER_OFFSET; i < HEADER_OFFSET + includedHashes; i += 2) {
            sb.append("\n  Ran Statement ").append(hashes[i]);
            sb.append(" with Parameters ").append(hashes[i + 1]);
        }
        if (hashes[2] > MAX_HASHES_COUNT) {
            sb.append("\n  Additional SQL statements truncated");
        }
        return sb.toString();
    }
}
