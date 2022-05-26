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
package org.voltdb.largequery;

import java.math.BigInteger;

/**
 * This class serves mostly as keys to the map from
 * siteId/blockId pairs to file names.  It's also used
 * to transmit these values to the LargeBlockManager
 * API operations.
 *
 * Each BlockId object is essentially a Pair<long, long>.
 */
public class BlockId {
    public BlockId(long siteId, long blockId) {
        m_siteId = siteId;
        m_blockId = blockId;
    }

    private final long m_siteId;
    private final long m_blockId;
    public final long getSiteId() {
        return m_siteId;
    }
    public final long getBlockId() {
        return m_blockId;
    }

    /**
     * Return a string of the form "siteId::blockId".  This is
     * used for display.
     *
     * @return A string of the form "SID::BLOCKID".
     */
    @Override
    public String toString() {
        return Long.toString(m_siteId) + "::" + Long.toString(m_blockId);
    }
    /**
     * Return a string of the form "siteId___blockId".  This is used
     * for file names and not for displaying in, say, error messages.
     * It would be weird to have a file names with minus signs,
     * so format the IDs as unsigned.
     *
     * @return A string of the form "SID___BLOCKID.block" where SID and BLOCKID are unsigned.
     */
    public String fileNameString() {
        BigInteger bigSiteId = BigInteger.valueOf(getSiteId());
        BigInteger bigBlockCounter = BigInteger.valueOf(getBlockId());
        final BigInteger BIT_64 = BigInteger.ONE.shiftLeft(64);

        if(bigSiteId.signum() < 0) {
            bigSiteId = bigSiteId.add(BIT_64);
        }
        if(bigBlockCounter.signum() < 0) {
            bigBlockCounter = bigBlockCounter.add(BIT_64);
        }
        return bigSiteId.toString() + "___" + bigBlockCounter.toString() + ".block";
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if ( ! (other instanceof BlockId)) {
            return false;
        }
        BlockId bOther = (BlockId)other;
        return (m_siteId == bOther.getSiteId() && m_blockId == bOther.getBlockId());
    }

    @Override
    public int hashCode() {
        // Josh Bloch's recipe for hashCode values.
        // Somewhere on StackOverflow.
        int result = 17;
        int sh = (int)(m_siteId ^ (m_siteId >>> 32));
        result = 32 + result * sh;

        int ch = (int)(m_blockId ^ (m_blockId >>> 32));
        result = 32 + result * ch;

        return result;
    }
 }

