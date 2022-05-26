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

public enum DRIdempotencyResult {
    SUCCESS((byte) 0, false),    // Is the expected next DR ID
    DUPLICATE((byte) -1, true),  // Is a duplicate DR ID seen before
    GAP((byte) 1, true),         // Is way in the future
    AMBIGUOUS((byte) -2, false); // DR was applied to a new partition that did not have a tracker

    private final byte m_id;
    private final boolean m_failure;

    DRIdempotencyResult(byte id, boolean failure) {
        m_id = id;
        m_failure = failure;
    }

    public byte id() {
        return m_id;
    }

    public boolean isFailure() {
        return m_failure;
    }

    public static DRIdempotencyResult fromID(byte id) {
        if (SUCCESS.id() == id) {
            return SUCCESS;
        } else if (DUPLICATE.id() == id) {
            return DUPLICATE;
        } else if (GAP.id() == id) {
            return GAP;
        } else if (AMBIGUOUS.id() == id) {
            return AMBIGUOUS;
        } else {
            throw new IllegalArgumentException("Invalid DRIdempotencyResult ID " + id);
        }
    }
}
