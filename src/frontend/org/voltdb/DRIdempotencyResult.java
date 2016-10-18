/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
    SUCCESS((byte) 0),    // Is the expect next DR ID
    DUPLICATE((byte) -1), // Is a duplicate DR ID seen before
    GAP((byte) 1);        // Is way in the future

    private final byte m_id;
    DRIdempotencyResult(byte id) {
        m_id = id;
    }

    public byte id() {
        return m_id;
    }

    public static DRIdempotencyResult fromID(byte id) {
        if (SUCCESS.id() == id) {
            return SUCCESS;
        } else if (DUPLICATE.id() == id) {
            return DUPLICATE;
        } else if (GAP.id() == id) {
            return GAP;
        } else {
            throw new IllegalArgumentException("Invalid DRIdempotencyResult ID " + id);
        }
    }
}
