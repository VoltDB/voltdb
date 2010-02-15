/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.elt;

import org.voltdb.utils.DBBPool.BBContainer;

/** one task for the loader */
public class ELTDataBlock {
    /** The loader MUST discard() all data it accepts from
     *  queue that is NOT successfully handed to BBInputStream.
     */
    public BBContainer m_data;
    public int m_tableId;

    /** A "stop message" instructs the loader to terminate.
      * Stopped loaders are not restarted.
      */
    public boolean isStopMessage() {
        return false;
    }

    /** Create a message with data for a specified table id */
    public ELTDataBlock(BBContainer data, int tid) {
        m_data = data;
        m_tableId = tid;
    }
    public ELTDataBlock() {
        m_data = null;
        m_tableId = -1;
    }
}
