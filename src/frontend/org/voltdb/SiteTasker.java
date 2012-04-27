/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb;

import org.voltdb.jni.ExecutionEngine;

public abstract class SiteTasker {

    /**
     * Run executes the task. Run is called on the ExecutionSite thread
     * and has exclusive access to the ee. Tasks are not preempted.
     */
    abstract public void run(ExecutionEngine ee);

    /**
     * Priority returns the relative task priority. 0 is the highest
     * priority.
     */
    abstract public int priority();

    // the scheduler guarntees equal priority tasks run in
    // insertion order. m_seq disambiguates equal priority tasks.
    private long m_seq;

    void setSeq(long seq)
    {
        m_seq = seq;
    }

    long seq()
    {
        return m_seq;
    }
}
