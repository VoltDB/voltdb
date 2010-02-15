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

package org.voltdb.utils;

/**
 * Interface for subsystems that can be quiesced or gracefully shutdown.
 */
public interface SysManageable {

    // valid operStatus results
    String IDLE = "idle";
    String RUNNING = "running";

    /**
     * An idle component, assuming no additional work is queued to it, can
     * be shut down cleanly without loss of data without relying on replication
     * or k-safety.
     * @return SysManageable.IDLE when component is idle with no active or queued work.
     * SysManageable.RUNNING when component is processing or has queued to work.
     */
    public String operStatus();

    /**
     * Instruct the component to quiesce. This may complete asynchronously
     * with the invocation. Poll operStatus() to determine completion.
     */
    public void quiesce();

}
