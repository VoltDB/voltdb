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

package org.voltdb;

import java.io.IOException;

public interface NodeDRGateway {

    /**
     * Start listening on the ports
     */
    public abstract void bindPorts();

    /**
     * Called by an EE to make the buffer server is aware it's going to be
     * handling buffers from a specific partition.
     * Call this at startup before sending buffers.
     * @param partitionId id of the initializing partition.
     * @throws IOException
     */
    public abstract void initForSite(int partitionId) throws IOException;

    /**
     * @param ib This is really the invocation buffer
     * @return
     */
    public abstract boolean offer(final Object ib);

    public abstract boolean isActive();
    public abstract void setActive(boolean active);

    public abstract void start();
    public abstract void shutdown() throws InterruptedException;

}