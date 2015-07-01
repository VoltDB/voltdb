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
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.FileChannel;

public abstract class LogEntry {
    /**
     * Read a single log entry.
     *
     * @param in
     *            The input file channel
     * @return The new log entry, or null if failed
     * @throws IOException
     *             If there is any error reading the log entry
     */
    public static LogEntry readExternal(ByteChannel in) throws IOException {
        throw new UnsupportedOperationException();
    }

    public abstract void writeExternal(FileChannel out) throws IOException;

    public abstract ByteBuffer getAsBuffer() throws IOException;
}