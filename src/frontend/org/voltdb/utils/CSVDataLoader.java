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

package org.voltdb.utils;

import org.voltdb.VoltType;
import org.voltdb.client.NoConnectionsException;

import java.util.concurrent.ExecutionException;

/**
 * The interface CSVLoader uses to insert rows into the database.
 */
public interface CSVDataLoader {
    /**
     * @return The column types of the table or the procedure that will be used by this loader.
     */
    public VoltType[] getColumnTypes();

    /**
     * Insert a single row.
     *
     * @param metaData The metadata of the line in the CSV file, can be used to generate error
     *                 messages.
     * @param values   The values to insert.
     * @throws InterruptedException
     */
    public void insertRow(RowWithMetaData metaData, Object[] values) throws InterruptedException;

    /**
     * Close the loader.
     * @throws InterruptedException
     * @throws NoConnectionsException
     */
    public void close() throws Exception;

    /**
     * @return The number of rows processed, including successfully inserted and failed ones.
     */
    public long getProcessedRows();

    /**
     * @return The number of rows failed to be inserted.
     */
    public long getFailedRows();

    public void setFlushInterval(int delay, int seconds);

    /**
     * Flush use this only when you think you are done and want to push everything before close/quit.
     */
    public void flush() throws ExecutionException, InterruptedException;
}
