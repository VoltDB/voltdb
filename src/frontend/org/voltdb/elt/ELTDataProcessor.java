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

import org.apache.log4j.Logger;

/**
 * Interface ELTManager imposes on processors.
 *
 *  The ELT Manager invokes this interface in known order:
 *    1. The logger is added via addLogger.
 *    2. Destination hosts are added via addHost.
 *    3. Table sources are added via addTable
 *    4. readyForData is invoked.
 *    5. process is called for flushed data blocks
 *
 *    isIdle may be invoked at any time. A processor should
 *    report idle if immediate termination would not cause loss of data
 *    queued to it via the process method.
 */
public interface ELTDataProcessor  {

    /**
     * Allow the processor access to the ELT logger. Processor may
     * log to this logger to produce ELT category output.
     * @param logger log4j logger created from VoltDB logger factory.
     */
    void addLogger(Logger logger);

    /** Allow the loader to establish any required network connections
     *  Called once for each ELT host entry in the catalog.
     *  @param host or ip address of destination.
     *  @param port number of destination.
     *  @param database name being connected to.
     *  @param username configured username from project.xml
     */
    void addHost(String url, String database,
                String username, String password);

    /** Pass the loader each table in each database.
     * Called once for each table in the catalog.
     * @param database these tables belong to.
     * @param tableName for the database
     * @param tableId corresponding to tableName.
     */
    void addTable(String database, String tableName, int tableId);

    /**
     * Inform the processor that work may start arriving. Initialization
     * is complete.
     */
    public void readyForData();

    /**
     * Pass a block to be processed to the loader. The loader must successfully
     * transfer ownership of the block (to a Connection.writeStream(), for example)
     * OR release the block's BBContainer. Not taking one of these actions
     * is a memory leak. Even when returning false to this method, the processor
     * must discard BBContainer's data.
     *
     * This method is reentrant: multiple execution sites can call it
     * concurrently.
     * @param block
     * @return false if the data was rejected by the processor
     */
    public boolean process(final ELTDataBlock block);

    /**
     * Query if the processor is idle.
     * @return true if all invocations of process() are completed.
     */
    public boolean isIdle();
}
