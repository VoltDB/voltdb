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
import org.voltdb.elt.processors.RawProcessor.ELTInternalMessage;

/**
 * Interface ELTManager imposes on processors.
 *
 *  The ELT Manager invokes this interface in known order:
 *    1. The logger is added via addLogger.
 *    2. Table sources are added via addTable
 *    3. readyForData is invoked.
 *    4. process is called for flushed data blocks
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

    /** Pass the loader each table in each database.
     * Called once for each table in the catalog.
     * @param database these tables belong to.
     * @param tableName for the database
     * @param tableId corresponding to tableName.
     */
    void addDataSource(ELTDataSource dataSource);

    /**
     * Inform the processor that work may start arriving. Initialization
     * is complete.
     */
    public void readyForData();

    /**
     * Query if the processor is idle.
     * @return true if all polled work is complete.
     */
    public boolean isIdle();

    /**
     * Queue a work message to the processor's mailbox.
     */
    public void queueMessage(ELTInternalMessage m);
}
