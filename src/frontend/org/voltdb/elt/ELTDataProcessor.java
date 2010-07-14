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

import org.voltdb.elt.processors.RawProcessor.ELTInternalMessage;
import org.voltdb.logging.VoltLogger;
import org.voltdb.network.InputHandler;

/**
 * Interface ELTManager imposes on processors.
 *
 *  The ELT Manager invokes this interface in known order:
 *    1. The logger is added via addLogger.
 *    2. Table sources are added via addTable
 *    3. readyForData is invoked.
 *    4. process is called for flushed data blocks
 */
public interface ELTDataProcessor  {

    /**
     * Allow the processor access to the ELT logger. Processor may
     * log to this logger to produce ELT category output.
     * @param logger log4j logger created from VoltDB logger factory.
     */
    void addLogger(VoltLogger logger);

    /** Pass the loader each table in each database.
     * This is called for each unique datasource. It may be recalled with
     * new datasources if the catalog changes at runtime.
     * @param database these tables belong to.
     * @param tableName for the database
     * @param tableId corresponding to tableName.
     */
    void addDataSource(ELTDataSource dataSource);

    /**
     * Inform the processor that initialization is complete; commence work.
     */
    public void readyForData();

    /**
     * Queue a work message to the processor's mailbox.
     */
    public void queueMessage(ELTInternalMessage m);

    /**
     * A client has connected. Create an InputHandler for it.
     * @param service The service requested.
     * @returns InputHandler or null if unable to create an input handler for the service.
     */
    public InputHandler createInputHandler(String service);

    /**
     * The system is terminating. Cleanup and exit the processor.
     */
    public void shutdown();

    /**
     * Allow connectors to claim responsibility for a service
     * @param service  Service, see wire protocol login message.
     * @return  true if connector implements the service.
     */
    boolean isConnectorForService(String service);
}
