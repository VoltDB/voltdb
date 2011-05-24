/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.export;

import org.voltdb.logging.VoltLogger;
import org.voltdb.network.InputHandler;

/**
 * Interface ExportManager imposes on processors.
 *
 *  The Export Manager invokes this interface in known order:
 *    1. The logger is added via addLogger.
 *    2. Table sources are added via addTable
 *    3. readyForData is invoked.
 *    4. process is called for flushed data blocks
 */
public interface ExportDataProcessor  {

    /**
     * Allow the processor access to the Export logger. Processor may
     * log to this logger to produce Export category output.
     * @param logger log4j logger created from VoltDB logger factory.
     */
    void addLogger(VoltLogger logger);

    void setExportGeneration(ExportGeneration generation);

    /**
     * Inform the processor that initialization is complete; commence work.
     */
    public void readyForData();

    /**
     * Queue a work message to the processor's mailbox.
     */
    public void queueWork(Runnable r);

    /**
     * A client has connected. Create an InputHandler for it.
     * @param service The service requested.
     * @param isAdminPort Whether or not the client is connecting on the admin port
     * @returns InputHandler or null if unable to create an input handler for the service.
     */
    public InputHandler createInputHandler(String service, boolean isAdminPort);

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

    void bootClient();

}
