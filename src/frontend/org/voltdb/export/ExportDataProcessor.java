/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.export;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.voltcore.utils.Pair;
import org.voltdb.exportclient.ExportClientBase;

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

    public static final String EXPORT_TO_TYPE = "__EXPORT_TO_TYPE__";

    void setExportGeneration(Generation generation);

    /**
     * Get export client from processor, used by initializing export data source
     * @param tableName
     * @return
     */
    public ExportClientBase getExportClient(String tableName);

    /**
     * Inform the processor that initialization is complete; commence work.
     */
    public void readyForData();

    /**
     * Allows processor to initiate polling
     */
    public void startPolling();

    /**
     * The system is terminating. Cleanup and exit the processor.
     */
    public void shutdown();

    /**
     * Pass processor specific processor configuration properties
     * @param config an instance of {@linkplain Properties}
     */
    public void setProcessorConfig(Map<String, Pair<Properties, Set<String>>> config);

    /**
     * Pass processor specific processor configuration properties for checking
     * @param config an instance of {@linkplain Properties}
     */
    public void checkProcessorConfig(Properties config);
}
