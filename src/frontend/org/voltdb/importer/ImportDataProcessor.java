/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.importer;

import java.util.Map;
import java.util.Properties;

import org.voltcore.messaging.HostMessenger;
import org.voltdb.CatalogContext;
import org.voltdb.utils.CatalogUtil.ImportConfiguration;

/**
 * Interface ImportDataProcessor imposes on processors.
 *
 */
public interface ImportDataProcessor  {

    public static final String IMPORT_MODULE = "__IMPORT_MODULE__";
    public static final String IMPORT_FORMATTER = "__IMPORT_FORMATTER__";
    public static final String IMPORT_PROCEDURE = "procedure";
    public static final String IMPORTER_CLASS = "impl";
    public static final String IMPORTER_SERVICE_CLASS = "org.voltdb.importer.ImportHandlerProxy";
    /**
     * Inform the processor that initialization is complete; commence work.
     * @param context
     * @param messenger to get handle to zookeeper
     */
    public void readyForData(CatalogContext context, HostMessenger messenger);

    /**
     * The system is terminating. Cleanup and exit the processor.
     */
    public void shutdown();

    /**
     * Pass processor specific processor configuration properties
     * @param context current catalog context
     * @param config an instance of {@linkplain Properties}
     */
    public void setProcessorConfig(CatalogContext context, Map<String, ImportConfiguration> config);

    public int getPartitionsCount();

}
