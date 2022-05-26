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

package org.voltdb.importer;

import java.util.Map;
import java.util.Properties;

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

    //used for kafka 10
    static final String KAFKA10_PROCEDURES = "import_kafka_procedures";
    static final String KAFKA10_FORMATTERS = "import_kafka_formatters";
    static final String VOLTDB_HOST_COUNT = "voltdb.host.count";
    static final String KAFKA10_CONSUMER_COUNT = "kafka.consumer.count";
    static final String POLL_TIMEOUT_MS = "poll.timeout.ms";

    /**
     * Inform the processor that initialization is complete; commence work.
     */
    public void readyForData();

    /**
     * The system is terminating. Cleanup and exit the processor.
     */
    public void shutdown();

    /**
     * Pass processor specific processor configuration properties
     * @param config an instance of {@linkplain Properties}
     * @param list of abstract importer factory
     */

    public void setProcessorConfig(Map<String, ImportConfiguration> config, final Map<String, AbstractImporterFactory> importers);

    public int getPartitionsCount();

}
