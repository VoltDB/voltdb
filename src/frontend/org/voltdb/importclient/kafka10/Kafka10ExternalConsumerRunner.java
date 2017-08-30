/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
package org.voltdb.importclient.kafka10;

import org.apache.kafka.clients.consumer.Consumer;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.importer.ImporterLifecycle;
import org.voltdb.utils.CSVDataLoader;
import org.voltdb.utils.RowWithMetaData;

public class Kafka10ExternalConsumerRunner extends Kafka10ConsumerRunner {

    private CSVDataLoader m_loader;

    public Kafka10ExternalConsumerRunner(ImporterLifecycle lifecycle,
            Kafka10StreamImporterConfig config, Consumer<byte[], byte[]> consumer, CSVDataLoader loader) throws Exception {
        super(lifecycle, config, consumer);
        m_loader = loader;
    }

    @Override
    public void invoke(String rawMessage, long offset, Object[] params, ProcedureCallback procedureCallback) throws Exception {
        m_loader.insertRow(new RowWithMetaData(rawMessage, offset, procedureCallback), params);
    }
}
