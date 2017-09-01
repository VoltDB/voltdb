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
import org.voltdb.importer.AbstractImporter;
import org.voltdb.importer.Invocation;

public class Kafka10InternalConsumerRunner extends Kafka10ConsumerRunner {

    private AbstractImporter m_importer;

    public Kafka10InternalConsumerRunner(AbstractImporter importer, Kafka10StreamImporterConfig config, Consumer<byte[], byte[]> consumer) throws Exception {
        super(importer, config, consumer);
        m_importer = importer;
    }

    @Override
    public boolean invoke(String rawMessage, long offset, Object[] params, ProcedureCallback procedureCallback) throws Exception {
        return m_importer.callProcedure(new Invocation(m_config.getProcedure(), params), procedureCallback);
    }
}
