/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
package org.voltdb.importclient.kafka;

import java.net.URI;

import org.voltdb.importclient.kafka.util.ProcedureInvocationCallback;
import org.voltdb.importer.AbstractImporter;
import org.voltdb.importer.ImporterLifecycle;
import org.voltdb.importer.ImporterLogger;
import org.voltdb.importer.Invocation;

/**
 * Implementation that imports from a single partition of a Kafka topic.
 */

public class KafkaTopicPartitionImporter extends AbstractImporter
{
    private VoltInternalTopicPartitionImporter delegate;
    private boolean hasTransaction = true;

    private static String KAFKA_IMPORTER_NAME = "KafkaImporter";

    public KafkaTopicPartitionImporter(KafkaStreamImporterConfig config) {
        delegate = new VoltInternalTopicPartitionImporter(config, this, this);
    }

    @Override
    public String getName()  {
        return KAFKA_IMPORTER_NAME;
    }

    @Override
    public String getTaskThreadName() {
        return getName() + " - " + (delegate.m_topicAndPartition == null ? "Unknown" : delegate.m_topicAndPartition.toString());
    }

    @Override
    public void stop() {
        delegate.stop();
    }

    @Override
    public URI getResourceID() {
        return delegate.getResourceID();
    }

    @Override
    protected void accept() {
        delegate.accept();
    }

    @Override
    public boolean hasTransaction() {
        return hasTransaction;
    }

    public void setTransaction(boolean flag) {
        hasTransaction = flag;
    }

    class VoltInternalTopicPartitionImporter extends BaseKafkaTopicPartitionImporter {

        public VoltInternalTopicPartitionImporter(KafkaStreamImporterConfig config, ImporterLifecycle lifecycle, ImporterLogger logger) {
            super(config, lifecycle, logger);
        }

        @Override
        public boolean invoke(Object[] params, ProcedureInvocationCallback cb) {
            return callProcedure(new Invocation(m_config.getProcedure(), params), cb);
        }

    }

}
