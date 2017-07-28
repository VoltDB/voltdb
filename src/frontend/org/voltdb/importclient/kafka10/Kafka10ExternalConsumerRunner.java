package org.voltdb.importclient.kafka10;

import org.apache.kafka.clients.consumer.Consumer;
import org.voltcore.logging.VoltLogger;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.importer.ImporterLifecycle;

public class Kafka10ExternalConsumerRunner extends Kafka10ConsumerRunner {

    private static final VoltLogger m_log = new VoltLogger("KAFKA10LOADER");

    public Kafka10ExternalConsumerRunner(ImporterLifecycle lifecycle, Kafka10StreamImporterConfig config, Consumer<byte[], byte[]> consumer) throws Exception {
        super(lifecycle, config, consumer);
    }

    @Override
    public void invoke(Object[] params, ProcedureCallback procedureCallback) {
        m_log.warn(">>> Invoke: params=" + params);
    }

}
