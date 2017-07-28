package org.voltdb.importclient.kafka10;

import org.apache.kafka.clients.consumer.Consumer;
import org.voltcore.logging.VoltLogger;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.importer.ImporterLifecycle;
import org.voltdb.utils.CSVDataLoader;
import org.voltdb.utils.RowWithMetaData;

public class Kafka10ExternalConsumerRunner extends Kafka10ConsumerRunner {

    private static final VoltLogger m_log = new VoltLogger("KAFKA10LOADER");
    private CSVDataLoader m_loader;

    public Kafka10ExternalConsumerRunner(ImporterLifecycle lifecycle, Kafka10StreamImporterConfig config, Consumer<byte[], byte[]> consumer, CSVDataLoader loader) throws Exception {
        super(lifecycle, config, consumer);
        m_loader = loader;
    }

    @Override
    public void invoke(String rawMessage, long offset, Object[] params, ProcedureCallback procedureCallback) throws Exception {
        m_log.warn(">>> Invoke: rawMessage=" + rawMessage);
        m_loader.insertRow(new RowWithMetaData(rawMessage, offset, procedureCallback), params);
    }

}
