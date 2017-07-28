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
    public void invoke(Object[] params, ProcedureCallback procedureCallback) {
        m_importer.callProcedure(new Invocation(m_config.getProcedure(), params), procedureCallback);
    }

}
