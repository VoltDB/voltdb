package org.voltdb.importclient.kafka10;

import org.voltdb.importclient.kafka.util.BaseKafkaLoaderCLIArguments;

public class Kafka10LoaderCLIArguments extends BaseKafkaLoaderCLIArguments {

    @Option(shortOpt = "n", desc = "Number of Kafka consumers.")
    public int consumercount = 1;

    public int getConsumerCount() {
        return consumercount;
    }

    @Override
    public void validate() {
        super.validate();
    }
}
