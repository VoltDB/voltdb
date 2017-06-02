package org.voltdb.importer.kafka;

import org.junit.Test;
import org.voltdb.importclient.kafka.KafkaImporterCommitPolicy;

import junit.framework.Assert;

public class KafkaLoaderUnitTest {

    @Test
    public void testCommitPolicyParsing() throws Exception {

        Assert.assertEquals(KafkaImporterCommitPolicy.NONE, KafkaImporterCommitPolicy.fromString("NONE"));

        Assert.assertEquals(KafkaImporterCommitPolicy.TIME, KafkaImporterCommitPolicy.fromString("3000"));
        Assert.assertEquals(3000, KafkaImporterCommitPolicy.fromStringTriggerValue("3000",KafkaImporterCommitPolicy.TIME));

        Assert.assertEquals(KafkaImporterCommitPolicy.TIME, KafkaImporterCommitPolicy.fromString("3000ms"));
        Assert.assertEquals(3000, KafkaImporterCommitPolicy.fromStringTriggerValue("3000ms",KafkaImporterCommitPolicy.TIME));
    }

}
