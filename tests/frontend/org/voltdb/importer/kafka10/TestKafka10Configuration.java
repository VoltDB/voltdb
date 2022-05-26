/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.importer.kafka10;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.voltdb.importclient.kafka.util.KafkaConstants;
import org.voltdb.importclient.kafka10.KafkaStreamImporterConfig;

public class TestKafka10Configuration {

    // This test verifies configuration processing from deployment XML.

    @Test
    public void testMissingBroker() throws Exception {

        Properties p = new Properties();
        p.setProperty("topics", "mytopic");
        p.setProperty("procedure", "myproc");

        try {
            new KafkaStreamImporterConfig(p);
            Assert.fail("Missing broker should have caused an exception, but didn't");
        }
        catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().equals("Kafka broker configuration is missing."));
        }

        p = new Properties();
        p.setProperty("topics", "mytopic");
        p.setProperty("procedure", "myproc");
        p.setProperty("broker", "   ");

        try {
            new KafkaStreamImporterConfig(p);
            Assert.fail("Missing broker should have caused an exception, but didn't");
        }
        catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().equals("Kafka broker configuration is missing."));
        }
    }

    @Test
    public void testMissingProcedure() throws Exception {

        Properties p = new Properties();
        p.setProperty("topics", "mytopic");
        p.setProperty("brokers", "localhost:9092");

        try {
            new KafkaStreamImporterConfig(p);
            Assert.fail("Missing procedure should have caused an exception, but didn't");
        }
        catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().equals("Missing procedure name"));
        }

        p = new Properties();
        p.setProperty("topics", "mytopic");
        p.setProperty("brokers", "localhost:9092");
        p.setProperty("procedure", "   ");

        try {
            new KafkaStreamImporterConfig(p);
            Assert.fail("Missing procedire should have caused an exception, but didn't");
        }
        catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().equals("Missing procedure name"));
        }
    }

    @Test
    public void testMissingTopics() throws Exception {

        Properties p = new Properties();
        p.setProperty("procedure", "myproc");
        p.setProperty("brokers", "localhost:9092");

        try {
            new KafkaStreamImporterConfig(p);
            Assert.fail("Missing topic should have caused an exception, but didn't");
        }
        catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().equals("Missing topic(s)."));
        }

        p = new Properties();
        p.setProperty("procedure", "myproc");
        p.setProperty("brokers", "localhost:9092");
        p.setProperty("topic", "");

        try {
            new KafkaStreamImporterConfig(p);
            Assert.fail("Missing topic should have caused an exception, but didn't");
        }
        catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().equals("Missing topic(s)."));
        }

    }

    @Test
    public void testTopicNameRestrictionsTooLong() throws Exception {

        Properties p = new Properties();
        p.setProperty("topics", StringUtils.repeat("T", KafkaConstants.TOPIC_MAX_NAME_LENGTH));
        p.setProperty("procedure", "myproc");
        p.setProperty("brokers", "localhost:9092");

        // 255 is the max, this is OK:
        new KafkaStreamImporterConfig(p);

        // Make the topic name too long:
        p.setProperty("topics", StringUtils.repeat("T", KafkaConstants.TOPIC_MAX_NAME_LENGTH + 1));
        try {
            new KafkaStreamImporterConfig(p);
            Assert.fail("Topic name that is too long should have caused an exception, but didn't");
        }
        catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("topic name can't be longer than"));
        }
    }

    @Test
    public void testTopicNameRestrictionsBadChars() throws Exception {

        Properties p = new Properties();
        p.setProperty("topics", "test_0-9-SpEcIaL_characters");
        p.setProperty("procedure", "myproc");
        p.setProperty("brokers", "localhost:9092");

        // ASCII alphanumerics, underscore, and hyphen are OK:
        new KafkaStreamImporterConfig(p);

        // Make the topic name too long:
        p.setProperty("topics", "this*has$bad+characters!");
        try {
            new KafkaStreamImporterConfig(p);
            Assert.fail("Topic name that is too long should have caused an exception, but didn't");
        }
        catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("contains a character other than ASCII alphanumerics"));
        }
    }
}
