/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
