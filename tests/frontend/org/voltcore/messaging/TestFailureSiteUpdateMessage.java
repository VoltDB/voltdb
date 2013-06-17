/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltcore.messaging;

import static com.natpryce.makeiteasy.MakeItEasy.a;
import static com.natpryce.makeiteasy.MakeItEasy.make;
import static com.natpryce.makeiteasy.MakeItEasy.with;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.voltcore.agreement.maker.FailureSiteUpdateMessageMaker.FailureSiteUpdateMessage;
import static org.voltcore.agreement.maker.FailureSiteUpdateMessageMaker.fsumEntries;
import static org.voltcore.agreement.maker.FailureSiteUpdateMessageMaker.fsumHsids;
import static org.voltcore.agreement.maker.FailureSiteUpdateMessageMaker.fsumSite;
import static org.voltcore.agreement.maker.FailureSiteUpdateMessageMaker.fsumSource;
import static org.voltcore.agreement.maker.FailureSiteUpdateMessageMaker.fsumTxnid;
import static org.voltcore.agreement.matcher.FailureSiteUpdateMatchers.failureUpdateMsgIs;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.natpryce.makeiteasy.Maker;

public class TestFailureSiteUpdateMessage {

    @SuppressWarnings("unchecked")
    Maker<FailureSiteUpdateMessage> fsum = a(FailureSiteUpdateMessage,
            with(fsumSource,1L),
            with(fsumTxnid,123456789012345L),
            with(fsumSite,3L),
            with(fsumEntries, fsumHsids(3L,false,4L,true))
            );
    VoltMessageFactory factory = new VoltMessageFactory();

    @Test
    public void testRoundTripSerialization() throws Exception {
        FailureSiteUpdateMessage msg = make(fsum);
        assertThat(msg, failureUpdateMsgIs(3L, 123456789012345L));

        ByteBuffer bb = VoltMessage.toBuffer(msg);

        VoltMessage vmsg = factory.createMessageFromBuffer(bb, 1L);
        assertTrue(vmsg instanceof FailureSiteUpdateMessage);

        FailureSiteUpdateMessage gsm = (FailureSiteUpdateMessage)vmsg;
        assertThat(msg, failureUpdateMsgIs(3L, 123456789012345L));

        assertEquals(ImmutableMap.<Long, Boolean>of(3L,false,4L,true), gsm.m_failedHSIds);
    }

   @Test
   public void testForwardRoundTripSerialization() throws Exception {
       FailureSiteUpdateMessage msg = make(fsum);
       assertThat(msg, failureUpdateMsgIs(3L, 123456789012345L));

       ByteBuffer bb = VoltMessage.toBuffer(msg);

       VoltMessage vmsg = factory.createMessageFromBuffer(bb, 1L);
       assertTrue(vmsg instanceof FailureSiteUpdateMessage);

       FailureSiteUpdateMessage gsm = (FailureSiteUpdateMessage)vmsg;
       assertEquals(123456789012345L, gsm.m_safeTxnId);
       assertEquals(3L, gsm.m_failedHSId);

       assertEquals(ImmutableMap.<Long, Boolean>of(3L,false,4L,true), gsm.m_failedHSIds);

       FailureSiteForwardMessage fmsg = new FailureSiteForwardMessage(gsm);

       bb = VoltMessage.toBuffer(fmsg);

       vmsg = factory.createMessageFromBuffer(bb, 1L);
       assertTrue(vmsg instanceof FailureSiteForwardMessage);

       FailureSiteForwardMessage gsmf = (FailureSiteForwardMessage)vmsg;
       assertEquals(123456789012345L, gsmf.m_safeTxnId);
       assertEquals(3L, gsmf.m_failedHSId);
       assertEquals(1L, gsmf.m_reportingHSId);

       assertEquals(ImmutableMap.<Long, Boolean>of(3L,false,4L,true), gsmf.m_failedHSIds);
   }

}
