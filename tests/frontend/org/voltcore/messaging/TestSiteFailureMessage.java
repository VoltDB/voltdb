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

package org.voltcore.messaging;

import static com.natpryce.makeiteasy.MakeItEasy.a;
import static com.natpryce.makeiteasy.MakeItEasy.make;
import static com.natpryce.makeiteasy.MakeItEasy.with;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.SiteFailureMessage;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.sfmDecision;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.sfmFailed;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.sfmFailures;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.sfmSafe;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.sfmSafeTxns;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.sfmSource;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.sfmSurvived;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.sfmSurvivors;
import static org.voltcore.agreement.matcher.SiteFailureMatchers.failureForwardMsgIs;
import static org.voltcore.agreement.matcher.SiteFailureMatchers.siteFailureIs;

import java.nio.ByteBuffer;
import java.util.Set;

import org.junit.Test;

import com.google_voltpatches.common.collect.ImmutableSet;
import com.natpryce.makeiteasy.Maker;

public class TestSiteFailureMessage {

    VoltMessageFactory factory = new VoltMessageFactory();

    @SuppressWarnings("unchecked")
    Maker<SiteFailureMessage> sfm = a(SiteFailureMessage,
            with(sfmSource,1L),
            with(sfmSurvivors, sfmSurvived(1,2,3)),
            with(sfmFailures, sfmFailed(4,5,6)),
            with(sfmSafeTxns, sfmSafe(4,44,5,55))
            );

    @Test
    public void testSiteFailureMessageRoundtripSerialization() throws Exception {
        SiteFailureMessage msg = make(sfm);
        assertThat(msg,siteFailureIs(sfmSafe(4,44,5,55),sfmFailed(4,5,6),sfmSurvived(1,2,3)));

        ByteBuffer bb = VoltMessage.toBuffer(msg);

        VoltMessage vmsg = factory.createMessageFromBuffer(bb, 1L);
        assertTrue(vmsg instanceof SiteFailureMessage);

        SiteFailureMessage gsm = (SiteFailureMessage)vmsg;
        assertThat(gsm,siteFailureIs(sfmSafe(4,44,5,55),sfmFailed(4,5,6),sfmSurvived(1,2,3)));
    }

    @Test
    public void testSiteFailureMessageRoundtripSerializationWithDecision() throws Exception {
        Set<Long> decision = ImmutableSet.of(1L,2L);

        @SuppressWarnings("unchecked")
        SiteFailureMessage msg = make(sfm.but(with(sfmDecision,decision)));
        assertThat(msg,siteFailureIs(sfmSafe(4,44,5,55),decision,1,2,3));

        ByteBuffer bb = VoltMessage.toBuffer(msg);

        VoltMessage vmsg = factory.createMessageFromBuffer(bb, 1L);
        assertTrue(vmsg instanceof SiteFailureMessage);

        SiteFailureMessage gsm = (SiteFailureMessage)vmsg;
        assertThat(gsm,siteFailureIs(sfmSafe(4,44,5,55),decision,1,2,3));
    }

   @Test
   public void testForwardRoundTripSerialization() throws Exception {
       SiteFailureMessage msg = make(sfm);
       assertThat(msg,siteFailureIs(sfmSafe(4,44,5,55),sfmFailed(4,5,6),sfmSurvived(1,2,3)));

       ByteBuffer bb = VoltMessage.toBuffer(msg);

       VoltMessage vmsg = factory.createMessageFromBuffer(bb, 1L);
       assertTrue(vmsg instanceof SiteFailureMessage);

       SiteFailureMessage gsm = (SiteFailureMessage)vmsg;
       assertThat(gsm,siteFailureIs(sfmSafe(4,44,5,55),sfmFailed(4,5,6),sfmSurvived(1,2,3)));

       SiteFailureForwardMessage fmsg = new SiteFailureForwardMessage(gsm);
       assertThat(fmsg, failureForwardMsgIs(1, sfmSafe(4,44,5,55),sfmFailed(4,5,6),sfmSurvived(1,2,3)));

       bb = VoltMessage.toBuffer(fmsg);

       vmsg = factory.createMessageFromBuffer(bb, 1L);
       assertTrue(vmsg instanceof SiteFailureForwardMessage);

       SiteFailureForwardMessage gsmf = (SiteFailureForwardMessage)vmsg;
       assertThat(gsmf, failureForwardMsgIs(1, sfmSafe(4,44,5,55),sfmFailed(4,5,6),sfmSurvived(1,2,3)));
   }

}
