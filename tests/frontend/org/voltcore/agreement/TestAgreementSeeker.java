/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltcore.agreement;

import static com.natpryce.makeiteasy.MakeItEasy.a;
import static com.natpryce.makeiteasy.MakeItEasy.make;
import static com.natpryce.makeiteasy.MakeItEasy.with;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.FailureSiteForwardMessage;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.SiteFailureMessage;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.fsfmMsg;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.sfmFailed;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.sfmFailures;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.sfmSafe;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.sfmSafeTxns;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.sfmSource;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.sfmSurvivors;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.voltcore.messaging.SiteFailureMessage;

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.primitives.Longs;
import com.natpryce.makeiteasy.Maker;

@SuppressWarnings("unchecked")
public class TestAgreementSeeker {

    AgreementSeeker s1 = new AgreementSeeker(ArbitrationStrategy.MATCHING_CARDINALITY,1);
    AgreementSeeker s2 = new AgreementSeeker(ArbitrationStrategy.MATCHING_CARDINALITY,2);
    AgreementSeeker s3 = new AgreementSeeker(ArbitrationStrategy.MATCHING_CARDINALITY,3);
    AgreementSeeker s4 = new AgreementSeeker(ArbitrationStrategy.MATCHING_CARDINALITY,4);

    final static Set<Long> hsids = ImmutableSet.of(1L,2L,3L,4L);

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testOneNodeDown() throws Exception {
        Maker<SiteFailureMessage> s2fail = a(SiteFailureMessage,
                with(sfmSurvivors,Longs.asList(1,3,4)),
                with(sfmSafeTxns,sfmSafe(2,22)),
                with(sfmFailures,sfmFailed(2)));

        s1.startSeekingFor(hsids, ImmutableMap.of(2L,true));
        s3.startSeekingFor(hsids, ImmutableMap.of(2L,true));
        s4.startSeekingFor(hsids, ImmutableMap.of(2L,true));

        s1.add(make(s2fail.but(with(sfmSource,1L))));
        s1.add(make(s2fail.but(with(sfmSource,3L))));
        s1.add(make(s2fail.but(with(sfmSource,4L))));

        s3.add(make(s2fail.but(with(sfmSource,1L))));
        s3.add(make(s2fail.but(with(sfmSource,3L))));
        s3.add(make(s2fail.but(with(sfmSource,4L))));

        s4.add(make(s2fail.but(with(sfmSource,1L))));
        s4.add(make(s2fail.but(with(sfmSource,3L))));
        s4.add(make(s2fail.but(with(sfmSource,4L))));

        assertThat(s1.nextKill(), contains(2L));
        assertThat(s3.nextKill(), contains(2L));
        assertThat(s4.nextKill(), contains(2L));

        assertThat(s1.needForward(), equalTo(false));
        assertThat(s3.needForward(), equalTo(false));
        assertThat(s4.needForward(), equalTo(false));

        assertThat(s1.forWhomSiteIsDead(2L),empty());
        assertThat(s3.forWhomSiteIsDead(2L),empty());
        assertThat(s4.forWhomSiteIsDead(2L),empty());
    }

    @Test
    public void testTwoNodesDown() throws Exception {
        Maker<SiteFailureMessage> s23fail = a(SiteFailureMessage,
                with(sfmSurvivors,Longs.asList(1,4)),
                with(sfmSafeTxns,sfmSafe(2,22,3,33)),
                with(sfmFailures,sfmFailed(2,3)));

        s1.startSeekingFor(hsids, ImmutableMap.of(2L,true,3L,true));
        s4.startSeekingFor(hsids, ImmutableMap.of(2L,true,3L,true));

        s1.add(make(s23fail.but(with(sfmSource,1L))));
        s1.add(make(s23fail.but(with(sfmSource,4L))));

        s4.add(make(s23fail.but(with(sfmSource,1L))));
        s4.add(make(s23fail.but(with(sfmSource,4L))));

        assertThat(s1.nextKill(), contains(2L,3L));
        assertThat(s4.nextKill(), contains(2L,3L));

        assertThat(s1.needForward(), equalTo(false));
        assertThat(s4.needForward(), equalTo(false));

        assertThat(s1.forWhomSiteIsDead(2L),empty());
        assertThat(s1.forWhomSiteIsDead(3L),empty());
        assertThat(s4.forWhomSiteIsDead(2L),empty());
        assertThat(s4.forWhomSiteIsDead(3L),empty());
    }

    @Test
    public void testOneLinkDownBetweenTwoNodes() throws Exception {
        Maker<SiteFailureMessage> msg = a(SiteFailureMessage,
                with(sfmSurvivors,Longs.asList(1,2,3,4)),
                with(sfmSafeTxns,sfmSafe(3,33,4,44)),
                with(sfmFailures,sfmFailed(3,4)));


        s1.startSeekingFor(hsids, ImmutableMap.of(3L,false,4L,false));
        s2.startSeekingFor(hsids, ImmutableMap.of(3L,false,4L,false));
        s3.startSeekingFor(hsids, ImmutableMap.of(3L,false,4L,true));
        s4.startSeekingFor(hsids, ImmutableMap.of(3L,true,4L,false));

        s1.add(make(msg.but(with(sfmSource,1L))));
        s1.add(make(msg.but(with(sfmSource,2L))));
        s1.add(make(msg.but(with(sfmSource,3L),with(sfmSurvivors,Longs.asList(1,2,3)))));
        s1.add(make(msg.but(with(sfmSource,4L),with(sfmSurvivors,Longs.asList(1,2,4)))));

        s2.add(make(msg.but(with(sfmSource,1L))));
        s2.add(make(msg.but(with(sfmSource,2L))));
        s2.add(make(msg.but(with(sfmSource,3L),with(sfmSurvivors,Longs.asList(1,2,3)))));
        s2.add(make(msg.but(with(sfmSource,4L),with(sfmSurvivors,Longs.asList(1,2,4)))));

        s3.add(make(msg.but(with(sfmSource,1L))));
        s3.add(make(msg.but(with(sfmSource,2L))));
        s3.add(make(msg.but(with(sfmSource,3L),with(sfmSurvivors,Longs.asList(1,2,3)))));

        s4.add(make(msg.but(with(sfmSource,1L))));
        s4.add(make(msg.but(with(sfmSource,2L))));
        s4.add(make(msg.but(with(sfmSource,3L),with(sfmSurvivors,Longs.asList(1,2,4)))));

        assertThat(s1.needForward(), equalTo(false));
        assertThat(s2.needForward(), equalTo(false));
        assertThat(s3.needForward(), equalTo(true));
        assertThat(s4.needForward(), equalTo(true));

        assertThat(s1.forWhomSiteIsDead(4L),contains(3L));
        assertThat(s1.forWhomSiteIsDead(3L),contains(4L));
        assertThat(s2.forWhomSiteIsDead(4L),contains(3L));
        assertThat(s2.forWhomSiteIsDead(3L),contains(4L));
        assertThat(s2.forWhomSiteIsDead(2L),empty());
        assertThat(s3.forWhomSiteIsDead(4L),empty());
        assertThat(s4.forWhomSiteIsDead(3L),empty());
        assertThat(s4.forWhomSiteIsDead(4L),empty());

        s3.add(make(a(FailureSiteForwardMessage,
                with(fsfmMsg, msg.but(with(sfmSource,4L),with(sfmSurvivors,Longs.asList(1,2,4)))))));
        s4.add(make(a(FailureSiteForwardMessage,
                with(fsfmMsg, msg.but(with(sfmSource,3L),with(sfmSurvivors,Longs.asList(1,2,3)))))));

        assertThat(s3.needForward(), equalTo(false));
        assertThat(s4.needForward(), equalTo(false));

        assertThat(s1.nextKill(), contains(4L));
        assertThat(s2.nextKill(), contains(4L));
        assertThat(s3.nextKill(), contains(4L));
        assertThat(s4.nextKill(), contains(3L));
    }
}
