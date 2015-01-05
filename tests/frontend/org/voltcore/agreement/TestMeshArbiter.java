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
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.FailureSiteForwardMessage;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.SiteFailureMessage;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.fsfmMsg;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.fsfmSource;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.sfmFailed;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.sfmFailures;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.sfmSafe;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.sfmSafeTxns;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.sfmSource;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.sfmSurvived;
import static org.voltcore.agreement.maker.SiteFailureMessageMaker.sfmSurvivors;
import static org.voltcore.agreement.matcher.SiteFailureMatchers.failureForwardMsgIs;
import static org.voltcore.agreement.matcher.SiteFailureMatchers.siteFailureIs;

import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.voltcore.messaging.FaultMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.SiteFailureForwardMessage;
import org.voltcore.messaging.SiteFailureMessage;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.primitives.Longs;
import com.natpryce.makeiteasy.Maker;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class TestMeshArbiter {

    @Mock
    Mailbox mbox;

    @Mock
    MeshAide aide;

    @Captor
    ArgumentCaptor<Set<Long>> destinationCaptor;

    MeshArbiter arbiter;
    final static Set<Long> hsids = ImmutableSet.of(0L,1L,2L,3L);

    @Before
    public void testSetup() {
        arbiter = new MeshArbiter(0L, mbox, aide);
    }

    @Test
    public void testBasicScenario() throws Exception {
        Maker<SiteFailureMessage> siteOneSfm = a(SiteFailureMessage,
                with(sfmSurvivors,sfmSurvived(0,2,3)),
                with(sfmFailures,sfmFailed(1)),
                with(sfmSafeTxns,sfmSafe(0,10,1,11,2,22,3,33))
                );

        when(aide.getNewestSafeTransactionForInitiator(anyLong())).thenReturn(11L);
        when(mbox.recvBlocking(any(Subject[].class), eq(5L)))
            .thenReturn(make(siteOneSfm.but(with(sfmSource, 0L))))
            .thenReturn(make(siteOneSfm.but(with(sfmSource, 2L))))
            .thenReturn(make(siteOneSfm.but(with(sfmSource, 3L))))
            ;

        Map<Long,Long> decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(0,1));

        verify(mbox,times(2)).send(any(long[].class), argThat(siteFailureIs(sfmFailed(1), sfmSurvived(0,2,3))));

        assertEquals(decision,ImmutableMap.<Long,Long>of(1L,11L));
    }

    @Test
    public void testSubsequentFailures() throws Exception {

        Maker<SiteFailureMessage> siteOneSfm = a(SiteFailureMessage,
                with(sfmSurvivors,Longs.asList(0,2,3)),
                with(sfmFailures,sfmFailed(1)),
                with(sfmSafeTxns,sfmSafe(0,10,1,11,2,22,3,33))
                );

        Maker<SiteFailureMessage> siteTwoSfm = a(SiteFailureMessage,
                with(sfmSurvivors,Longs.asList(0,3)),
                with(sfmFailures,sfmFailed(2)),
                with(sfmSafeTxns,sfmSafe(0,10,1,11,2,22,3,33))
                );

        when(aide.getNewestSafeTransactionForInitiator(1L)).thenReturn(11L);
        when(aide.getNewestSafeTransactionForInitiator(2L)).thenReturn(22L);

        when(mbox.recvBlocking(any(Subject[].class), eq(5L)))
            .thenReturn(make(siteOneSfm.but(with(sfmSource, 0L))))
            .thenReturn(make(siteOneSfm.but(with(sfmSource, 2L))))
            .thenReturn(make(siteOneSfm.but(with(sfmSource, 3L))))
            ;

        Map<Long,Long> decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(0,1));

        verify(mbox,times(2)).send(any(long[].class), argThat(siteFailureIs(sfmFailed(1),sfmSurvived(0,2,3))));

        assertEquals(decision,ImmutableMap.<Long,Long>of(1L,11L));

        reset(mbox);
        when(mbox.recvBlocking(any(Subject[].class),eq(5L)))
            .thenReturn(make(siteTwoSfm.but(with(sfmSource,0L))))
            .thenReturn(make(siteTwoSfm.but(with(sfmSource,3L))))
        ;

        decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(0,2));

        verify(mbox,never()).deliverFront(any(VoltMessage.class));
        verify(mbox,times(2)).send(any(long[].class), argThat(siteFailureIs(sfmFailed(2), sfmSurvived(0,3))));

        assertEquals(decision,ImmutableMap.<Long,Long>of(2L,22L));
    }

    @Test
    public void testOverlappingFailures() throws Exception {

        Maker<SiteFailureMessage> site12Sfm = a(SiteFailureMessage,
                with(sfmSurvivors,Longs.asList(0,3)),
                with(sfmFailures,sfmFailed(1,2)),
                with(sfmSafeTxns,sfmSafe(0,10,1,11,2,22,3,33))
                );

        when(aide.getNewestSafeTransactionForInitiator(1L)).thenReturn(11L);
        when(aide.getNewestSafeTransactionForInitiator(2L)).thenReturn(22L);

        when(mbox.recv(any(Subject[].class)))
            .thenReturn(new FaultMessage(0,2))
            .thenReturn((VoltMessage)null);
        when(mbox.recvBlocking(any(Subject[].class), eq(5L)))
            .thenReturn(make(site12Sfm.but(with(sfmSource, 0L))))
            .thenReturn(make(site12Sfm.but(with(sfmSource, 3L))))
            ;

        Map<Long,Long> decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(0,1));

        verify(mbox,times(2)).send(any(long[].class), argThat(siteFailureIs(sfmFailed(1,2), sfmSurvived(0,3))));

        assertEquals(decision,ImmutableMap.<Long,Long>of(1L,11L,2L,22L));
    }

    @Test
    public void testInterleavedFailures() throws Exception {
        Maker<SiteFailureMessage> siteOneSfm = a(SiteFailureMessage,
                with(sfmSurvivors,Longs.asList(0,2,3)),
                with(sfmFailures,sfmFailed(1)),
                with(sfmSafeTxns,sfmSafe(0,10,1,11,2,22,3,33))
                );

        Maker<SiteFailureMessage> siteTwoSfm = a(SiteFailureMessage,
                with(sfmSurvivors,Longs.asList(0,3)),
                with(sfmFailures,sfmFailed(1,2)),
                with(sfmSafeTxns,sfmSafe(0,10,1,11,2,22,3,33))
                );

        when(aide.getNewestSafeTransactionForInitiator(1L)).thenReturn(11L);
        when(aide.getNewestSafeTransactionForInitiator(2L)).thenReturn(22L);
        when(mbox.recvBlocking(any(Subject[].class), eq(5L)))
            .thenReturn(make(siteOneSfm.but(with(sfmSource, 0L))))
            .thenReturn(new FaultMessage(0,2L))
            ;

        Map<Long,Long> decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(0,1));

        verify(mbox,times(1)).deliverFront(any(VoltMessage.class));
        verify(mbox,times(1)).send(any(long[].class), any(VoltMessage.class));
        verify(mbox).send(any(long[].class), argThat(siteFailureIs(sfmFailed(1), sfmSurvived(0,2,3))));

        assertEquals(decision,ImmutableMap.<Long,Long>of());

        reset(mbox);
        when(mbox.recvBlocking(any(Subject[].class),eq(5L)))
            .thenReturn(make(siteOneSfm.but(with(sfmSource,3L))))
            .thenReturn(make(siteTwoSfm.but(with(sfmSource,0L))))
            .thenReturn(make(siteTwoSfm.but(with(sfmSource,3L))))
        ;

        decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(0,2));

        verify(mbox,never()).deliverFront(any(VoltMessage.class));
        verify(mbox,times(2)).send(any(long[].class), any(VoltMessage.class));
        verify(mbox,times(2)).send(any(long[].class), argThat(siteFailureIs(sfmFailed(1,2),sfmSurvived(0,3))));

        assertEquals(decision,ImmutableMap.<Long,Long>of(1L,11L,2L,22L));
    }

    @Test
    public void testPingsOnLongReceives() throws Exception {
        Maker<SiteFailureMessage> siteOneSfm = a(SiteFailureMessage,
                with(sfmSurvivors,Longs.asList(0,2,3)),
                with(sfmFailures,sfmFailed(1)),
                with(sfmSafeTxns,sfmSafe(0,10,1,11,2,22,3,33))
                );

        when(aide.getNewestSafeTransactionForInitiator(1L)).thenReturn(11L);
        when(mbox.recvBlocking(any(Subject[].class), eq(5L)))
            .thenReturn((VoltMessage)null)
            .thenReturn((VoltMessage)null)
            .thenReturn((VoltMessage)null)
            .thenReturn(make(siteOneSfm.but(with(sfmSource, 0L))))
            .thenReturn(make(siteOneSfm.but(with(sfmSource, 2L))))
            .thenReturn(make(siteOneSfm.but(with(sfmSource, 3L))))
            ;

        Map<Long,Long> decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(0,1));

        verify(mbox,times(2)).send(any(long[].class), argThat(siteFailureIs(sfmFailed(1), sfmSurvived(0,2,3))));
        verify(aide,atLeast(2)).sendHeartbeats(destinationCaptor.capture());
        assertEquals(destinationCaptor.getValue(), sfmSurvived(0,2,3));

        assertEquals(decision,ImmutableMap.<Long,Long>of(1L,11L));
    }

    @Test
    public void testMixOfWitnessedAndNon() throws Exception {
        Maker<SiteFailureMessage> um = a(SiteFailureMessage,
                with(sfmSurvivors,Longs.asList(0,2,3)),
                with(sfmFailures,sfmFailed(1)),
                with(sfmSafeTxns,sfmSafe(0,10,1,11,2,22,3,33))
                );

        when(aide.getNewestSafeTransactionForInitiator(1L)).thenReturn(11L);

        when(mbox.recvBlocking(any(Subject[].class), eq(5L)))
            .thenReturn(make(um.but(with(sfmSource, 2L))))
            .thenReturn(new FaultMessage(0,1))
        ;
        Map<Long,Long> decision =
                arbiter.reconfigureOnFault(hsids, new FaultMessage(2,1,ImmutableSet.of(0L,2L,3L)));

        verify(mbox,times(1)).deliverFront(any(VoltMessage.class));
        verify(mbox,times(1)).send(any(long[].class), any(VoltMessage.class));
        verify(mbox).send(any(long[].class), argThat(siteFailureIs(sfmFailed(1), sfmSurvived(0,1,2,3))));

        assertEquals(decision,ImmutableMap.<Long,Long>of());

        reset(mbox);
        when(mbox.recvBlocking(any(Subject[].class), eq(5L)))
            .thenReturn(make(um.but(with(sfmSource, 0L))))
            .thenReturn(make(um.but(with(sfmSource, 3L))))
        ;
        decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(0,1));

        verify(mbox,never()).deliverFront(any(VoltMessage.class));
        verify(mbox,times(2)).send(any(long[].class), argThat(siteFailureIs(sfmFailed(1), sfmSurvived(0,2,3))));

        assertEquals(decision,ImmutableMap.<Long,Long>of(1L,11L));
    }

    @Test
    public void testOneLinkDownFromThePerspictiveOfWitness() throws Exception {
        Maker<SiteFailureMessage> s1f = a(SiteFailureMessage,
                with(sfmSurvivors,Longs.asList(0,2,3)),
                with(sfmFailures,sfmFailed(1)),
                with(sfmSafeTxns,sfmSafe(0,10,1,11,2,22,3,33))
                );
        Maker<SiteFailureMessage> s0f = a(SiteFailureMessage,
                with(sfmSurvivors,Longs.asList(1,2,3)),
                with(sfmFailures,sfmFailed(0)),
                with(sfmSafeTxns,sfmSafe(0,10,1,11,2,22,3,33))
                );

        Maker<SiteFailureMessage> s23f = a(SiteFailureMessage,
                with(sfmSurvivors,Longs.asList(0,1,2,3)),
                with(sfmFailures,sfmFailed(0,1)),
                with(sfmSafeTxns,sfmSafe(0,10,1,11,2,22,3,33))
                );
        Maker<SiteFailureForwardMessage> uf = a(FailureSiteForwardMessage);

        when(aide.getNewestSafeTransactionForInitiator(0L)).thenReturn(10L);
        when(aide.getNewestSafeTransactionForInitiator(1L)).thenReturn(11L);

        when(mbox.recvBlocking(any(Subject[].class), eq(5L)))
            .thenReturn(make(s23f.but(with(sfmSource,2L),with(sfmFailures,sfmFailed(0)))))
            .thenReturn(new FaultMessage(2L,0L,ImmutableSet.of(1L,2L,3L)))
            .thenReturn(make(s1f.but(with(sfmSource,0L))))
            .thenReturn(make(s23f.but(with(sfmSource,2L))))
            .thenReturn(make(s23f.but(with(sfmSource,3L))))
            .thenReturn(make(uf.but(with(fsfmSource,2L),with(fsfmMsg,s0f))))
            .thenReturn(make(uf.but(with(fsfmSource,3L),with(fsfmMsg,s0f))))
        ;
        Map<Long,Long> decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(0,1));

        verify(mbox,times(0)).deliverFront(any(VoltMessage.class));
        verify(mbox,times(2)).send(any(long[].class), argThat(siteFailureIs(sfmFailed(1), sfmSurvived(0,2,3))));

        assertEquals(decision,ImmutableMap.<Long,Long>of(1L,11L));
    }

    @Test
    public void testOneLinkDownFromThePerspectiveOfNonWitness() throws Exception {
        Maker<SiteFailureMessage> s1f = a(SiteFailureMessage,
                with(sfmSurvivors,Longs.asList(0,2,3)),
                with(sfmFailures,sfmFailed(1)),
                with(sfmSafeTxns,sfmSafe(0,10,1,11,2,22,3,33))
                );
        Maker<SiteFailureMessage> s2f = a(SiteFailureMessage,
                with(sfmSurvivors,Longs.asList(0,1,3)),
                with(sfmFailures,sfmFailed(2)),
                with(sfmSafeTxns,sfmSafe(0,10,1,11,2,22,3,33))
                );

        Maker<SiteFailureMessage> s03f = a(SiteFailureMessage,
                with(sfmSurvivors,Longs.asList(0,1,2,3)),
                with(sfmFailures,sfmFailed(1,2)),
                with(sfmSafeTxns,sfmSafe(0,10,1,11,2,22,3,33))
                );

        when(aide.getNewestSafeTransactionForInitiator(1L)).thenReturn(11L);
        when(aide.getNewestSafeTransactionForInitiator(2L)).thenReturn(22L);

        when(mbox.recvBlocking(any(Subject[].class), eq(5L)))
            .thenReturn(make(s2f.but(with(sfmSource,1L))))
            .thenReturn(make(s03f.but(with(sfmSource,3L),with(sfmFailures,sfmFailed(1)))))
            .thenReturn(new FaultMessage(1,2, ImmutableSet.of(0L,1L,3L)))
        ;

        Map<Long,Long> decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(2,1,ImmutableSet.of(0L,2L,3L)));

        verify(mbox,times(1)).deliverFront(any(VoltMessage.class));
        verify(mbox,times(1)).send(any(long[].class), any(VoltMessage.class));
        verify(mbox).send(any(long[].class), argThat(siteFailureIs(sfmFailed(1), sfmSurvived(0,1,2,3))));

        assertEquals(decision,ImmutableMap.<Long,Long>of());

        reset(mbox);
        when(mbox.recvBlocking(any(Subject[].class), eq(5L)))
            .thenReturn(make(s1f.but(with(sfmSource,2L))))
            .thenReturn(make(s03f.but(with(sfmSource,0L))))
            .thenReturn(make(s03f.but(with(sfmSource,3L))))
        ;

        decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(1,2,ImmutableSet.of(0L,1L,3L)));

        // promotion from un to witnessed
        verify(mbox,atLeast(2)).send(any(long[].class), any(VoltMessage.class));
        verify(mbox).send(any(long[].class), argThat(siteFailureIs(sfmFailed(1,2), sfmSurvived(0,1,2,3))));
        verify(mbox).send(eq(new long[]{1}), argThat(failureForwardMsgIs(2,sfmFailed(1), sfmSurvived(0,2,3))));

        assertEquals(decision,ImmutableMap.<Long,Long>of(2L,22L));
    }
}
