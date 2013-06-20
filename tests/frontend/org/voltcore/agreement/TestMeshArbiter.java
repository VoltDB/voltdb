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

package org.voltcore.agreement;

import static com.natpryce.makeiteasy.MakeItEasy.a;
import static com.natpryce.makeiteasy.MakeItEasy.make;
import static com.natpryce.makeiteasy.MakeItEasy.with;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
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
import static org.voltcore.agreement.maker.FailureSiteUpdateMessageMaker.FailureSiteForwardMessage;
import static org.voltcore.agreement.maker.FailureSiteUpdateMessageMaker.FailureSiteUpdateMessage;
import static org.voltcore.agreement.maker.FailureSiteUpdateMessageMaker.fsfmMsg;
import static org.voltcore.agreement.maker.FailureSiteUpdateMessageMaker.fsumHsids;
import static org.voltcore.agreement.maker.FailureSiteUpdateMessageMaker.fsumMap;
import static org.voltcore.agreement.maker.FailureSiteUpdateMessageMaker.fsumSite;
import static org.voltcore.agreement.maker.FailureSiteUpdateMessageMaker.fsumSource;
import static org.voltcore.agreement.maker.FailureSiteUpdateMessageMaker.fsumTxnid;
import static org.voltcore.agreement.matcher.FailureSiteUpdateMatchers.failureForwardMsgIs;
import static org.voltcore.agreement.matcher.FailureSiteUpdateMatchers.failureUpdateMsgIs;

import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.voltcore.messaging.FailureSiteForwardMessage;
import org.voltcore.messaging.FailureSiteUpdateMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Longs;
import com.natpryce.makeiteasy.Maker;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class TestMeshArbiter {

    @Mock
    Mailbox mbox;

    @Mock
    MeshAide aide;

    @Captor
    ArgumentCaptor<long[]> survivorCaptor;

    @Captor
    ArgumentCaptor<Set<Long>> destinationCaptor;

    MeshArbiter arbiter;
    final static Set<Long> hsids = ImmutableSet.of(0L,1L,2L,3L);

    Maker<FailureSiteUpdateMessage> fsum =
            a(FailureSiteUpdateMessage, with(fsumSite,1L),with(fsumTxnid,2L));

    @Before
    public void testSetup() {
        arbiter = new MeshArbiter(0L, mbox, aide);
    }

    @Test
    public void testBasicScenario() throws Exception {
        Maker<FailureSiteUpdateMessage> siteOneFsum = fsum.but(with(fsumTxnid,2L),with(fsumMap,fsumHsids(1L,true)));

        when(aide.getNewestSafeTransactionForInitiator(anyLong())).thenReturn(2L);
        when(mbox.recvBlocking(any(Subject[].class), eq(5L)))
            .thenReturn(make(siteOneFsum.but(with(fsumSource, 0L))))
            .thenReturn(make(siteOneFsum.but(with(fsumSource, 2L))))
            .thenReturn(make(siteOneFsum.but(with(fsumSource, 3L))))
            ;

        Map<Long,Long> decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(1L, true));

        verify(mbox,times(1)).send(any(long[].class), any(VoltMessage.class));
        verify(mbox).send(survivorCaptor.capture(), argThat(failureUpdateMsgIs(1L, 2L,fsumHsids(1L,true))));
        assertThat(Longs.asList(survivorCaptor.getValue()), contains(0L,2L,3L));

        assertEquals(decision,ImmutableMap.<Long,Long>of(1L,2L));
    }

    @Test
    public void testSubsequentFailures() throws Exception {
        Maker<FailureSiteUpdateMessage> siteOneFsum =
                fsum.but(with(fsumTxnid,2L));
        Maker<FailureSiteUpdateMessage> siteTwoFsum =
                fsum.but(with(fsumSite,2L),with(fsumTxnid,3L));

        when(aide.getNewestSafeTransactionForInitiator(1L)).thenReturn(2L);
        when(aide.getNewestSafeTransactionForInitiator(2L)).thenReturn(3L);

        when(mbox.recvBlocking(any(Subject[].class), eq(5L)))
            .thenReturn(make(siteOneFsum.but(with(fsumSource, 0L),with(fsumMap,fsumHsids(1L,true)))))
            .thenReturn(make(siteOneFsum.but(with(fsumSource, 2L),with(fsumMap,fsumHsids(1L,true)))))
            .thenReturn(make(siteOneFsum.but(with(fsumSource, 3L),with(fsumMap,fsumHsids(1L,true)))))
            ;

        Map<Long,Long> decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(1L, true));

        verify(mbox,times(1)).send(any(long[].class), any(VoltMessage.class));
        verify(mbox).send(survivorCaptor.capture(), argThat(failureUpdateMsgIs(1L, 2L,fsumHsids(1L,true))));
        assertThat(Longs.asList(survivorCaptor.getValue()), contains(0L,2L,3L));

        assertEquals(decision,ImmutableMap.<Long,Long>of(1L,2L));

        reset(mbox);
        when(mbox.recvBlocking(any(Subject[].class),eq(5L)))
            .thenReturn(make(siteOneFsum.but(with(fsumSource,0L),with(fsumMap,fsumHsids(2L,true)))))
            .thenReturn(make(siteTwoFsum.but(with(fsumSource,0L),with(fsumMap,fsumHsids(2L,true)))))
            .thenReturn(make(siteOneFsum.but(with(fsumSource,3L),with(fsumMap,fsumHsids(2L,true)))))
            .thenReturn(make(siteTwoFsum.but(with(fsumSource,3L),with(fsumMap,fsumHsids(2L,true)))))
        ;

        decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(2L, true));

        verify(mbox,never()).deliverFront(any(VoltMessage.class));
        verify(mbox,times(1)).send(any(long[].class), any(VoltMessage.class));
        verify(mbox).send(survivorCaptor.capture(), argThat(failureUpdateMsgIs(2L,3L,fsumHsids(2L,true))));
        assertThat(Longs.asList(survivorCaptor.getValue()), contains(0L,3L));

        assertEquals(decision,ImmutableMap.<Long,Long>of(2L,3L));
    }

    @Test
    public void testInterleavedFailures() throws Exception {
        Maker<FailureSiteUpdateMessage> siteOneFsum =
                fsum.but(with(fsumTxnid,3L));
        Maker<FailureSiteUpdateMessage> siteTwoFsum =
                fsum.but(with(fsumSite,2L),with(fsumTxnid,4L));

        when(aide.getNewestSafeTransactionForInitiator(1L)).thenReturn(3L);
        when(aide.getNewestSafeTransactionForInitiator(2L)).thenReturn(4L);
        when(mbox.recvBlocking(any(Subject[].class), eq(5L)))
            .thenReturn(make(siteOneFsum.but(with(fsumSource, 0L),with(fsumMap,fsumHsids(1L,true)))))
            .thenReturn(new FaultMessage(2L, true))
            ;

        Map<Long,Long> decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(1L, true));

        verify(mbox,times(1)).deliverFront(any(VoltMessage.class));
        verify(mbox,times(1)).send(any(long[].class), any(VoltMessage.class));
        verify(mbox).send(survivorCaptor.capture(), argThat(failureUpdateMsgIs(1L,3L,fsumHsids(1L,true))));
        assertThat(Longs.asList(survivorCaptor.getValue()), contains(0L,2L,3L));

        assertEquals(decision,ImmutableMap.<Long,Long>of());

        reset(mbox);
        when(mbox.recvBlocking(any(Subject[].class),eq(5L)))
            .thenReturn(make(siteOneFsum.but(with(fsumSource,0L),with(fsumMap,fsumHsids(1L,true,2L,true)))))
            .thenReturn(make(siteTwoFsum.but(with(fsumSource,0L),with(fsumMap,fsumHsids(1L,true,2L,true)))))
            .thenReturn(make(siteOneFsum.but(with(fsumSource,3L),with(fsumMap,fsumHsids(1L,true,2L,true)))))
            .thenReturn(make(siteTwoFsum.but(with(fsumSource,3L),with(fsumMap,fsumHsids(1L,true,2L,true)))))
        ;

        decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(2L, true));

        verify(mbox,never()).deliverFront(any(VoltMessage.class));
        verify(mbox,times(2)).send(any(long[].class), any(VoltMessage.class));
        verify(mbox).send(survivorCaptor.capture(), argThat(failureUpdateMsgIs(1L,3L,fsumHsids(1L,true,2L,true))));
        assertThat(Longs.asList(survivorCaptor.getValue()), contains(0L,3L));
        verify(mbox).send(survivorCaptor.capture(), argThat(failureUpdateMsgIs(2L,4L,fsumHsids(1L,true,2L,true))));

        assertEquals(decision,ImmutableMap.<Long,Long>of(1L,3L,2L,4L));
    }

    @Test
    public void testDuplicateSiteFaults() throws Exception {
        Maker<FailureSiteUpdateMessage> siteOneFsum  =
                fsum.but(with(fsumTxnid,5L));
        Maker<FailureSiteUpdateMessage> siteTwoFsum =
                fsum.but(with(fsumSite,2L),with(fsumTxnid,6L));

        when(aide.getNewestSafeTransactionForInitiator(1L)).thenReturn(5L);
        when(aide.getNewestSafeTransactionForInitiator(2L)).thenReturn(6L);
        when(mbox.recvBlocking(any(Subject[].class), eq(5L)))
            .thenReturn(make(siteOneFsum.but(with(fsumSource, 0L),with(fsumMap,fsumHsids(1L,true)))))
            .thenReturn(new FaultMessage(2L, true))
            ;

        Map<Long,Long> decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(1L, true));

        verify(mbox,times(1)).deliverFront(any(VoltMessage.class));
        verify(mbox,times(1)).send(any(long[].class), any(VoltMessage.class));
        verify(mbox).send(survivorCaptor.capture(), argThat(failureUpdateMsgIs(1L,5L,fsumHsids(1L,true))));
        assertThat(Longs.asList(survivorCaptor.getValue()), contains(0L,2L,3L));

        assertEquals(decision,ImmutableMap.<Long,Long>of());

        reset(mbox);
        decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(1L, true));

        verify(mbox,never()).deliverFront(any(VoltMessage.class));
        verify(mbox,never()).send(any(long[].class),any(FailureSiteUpdateMessage.class));
        verify(mbox,never()).recvBlocking(any(Subject[].class), eq(5L));

        assertEquals(decision,ImmutableMap.<Long,Long>of());

        reset(mbox);
        when(mbox.recvBlocking(any(Subject[].class),eq(5L)))
            .thenReturn(make(siteOneFsum.but(with(fsumSource,0L),with(fsumMap,fsumHsids(1L,true,2L,true)))))
            .thenReturn(new FaultMessage(1L, true))
            .thenReturn(make(siteOneFsum.but(with(fsumSource,0L),with(fsumMap,fsumHsids(1L,true,2L,true)))))
            .thenReturn(make(siteTwoFsum.but(with(fsumSource,0L),with(fsumMap,fsumHsids(1L,true,2L,true)))))
            .thenReturn(new FaultMessage(2L, true))
            .thenReturn(make(siteTwoFsum.but(with(fsumSource,0L),with(fsumMap,fsumHsids(1L,true,2L,true)))))
            .thenReturn(make(siteOneFsum.but(with(fsumSource,3L),with(fsumMap,fsumHsids(1L,true,2L,true)))))
            .thenReturn(make(siteOneFsum.but(with(fsumSource,3L),with(fsumMap,fsumHsids(1L,true,2L,true)))))
            .thenReturn(make(siteTwoFsum.but(with(fsumSource,3L),with(fsumMap,fsumHsids(1L,true,2L,true)))))
            .thenReturn(make(siteTwoFsum.but(with(fsumSource,3L),with(fsumMap,fsumHsids(1L,true,2L,true)))))
        ;

        decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(2L, true));

        verify(mbox,never()).deliverFront(any(VoltMessage.class));
        verify(mbox,times(2)).send(any(long[].class), any(VoltMessage.class));
        verify(mbox).send(survivorCaptor.capture(), argThat(failureUpdateMsgIs(1L,5L,fsumHsids(1L,true,2L,true))));
        assertThat(Longs.asList(survivorCaptor.getValue()), contains(0L,3L));
        verify(mbox).send(survivorCaptor.capture(), argThat(failureUpdateMsgIs(2L,6L,fsumHsids(1L,true,2L,true))));

        assertEquals(decision,ImmutableMap.<Long,Long>of(1L,5L,2L,6L));

        reset(mbox);
        decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(2L, true));

        verify(mbox,never()).deliverFront(any(VoltMessage.class));
        verify(mbox,never()).send(any(long[].class),any(FailureSiteUpdateMessage.class));
        verify(mbox,never()).recvBlocking(any(Subject[].class), eq(5L));

        assertEquals(decision,ImmutableMap.<Long,Long>of());
    }

    @Test
    public void testPingsOnLongReceives() throws Exception {
        Maker<FailureSiteUpdateMessage> siteOneFsum = fsum.but(with(fsumTxnid,2L));

        when(aide.getNewestSafeTransactionForInitiator(1L)).thenReturn(2L);
        when(mbox.recvBlocking(any(Subject[].class), eq(5L)))
            .thenReturn((VoltMessage)null)
            .thenReturn((VoltMessage)null)
            .thenReturn((VoltMessage)null)
            .thenReturn(make(siteOneFsum.but(with(fsumSource, 0L),with(fsumMap,fsumHsids(1L,true)))))
            .thenReturn(make(siteOneFsum.but(with(fsumSource, 2L),with(fsumMap,fsumHsids(1L,true)))))
            .thenReturn(make(siteOneFsum.but(with(fsumSource, 3L),with(fsumMap,fsumHsids(1L,true)))))
            ;

        Map<Long,Long> decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(1L, true));

        verify(mbox,times(1)).send(any(long[].class), any(VoltMessage.class));
        verify(mbox).send(survivorCaptor.capture(), argThat(failureUpdateMsgIs(1L, 2L,fsumHsids(1L,true))));
        assertThat(Longs.asList(survivorCaptor.getValue()), contains(0L,2L,3L));
        verify(aide,atLeast(2)).sendHeartbeats(destinationCaptor.capture());
        assertEquals(destinationCaptor.getValue(), hsids);

        assertEquals(decision,ImmutableMap.<Long,Long>of(1L,2L));
    }

    @Test
    public void testMixOfWitnessedAndNon() throws Exception {
        Maker<FailureSiteUpdateMessage> um = a(FailureSiteUpdateMessage,with(fsumTxnid,2L));

        when(aide.getNewestSafeTransactionForInitiator(1L)).thenReturn(2L);

        when(mbox.recvBlocking(any(Subject[].class), eq(5L)))
            .thenReturn(make(um.but(with(fsumSite,1L),with(fsumSource, 2L),with(fsumMap,fsumHsids(1L,true)))))
            .thenReturn(new FaultMessage(1L, true))
        ;
        Map<Long,Long> decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(1L, false));

        verify(mbox,times(1)).deliverFront(any(VoltMessage.class));
        verify(mbox,times(1)).send(any(long[].class), any(VoltMessage.class));
        verify(mbox).send(survivorCaptor.capture(), argThat(failureUpdateMsgIs(1L,2L,fsumHsids(1L,false))));
        assertThat(Longs.asList(survivorCaptor.getValue()), contains(0L,1L,2L,3L));

        assertEquals(decision,ImmutableMap.<Long,Long>of());

        reset(mbox);
        when(mbox.recvBlocking(any(Subject[].class), eq(5L)))
            .thenReturn(make(um.but(with(fsumSite,1L),with(fsumSource, 0L),with(fsumMap,fsumHsids(1L,true)))))
            .thenReturn(make(um.but(with(fsumSite,1L),with(fsumSource, 3L),with(fsumMap,fsumHsids(1L,false)))))
            .thenReturn(make(um.but(with(fsumSite,1L),with(fsumSource, 3L),with(fsumMap,fsumHsids(1L,true)))))
        ;
        decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(1L, true));

        verify(mbox,never()).deliverFront(any(VoltMessage.class));
        verify(mbox,times(1)).send(any(long[].class), any(VoltMessage.class));
        verify(mbox).send(survivorCaptor.capture(), argThat(failureUpdateMsgIs(1L,2L,fsumHsids(1L,true))));
        assertThat(Longs.asList(survivorCaptor.getValue()), contains(0L,02L,3L));

        assertEquals(decision,ImmutableMap.<Long,Long>of(1L,2L));
    }

    @Test
    public void testOneLinkDownFromThePerspictiveOfAWitness() throws Exception {
        Maker<FailureSiteUpdateMessage> um = a(FailureSiteUpdateMessage,with(fsumTxnid,10L));
        Maker<FailureSiteForwardMessage> uf = a(FailureSiteForwardMessage);

        when(aide.getNewestSafeTransactionForInitiator(0L)).thenReturn(10L);
        when(aide.getNewestSafeTransactionForInitiator(1L)).thenReturn(10L);

        when(mbox.recvBlocking(any(Subject[].class), eq(5L)))
            .thenReturn(make(um.but(with(fsumSite,0L),with(fsumSource, 2L),with(fsumMap,fsumHsids(0L,false)))))
            .thenReturn(new FaultMessage(0L, false))
        ;
        Map<Long,Long> decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(1L,true));

        verify(mbox,times(1)).deliverFront(any(VoltMessage.class));
        verify(mbox,times(1)).send(any(long[].class), any(VoltMessage.class));
        verify(mbox).send(survivorCaptor.capture(), argThat(failureUpdateMsgIs(1L,10L,fsumHsids(1L,true))));
        assertThat(Longs.asList(survivorCaptor.getValue()), contains(0L,2L,3L));

        assertEquals(decision,ImmutableMap.<Long,Long>of());

        reset(mbox);
        when(mbox.recvBlocking(any(Subject[].class), eq(5L)))
            .thenReturn(make(um.but(with(fsumSite,0L),with(fsumSource,0L),with(fsumMap,fsumHsids(0L,false,1L,true)))))
            .thenReturn(make(um.but(with(fsumSite,0L),with(fsumSource,2L),with(fsumMap,fsumHsids(0L,false,1L,false)))))
            .thenReturn(make(um.but(with(fsumSite,0L),with(fsumSource,3L),with(fsumMap,fsumHsids(0L,false,1L,false)))))
            .thenReturn(make(um.but(with(fsumSite,1L),with(fsumSource,0L),with(fsumMap,fsumHsids(1L,true,0L,false)))))
            .thenReturn(make(um.but(with(fsumSite,1L),with(fsumSource,2L),with(fsumMap,fsumHsids(1L,false,0L,false)))))
            .thenReturn(make(um.but(with(fsumSite,1L),with(fsumSource,3L),with(fsumMap,fsumHsids(1L,false,0L,false)))))
            .thenReturn(make(uf.but(with(fsfmMsg,
                    um.but(with(fsumSite,0L),with(fsumSource, 1L),with(fsumMap,fsumHsids(0L,true,1L,false)))))))
            .thenReturn(make(uf.but(with(fsfmMsg,
                    um.but(with(fsumSite,0L),with(fsumSource, 1L),with(fsumMap,fsumHsids(0L,true,1L,false)))))))
        ;
        decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(0L,false));

        verify(mbox,never()).deliverFront(any(VoltMessage.class));
        verify(mbox,times(2)).send(any(long[].class), any(VoltMessage.class));
        verify(mbox).send(survivorCaptor.capture(), argThat(failureUpdateMsgIs(0L,10L,fsumHsids(1L,true,0L,false))));
        assertThat(Longs.asList(survivorCaptor.getValue()), contains(0L,2L,3L));
        verify(mbox).send(survivorCaptor.capture(), argThat(failureUpdateMsgIs(1L,10L,fsumHsids(1L,true,0L,false))));

        assertEquals(decision,ImmutableMap.<Long,Long>of(1L,10L));
    }

    @Test
    public void testOneLinkDownFromThePerspectiveOfNonWitness() throws Exception {
        Maker<FailureSiteUpdateMessage> um = a(FailureSiteUpdateMessage,with(fsumTxnid,10L));

        when(aide.getNewestSafeTransactionForInitiator(1L)).thenReturn(10L);
        when(aide.getNewestSafeTransactionForInitiator(2L)).thenReturn(10L);

        when(mbox.recvBlocking(any(Subject[].class), eq(5L)))
            .thenReturn(make(um.but(with(fsumSite,1L),with(fsumSource, 3L),with(fsumMap,fsumHsids(1L,false)))))
            .thenReturn(new FaultMessage(0L, false))
        ;
        Map<Long,Long> decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(2L,false));

        verify(mbox,times(1)).deliverFront(any(VoltMessage.class));
        verify(mbox,times(1)).send(any(long[].class), any(VoltMessage.class));
        verify(mbox).send(survivorCaptor.capture(), argThat(failureUpdateMsgIs(2L,10L,fsumHsids(2L,false))));
        assertThat(Longs.asList(survivorCaptor.getValue()), contains(0L,1L,2L,3L));

        assertEquals(decision,ImmutableMap.<Long,Long>of());

        reset(mbox);
        when(mbox.recvBlocking(any(Subject[].class), eq(5L)))
            .thenReturn(make(um.but(with(fsumSite,1L),with(fsumSource, 0L),with(fsumMap,fsumHsids(1L,false,2L,false)))))
            .thenReturn(make(um.but(with(fsumSite,1L),with(fsumSource, 1L),with(fsumMap,fsumHsids(1L,false,2L,true)))))
            .thenReturn(make(um.but(with(fsumSite,1L),with(fsumSource, 2L),with(fsumMap,fsumHsids(1L,true,2L,false)))))
            .thenReturn(make(um.but(with(fsumSite,1L),with(fsumSource, 3L),with(fsumMap,fsumHsids(1L,false,2L,false)))))
            .thenReturn(make(um.but(with(fsumSite,2L),with(fsumSource, 0L),with(fsumMap,fsumHsids(1L,false,2L,false)))))
            .thenReturn(make(um.but(with(fsumSite,2L),with(fsumSource, 1L),with(fsumMap,fsumHsids(1L,false,2L,true)))))
            .thenReturn(make(um.but(with(fsumSite,2L),with(fsumSource, 2L),with(fsumMap,fsumHsids(1L,true,2L,false)))))
            .thenReturn(make(um.but(with(fsumSite,2L),with(fsumSource, 3L),with(fsumMap,fsumHsids(1L,false,2L,false)))))
        ;
        decision = arbiter.reconfigureOnFault(hsids, new FaultMessage(1L,false));

        verify(mbox,never()).deliverFront(any(VoltMessage.class));
        verify(mbox,times(4)).send(any(long[].class), any(VoltMessage.class));
        verify(mbox).send(survivorCaptor.capture(), argThat(failureUpdateMsgIs(1L,10L,fsumHsids(1L,false,2L,false))));
        assertThat(Longs.asList(survivorCaptor.getValue()), contains(0L,01L,2L,3L));
        verify(mbox).send(survivorCaptor.capture(), argThat(failureUpdateMsgIs(2L,10L,fsumHsids(1L,false,2L,false))));
        assertThat(Longs.asList(survivorCaptor.getValue()), contains(0L,01L,2L,3L));
        verify(mbox).send(eq(new long[] {2L}), argThat(failureForwardMsgIs(1L,10L,fsumHsids(1L,false,2L,true))));
        verify(mbox).send(eq(new long[] {1L}), argThat(failureForwardMsgIs(2L,10L,fsumHsids(1L,true,2L,false))));
        assertEquals(decision,ImmutableMap.<Long,Long>of(2L,10L));
    }
}
