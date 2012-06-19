/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.compiler;

import java.util.concurrent.Semaphore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.LocalObjectMessage;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestAsyncCompilerAgent {
    // this object is spied on using mockito
    private AsyncCompilerAgent m_agent = null;

    @Before
    public void setUp() {
        m_agent = spy(new AsyncCompilerAgent());
    }

    @After
    public void tearDown() throws InterruptedException {
        if (m_agent != null) {
            m_agent.shutdown();
        }
    }

    class BlockingAnswer implements Answer<AsyncCompilerResult> {
        public final Semaphore flag = new Semaphore(0);

        @Override
        public AsyncCompilerResult answer(InvocationOnMock invocation) throws Throwable {
            flag.acquire();
            return null;
        }
    }

    /**
     * Checks if a proper response is sent back when the max queue depth is
     * reached.
     * @throws MessagingException
     * @throws InterruptedException
     */
    @Test
    public void testMaxQueueDepth() throws InterruptedException {
        /*
         * mock the compileAdHocPlan method so that we can control how many
         * things will be waiting in the queue
         */
        BlockingAnswer blockingAnswer = new BlockingAnswer();
        doAnswer(blockingAnswer).when(m_agent).compileAdHocPlan(any(AdHocPlannerWork.class));

        m_agent.createMailbox(mock(HostMessenger.class), 100);
        m_agent.m_mailbox = spy(m_agent.m_mailbox);

        /*
         * send max + 2 messages to the agent. The first one will be executed
         * immediately so it doesn't consume queue capacity, the next max number
         * of messages will use up all the capacity, the last one will be
         * rejected.
         */
        for (int i = 0; i < AsyncCompilerAgent.MAX_QUEUE_DEPTH + 2; ++i) {
            AdHocPlannerWork work =
                    new AdHocPlannerWork(100l, false, 0, 0, "localhost", false, null,
                                         "select * from a", 0);
            LocalObjectMessage msg = new LocalObjectMessage(work);
            msg.m_sourceHSId = 100;
            m_agent.m_mailbox.deliver(msg);
        }

        // check for one rejected request
        ArgumentCaptor<LocalObjectMessage> captor = ArgumentCaptor.forClass(LocalObjectMessage.class);
        verify(m_agent.m_mailbox).send(eq(100L), captor.capture());
        assertNotNull(((AsyncCompilerResult) captor.getValue().payload).errorMsg);
        // let all requests return
        blockingAnswer.flag.release(AsyncCompilerAgent.MAX_QUEUE_DEPTH + 1);

        // check if all previous requests finish
        m_agent.shutdown();
        VerificationMode expected = times(AsyncCompilerAgent.MAX_QUEUE_DEPTH + 2);
        verify(m_agent.m_mailbox, expected).send(eq(100L), any(LocalObjectMessage.class));
    }
}
