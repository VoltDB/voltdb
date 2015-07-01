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

package org.voltdb.compiler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.LocalObjectMessage;
import org.voltdb.compiler.AsyncCompilerWork.AsyncCompilerWorkCompletionHandler;

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
        final AtomicInteger completedRequests = new AtomicInteger();
        final AtomicReference<AsyncCompilerResult> result = new AtomicReference<AsyncCompilerResult>();
        final long threadId = Thread.currentThread().getId();
        for (int i = 0; i < AsyncCompilerAgent.MAX_QUEUE_DEPTH + 2; ++i) {
            AsyncCompilerWorkCompletionHandler handler = new AsyncCompilerWorkCompletionHandler() {
                @Override
                public void onCompletion(AsyncCompilerResult compilerResult) {
                    completedRequests.incrementAndGet();
                    /*
                     * A rejected request will be handled in the current thread invoking deliver
                     * so use that to record the error response
                     */
                    if (Thread.currentThread().getId() == threadId) {
                        result.set(compilerResult);
                    }
                }
            };
            // This API that underlies dynamic ad hoc invocation from a stored procedure is
            // also handy here for setting up a mocked up planner work request for testing purposes.
            AdHocPlannerWork work = AdHocPlannerWork.makeStoredProcAdHocPlannerWork(100, "select * from a",
                                                                                    null, false, null,
                                                                                    handler);
            LocalObjectMessage msg = new LocalObjectMessage(work);
            msg.m_sourceHSId = 100;
            m_agent.m_mailbox.deliver(msg);
        }

        // check for one rejected request
        assertNotNull(result.get().errorMsg);

        // let all requests return
        blockingAnswer.flag.release(AsyncCompilerAgent.MAX_QUEUE_DEPTH + 5);

        // check if all previous requests finish
        m_agent.shutdown();
        assertEquals(AsyncCompilerAgent.MAX_QUEUE_DEPTH + 2, completedRequests.get());
    }
}
