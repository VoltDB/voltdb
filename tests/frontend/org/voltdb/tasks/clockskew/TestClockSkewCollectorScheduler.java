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
package org.voltdb.tasks.clockskew;

import org.junit.Test;
import org.mockito.Mockito;
import org.voltdb.ClientInterface;
import org.voltdb.InternalConnectionHandler;
import org.voltdb.VoltDBInterface;
import org.voltdb.task.ActionResult;
import org.voltdb.task.ActionType;
import org.voltdb.task.ScheduledAction;
import org.voltdb.task.TaskHelper;

import java.time.Duration;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.voltdb.ClockSkewCollectorAgent.COLLECT;
import static org.voltdb.ClockSkewCollectorAgent.PROCEDURE;
import static org.voltdb.tasks.clockskew.ClockSkewCollectorScheduler.DEFAULT;

public class TestClockSkewCollectorScheduler {
    private final TaskHelper helper = Mockito.mock(TaskHelper.class);
    private final ClockSkewStats stats = Mockito.mock(ClockSkewStats.class);
    private final VoltDBInterface voltDb = Mockito.mock(VoltDBInterface.class);

    @Test
    public void shouldAcceptValidSchedulerInterval() {
        // when
        ClockSkewCollectorScheduler actual = new ClockSkewCollectorScheduler(voltDb, helper, stats, Duration.parse("PT1M"));

        // then
        ScheduledAction action = actual.getFirstScheduledAction();
        assertEquals(ActionType.CALLBACK, action.getType());
        assertEquals(Duration.ofMillis(100).toMillis(), action.getInterval(MILLISECONDS));
        assertNotNull(action.getCallback());
    }

    @Test
    public void shouldDisableScheduler() {
        // when
        ClockSkewCollectorScheduler actual = new ClockSkewCollectorScheduler(voltDb, helper, stats, Duration.parse("PT0S"));

        // then
        ScheduledAction action = actual.getFirstScheduledAction();
        assertEquals(ActionType.EXIT, action.getType());
        assertEquals(-1, action.getInterval(NANOSECONDS));
        assertNull(action.getCallback());
    }

    @Test
    public void shouldCheckForValidIntervalAndFallbackToDefault() {
        // when
        ClockSkewCollectorScheduler actual = new ClockSkewCollectorScheduler(voltDb, helper, stats, Duration.parse("PT59S"));

        // then
        ScheduledAction firstAction = actual.getFirstScheduledAction();
        assertEquals(ActionType.CALLBACK, firstAction.getType());
        assertEquals(Duration.ofMillis(100).toMillis(), firstAction.getInterval(MILLISECONDS));
        assertNotNull(firstAction.getCallback());

        ScheduledAction action = actual.getAction();
        assertEquals(ActionType.CALLBACK, action.getType());
        assertEquals(DEFAULT.toMillis(), action.getInterval(MILLISECONDS));
        assertNotNull(action.getCallback());
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void shouldExecuteAction() {
        ActionResult NOT_IMPORTANT = null;

        ClientInterface clientInterface = mock(ClientInterface.class);
        InternalConnectionHandler connectionHandler = mock(InternalConnectionHandler.class);

        when(clientInterface.getInternalConnectionHandler()).thenReturn(connectionHandler);
        when(voltDb.getClientInterface()).thenReturn(clientInterface);

        ClockSkewCollectorScheduler scheduler = new ClockSkewCollectorScheduler(voltDb, helper, stats, Duration.parse("PT2M"));
        Function<ActionResult, ScheduledAction> action = scheduler.getFirstScheduledAction().getCallback();

        // when
        ScheduledAction actual = action.apply(NOT_IMPORTANT);

        // then
        Mockito.verify(connectionHandler).callProcedure(notNull(), eq(stats), notNull(), eq(PROCEDURE), eq(COLLECT));
        assertEquals(ActionType.CALLBACK, actual.getType());
        assertEquals(Duration.ofMinutes(2).toMillis(), actual.getInterval(MILLISECONDS));
        assertNotNull(actual.getCallback());
    }
}
