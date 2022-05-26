/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
package org.voltdb.stats;

import com.sun.management.UnixOperatingSystemMXBean;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class TestFileDescriptorTracker {

    private VoltDBInterface m_testInstance;

    @Before
    public void setUp() throws Exception {
        m_testInstance = mock(VoltDBInterface.class);
        VoltDB.replaceVoltDBInstanceForTest(m_testInstance);
    }

    @Test
    public void shouldGiveCorrectAnswerWhenNotYetStarted() {
        // Given
        FileDescriptorsTracker tracker = new FileDescriptorsTracker();

        // When
        int count = tracker.getOpenFileDescriptorCount();
        int limit = tracker.getOpenFileDescriptorLimit();

        // Then
        UnixOperatingSystemMXBean operatingSystemMXBean = (UnixOperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        assertThat(count).isEqualTo(operatingSystemMXBean.getOpenFileDescriptorCount());
        assertThat(limit).isEqualTo(operatingSystemMXBean.getMaxFileDescriptorCount());
    }

    @Test
    public void shutdownShouldNotBlowUpWhenNotYetStarted() {
        // Given
        FileDescriptorsTracker tracker = new FileDescriptorsTracker();

        // When
        tracker.shutdown();

        // Then
        verifyZeroInteractions(m_testInstance);
    }

    @Test
    public void shouldUpdateFileDescriptorLimitToSoftLimit() {
        // Given
        FileDescriptorsTracker tracker = new FileDescriptorsTracker();

        CLibraryForTests.Rlimit currentLimit = CLibraryForTests.getCurrentLimit();
        currentLimit.rlim_cur = currentLimit.rlim_cur - 2;
        currentLimit.rlim_max = currentLimit.rlim_max - 1;
        CLibraryForTests.setLimit(currentLimit);

        // When
        tracker.update();

        // Then
        int actual = tracker.getOpenFileDescriptorLimit();
        assertThat(actual).isEqualTo(currentLimit.rlim_cur);
    }

    @Test
    public void shouldScheduleFutureWhenStarted() {
        // Given
        FileDescriptorsTracker tracker = new FileDescriptorsTracker();

        // When
        tracker.start();

        // Then
        verify(m_testInstance).scheduleWork(
                any(),
                eq(0L),
                eq(10L),
                eq(TimeUnit.MINUTES)
        );
    }

    @Test
    public void shouldScheduleUpdate() {
        // Given
        when(m_testInstance.scheduleWork(any(), anyLong(), anyLong(), any())).then(invocationOnMock -> {
            Runnable argument = invocationOnMock.getArgument(0);
            argument.run();
            return null;
        });

        FileDescriptorsTracker tracker = new FileDescriptorsTracker();

        // When
        tracker.start();

        // Then
        int count = tracker.getOpenFileDescriptorCount();
        int limit = tracker.getOpenFileDescriptorLimit();
        assertThat(count).isGreaterThan(0);
        assertThat(limit).isGreaterThan(0);
    }

    @Test
    public void shouldWarnIfMaxFileDescriptorsBelowThreshold() {
        // Given
        VoltLogger logger = mock(VoltLogger.class);
        FileDescriptorsTracker.logger = logger;

        UnixOperatingSystemMXBean operatingSystemMXBean = mock(UnixOperatingSystemMXBean.class);
        when(operatingSystemMXBean.getMaxFileDescriptorCount()).thenReturn(1024L);
        when(operatingSystemMXBean.getOpenFileDescriptorCount()).thenReturn(10L);

        // When
        new FileDescriptorsTracker(operatingSystemMXBean);

        // Then
        verify(logger).warnFmt(
                "File descriptor limit is low. Current: %d, recommended at least: %d",
                1024,
                1024 * 10
        );
    }

    @Test
    public void shouldNotWarnIfOpenAndMaxFileDescriptorCountsAreOk() {
        // Given
        VoltLogger logger = mock(VoltLogger.class);
        FileDescriptorsTracker.logger = logger;

        UnixOperatingSystemMXBean operatingSystemMXBean = mock(UnixOperatingSystemMXBean.class);
        when(operatingSystemMXBean.getMaxFileDescriptorCount()).thenReturn(10 * 1024L);
        when(operatingSystemMXBean.getOpenFileDescriptorCount()).thenReturn(10L);

        // When
        new FileDescriptorsTracker(operatingSystemMXBean);

        // Then
        verifyZeroInteractions(logger);
    }

    @Test
    public void shouldWarnIfTooManyOpenFileDescriptors() {
        // Given
        VoltLogger logger = mock(VoltLogger.class);
        FileDescriptorsTracker.logger = logger;

        UnixOperatingSystemMXBean operatingSystemMXBean = mock(UnixOperatingSystemMXBean.class);
        when(operatingSystemMXBean.getMaxFileDescriptorCount()).thenReturn(10 * 1024L);
        when(operatingSystemMXBean.getOpenFileDescriptorCount()).thenReturn(10 * 1024L - 100L);

        // When
        new FileDescriptorsTracker(operatingSystemMXBean);

        // Then
        verify(logger).warnFmt(
                "Number of used file descriptors (%d) is approaching limit (%d)",
                10_140,
                1024 * 10
        );
    }
}
