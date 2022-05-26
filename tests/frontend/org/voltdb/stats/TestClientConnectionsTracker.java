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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestClientConnectionsTracker {

    @Test
    public void shouldAllowConnectionsBelowLimit() {
        // Given
        FileDescriptorsTracker maxFileDescriptorTracker = new FileDescriptorsTracker();
        ClientConnectionsTracker tracker = new ClientConnectionsTracker(maxFileDescriptorTracker);

        // When
        boolean actual = tracker.isConnectionsLimitReached();

        // Then
        assertThat(actual).isFalse();
    }

    @Test
    public void shouldReturnMaxAllowedConnectionsBasedOnMaxFileDescriptorCountMinusHeadroom() {
        // Given
        FileDescriptorsTracker maxFileDescriptorTracker = new FileDescriptorsTracker();
        ClientConnectionsTracker tracker = new ClientConnectionsTracker(maxFileDescriptorTracker);

        int expected = maxFileDescriptorTracker.getOpenFileDescriptorLimit() - ClientConnectionsTracker.FILE_DESCRIPTOR_HEADROOM;

        // When
        int actual = tracker.getMaxNumberOfAllowedConnections();

        // Then
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void shouldTrackConnectionsWhenOpened() {
        // Given
        FileDescriptorsTracker maxFileDescriptorTracker = new FileDescriptorsTracker();
        ClientConnectionsTracker tracker = new ClientConnectionsTracker(maxFileDescriptorTracker);

        // When
        tracker.connectionOpened();

        // Then
        assertThat(tracker.isConnectionsLimitReached()).isFalse();
        assertThat(tracker.getConnectionsCount()).isOne();
    }

    @Test
    public void shouldTrackConnectionsWhenClosed() {
        // Given
        FileDescriptorsTracker maxFileDescriptorTracker = new FileDescriptorsTracker();
        ClientConnectionsTracker tracker = new ClientConnectionsTracker(maxFileDescriptorTracker);

        // When
        tracker.connectionOpened();
        tracker.connectionClosed();

        // Then
        assertThat(tracker.isConnectionsLimitReached()).isFalse();
        assertThat(tracker.getConnectionsCount()).isZero();
    }

    @Test
    public void shouldTrackConnections() {
        // Given
        FileDescriptorsTracker maxFileDescriptorTracker = new FileDescriptorsTracker();
        ClientConnectionsTracker tracker = new ClientConnectionsTracker(maxFileDescriptorTracker);

        int connectionsToOpen = 100;
        for (int i = 0; i < connectionsToOpen; i++) {
            tracker.connectionOpened();
            tracker.connectionOpened();
            tracker.connectionClosed();
        }

        // When
        int actual = tracker.getConnectionsCount();

        // Then
        assertThat(tracker.isConnectionsLimitReached()).isFalse();
        assertThat(actual).isEqualTo(connectionsToOpen);
    }

    @Test
    public void shouldIndicateThatConnectionsLimitIsReached() {
        // Given
        FileDescriptorsTracker maxFileDescriptorTracker = new FileDescriptorsTracker();
        ClientConnectionsTracker tracker = new ClientConnectionsTracker(maxFileDescriptorTracker);

        int connectionsToOpen = tracker.getMaxNumberOfAllowedConnections();
        for (int i = 0; i < connectionsToOpen; i++) {
            tracker.connectionOpened();
            tracker.connectionOpened();
            tracker.connectionClosed();
        }

        // When
        int actual = tracker.getConnectionsCount();

        // Then
        assertThat(tracker.isConnectionsLimitReached()).isTrue();
        assertThat(actual).isEqualTo(connectionsToOpen);
    }
}
