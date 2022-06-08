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

package org.voltcore.messaging.messages;

import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.voltcore.messaging.SocketJoiner.ConnectionType.REQUEST_CONNECTION;
import static org.voltcore.messaging.SocketJoiner.ConnectionType.REQUEST_HOSTID;

public class TestRequestForConnectionRequest {

    @Test
    public void shouldSerializeAndDeserialize() throws Exception {
        // Given
        String version = "version";
        int hostId = 123;
        int port = 123;
        String address = "address";
        String hostDisplayName = "hostDisplayName";

        // When
        RequestForConnectionRequest requestForConnectionRequest1 = RequestForConnectionRequest.create(version, hostId, port, hostDisplayName, address);
        RequestForConnectionRequest requestForConnectionRequest2 = (RequestForConnectionRequest) SocketJoinerMessageParser.parse(requestForConnectionRequest1.getJsonObject());

        // Then
        assertThat(requestForConnectionRequest2.getType()).isEqualTo(REQUEST_CONNECTION.name());
        assertThat(requestForConnectionRequest2.getVersionString()).isEqualTo(version);
        assertThat(requestForConnectionRequest2.getHostId()).isEqualTo(hostId);
        assertThat(requestForConnectionRequest2.getPort()).isEqualTo(port);
        assertThat(requestForConnectionRequest2.getAddress()).isEqualTo(address);
        assertThat(requestForConnectionRequest2.getHostDisplayName()).isEqualTo(hostDisplayName);
    }
}
