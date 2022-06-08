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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.voltcore.messaging.SocketJoiner.ConnectionType.REQUEST_HOSTID;

public class TestRequestHostIdRequest {

    @Test
    public void shouldSerializeAndDeserializeWithAddress() throws Exception {
        // Given
        String version = "version";
        int port = 123;
        String address = "address";
        String hostDisplayName = "hostDisplayName";

        // When
        RequestHostIdRequest requestHostIdRequest1 = RequestHostIdRequest.createWithAddress(version, port, hostDisplayName, address);
        RequestHostIdRequest requestHostIdRequest2 = (RequestHostIdRequest) SocketJoinerMessageParser.parse(requestHostIdRequest1.getJsonObject());

        // Then
        assertThat(requestHostIdRequest2.getType()).isEqualTo(REQUEST_HOSTID.name());
        assertThat(requestHostIdRequest2.getVersionString()).isEqualTo(version);
        assertThat(requestHostIdRequest2.getPort()).isEqualTo(port);
        assertThat(requestHostIdRequest2.getAddress()).isEqualTo(address);
        assertThat(requestHostIdRequest2.getHostDisplayName()).isEqualTo(hostDisplayName);
    }

    @Test
    public void shouldSerializeAndDeserializeWithoutAddress() throws Exception {
        // Given
        String version = "version";
        int port = 123;
        String hostDisplayName = "hostDisplayName";

        // When
        RequestHostIdRequest requestHostIdRequest1 = RequestHostIdRequest.createWithoutAddress(version, port, hostDisplayName);
        RequestHostIdRequest requestHostIdRequest2 = (RequestHostIdRequest) SocketJoinerMessageParser.parse(requestHostIdRequest1.getJsonObject());

        // Then
        assertThat(requestHostIdRequest2.getType()).isEqualTo(REQUEST_HOSTID.name());
        assertThat(requestHostIdRequest2.getVersionString()).isEqualTo(version);
        assertThat(requestHostIdRequest2.getPort()).isEqualTo(port);
        assertThat(requestHostIdRequest2.getHostDisplayName()).isEqualTo(hostDisplayName);
    }
}
