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
import static org.voltcore.messaging.SocketJoiner.ConnectionType.PUBLISH_HOSTID;
import static org.voltcore.messaging.SocketJoiner.ConnectionType.REQUEST_CONNECTION;

public class TestPublishHostIdRequest {

    @Test
    public void shouldSerializeAndDeserialize() throws Exception {
        // Given
        String version = "version";
        int hostId = 123;
        int port = 123;
        String address = "address";
        String hostDisplayName = "hostDisplayName";

        // When
        PublishHostIdRequest publishHostIdRequest1 = PublishHostIdRequest.create(hostId, port, hostDisplayName, address, version);
        PublishHostIdRequest publishHostIdRequest2 = (PublishHostIdRequest) SocketJoinerMessageParser.parse(publishHostIdRequest1.getJsonObject());

        // Then
        assertThat(publishHostIdRequest2.getType()).isEqualTo(PUBLISH_HOSTID.name());
        assertThat(publishHostIdRequest2.getVersionString()).isEqualTo(version);
        assertThat(publishHostIdRequest2.getHostId()).isEqualTo(hostId);
        assertThat(publishHostIdRequest2.getPort()).isEqualTo(port);
        assertThat(publishHostIdRequest2.getAddress()).isEqualTo(address);
        assertThat(publishHostIdRequest2.getHostDisplayName()).isEqualTo(hostDisplayName);
    }
}
