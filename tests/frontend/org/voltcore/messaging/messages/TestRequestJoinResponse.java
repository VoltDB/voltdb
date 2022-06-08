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

public class TestRequestJoinResponse {

    @Test
    public void shouldSerializeAndDeserializeAccepted() throws Exception {
        // Given
        int newNodeHostId = 1;
        String reportedAddress = "reportedAddress";
        int hostId = 2;
        String address = "address";
        int port = 12345;
        String hostDisplayName = "hostDisplayName";

        List<HostInformation> hostInformationList = new ArrayList<>();
        int additionalHostId = 3;
        String additionalHostAddress = "address2";
        int additionalHostPort = 54321;
        String additionalHostDisplayName = "additionalHostDisplayName";

        HostInformation hostInformation = HostInformation.create(additionalHostId, additionalHostAddress, additionalHostPort, additionalHostDisplayName);
        hostInformationList.add(hostInformation);

        // When
        RequestJoinResponse requestJoinResponse1 = RequestJoinResponse.createAccepted(newNodeHostId, reportedAddress, hostId, address, port, hostDisplayName, hostInformationList);
        RequestJoinResponse requestJoinResponse2 = RequestJoinResponse.fromJsonObject(requestJoinResponse1.getJsonObject());

        // Then
        assertThat(requestJoinResponse2.isAccepted()).contains(true);
        assertThat(requestJoinResponse2.getReason()).isEmpty();
        assertThat(requestJoinResponse2.mayRetry()).isEmpty();

        assertThat(requestJoinResponse2.getNewHostId()).isEqualTo(newNodeHostId);
        assertThat(requestJoinResponse2.getReportedAddress()).isEqualTo(reportedAddress);

        assertThat(requestJoinResponse2.getHosts().size()).isEqualTo(2);

        HostInformation hostInformation1 = requestJoinResponse2.getHosts().get(0);
        assertThat(hostInformation1.getHostId()).isEqualTo(hostId);
        assertThat(hostInformation1.getAddress()).isEqualTo(address);
        assertThat(hostInformation1.getPort()).isEqualTo(port);
        assertThat(hostInformation1.getHostDisplayName()).isEqualTo(hostDisplayName);

        HostInformation hostInformation2 = requestJoinResponse2.getHosts().get(1);
        assertThat(hostInformation2.getHostId()).isEqualTo(additionalHostId);
        assertThat(hostInformation2.getAddress()).isEqualTo(additionalHostAddress);
        assertThat(hostInformation2.getPort()).isEqualTo(additionalHostPort);
        assertThat(hostInformation2.getHostDisplayName()).isEqualTo(additionalHostDisplayName);
    }

    @Test
    public void shouldSerializeAndDeserializeNotAccepted() throws Exception {
        // Given
        String reason = "reason";
        boolean mayRetry = false;

        // When
        RequestJoinResponse requestJoinResponse1 = RequestJoinResponse.createNotAccepted(reason, mayRetry);
        RequestJoinResponse requestJoinResponse2 = RequestJoinResponse.fromJsonObject(requestJoinResponse1.getJsonObject());

        // Then
        assertThat(requestJoinResponse2.isAccepted()).contains(false);
        assertThat(requestJoinResponse2.getReason()).contains(reason);
        assertThat(requestJoinResponse2.mayRetry()).contains(mayRetry);
    }
}
