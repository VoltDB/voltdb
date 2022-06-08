/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltcore.messaging.messages;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.voltcore.messaging.messages.FieldNames.ACCEPTED;
import static org.voltcore.messaging.messages.FieldNames.HOST_DISPLAY_NAME;
import static org.voltcore.messaging.messages.FieldNames.HOSTS;
import static org.voltcore.messaging.messages.FieldNames.MAY_RETRY;
import static org.voltcore.messaging.messages.FieldNames.NEW_HOST_ID;
import static org.voltcore.messaging.messages.FieldNames.REASON;
import static org.voltcore.messaging.messages.FieldNames.REPORTED_ADDRESS;

public class RequestJoinResponse extends MessageBase {

    RequestJoinResponse(JSONObject jsonObject) {
        super(jsonObject);
    }

    public int getNewHostId() throws JSONException {
        return jsonObject.getInt(NEW_HOST_ID);
    }

    public String getReportedAddress() throws JSONException {
        return jsonObject.getString(REPORTED_ADDRESS);
    }

    public List<HostInformation> getHosts() throws JSONException {
        JSONArray hostsJsonArray = jsonObject.getJSONArray(HOSTS);
        List<HostInformation> hosts = new ArrayList<>();
        for (int i = 0; i < hostsJsonArray.length(); i++) {
            JSONObject jsonObject = hostsJsonArray.getJSONObject(i);
            HostInformation hostInformation = HostInformation.fromJsonObject(jsonObject);
            hosts.add(hostInformation);
        }
        return hosts;
    }

    public Optional<Boolean> isAccepted() throws JSONException {
        return jsonObject.has(ACCEPTED) ? Optional.of(jsonObject.getBoolean(ACCEPTED)) : Optional.empty();
    }

    public Optional<Boolean> mayRetry() throws JSONException {
        return jsonObject.has(MAY_RETRY) ? Optional.of(jsonObject.getBoolean(MAY_RETRY)) : Optional.empty();
    }

    public Optional<String> getReason() throws JSONException {
        return jsonObject.has(REASON) ? Optional.of(jsonObject.getString(REASON)) : Optional.empty();
    }

    public static RequestJoinResponse createAccepted(int newNodeHostId,
                                                     String reportedAddress,
                                                     int hostId,
                                                     String address,
                                                     int port,
                                                     String hostDisplayName,
                                                     List<HostInformation> hosts) throws JSONException {
        HostInformation thisHostInformation = HostInformation.create(
                hostId,
                address,
                port,
                hostDisplayName
        );
        JSONArray hostsJson = new JSONArray();
        hostsJson.put(thisHostInformation.getJsonObject());
        hosts.forEach(hostInformation -> hostsJson.put(hostInformation.getJsonObject()));

        JSONObject jsonObject = new JSONObject();
        jsonObject.put(ACCEPTED, true);
        jsonObject.put(NEW_HOST_ID, newNodeHostId);
        jsonObject.put(REPORTED_ADDRESS, reportedAddress);
        jsonObject.put(HOST_DISPLAY_NAME, hostDisplayName);
        jsonObject.put(HOSTS, hostsJson);
        return new RequestJoinResponse(jsonObject);
    }

    public static RequestJoinResponse createNotAccepted(String reason, boolean mayRetry) throws JSONException {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(ACCEPTED, false);
        jsonObject.put(REASON, reason);
        jsonObject.put(MAY_RETRY, mayRetry);
        return new RequestJoinResponse(jsonObject);
    }

    public static RequestJoinResponse fromJsonObject(JSONObject jsonObject) {
        return new RequestJoinResponse(jsonObject);
    }
}
