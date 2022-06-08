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

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.messaging.SocketJoiner;

import static org.voltcore.messaging.messages.FieldNames.ADDRESS;
import static org.voltcore.messaging.messages.FieldNames.HOST_DISPLAY_NAME;
import static org.voltcore.messaging.messages.FieldNames.PORT;
import static org.voltcore.messaging.messages.FieldNames.TYPE;
import static org.voltcore.messaging.messages.FieldNames.VERSION_STRING;

public class RequestHostIdRequest extends SocketJoinerMessageBase {

    RequestHostIdRequest(JSONObject jsonObject) {
        super(jsonObject);
    }

    public static RequestHostIdRequest createWithAddress(String versionString,
                                                         int port,
                                                         String hostDisplayName,
                                                         String address) throws JSONException {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(TYPE, SocketJoiner.ConnectionType.REQUEST_HOSTID.name());
        jsonObject.put(VERSION_STRING, versionString);
        jsonObject.put(PORT, port);
        jsonObject.put(ADDRESS, address);
        jsonObject.put(HOST_DISPLAY_NAME, hostDisplayName);
        return new RequestHostIdRequest(jsonObject);
    }

    public static RequestHostIdRequest createWithoutAddress(String versionString,
                                                            int port,
                                                            String hostDisplayName) throws JSONException {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(TYPE, SocketJoiner.ConnectionType.REQUEST_HOSTID.name());
        jsonObject.put(VERSION_STRING, versionString);
        jsonObject.put(PORT, port);
        jsonObject.put(HOST_DISPLAY_NAME, hostDisplayName);
        return new RequestHostIdRequest(jsonObject);
    }
}
