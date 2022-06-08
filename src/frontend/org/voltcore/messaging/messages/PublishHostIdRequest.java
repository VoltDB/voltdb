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
import static org.voltcore.messaging.messages.FieldNames.HOST_ID;
import static org.voltcore.messaging.messages.FieldNames.PORT;
import static org.voltcore.messaging.messages.FieldNames.TYPE;
import static org.voltcore.messaging.messages.FieldNames.VERSION_STRING;

public class PublishHostIdRequest extends SocketJoinerMessageBase {

    public PublishHostIdRequest(JSONObject jsonObject) {
        super(jsonObject);
    }

    public int getHostId() throws JSONException {
        return getJsonObject().getInt(HOST_ID);
    }

    public static PublishHostIdRequest create(int hostId,
                                              int port,
                                              String hostDisplayName,
                                              String address,
                                              String versionString) throws JSONException {
        JSONObject jsObj = new JSONObject();
        jsObj.put(TYPE, SocketJoiner.ConnectionType.PUBLISH_HOSTID.name());
        jsObj.put(VERSION_STRING, versionString);
        jsObj.put(HOST_ID, hostId);
        jsObj.put(PORT, port);
        jsObj.put(ADDRESS, address);
        jsObj.put(HOST_DISPLAY_NAME, hostDisplayName);
        return new PublishHostIdRequest(jsObj);
    }
}
