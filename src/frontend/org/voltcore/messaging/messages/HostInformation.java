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

import static org.voltcore.messaging.messages.FieldNames.ADDRESS;
import static org.voltcore.messaging.messages.FieldNames.HOST_DISPLAY_NAME;
import static org.voltcore.messaging.messages.FieldNames.HOST_ID;
import static org.voltcore.messaging.messages.FieldNames.PORT;

public class HostInformation extends MessageBase {

    HostInformation(JSONObject jsonObject) {
        super(jsonObject);
    }

    public String getAddress() throws JSONException {
        return jsonObject.getString(ADDRESS);
    }

    public int getPort() throws JSONException {
        return jsonObject.getInt(PORT);
    }

    public int getHostId() throws JSONException {
        return jsonObject.getInt(HOST_ID);
    }

    public String getHostDisplayName() throws JSONException {
        return jsonObject.getString(HOST_DISPLAY_NAME);
    }

    public static HostInformation create(int hostId, String address, int port, String hostDisplayName) throws JSONException {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(HOST_ID, hostId);
        jsonObject.put(ADDRESS, address);
        jsonObject.put(PORT, port);
        jsonObject.put(HOST_DISPLAY_NAME, hostDisplayName);
        return new HostInformation(jsonObject);
    }

    public static HostInformation fromJsonObject(JSONObject jsonObject) {
        return new HostInformation(jsonObject);
    }
}
