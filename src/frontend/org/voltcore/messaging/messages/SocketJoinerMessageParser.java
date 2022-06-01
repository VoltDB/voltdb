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

import static org.voltcore.messaging.messages.FieldNames.TYPE;

public class SocketJoinerMessageParser {

    public static SocketJoinerMessageBase parse(JSONObject jsonObject) throws JSONException {
        String type = jsonObject.getString(TYPE);

        if (type.equals(SocketJoiner.ConnectionType.REQUEST_HOSTID.name())) {
            return new RequestHostIdRequest(jsonObject);
        }
        if (type.equals(SocketJoiner.ConnectionType.PUBLISH_HOSTID.name())) {
            return new PublishHostIdRequest(jsonObject);
        }
        if (type.equals(SocketJoiner.ConnectionType.REQUEST_CONNECTION.name())) {
            return new RequestForConnectionRequest(jsonObject);
        }

        throw new RuntimeException("Unexpected message type " + type);
    }
}
