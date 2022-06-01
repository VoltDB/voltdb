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

import static org.voltcore.messaging.messages.FieldNames.BUILD_STRING;
import static org.voltcore.messaging.messages.FieldNames.VERSION_COMPATIBLE;
import static org.voltcore.messaging.messages.FieldNames.VERSION_STRING;

public class VersionBuildMessage extends MessageBase {

    public VersionBuildMessage(JSONObject jsonObject) {
        super(jsonObject);
    }

    public String getVersionString() throws JSONException {
        return jsonObject.getString(VERSION_STRING);
    }

    public String getBuildString() throws JSONException {
        return jsonObject.getString(BUILD_STRING);
    }

    public boolean isVersionCompatible() throws JSONException {
        return jsonObject.getBoolean(VERSION_COMPATIBLE);
    }

    public static VersionBuildMessage create(String versionString,
                                             String buildString,
                                             boolean versionCompatible) throws JSONException {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(VERSION_STRING, versionString);
        jsonObject.put(BUILD_STRING, buildString);
        jsonObject.put(VERSION_COMPATIBLE, versionCompatible);
        return new VersionBuildMessage(jsonObject);
    }

    public static VersionBuildMessage fromJsonObject(JSONObject jsonObject) throws JSONException {
        return new VersionBuildMessage(jsonObject);
    }
}
