/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.sysprocs;

import com.google_voltpatches.common.collect.ImmutableSet;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltdb.TheHashinator;
import org.voltdb.VoltType;

import java.util.Collection;
import java.util.Set;

public class AdHocNPPartitions implements JSONString {

    public final Set<Integer> partitions;

    public AdHocNPPartitions(Collection<Integer> partitions)
    {
        this.partitions = ImmutableSet.copyOf(partitions);
    }

    /**
     *
     * @param jsObj type and keys in json format or partitions in json format
     * @param fromKey true if jsObj is type and keys, false if jsObj is partitions
     * @throws JSONException
     */
    public AdHocNPPartitions(JSONObject jsObj, boolean fromKey) throws JSONException
    {
        if (fromKey) {
            partitions = parseKeys(jsObj);
        }
        else {
            partitions = parsePartitions(jsObj);
        }
    }

    /**
     * Reads key list from json object and hashinates them
     * @param jsObj
     * @return
     * @throws JSONException
     */
    private Set<Integer> parseKeys(JSONObject jsObj) throws JSONException
    {
        ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();
        String typeString = jsObj.getString("Type");
        int type = VoltType.typeFromString(typeString).getValue();

        JSONArray keysArray = jsObj.getJSONArray("Keys");

        for (int i = 0; i < keysArray.length(); i++) {
            JSONObject keyObj = keysArray.getJSONObject(i);
            Object key = keyObj.getString("Key");
            int site = TheHashinator.getPartitionForParameter(type, key);

            builder.add(site);
        }
        return builder.build();
    }

    private Set<Integer> parsePartitions(JSONObject jsObj) throws JSONException
    {
        ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();
        JSONArray partsArray = jsObj.getJSONArray("PartitionId");

        for (int i = 0; i < partsArray.length(); i++) {
            JSONObject partObj = partsArray.getJSONObject(i);
            builder.add(partObj.getInt("ID"));
        }

        return builder.build();
    }

    @Override
    public String toJSONString()
    {
        JSONStringer stringer = new JSONStringer();

        try {
            stringer.object();
            stringer.key("PartitionId").array();

            for (Integer id : partitions) {
                stringer.object();

                stringer.key("ID").value(id);

                stringer.endObject();
            }

            stringer.endArray();
            stringer.endObject();

            return stringer.toString();
        } catch (JSONException e) {
            return null;
        }
    }
}
