/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import com.google_voltpatches.common.collect.ImmutableList;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;

import java.util.Collection;
import java.util.List;

public class BalancePartitionsRequest implements JSONString {
    public static class PartitionPair {
        public final int srcPartition;
        public final int destPartition;
        public final int rangeStart;
        public final int rangeEnd;

        public PartitionPair(int srcPartition, int destPartition, int rangeStart, int rangeEnd)
        {
            this.srcPartition = srcPartition;
            this.destPartition = destPartition;
            this.rangeStart = rangeStart;
            this.rangeEnd = rangeEnd;
        }
    }

    public final List<PartitionPair> partitionPairs;

    public BalancePartitionsRequest(Collection<PartitionPair> partitionPairs)
    {
        this.partitionPairs = ImmutableList.copyOf(partitionPairs);
    }

    public BalancePartitionsRequest(JSONObject jsObj) throws JSONException
    {
        partitionPairs = parseRanges(jsObj);
    }

    private List<PartitionPair> parseRanges(JSONObject jsObj) throws JSONException
    {
        ImmutableList.Builder<PartitionPair> builder = ImmutableList.builder();
        JSONArray pairsArray = jsObj.getJSONArray("partitionPairs");

        for (int i = 0; i < pairsArray.length(); i++) {
            JSONObject pairObj = pairsArray.getJSONObject(i);

            builder.add(new PartitionPair(pairObj.getInt("srcPartition"),
                                          pairObj.getInt("destPartition"),
                                          pairObj.getInt("rangeStart"),
                                          pairObj.getInt("rangeEnd")));
        }

        return builder.build();
    }

    @Override
    public String toJSONString()
    {
        JSONStringer stringer = new JSONStringer();

        try {
            stringer.object();
            stringer.key("partitionPairs").array();

            for (PartitionPair pair : partitionPairs) {
                stringer.object();

                stringer.key("srcPartition").value(pair.srcPartition);
                stringer.key("destPartition").value(pair.destPartition);
                stringer.key("rangeStart").value(pair.rangeStart);
                stringer.key("rangeEnd").value(pair.rangeEnd);

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
