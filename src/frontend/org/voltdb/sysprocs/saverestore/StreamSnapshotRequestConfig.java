/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.sysprocs.saverestore;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.SnapshotTableInfo;
import org.voltdb.catalog.Database;

import com.google_voltpatches.common.collect.ArrayListMultimap;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMultimap;
import com.google_voltpatches.common.collect.Multimap;

public class StreamSnapshotRequestConfig extends SnapshotRequestConfig {

    /**
     * Represents a stream in the snapshot. A stream has its own stream pairs,
     * may or may not specify a partition to add and the hash ranges that partition owns.
     *
     * A single snapshot can contain multiple streams.
     */
    public static class Stream {
        // src -> (dest1, dest2,...)
        public final Multimap<Long, Long> streamPairs;

        public final Long lowestSiteSinkHSId;

        /**
         * @param streamPairs     src - > (dest1, dest2,...)
         * @param newPartition    New partition for this stream, if not null, will create a
         *                        post-snapshot task to increment the partition count
         */
        public Stream(Multimap<Long, Long> streamPairs, Long lowestSiteSinkHSId)
        {
            this.streamPairs = ImmutableMultimap.copyOf(streamPairs);
            this.lowestSiteSinkHSId = lowestSiteSinkHSId;
        }
    }

    // stream configs
    public final List<Stream> streams;
    // true to also do a truncation snapshot
    public final boolean shouldTruncate;

    /**
     * @param tables             See {@link #SnapshotRequestConfig(java.util.List)} for more
     * @param streams            Stream configurations
     * @param shouldTruncate     true to also generate a truncation snapshot
     */
    public StreamSnapshotRequestConfig(List<SnapshotTableInfo> tables,
                                       List<Stream> streams,
                                       boolean shouldTruncate)
    {
        super(tables);

        this.streams = ImmutableList.copyOf(streams);
        this.shouldTruncate = shouldTruncate;
    }

    public StreamSnapshotRequestConfig(List<SnapshotTableInfo> tables, int newPartitionCount, List<Stream> streams,
            boolean shouldTruncate) {
        super(tables, newPartitionCount);

        this.streams = ImmutableList.copyOf(streams);
        this.shouldTruncate = shouldTruncate;
    }

    public StreamSnapshotRequestConfig(JSONObject jsData,
                                       Database catalogDatabase)
    {
        super(jsData, catalogDatabase);

        this.streams = parseStreams(jsData);
        this.shouldTruncate = jsData.optBoolean("shouldTruncate", false);
    }

    private ImmutableList<Stream> parseStreams(JSONObject jsData)
    {
        ImmutableList.Builder<Stream> builder = ImmutableList.builder();

        try {
            JSONArray streamArray = jsData.getJSONArray("streams");

            for (int i = 0; i < streamArray.length(); i++) {
                JSONObject streamObj = streamArray.getJSONObject(i);

                Long lowestSiteSinkHSId = Long.parseLong(streamObj.getString("lowestSiteSinkHSId"));

                Stream config = new Stream(parseStreamPairs(streamObj), lowestSiteSinkHSId);

                builder.add(config);
            }
        } catch (JSONException e) {
            SNAP_LOG.warn("Failed to parse stream snapshot request config", e);
        }

        return builder.build();
    }

    private static Multimap<Long, Long> parseStreamPairs(JSONObject jsData)
    {
        ArrayListMultimap<Long, Long> streamPairs = ArrayListMultimap.create();

        if (jsData != null) {
            try {
                JSONObject sp = jsData.getJSONObject("streamPairs");
                Iterator<String> it = sp.keys();
                while (it.hasNext()) {
                    String key = it.next();
                    long sourceHSId = Long.valueOf(key);
                    JSONArray destJSONArray = sp.getJSONArray(key);
                    for (int i = 0; i < destJSONArray.length(); i++) {
                        long destHSId = destJSONArray.getLong(i);
                        streamPairs.put(sourceHSId, destHSId);
                    }
                }
            } catch (JSONException e) {
                SNAP_LOG.warn("Failed to parse stream pair information", e);
            }
        }

        return streamPairs;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException
    {
        super.toJSONString(stringer);

        stringer.keySymbolValuePair("shouldTruncate", shouldTruncate);
        stringer.key("streams").array();

        for (Stream stream : streams) {
            stringer.object();

            stringer.keySymbolValuePair("lowestSiteSinkHSId", stream.lowestSiteSinkHSId);
            stringer.key("streamPairs").object();
            for (Map.Entry<Long, Collection<Long>> entry : stream.streamPairs.asMap().entrySet()) {
                stringer.key(Long.toString(entry.getKey())).array();
                for (long destHSId : entry.getValue()) {
                    stringer.value(destHSId);
                }
                stringer.endArray();
            }
            stringer.endObject();

            stringer.endObject();
        }

        stringer.endArray();
    }
}
