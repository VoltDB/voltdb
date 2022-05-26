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
package org.voltdb;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.DRCatalogDiffEngine;
import org.voltdb.utils.Encoder;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * Simple class to encapsulate and manage the remote producer catalog commands. These are used during recovery and
 * catalog update to calculate new sets of replicable tables between this cluster and another.
 * <p>
 * This is a separate class from {@link ConsumerDRGateway} because it needs to be available during command log replay
 * and the gateway is not available until after replay has completed
 */
public class DrProducerCatalogCommands {
    private final Map<Byte, byte[]> m_commands = new ConcurrentHashMap<>();

    /**
     * Set the catalog commands for {@code clusterId}
     *
     * @param clusterId       of the cluster these catalog commands are from
     * @param catalogCommands from remote cluster
     */
    public void set(byte clusterId, byte[] catalogCommands) {
        m_commands.put(clusterId, catalogCommands);
    }

    /**
     * Replace the current set of catalog commands with those in {@code catalogCommands}. This clears any currently
     * catalog commands and replaces it with those provided
     *
     * @param catalogCommands to be set in this instance
     */
    public void setAll(Map<Byte, byte[]> catalogCommands) {
        clear();
        m_commands.putAll(catalogCommands);
    }

    /**
     * Remove the catalog commands associated with {@code clusterId}
     *
     * @param clusterId of catalog commands to be removed
     */
    public void remove(byte clusterId) {
        m_commands.remove(clusterId);
    }

    /**
     * @return an unmodifiable view of the current catalog commands
     */
    public Map<Byte, byte[]> get() {
        return Collections.unmodifiableMap(m_commands);
    }

    /**
     * @return a {@link JSONObject} representation of this instance that can be passed to {@link #restore(JSONObject)}
     */
    public JSONObject getForRestore() {
        try {
            JSONObject json = new JSONObject();
            for (Map.Entry<Byte, byte[]> entry : m_commands.entrySet()) {
                json.put(entry.getKey().toString(), Encoder.base64Encode(entry.getValue()));
            }
            return json;
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Restore this instance using the data in {@code clusterCatalogCommands}. {@code clusterCatalogCommands} should be
     * the result from {@link #getForRestore()}
     *
     * @param clusterCatalogCommands json serialized representation of this class
     * @throws JSONException                   If there is an error retrieving data from {@code clusterCatalogCommands}
     * @throws NumberFormatException           If the data in {@code clusterCatalogCommands} cannot be parsed
     * @throws StringIndexOutOfBoundsException If the data in {@code clusterCatalogCommands} cannot be parsed
     */
    public void restore(JSONObject clusterCatalogCommands)
            throws JSONException, NumberFormatException, StringIndexOutOfBoundsException {
        m_commands.clear();
        Iterator<String> clusterIds = clusterCatalogCommands.keys();
        while (clusterIds.hasNext()) {
            String clusterIdString = clusterIds.next();
            Byte clusterId = Byte.valueOf(clusterIdString);
            String catalogCommands = clusterCatalogCommands.getString(clusterIdString);
            m_commands.put(clusterId, Encoder.base64Decode(catalogCommands));
        }
    }

    /**
     * Comparing {@code catalog} to the catalogs in this instance, generate a map from clusterId to array of table
     * names. The array of table names represents the tables which can be a target for replication from the producer
     * with that clusterId.
     *
     * @param catalog to compare against
     * @return {@link Map} of {@code clusterId} to array of table names
     */
    public Map<Byte, String[]> calculateReplicableTables(Catalog catalog) {
        Catalog local = DRCatalogDiffEngine.getDrCatalog(catalog);

        ImmutableMap.Builder<Byte, String[]> builder = ImmutableMap.builder();

        for (Map.Entry<Byte, byte[]> entry : m_commands.entrySet()) {
            String[] tables = DRCatalogDiffEngine.calculateReplicableTables(entry.getKey(), local, entry.getValue());
            builder.put(entry.getKey(), tables);
        }

        return builder.build();
    }

    /**
     * Clear all commands in this instance
     */
    public void clear() {
        m_commands.clear();
    }
}
