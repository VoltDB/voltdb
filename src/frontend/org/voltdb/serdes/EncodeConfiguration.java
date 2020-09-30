/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.serdes;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.utils.DeferredSerialization;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.FormatParameter;
import org.voltdb.catalog.Topic;
import org.voltdb.utils.SerializationHelper;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * A class encapsulating the encoding configuration for keys or values in catalog {@link Topic}
 */
public class EncodeConfiguration implements DeferredSerialization {

    private static short LATEST_VERSION = 1;
    private final EncodeFormat m_format;
    private final ImmutableMap<String, String> m_parameters;

    /**
     * Empty constructor
     */
    public EncodeConfiguration() {
        m_format = EncodeFormat.INVALID;
        m_parameters = ImmutableMap.of();
    }

    /**
     * Constructor from {@link Topic} fields
     *
     * @param isKey {@code true} if for key format, {@code false} for value format
     * @param topic {@link Topic} from catalog
     */
    public EncodeConfiguration(boolean isKey, Topic topic) {
        m_format = EncodeFormat.parseFormat(isKey, topic.getIsopaque(),
                isKey ? topic.getKeyformatname() : topic.getValueformatname());

        CatalogMap<FormatParameter> parameters = isKey ? topic.getKeyformatproperties() : topic.getValueformatproperties();
        if (parameters == null) {
            m_parameters = ImmutableMap.of();
        }
        else {
            ImmutableMap.Builder<String, String> bldr = ImmutableMap.<String,String>builder();
            for (FormatParameter parameter : parameters) {
                bldr.put(parameter.getName(), parameter.getValue());
            }
            m_parameters = bldr.build();
        }
    }

    /**
     * Copy constructor
     *
     * @param other
     */
    public EncodeConfiguration(EncodeConfiguration other) {
        m_format = other.m_format;
        m_parameters = other.m_parameters;
    }

    /**
     * Private constructor for {@code deserialize}
     *
     * @param format
     * @param parameters
     */
    private EncodeConfiguration(EncodeFormat format, ImmutableMap<String, String> parameters) {
        m_format = format;
        m_parameters = parameters;
    }

    public static EncodeConfiguration deserialize(ByteBuffer buf) throws IOException {
        short version = buf.getShort();
        if (version != LATEST_VERSION) {
            throw new IOException("Unsupported serialization version: " + version);
        }
       EncodeFormat format = EncodeFormat.byId(buf.get());
       int paramSize = buf.getInt();


       ImmutableMap.Builder<String, String> bldr = ImmutableMap.<String,String>builder();
       for (int i = 0; i < paramSize; i++) {
           bldr.put(SerializationHelper.getString(buf), SerializationHelper.getString(buf));
       }
       return new EncodeConfiguration(format, bldr.build());
    }

    @Override
    public void serialize(ByteBuffer buf) throws IOException {
        buf.putShort(LATEST_VERSION);
        buf.put(m_format.getId());
        buf.putInt(m_parameters.size());
        m_parameters.forEach((k, v) -> { SerializationHelper.writeString(k, buf); SerializationHelper.writeString(v, buf); });
    }

    @Override
    public void cancel() {
    }

    @Override
    public int getSerializedSize() throws IOException {
        return Short.BYTES + Byte.BYTES + Integer.BYTES +
                m_parameters.keySet().stream().mapToInt(SerializationHelper::calculateSerializedSize).sum() +
                m_parameters.values().stream().mapToInt(SerializationHelper::calculateSerializedSize).sum();
    }

    @Override
    public String toString() {
        StringBuilder bldr = new StringBuilder()
                .append("[format: ").append(m_format.toString()).append(", parameters: ")
                .append(m_parameters.toString())
                .append("]");
        return bldr.toString();
    }

    public EncodeFormat getFormat() {
        return m_format;
    }

    public ImmutableMap<String, String> getParameters() {
        return m_parameters;
    }
}
