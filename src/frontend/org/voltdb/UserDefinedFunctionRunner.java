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

package org.voltdb;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.voltcore.logging.VoltLogger;
import org.voltdb.common.Constants;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.SerializationHelper;

public class UserDefinedFunctionRunner {
        static final int VAR_LEN_SIZE = Integer.SIZE/8;
        protected static VoltLogger m_logger = new VoltLogger("UDF");

        public static byte[] readVarbinary(ByteBuffer buffer) {
            // Sanity check the size against the remaining buffer size.
            if (VAR_LEN_SIZE > buffer.remaining()) {
                throw new RuntimeException(String.format(
                        "Can't read varbinary size as %d byte integer " +
                        "from buffer with %d bytes remaining.",
                        VAR_LEN_SIZE, buffer.remaining()));
            }
            final int len = buffer.getInt();
            if (len == VoltTable.NULL_STRING_INDICATOR) {
                return null;
            }
            if (len < 0) {
                throw new RuntimeException("Invalid object length.");
            }
            byte[] data = new byte[len];
            buffer.get(data);
            return data;
        }

        public static Object getValueFromBuffer(ByteBuffer buffer, VoltType type) {
            switch (type) {
            case TINYINT:
                return buffer.get();
            case SMALLINT:
                return buffer.getShort();
            case INTEGER:
                return buffer.getInt();
            case BIGINT:
                return buffer.getLong();
            case FLOAT:
                return buffer.getDouble();
            case STRING:
                byte[] stringAsBytes = readVarbinary(buffer);
                if (stringAsBytes == null) {
                    return null;
                }
                return new String(stringAsBytes, VoltTable.ROWDATA_ENCODING);
            case VARBINARY:
                return readVarbinary(buffer);
            case TIMESTAMP:
                long timestampValue = buffer.getLong();
                if (timestampValue == Long.MIN_VALUE) {
                    return null;
                }
                return new TimestampType(timestampValue);
            case DECIMAL:
                return VoltDecimalHelper.deserializeBigDecimal(buffer);
            case GEOGRAPHY_POINT:
                return GeographyPointValue.unflattenFromBuffer(buffer);
            case GEOGRAPHY:
                byte[] geographyValueBytes = readVarbinary(buffer);
                if (geographyValueBytes == null) {
                    return null;
                }
                return GeographyValue.unflattenFromBuffer(ByteBuffer.wrap(geographyValueBytes));
            default:
                throw new RuntimeException("Cannot read from VoltDB UDF buffer.");
            }
        }

        public static void writeValueToBuffer(ByteBuffer buffer, VoltType type, Object value) throws IOException {
            buffer.put(type.getValue());
            if (VoltType.isVoltNullValue(value)) {
                value = type.getNullValue();
                if (value == VoltType.NULL_TIMESTAMP) {
                    buffer.putLong(VoltType.NULL_BIGINT);  // corresponds to EE value.h isNull()
                    return;
                }
                else if (value == VoltType.NULL_STRING_OR_VARBINARY) {
                    buffer.putInt(VoltType.NULL_STRING_LENGTH);
                    return;
                }
                else if (value == VoltType.NULL_DECIMAL) {
                    VoltDecimalHelper.serializeNull(buffer);
                    return;
                }
                else if (value == VoltType.NULL_POINT) {
                    GeographyPointValue.serializeNull(buffer);
                    return;
                }
                else if (value == VoltType.NULL_GEOGRAPHY) {
                    buffer.putInt(VoltType.NULL_STRING_LENGTH);
                    return;
                }
            }
            switch (type) {
            case TINYINT:
                buffer.put((Byte)value);
                break;
            case SMALLINT:
                buffer.putShort((Short)value);
                break;
            case INTEGER:
                buffer.putInt((Integer) value);
                break;
            case BIGINT:
                buffer.putLong((Long) value);
                break;
            case FLOAT:
                buffer.putDouble(((Double) value).doubleValue());
                break;
            case STRING:
                byte[] stringAsBytes = ((String)value).getBytes(Constants.UTF8ENCODING);
                SerializationHelper.writeVarbinary(stringAsBytes, buffer);
                break;
            case VARBINARY:
                if (value instanceof byte[]) {
                    SerializationHelper.writeVarbinary(((byte[])value), buffer);
                }
                else if (value instanceof Byte[]) {
                    SerializationHelper.writeVarbinary(((Byte[])value), buffer);
                }
                break;
            case TIMESTAMP:
                buffer.putLong(((TimestampType)value).getTime());
                break;
            case DECIMAL:
                VoltDecimalHelper.serializeBigDecimal((BigDecimal)value, buffer);
                break;
            case GEOGRAPHY_POINT:
                GeographyPointValue geoValue = (GeographyPointValue)value;
                geoValue.flattenToBuffer(buffer);
                break;
            case GEOGRAPHY:
                GeographyValue gv = (GeographyValue)value;
                buffer.putInt(gv.getLengthInBytes());
                gv.flattenToBuffer(buffer);
                break;
            default:
                throw new RuntimeException("Cannot write to VoltDB UDF buffer.");
            }
        }
    }
