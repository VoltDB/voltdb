/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.exceptions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

import org.voltdb.types.ConstraintType;
import org.voltdb.*;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;

/**
 * Exception generated when a constraint is violated. Contains more information then a SQLException.
 *
 */
public class ConstraintFailureException extends SQLException {

    protected static final Logger LOG = Logger.getLogger(ConstraintFailureException.class.getName());

    /**
     * Constructor for deserializing an exception returned from the EE.
     * @param exceptionBuffer
     */
    public ConstraintFailureException(ByteBuffer exceptionBuffer) {
        super(exceptionBuffer);
        type = ConstraintType.get(exceptionBuffer.getInt());
        try {
            tableName = FastDeserializer.readString(exceptionBuffer);
        }
        catch (IOException e) {
            // implies that the EE created an invalid constraint
            // failure, which would be a corruption/defect.
            VoltDB.crashVoltDB();
        }
        if (exceptionBuffer.hasRemaining()) {
            int tableSize = exceptionBuffer.getInt();
            buffer = ByteBuffer.allocate(tableSize);
            //Copy the exception details.
            exceptionBuffer.get(buffer.array());
        } else {
            buffer = ByteBuffer.allocate(0);
        }
    }

    /**
     * Get the type of constraint violation that occurred
     * @return Type of constraint violation
     */
    public ConstraintType getType() {
        return type;
    }

    public String getTableName() {
        return tableName;
    }

    /**
     * Get the table containing the involved tuples (target and source) that caused the constraint violation. The table
     * is lazy deserialized for performance.
     */
    public VoltTable getTuples() {
        if (table != null) {
            return table;
        }

        table = PrivateVoltTableFactory.createUninitializedVoltTable();
        try {
            table.readExternal(new FastDeserializer(buffer));
        } catch (IOException e) {
            LOG.severe(e.toString());
            return null;
        }

        return table;
    }

    /**
     * ByteBuffer containing a serialized copy of a table with the tuples involved in the constraint violation
     */
    private final ByteBuffer buffer;

    /**
     * Type of constraint violation that caused this exception to be thrown
     */
    private final ConstraintType type;

    /**
     * Name of the table that the constraint violation occurred in.
     */
    private String tableName = null;

    /**
     * Lazy deserialized copy of a table with the tuples involved in the constraint violation
     */
    private VoltTable table = null;

    @Override
    public String getMessage() {
        if (buffer.capacity() == 0) {
            return super.getMessage();
        } else {
            StringBuilder sb = new StringBuilder(super.getMessage());
            sb.append('\n');
            sb.append("Constraint Type ");
            sb.append(type);
            sb.append(", Table CatalogId ");
            sb.append(tableName);
            sb.append('\n');
            sb.append(getTuples().toString());
            return sb.toString();
        }
    }
    /**
     * Retrieve a string representation of the exception. If this is a rethrown HSQLDB exception it returns the enclosed exceptions
     * string. Otherwise a new string is generated with the details of the constraint violation.
     */
    @Override
    public String toString() {
        return getMessage();
    }

    @Override
    protected SerializableExceptions getExceptionType() {
        return SerializableExceptions.ConstraintFailureException;
    }

    @Override
    protected int p_getSerializedSize() {
        // ... + 8 + string prefix + string length + ...
        return super.p_getSerializedSize() + 8 + 4 + tableName.length() + buffer.capacity();
    }

    @Override
    protected void p_serializeToBuffer(ByteBuffer b) throws IOException {
        super.p_serializeToBuffer(b);
        b.putInt(type.getValue());
        FastSerializer.writeString(tableName, b);
        b.putInt(buffer.capacity());
        buffer.rewind();
        b.put(buffer);
    }

    /**
     *
     */
    private static final long serialVersionUID = 1L;

}
