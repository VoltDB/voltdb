/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.VoltTable;
import org.voltdb.messaging.FastDeserializer;

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
        tableId = exceptionBuffer.getInt();
        if (exceptionBuffer.hasRemaining()) {
            int tableSize = exceptionBuffer.getInt();
            buffer = ByteBuffer.allocate(tableSize);//Don't bother to copy the bytes for type and table id
            //buffer.order(exceptionBuffer.order());
            exceptionBuffer.get(buffer.array());//Copy the exception details.
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

    /**
     * Retrieve the CatalogId of the table that the constraint violation occurred in
     */
    public int getTableId() {
        return tableId;
    }

    /**
     * Get the table containing the involved tuples (target and source) that caused the constraint violation. The table
     * is lazy deserialized for performance.
     */
    public VoltTable getTuples() {
        if (table != null) {
            return table;
        }

        table = PrivateVoltTableFactory.createUnititializedVoltTable();
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
     * CatalogId of the table that the contraint violation occured in.
     */
    private final int tableId;

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
            sb.append(tableId);
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
        return super.p_getSerializedSize() + 12 + buffer.capacity();
    }

    @Override
    protected void p_serializeToBuffer(ByteBuffer b) {
        super.p_serializeToBuffer(b);
        b.putInt(type.getValue());
        b.putInt(tableId);
        b.putInt(buffer.capacity());
        buffer.rewind();
        b.put(buffer);
    }

    /**
     *
     */
    private static final long serialVersionUID = 1L;

}
