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

package org.voltdb.jdbc;

import java.sql.SQLException;
import java.sql.Statement;

public class JDBC4ParameterMetaData implements java.sql.ParameterMetaData
{
    private final Statement owner;
    private final int parameterCount;
    JDBC4ParameterMetaData(Statement owner, int parameterCount) throws SQLException
    {
        this.owner = owner;
        this.parameterCount = parameterCount;
    }

    private void checkClosed() throws SQLException
    {
        if (this.owner.isClosed())
            throw SQLError.get(SQLError.CONNECTION_CLOSED);
    }

    // Retrieves the fully-qualified name of the Java class whose instances should be passed to the method PreparedStatement.setObject.
    public String getParameterClassName(int param) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the number of parameters in the PreparedStatement object for which this ParameterMetaData object contains information.
    public int getParameterCount() throws SQLException
    {
        checkClosed();
        return this.parameterCount;
    }

    // Retrieves the designated parameter's mode.
    public int getParameterMode(int param) throws SQLException
    {
        checkClosed();
        return parameterModeIn; // Only mode supported by provider
    }

    // Retrieves the designated parameter's SQL type.
    public int getParameterType(int param) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the designated parameter's database-specific type name.
    public String getParameterTypeName(int param) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the designated parameter's specified column size.
    public int getPrecision(int param) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves the designated parameter's number of digits to right of the decimal point.
    public int getScale(int param) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }

    // Retrieves whether null values are allowed in the designated parameter.
    public int isNullable(int param) throws SQLException
    {
        checkClosed();
        return parameterNullable; // Always permissible in principle (no provider control at the statement-level; the statement itself might fail validity constraints upon execution however!)
    }

    // Retrieves whether values for the designated parameter can be signed numbers.
    public boolean isSigned(int param) throws SQLException
    {
        checkClosed();
        throw SQLError.noSupport();
    }


    // Returns true if this either implements the interface argument or is directly or indirectly a wrapper for an object that does.
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return iface.isInstance(this);
    }

    // Returns an object that implements the given interface to allow access to non-standard methods, or standard methods not exposed by the proxy.
    public <T> T unwrap(Class<T> iface)    throws SQLException
    {
        try
        {
            return iface.cast(this);
        }
         catch (ClassCastException cce)
         {
            throw SQLError.get(SQLError.ILLEGAL_ARGUMENT, iface.toString());
        }
    }
}
