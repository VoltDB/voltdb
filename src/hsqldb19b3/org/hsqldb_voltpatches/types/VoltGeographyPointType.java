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

package org.hsqldb_voltpatches.types;

import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.Types;

/**
 * This is the HSQL representation of a VoltDB Geography point.
 */
public class VoltGeographyPointType extends Type {

    VoltGeographyPointType() {
        super(Types.VOLT_GEOGRAPHY_POINT, // comparison group
                Types.VOLT_GEOGRAPHY_POINT,  // type
                0, // precision
                0); // scale
    }

    // The length of "geography_point(<float value> <float value>)"
    private static final int DISPLAY_SIZE = String.valueOf(-Float.MAX_VALUE).length() * 2 + 8;

    @Override
    public int displaySize() {
        return DISPLAY_SIZE;
    }

    @Override
    public String getNameString() {
        return "GEOGRAPHY_POINT";
    }

    @Override
    String getDefinition() {
        return getNameString();
    }

    @Override
    public boolean isGeographyPointType() {
        return true;
    }

    @Override
    public int compare(Object a, Object b) {
        // Incompatible type in operation
        throw Error.error(ErrorCode.X_42565);
    }

    @Override
    public Object convertToType(SessionInterface session, Object a, Type type) {
        // We come here when parsing default values.
        if (type instanceof VoltGeographyPointType) {
            // This is currently unreachable, since there's no way to
            // create a POINT object in a DEFAULT clause given allowable syntax.
            assert(false);
        }

        // incompatible types
        throw Error.error(ErrorCode.X_42561);
    }

    @Override
    public Object convertToDefaultType(SessionInterface sessionInterface,
            Object o) {
        // incompatible types
        throw Error.error(ErrorCode.X_42561);
    }

    @Override
    public String convertToString(Object a) {
        return getNameString();
    }

    @Override
    public String convertToSQLString(Object a) {
        return getNameString();
    }

    @Override
    public boolean canConvertFrom(Type otherType) {
        return otherType instanceof VoltGeographyPointType;
    }

    @Override
    public Type getAggregateType(Type otherType) {
        if (otherType instanceof VoltGeographyPointType) {
            return this;
        }
        // incompatible types in combination
        throw Error.error(ErrorCode.X_42562);
    }

    @Override
    public Type getCombinedType(Type other, int operation) {
        // The 'combined type' seems to refer to combining types with
        // operators other than comparisons.   Comparisons always
        // return BOOLEAN, and are valid for POINTs.
        //
        // We don't allow any non-comparison operators on POINTs, so
        // just throw here if anyone tries.
        //
        // incompatible types in combination
        throw Error.error(ErrorCode.X_42562);
    }

    // *********
    // Below are methods that are stubs that have not been filled in:
    //   - JDBC attributes (TODO)
    //   - Methods that define runtime behavior in HSQL

    @Override
    public int getJDBCTypeCode() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getJDBCClassName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object convertToTypeLimits(SessionInterface session, Object a) {
        // TODO Auto-generated method stub
        return null;
    }
}
