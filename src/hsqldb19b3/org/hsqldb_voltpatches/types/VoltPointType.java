package org.hsqldb_voltpatches.types;

import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.Types;
import org.hsqldb_voltpatches.OpTypes;

public class VoltPointType extends Type {

    VoltPointType() {
        // These settings are copied from SQL_BIGINT.
        super(Types.VOLT_POINT, // comparison group
                Types.VOLT_POINT,  // type
                NumberType.bigintPrecision, // precision
                8); // scale
    }

    // The length of "point(<double value> <double value>)"
    private static final int DISPLAY_SIZE = String.valueOf(-Double.MAX_VALUE).length() * 2 + 8;

    @Override
    public int displaySize() {
        return DISPLAY_SIZE;
    }

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
    public String getNameString() {
        return "POINT";
    }

    @Override
    String getDefinition() {
        return getNameString();
    }

    @Override
    public int compare(Object a, Object b) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Object convertToTypeLimits(SessionInterface session, Object a) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object convertToType(SessionInterface session, Object a, Type type) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object convertToDefaultType(SessionInterface sessionInterface,
            Object o) {
        // TODO Auto-generated method stub
        return null;
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
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Type getAggregateType(Type other) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Type getCombinedType(Type other, int operation) {
        switch (operation) {
        case OpTypes.EQUAL :
        case OpTypes.GREATER :
        case OpTypes.GREATER_EQUAL :
        case OpTypes.SMALLER_EQUAL :
        case OpTypes.SMALLER :
        case OpTypes.NOT_EQUAL :
            if (other instanceof VoltPointType)
                return this;
            // else, fall through
        default:
            // incompatible types
            throw Error.error(ErrorCode.X_42561);
        }
    }
}
