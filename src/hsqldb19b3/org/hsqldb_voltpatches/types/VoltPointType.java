package org.hsqldb_voltpatches.types;

import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.Types;

public class VoltPointType extends Type {

    VoltPointType() {
        // These settings are copied from SQL_BIGINT.
        super(Types.VOLT_POINT, // comparison group
                Types.VOLT_POINT,  // type
                NumberType.bigintPrecision, // precision
                8); // scale
    }

    @Override
    public int displaySize() {
        // TODO Auto-generated method stub
        return 0;
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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    String getDefinition() {
        // TODO Auto-generated method stub
        return null;
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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String convertToSQLString(Object a) {
        // TODO Auto-generated method stub
        return null;
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
        // TODO Auto-generated method stub
        return null;
    }

}
