package org.hsqldb_voltpatches.types;

import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.Types;

public class VoltGeographyType extends Type {

    VoltGeographyType() {
        super(Types.VOLT_GEOGRAPHY,    // comparison group
                Types.VOLT_GEOGRAPHY,  // type id
                0,                     // precision
                0);                    // scale
    }

    @Override
    public int displaySize() {
        return -1;
    }

    @Override
    public String getNameString() {
        return "GEOGRAPHY";
    }

    @Override
    String getDefinition() {
        return getNameString();
    }

    @Override
    public int compare(Object a, Object b) {
        // Incompatible type in operation
        throw Error.error(ErrorCode.X_42565);
    }

    @Override
    public Object convertToType(SessionInterface session, Object a, Type type) {
        // We come here when parsing default values.
        if (type instanceof VoltGeographyType) {
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
        return false;
    }

    @Override
    public Type getAggregateType(Type other) {
        // incompatible types in operation
        throw Error.error(ErrorCode.X_42561);
    }

    @Override
    public Type getCombinedType(Type other, int operation) {
        // The 'combined type' seems to refer combining types with
        // operators other than comparisons.   Comparisons always
        // return BOOLEAN, and are valid for POINTs.
        //
        // We don't allow any non-comparison operators on POINTs, so
        // just throw here if anyone tries.
        //
        // incompatible types in operation
        throw Error.error(ErrorCode.X_42561);
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
