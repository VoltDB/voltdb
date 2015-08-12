package org.voltdb.sqlparser.semantics.symtab;

public class DecimalType extends NumericType {
    private final static int DEFAULT_SCALE = 12;
    private final static int DEFAULT_PRECISION = 38;
    public DecimalType(String aName, TypeKind aKind) {
        super(aName, aKind);
    }
}
