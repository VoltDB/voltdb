package org.voltdb.sqlparser.semantics.symtab;

import org.voltdb.sqlparser.syntax.symtab.ITop;

public class FloatingPointType extends NumericType implements ITop {
    public FloatingPointType(String aName, TypeKind aKind) {
        super(aName, aKind);
    }
}
