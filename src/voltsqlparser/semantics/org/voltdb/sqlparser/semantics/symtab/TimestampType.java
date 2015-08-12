package org.voltdb.sqlparser.semantics.symtab;

import org.voltdb.sqlparser.syntax.symtab.ITop;

public class TimestampType extends Type implements ITop {

    public TimestampType(String aName) {
        super(aName, TypeKind.TIMESTAMP);
    }

}
