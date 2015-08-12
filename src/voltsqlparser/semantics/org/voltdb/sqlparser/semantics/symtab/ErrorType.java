package org.voltdb.sqlparser.semantics.symtab;

import org.voltdb.sqlparser.syntax.symtab.ITop;

public class ErrorType extends Type implements ITop {

    public ErrorType(String aName, TypeKind aKind) {
        super(aName, aKind);
        // TODO Auto-generated constructor stub
    }

}
