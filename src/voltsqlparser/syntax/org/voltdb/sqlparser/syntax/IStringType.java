package org.voltdb.sqlparser.syntax;

import org.voltdb.sqlparser.syntax.symtab.IType;

public interface IStringType extends IType {

    public abstract IStringType makeInstance(long aSize);

}